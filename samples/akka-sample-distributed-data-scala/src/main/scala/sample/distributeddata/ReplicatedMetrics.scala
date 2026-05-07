package sample.distributeddata

import java.lang.management.ManagementFactory
import java.lang.management.MemoryMXBean
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import akka.actor.Address
import akka.actor.typed.Behavior
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator.Update
import akka.cluster.typed.Cluster
import akka.cluster.typed.Subscribe

object ReplicatedMetrics {
  sealed trait Command
  private case object Tick extends Command
  private case object Cleanup extends Command

  private sealed trait InternalCommand extends Command
  private case class InternalSubscribeResponse(rsp: SubscribeResponse[LWWMap[String, Long]]) extends InternalCommand
  private case class InternalClusterMemberUp(msg: ClusterEvent.MemberUp) extends InternalCommand
  private case class InternalClusterMemberRemoved(msg: ClusterEvent.MemberRemoved) extends InternalCommand
  private case class InternalUpdateResponse[A <: ReplicatedData](rsp: UpdateResponse[A]) extends InternalCommand

  case class UsedHeap(percentPerNode: Map[String, Double]) {
    override def toString =
      percentPerNode.toSeq.sortBy(_._1).map {
        case (key, value) => key + " --> " + value + " %"
      }.mkString("\n")
  }

  def nodeKey(address: Address): String = address.host.get + ":" + address.port.get

  def apply: Behavior[Command] = apply(1.second, 1.minute)

  def apply(measureInterval: FiniteDuration, cleanupInterval: FiniteDuration): Behavior[Command] =
    Behaviors.setup { context =>
      DistributedData.withReplicatorMessageAdapter[Command, LWWMap[String, Long]] { replicator =>
        Behaviors.withTimers[Command] { timers =>
          implicit val selfUniqueAddress: SelfUniqueAddress =
            DistributedData(context.system).selfUniqueAddress
          implicit val cluster = Cluster(context.system)
          val node = nodeKey(cluster.selfMember.address)

          timers.startTimerWithFixedDelay(Tick, Tick, measureInterval)
          timers.startTimerWithFixedDelay(Cleanup, Cleanup, cleanupInterval)
          val memoryMBean: MemoryMXBean = ManagementFactory.getMemoryMXBean

          val UsedHeapKey = LWWMapKey[String, Long]("usedHeap")
          val MaxHeapKey = LWWMapKey[String, Long]("maxHeap")

          replicator.subscribe(UsedHeapKey, InternalSubscribeResponse.apply)
          replicator.subscribe(MaxHeapKey, InternalSubscribeResponse.apply)

          val memberUpRef      = context.messageAdapter(InternalClusterMemberUp.apply)
          val memberRemovedRef = context.messageAdapter(InternalClusterMemberRemoved.apply)
          cluster.subscriptions ! Subscribe(memberUpRef, classOf[ClusterEvent.MemberUp])
          cluster.subscriptions ! Subscribe(memberRemovedRef, classOf[ClusterEvent.MemberRemoved])

          var maxHeap = Map.empty[String, Long]
          var nodesInCluster = Set.empty[String]

          Behaviors.receiveMessage {
            case Tick =>
              val heap = memoryMBean.getHeapMemoryUsage
              val used = heap.getUsed
              val max = heap.getMax

              replicator.askUpdate(
                askReplyTo => Update(UsedHeapKey, LWWMap.empty[String, Long], WriteLocal, askReplyTo)(_ :+ (node -> used)),
                InternalUpdateResponse.apply)

              replicator.askUpdate(
                askReplyTo => Update(MaxHeapKey, LWWMap.empty[String, Long], WriteLocal, askReplyTo) { data =>
                  data.get(node) match {
                    case Some(`max`) => data // unchanged
                    case _           => data :+ (node -> max)
                  }
                },
                InternalUpdateResponse.apply)

              Behaviors.same

            case InternalSubscribeResponse(c @ Changed(MaxHeapKey)) =>
              maxHeap = c.get(MaxHeapKey).entries
              Behaviors.same

            case InternalSubscribeResponse(c @ Changed(UsedHeapKey)) =>
              val usedHeapPercent = UsedHeap(c.get(UsedHeapKey).entries.collect {
                case (key, value) if maxHeap.contains(key) =>
                  (key -> (value.toDouble / maxHeap(key)) * 100.0)
              })
              context.log.debug("Node {} observed:\n{}", node, usedHeapPercent)
              context.system.eventStream ! EventStream.Publish(usedHeapPercent)
              Behaviors.same

            case InternalSubscribeResponse(_)                 => Behaviors.same // ok
            case InternalUpdateResponse(_: UpdateResponse[_]) => Behaviors.same // ok

            case InternalClusterMemberUp(ClusterEvent.MemberUp(m)) =>
              nodesInCluster += nodeKey(m.address)
              Behaviors.same

            case InternalClusterMemberRemoved(ClusterEvent.MemberRemoved(m, _)) =>
              nodesInCluster -= nodeKey(m.address)
              if (m.address == cluster.selfMember.uniqueAddress.address)
                Behaviors.stopped
              else
                Behaviors.same

            case Cleanup =>
              def cleanupRemoved(data: LWWMap[String, Long]): LWWMap[String, Long] =
                (data.entries.keySet -- nodesInCluster).foldLeft(data) { case (d, key) => d.remove(selfUniqueAddress, key) }

              replicator.askUpdate(
                askReplyTo => Update(UsedHeapKey, LWWMap.empty[String, Long], WriteLocal, askReplyTo)(cleanupRemoved),
                InternalUpdateResponse.apply)

              replicator.askUpdate(
                askReplyTo => Update(MaxHeapKey, LWWMap.empty[String, Long], WriteLocal, askReplyTo)(cleanupRemoved),
                InternalUpdateResponse.apply)

              Behaviors.same
          }
        }
      }
    }
}
