/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.scaladsl

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.MemberStatus
import akka.cluster.sharding.ShardRegion.CurrentShardRegionState
import akka.cluster.sharding.typed.ChangeNumberOfProcesses
import akka.cluster.sharding.typed.GetShardRegionState
import akka.cluster.sharding.typed.ShardedDaemonProcessCommand
import akka.cluster.sharding.typed.ShardedDaemonProcessContext
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.pattern.StatusReply

object ShardedDaemonProcessRescaleEmptyShardsSpec {
  def config = ConfigFactory.parseString("""
      akka.actor.provider = cluster
      akka.actor.testkit.typed.single-expect-default = 10s

      akka.remote.artery.canonical.port = 0
      akka.remote.artery.canonical.hostname = 127.0.0.1

      akka.cluster.jmx.multi-mbeans-in-same-jvm = on

      akka.cluster.sharded-daemon-process.keep-alive-interval = 1s
      akka.cluster.sharded-daemon-process.keep-alive-throttle-interval = 20ms

      akka.coordinated-shutdown.terminate-actor-system = off
      akka.coordinated-shutdown.run-by-actor-system-terminate = off
      """)

  object Worker {
    sealed trait Command
    case object Stop extends Command

    case class Started(id: Int, totalCount: Int, selfRef: ActorRef[Command])
    case class Stopping(selfRef: ActorRef[Command])

    def apply(id: Int, totalCount: Int, probe: ActorRef[Any]): Behavior[Command] = Behaviors.setup { ctx =>
      probe ! Started(id, totalCount, ctx.self)
      Behaviors.receiveMessage {
        case Stop =>
          probe ! Stopping(ctx.self)
          Behaviors.stopped
      }
    }
  }
}

class ShardedDaemonProcessRescaleEmptyShardsSpec
    extends ScalaTestWithActorTestKit(ShardedDaemonProcessRescaleEmptyShardsSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  import ShardedDaemonProcessRescaleEmptyShardsSpec._

  private val workerLifecycleProbe: TestProbe[Any] = createTestProbe[Any]()

  private val daemonProcessName = "empty-shards-repro"

  // SDP names its EntityTypeKey internally as "sharded-daemon-process-<name>"
  private val typeKey: EntityTypeKey[Worker.Command] =
    EntityTypeKey[Worker.Command](s"sharded-daemon-process-$daemonProcessName")

  "ShardedDaemonProcess scale-down" must {

    "form a single-node cluster" in {
      val probe = createTestProbe()
      Cluster(system).manager ! Join(Cluster(system).selfMember.address)
      probe.awaitAssert(Cluster(system).selfMember.status should ===(MemberStatus.Up), 3.seconds)
    }

    "not leave empty shards behind after scaling down from 4 to 2" in {
      val sdp: ActorRef[ShardedDaemonProcessCommand] =
        ShardedDaemonProcess(system).initWithContext[Worker.Command](
          daemonProcessName,
          4,
          (ctx: ShardedDaemonProcessContext) => Worker(ctx.processNumber, ctx.totalProcesses, workerLifecycleProbe.ref),
          ShardedDaemonProcessSettings(system),
          Some(Worker.Stop),
          None)

      val initiallyStarted = (1 to 4).map(_ => workerLifecycleProbe.expectMessageType[Worker.Started]).toSet
      initiallyStarted.map(_.id) should ===(Set(0, 1, 2, 3))

      val stateProbe = TestProbe[CurrentShardRegionState]()

      // baseline: 4 shards, each with one entity
      stateProbe.awaitAssert({
        ClusterSharding(system).shardState ! GetShardRegionState(typeKey, stateProbe.ref)
        val state = stateProbe.receiveMessage()
        state.shards.size should ===(4)
        state.shards.foreach { s =>
          s.entityIds.size should ===(1)
        }
      }, 10.seconds)

      // scale down to 2
      val rescaleProbe = createTestProbe[StatusReply[Done]]()
      sdp ! ChangeNumberOfProcesses(2, rescaleProbe.ref)

      (1 to 4).foreach(_ => workerLifecycleProbe.expectMessageType[Worker.Stopping])
      val newlyStarted = (1 to 2).map(_ => workerLifecycleProbe.expectMessageType[Worker.Started]).toSet
      newlyStarted.map(_.id) should ===(Set(0, 1))
      rescaleProbe.expectMessage(StatusReply.Ack)

      // After scale-down, the region should host exactly the 2 new shards.
      // If old-revision shards are re-allocated as empty Shard actors, this will fail.
      stateProbe.awaitAssert({
        ClusterSharding(system).shardState ! GetShardRegionState(typeKey, stateProbe.ref)
        val state = stateProbe.receiveMessage()
        val nonEmptyShards = state.shards.filter(_.entityIds.nonEmpty)
        val emptyShards = state.shards.filter(_.entityIds.isEmpty)
        withClue(s"nonEmpty=$nonEmptyShards empty=$emptyShards: ") {
          nonEmptyShards.size should ===(2)
          emptyShards shouldBe empty
        }
      }, 15.seconds)
    }
  }
}
