package sample.distributeddata

import scala.concurrent.duration._
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorSystem
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.ddata.Replicator
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator.GetReplicaCount
import akka.cluster.ddata.typed.scaladsl.Replicator.ReplicaCount
import akka.cluster.typed.{ Cluster, Join, Leave }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import com.typesafe.config.ConfigFactory

object ReplicatedMetricsSpec extends MultiNodeConfig {
  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.log-dead-letters-during-shutdown = off
    """))

}

class ReplicatedMetricsSpecMultiJvmNode1 extends ReplicatedMetricsSpec
class ReplicatedMetricsSpecMultiJvmNode2 extends ReplicatedMetricsSpec
class ReplicatedMetricsSpecMultiJvmNode3 extends ReplicatedMetricsSpec

class ReplicatedMetricsSpec extends MultiNodeSpec(ReplicatedMetricsSpec) with STMultiNodeSpec {
  import ReplicatedMetricsSpec._
  import ReplicatedMetrics._

  override def initialParticipants = roles.size

  implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
  val cluster = Cluster(typedSystem)
  system.spawnAnonymous(ReplicatedMetrics(1.second, 3.seconds))

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.manager ! Join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "Demo of a replicated metrics" must {
    "join cluster" in within(20.seconds) {
      join(node1, node1)
      join(node2, node1)
      join(node3, node1)

      awaitAssert {
        val probe = TestProbe[ReplicaCount]()
        DistributedData(typedSystem).replicator ! GetReplicaCount(probe.ref)
        probe.expectMessage(Replicator.ReplicaCount(roles.size))
      }
      enterBarrier("after-1")
    }

    "replicate metrics" in within(10.seconds) {
      val probe = TestProbe[UsedHeap]()
      typedSystem.eventStream ! EventStream.Subscribe(probe.ref)
      awaitAssert {
        probe.expectMessageType[UsedHeap](1.second).percentPerNode.size should be(3)
      }
      probe.expectMessageType[UsedHeap].percentPerNode.size should be(3)
      probe.expectMessageType[UsedHeap].percentPerNode.size should be(3)
      enterBarrier("after-2")
    }

    "cleanup removed node" in within(25.seconds) {
      val node3Address = node(node3).address
      runOn(node1) {
        cluster.manager ! Leave(node3Address)
      }
      runOn(node1, node2) {
        val probe = TestProbe[UsedHeap]()
        typedSystem.eventStream ! EventStream.Subscribe(probe.ref)
        awaitAssert {
          probe.expectMessageType[UsedHeap](1.second).percentPerNode.size should be(2)
        }
        probe.expectMessageType[UsedHeap].percentPerNode should not contain (
          nodeKey(node3Address))
      }
      enterBarrier("after-3")
    }

  }

}

