/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._

object DurablePruningSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(debugConfig(on = false).withFallback(ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.log-dead-letters-during-shutdown = off
    akka.cluster.distributed-data.durable.keys = ["*"]
    akka.cluster.distributed-data.durable.lmdb {
      dir = target/DurablePruningSpec-${System.currentTimeMillis}-ddata
      map-size = 10 MiB
    }
    """)))

}

class DurablePruningSpecMultiJvmNode1 extends DurablePruningSpec
class DurablePruningSpecMultiJvmNode2 extends DurablePruningSpec

class DurablePruningSpec extends MultiNodeSpec(DurablePruningSpec) with STMultiNodeSpec with ImplicitSender {
  import DurablePruningSpec._
  import Replicator._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)
  implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  val maxPruningDissemination = 3.seconds

  def startReplicator(sys: ActorSystem): ActorRef =
    sys.actorOf(
      Replicator.props(
        ReplicatorSettings(sys)
          .withGossipInterval(1.second)
          .withPruning(pruningInterval = 1.second, maxPruningDissemination)),
      "replicator")
  val replicator = startReplicator(system)
  val timeout = 5.seconds.dilated

  val KeyA = GCounterKey("A")

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "Pruning of durable CRDT" must {

    "move data from removed node" in {
      join(first, first)
      join(second, first)

      val sys2 = ActorSystem(system.name, MultiNodeSpec.configureNextPortIfFixed(system.settings.config))
      val cluster2 = Cluster(sys2)
      val distributedData2 = DistributedData(sys2)
      val replicator2 = startReplicator(sys2)
      val probe2 = TestProbe()(sys2)
      Cluster(sys2).join(node(first).address)
      awaitAssert({
        Cluster(system).state.members.size should ===(4)
        Cluster(system).state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
        Cluster(sys2).state.members.size should ===(4)
        Cluster(sys2).state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
      }, 10.seconds)
      enterBarrier("joined")

      within(5.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(4))
          replicator2.tell(GetReplicaCount, probe2.ref)
          probe2.expectMsg(ReplicaCount(4))
        }
      }

      replicator ! Update(KeyA, GCounter(), WriteLocal)(_ :+ 3)
      expectMsg(UpdateSuccess(KeyA, None))

      replicator2.tell(
        Update(KeyA, GCounter(), WriteLocal)(_.increment(distributedData2.selfUniqueAddress, 2)),
        probe2.ref)
      probe2.expectMsg(UpdateSuccess(KeyA, None))

      enterBarrier("updates-done")

      within(10.seconds) {
        awaitAssert {
          replicator ! Get(KeyA, ReadAll(1.second))
          val counter1 = expectMsgType[GetSuccess[GCounter]].dataValue
          counter1.value should be(10)
          counter1.state.size should be(4)
        }
      }

      within(10.seconds) {
        awaitAssert {
          replicator2.tell(Get(KeyA, ReadAll(1.second)), probe2.ref)
          val counter2 = probe2.expectMsgType[GetSuccess[GCounter]].dataValue
          counter2.value should be(10)
          counter2.state.size should be(4)
        }
      }
      enterBarrier("get1")

      runOn(first) {
        cluster.leave(cluster2.selfAddress)
      }

      within(15.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(3))
        }
      }
      enterBarrier("removed")
      runOn(first) {
        Await.ready(sys2.terminate(), 5.seconds)
      }

      within(15.seconds) {
        var values = Set.empty[Int]
        awaitAssert {
          replicator ! Get(KeyA, ReadLocal)
          val counter3 = expectMsgType[GetSuccess[GCounter]].dataValue
          val value = counter3.value.intValue
          values += value
          value should be(10)
          counter3.state.size should be(3)
        }
        values should ===(Set(10))
      }
      enterBarrier("pruned")

      runOn(first) {
        val address = cluster2.selfAddress
        val sys3 = ActorSystem(
          system.name,
          ConfigFactory.parseString(s"""
                  akka.remote.artery.canonical.port = ${address.port.get}
                  """).withFallback(system.settings.config))
        val cluster3 = Cluster(sys3)
        val replicator3 = startReplicator(sys3)
        val probe3 = TestProbe()(sys3)
        cluster3.join(node(first).address)

        awaitAssert({
          cluster.state.members.exists(m =>
            m.uniqueAddress == cluster3.selfUniqueAddress && m.status == MemberStatus.Up) should ===(true)
        }, 10.seconds)

        within(10.seconds) {
          var values = Set.empty[Int]
          awaitAssert {
            replicator3.tell(Get(KeyA, ReadLocal), probe3.ref)
            val counter4 = probe3.expectMsgType[GetSuccess[GCounter]].dataValue
            val value = counter4.value.intValue
            values += value
            value should be(10)
            counter4.state.size should be(3)
          }
          values should ===(Set(10))
        }

        // all must at least have seen it as joining
        awaitAssert({
          cluster3.state.members.size should ===(4)
          cluster3.state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
        }, 10.seconds)

        // after merging with others
        replicator3 ! Get(KeyA, ReadAll(remainingOrDefault))
        val counter5 = expectMsgType[GetSuccess[GCounter]].dataValue
        counter5.value should be(10)
        counter5.state.size should be(3)
      }

      enterBarrier("sys3-started")
      replicator ! Get(KeyA, ReadAll(remainingOrDefault))
      val counter6 = expectMsgType[GetSuccess[GCounter]].dataValue
      counter6.value should be(10)
      counter6.state.size should be(3)

      enterBarrier("after-1")
    }
  }

}
