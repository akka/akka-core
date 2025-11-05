/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._

object ExpirySpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.distributed-data {
      gossip-interval = 500 millis
      delta-crdt.enabled = off
      expire-keys-after-inactivity {
        "expiry-*" = 2 seconds
      }
    }
    """))

}

class ExpirySpecMultiJvmNode1 extends ExpirySpec
class ExpirySpecMultiJvmNode2 extends ExpirySpec

class ExpirySpec extends MultiNodeSpec(ExpirySpec) with STMultiNodeSpec with ImplicitSender {
  import ExpirySpec._
  import Replicator._

  override def initialParticipants: Int = roles.size

  private val cluster = Cluster(system)
  private implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  private val replicator = system.actorOf(Replicator.props(ReplicatorSettings(system)), "replicator")
  private val writeAll = WriteAll(5.seconds)

  private val KeyA = GCounterKey("expiry-A")
  private val KeyB = GCounterKey("expiry-B")
  private val KeyC = GCounterKey("expiry-C")
  private val KeyD = GCounterKey("expiry-D")
  private val KeyE = GCounterKey("expiry-E")
  private val KeyF = GCounterKey("expiry-F")
  private val KeyOtherA = GCounterKey("other-A")

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "Replicator expiry" must {

    "remove expired entry" in {
      join(first, first)

      runOn(first) {
        val subscriberProbe = TestProbe()
        replicator ! Subscribe(KeyA, subscriberProbe.ref)

        replicator ! Update(KeyA, GCounter.empty, WriteLocal)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]
        subscriberProbe.expectMsgType[Changed[GCounter]]

        replicator ! Get(KeyA, ReadLocal)
        expectMsgType[GetSuccess[GCounter]].get(KeyA).value should ===(1)

        expectNoMessage(5.seconds)
        replicator ! Get(KeyA, ReadLocal)
        expectMsg(NotFound(KeyA, None))
        subscriberProbe.expectMsg(Expired[GCounter](KeyA))

        // same key can be used again
        replicator ! Update(KeyA, GCounter.empty, WriteLocal)(_ :+ 2)
        expectMsgType[UpdateSuccess[_]]
        subscriberProbe.expectMsgType[Changed[GCounter]]
        replicator ! Get(KeyA, ReadLocal)
        expectMsgType[GetSuccess[GCounter]].get(KeyA).value should ===(2)
      }

      enterBarrier("done-1")
    }

    "keep entry when updated" in {
      runOn(first) {
        (1 to 6).foreach { _ =>
          replicator ! Update(KeyB, GCounter.empty, WriteLocal)(_ :+ 1)
          expectMsgType[UpdateSuccess[_]]
          expectNoMessage(500.millis)
        }

        replicator ! Get(KeyB, ReadLocal)
        expectMsgType[GetSuccess[GCounter]].get(KeyB).value should ===(6)
      }

      enterBarrier("done-2")
    }

    "keep entry when accessed" in {
      replicator ! Update(KeyC, GCounter.empty, WriteLocal)(_ :+ 1)
      expectMsgType[UpdateSuccess[_]]

      runOn(first) {
        (1 to 6).foreach { _ =>
          replicator ! Get(KeyC, ReadLocal)
          expectMsgType[GetSuccess[GCounter]]
          expectNoMessage(500.millis)
        }
      }

      replicator ! Get(KeyC, ReadLocal)
      expectMsgType[GetSuccess[GCounter]].get(KeyC).value should ===(1)

      enterBarrier("done-3")
    }

    "remove expired tombstone" in {
      runOn(first) {
        replicator ! Update(KeyD, GCounter.empty, WriteLocal)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]

        replicator ! Delete(KeyD, WriteLocal)
        expectMsgType[DeleteSuccess[_]]

        replicator ! Get(KeyD, ReadLocal)
        expectMsgType[GetDataDeleted[GCounter]]

        expectNoMessage(3.seconds)
        replicator ! Get(KeyD, ReadLocal)
        expectMsg(NotFound(KeyD, None))

        // same key can be used again
        replicator ! Update(KeyD, GCounter.empty, WriteLocal)(_ :+ 2)
        expectMsgType[UpdateSuccess[_]]
        replicator ! Get(KeyD, ReadLocal)
        expectMsgType[GetSuccess[GCounter]].get(KeyD).value should ===(2)
      }

      enterBarrier("done-4")
    }

    "remove expired entry from all nodes" in {
      join(second, first)

      within(15.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(1.second, ReplicaCount(2))
        }
      }
      enterBarrier("joined-second")

      runOn(first) {
        replicator ! Update(KeyE, GCounter.empty, writeAll)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]
      }
      enterBarrier("updated")

      replicator ! Get(KeyE, ReadLocal)
      expectMsgType[GetSuccess[GCounter]].get(KeyE).value should ===(1)

      expectNoMessage(3.seconds)
      replicator ! Get(KeyE, ReadLocal)
      expectMsg(NotFound(KeyE, None))

      enterBarrier("done-5")
    }

    "keep entry on all nodes when accessed from one" in {
      runOn(first) {
        replicator ! Update(KeyF, GCounter.empty, writeAll)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]
      }
      enterBarrier("updated")

      runOn(second) {
        (1 to 6).foreach { _ =>
          replicator ! Get(KeyF, ReadLocal)
          expectMsgType[GetSuccess[GCounter]]
          expectNoMessage(500.millis)
        }
      }

      enterBarrier("expiry time elapsed")

      replicator ! Get(KeyF, ReadLocal)
      expectMsgType[GetSuccess[GCounter]].get(KeyF).value should ===(1)

      enterBarrier("done-6")
    }

    "don't impact gossip of other keys" in {
      runOn(first) {
        enterBarrier("subscribed")

        replicator ! Update(KeyOtherA, GCounter.empty, writeLocal)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]
        enterBarrier("updated")
      }

      runOn(second) {
        val subscriberProbe = TestProbe()
        replicator ! Subscribe(KeyOtherA, subscriberProbe.ref)
        enterBarrier("subscribed")

        enterBarrier("updated")

        subscriberProbe.expectMsgType[Changed[GCounter]].get(KeyOtherA).value should ===(1)
        replicator ! Get(KeyOtherA, ReadLocal)
        expectMsgType[GetSuccess[GCounter]].get(KeyOtherA).value should ===(1)
      }

      enterBarrier("done-7")
    }
  }
}
