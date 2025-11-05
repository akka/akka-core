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

object WildcardSubscribeSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.distributed-data {
      gossip-interval = 500 millis
      notify-subscribers-interval = 100 millis
      expire-keys-after-inactivity {
        "expiry-*" = 2 seconds
      }
    }
    
    """))

}

class WildcardSubscribeSpecMultiJvmNode1 extends WildcardSubscribeSpec
class WildcardSubscribeSpecMultiJvmNode2 extends WildcardSubscribeSpec

class WildcardSubscribeSpec extends MultiNodeSpec(WildcardSubscribeSpec) with STMultiNodeSpec with ImplicitSender {
  import Replicator._
  import WildcardSubscribeSpec._

  override def initialParticipants: Int = roles.size

  private val cluster = Cluster(system)
  private implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  private val replicator = system.actorOf(Replicator.props(ReplicatorSettings(system)), "replicator")

  private val KeyA = GCounterKey("counter-A")
  private val KeyB = GCounterKey("counter-B")
  private val KeyOtherA = GCounterKey("other-A")
  private val KeyExpiryA = GCounterKey("expiry-A")

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "Replicator wildcard subscriptions" must {

    "notify changed entry" in {
      join(first, first)

      runOn(first) {
        val subscriberProbe = TestProbe()
        replicator ! Subscribe(GCounterKey("counter-*"), subscriberProbe.ref)

        replicator ! Update(KeyA, GCounter.empty, WriteLocal)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]
        val chg1 = subscriberProbe.expectMsgType[Changed[GCounter]]
        chg1.key should ===(KeyA)
        chg1.key.getClass should ===(KeyA.getClass)
        chg1.get(KeyA).value should ===(1)

        replicator ! Update(KeyA, GCounter.empty, WriteLocal)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]
        val chg2 = subscriberProbe.expectMsgType[Changed[GCounter]]
        chg2.key should ===(KeyA)
        chg2.get(KeyA).value should ===(2)

        replicator ! Update(KeyB, GCounter.empty, WriteLocal)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]
        val chg3 = subscriberProbe.expectMsgType[Changed[GCounter]]
        chg3.key should ===(KeyB)
        chg3.get(KeyB).value should ===(1)

        replicator ! Update(KeyOtherA, GCounter.empty, WriteLocal)(_ :+ 17)
        expectMsgType[UpdateSuccess[_]]
        subscriberProbe.expectNoMessage(200.millis)

        // a few more subscribers
        val subscriberProbe2 = TestProbe()
        replicator ! Subscribe(GCounterKey("counter-*"), subscriberProbe2.ref)
        val subscriberProbeA = TestProbe()
        replicator ! Subscribe(KeyA, subscriberProbeA.ref)
        val subscriberProbeOther = TestProbe()
        replicator ! Subscribe(GCounterKey("other-*"), subscriberProbeOther.ref)
        subscriberProbe.expectNoMessage(200.millis)
        subscriberProbe2.receiveN(2).foreach {
          case chg: Changed[GCounter] @unchecked =>
            if (chg.key == KeyA)
              chg.get(KeyA).value should ===(2)
            else if (chg.key == KeyB)
              chg.get(KeyB).value should ===(1)
            else
              fail(s"unexpected change ${chg.key}")
          case other =>
            fail(s"Unexpected $other")
        }
        subscriberProbe2.expectNoMessage()
        subscriberProbeA.expectMsgType[Changed[GCounter]].get(KeyA).value should ===(2)
        subscriberProbeOther.expectMsgType[Changed[GCounter]].get(KeyOtherA).value should ===(17)

        replicator ! Update(KeyB, GCounter.empty, WriteLocal)(_ :+ 10)
        expectMsgType[UpdateSuccess[_]]
        val chg4 = subscriberProbe.expectMsgType[Changed[GCounter]]
        chg4.key should ===(KeyB)
        chg4.get(KeyB).value should ===(11)
        val chg5 = subscriberProbe2.expectMsgType[Changed[GCounter]]
        chg5.key should ===(KeyB)
        chg5.get(KeyB).value should ===(11)

        // unsubscribe
        replicator ! Unsubscribe(GCounterKey("counter-*"), subscriberProbe.ref)
        replicator ! Update(KeyB, GCounter.empty, WriteLocal)(_ :+ 5)
        expectMsgType[UpdateSuccess[_]]
        subscriberProbe.expectNoMessage(200.millis)
        val chg6 = subscriberProbe2.expectMsgType[Changed[GCounter]]
        chg6.key should ===(KeyB)
        chg6.get(KeyB).value should ===(16)
      }

      enterBarrier("done-1")
    }

    "notify expired entry" in {
      runOn(first) {
        val subscriberProbe = TestProbe()
        replicator ! Subscribe(GCounterKey("expiry-*"), subscriberProbe.ref)

        replicator ! Update(KeyExpiryA, GCounter.empty, WriteLocal)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]
        subscriberProbe.expectMsgType[Changed[GCounter]]

        replicator ! Get(KeyExpiryA, ReadLocal)
        expectMsgType[GetSuccess[GCounter]].get(KeyExpiryA).value should ===(1)

        expectNoMessage(5.seconds)
        replicator ! Get(KeyExpiryA, ReadLocal)
        expectMsg(NotFound(KeyExpiryA, None))
        subscriberProbe.expectMsg(Expired[GCounter](KeyExpiryA))

        // same key can be used again
        replicator ! Update(KeyExpiryA, GCounter.empty, WriteLocal)(_ :+ 2)
        expectMsgType[UpdateSuccess[_]]
        subscriberProbe.expectMsgType[Changed[GCounter]]
        replicator ! Get(KeyExpiryA, ReadLocal)
        expectMsgType[GetSuccess[GCounter]].get(KeyExpiryA).value should ===(2)
      }

      enterBarrier("done-2")
    }

    "notify when changed from another node" in {
      runOn(first) {
        replicator ! Update(KeyOtherA, GCounter.empty, WriteLocal)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]
        enterBarrier("updated-1")

        enterBarrier("second-joined")

        enterBarrier("received-1")

        replicator ! Update(KeyOtherA, GCounter.empty, WriteLocal)(_ :+ 1)
        expectMsgType[UpdateSuccess[_]]
        enterBarrier("updated-2")

        enterBarrier("received-2")
      }
      runOn(second) {
        enterBarrier("updated-1")

        // it's possible to subscribe before join
        val subscriberProbe = TestProbe()
        replicator ! Subscribe(GCounterKey("other-*"), subscriberProbe.ref)

        join(second, first)

        val chg1 = subscriberProbe.expectMsgType[Changed[GCounter]](10.seconds)
        chg1.key should ===(KeyOtherA)
        chg1.get(KeyOtherA).value should ===(18) // it was also incremented to 17 in earlier test
        enterBarrier("received-1")

        enterBarrier("updated-2")

        val chg2 = subscriberProbe.expectMsgType[Changed[GCounter]]
        chg2.key should ===(KeyOtherA)
        chg2.get(KeyOtherA).value should ===(19)
        enterBarrier("received-2")
      }
    }

    enterBarrier("done-3")
  }
}
