/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._

object ReplicatorORSetDeltaSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.log-dead-letters-during-shutdown = off
    """))

  testTransport(on = true)
}

class ReplicatorORSetDeltaSpecMultiJvmNode1 extends ReplicatorORSetDeltaSpec
class ReplicatorORSetDeltaSpecMultiJvmNode2 extends ReplicatorORSetDeltaSpec
class ReplicatorORSetDeltaSpecMultiJvmNode3 extends ReplicatorORSetDeltaSpec

class ReplicatorORSetDeltaSpec
    extends MultiNodeSpec(ReplicatorORSetDeltaSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import Replicator._
  import ReplicatorORSetDeltaSpec._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)
  implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  val replicator =
    system.actorOf(Replicator.props(ReplicatorSettings(system).withGossipInterval(1.second)), "replicator")
  val timeout = 3.seconds.dilated

  val KeyA = ORSetKey[String]("A")
  val KeyB = ORSetKey[String]("B")
  val KeyC = ORSetKey[String]("C")

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  def assertValue(key: Key[ReplicatedData], expected: Any): Unit =
    within(10.seconds) {
      awaitAssert {
        replicator ! Get(key, ReadLocal)
        val value = expectMsgPF() {
          case g @ GetSuccess(`key`, _) =>
            g.dataValue match {
              case c: ORSet[_] => c.elements
              case _           => fail()
            }
        }
        value should be(expected)
      }
    }

  "ORSet delta" must {

    "replicate data in initial phase" in {
      join(first, first)
      join(second, first)
      join(third, first)

      replicator ! Replicator.Internal.TestFullStateGossip(enabled = false)

      within(10.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(3))
        }
      }

      runOn(first) {
        replicator ! Update(KeyA, ORSet.empty[String], WriteLocal)(_ :+ "a")
        expectMsg(UpdateSuccess(KeyA, None))
      }

      enterBarrier("initial-updates-done")

      assertValue(KeyA, Set("a"))

      enterBarrier("after-1")
    }

    "be propagated with causal consistency during network split" in {
      runOn(first) {
        // third is isolated
        testConductor.blackhole(first, third, Direction.Both).await
        testConductor.blackhole(second, third, Direction.Both).await
      }
      enterBarrier("split")

      runOn(first) {
        replicator ! Update(KeyA, ORSet.empty[String], WriteLocal)(_ :+ "b")
        expectMsg(UpdateSuccess(KeyA, None))
      }
      runOn(second) {
        replicator ! Update(KeyA, ORSet.empty[String], WriteLocal)(_ :+ "d")
        expectMsg(UpdateSuccess(KeyA, None))
      }
      runOn(first, second) {
        assertValue(KeyA, Set("a", "b", "d"))
        Thread.sleep(2000) // all deltas sent
      }
      enterBarrier("added-b-and-d")

      runOn(first) {
        testConductor.passThrough(first, third, Direction.Both).await
        testConductor.passThrough(second, third, Direction.Both).await
      }
      enterBarrier("healed")

      runOn(first) {
        // delta for "c" will be sent to third, but it has not received the previous delta for "b"
        replicator ! Update(KeyA, ORSet.empty[String], WriteLocal)(_ :+ "c")
        expectMsg(UpdateSuccess(KeyA, None))
        // let the delta be propagated (will not fail if it takes longer)
        Thread.sleep(1000)
      }
      enterBarrier("added-c")

      runOn(first, second) {
        assertValue(KeyA, Set("a", "b", "c", "d"))
      }
      runOn(third) {
        // the delta for "c" should not be applied because it has not received previous delta for "b"
        // and full gossip is turned off so far
        assertValue(KeyA, Set("a"))
      }
      enterBarrier("verified-before-full-gossip")

      replicator ! Replicator.Internal.TestFullStateGossip(enabled = true)
      assertValue(KeyA, Set("a", "b", "c", "d"))
      enterBarrier("verified-after-full-gossip")

      replicator ! Replicator.Internal.TestFullStateGossip(enabled = false)

      // and now the delta seqNr should be in sync again
      runOn(first) {
        replicator ! Update(KeyA, ORSet.empty[String], WriteLocal)(_ :+ "e")
        expectMsg(UpdateSuccess(KeyA, None))
      }
      assertValue(KeyA, Set("a", "b", "c", "d", "e"))

      enterBarrier("after-2")
    }

  }

}
