/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.cluster.Cluster
import akka.cluster.ClusterEvent.InitialStateAsEvents
import akka.cluster.ClusterEvent.MemberUp
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._

object ReplicatorPruningSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    # we use 3s as write timeouts in test, make sure we see that
    # and not time out the expectMsg at the same time
    akka.test.single-expect-default = 5s
    akka.actor.provider = "cluster"
    akka.log-dead-letters-during-shutdown = off
    """))

}

class ReplicatorPruningSpecMultiJvmNode1 extends ReplicatorPruningSpec
class ReplicatorPruningSpecMultiJvmNode2 extends ReplicatorPruningSpec
class ReplicatorPruningSpecMultiJvmNode3 extends ReplicatorPruningSpec

class ReplicatorPruningSpec extends MultiNodeSpec(ReplicatorPruningSpec) with STMultiNodeSpec with ImplicitSender {
  import Replicator._
  import ReplicatorPruningSpec._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)
  implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  val maxPruningDissemination = 3.seconds
  val replicator = system.actorOf(
    Replicator.props(
      ReplicatorSettings(system)
        .withGossipInterval(1.second)
        .withPruning(pruningInterval = 1.second, maxPruningDissemination)),
    "replicator")
  val timeout = 3.seconds.dilated

  val KeyA = GCounterKey("A")
  val KeyB = ORSetKey[String]("B")
  val KeyC = PNCounterMapKey[String]("C")
  val KeyD = ORMultiMapKey[String, String]("D")
  val KeyE = ORMapKey[String, GSet[String]]("E")

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "Pruning of CRDT" must {

    "move data from removed node" in {
      join(first, first)
      join(second, first)
      join(third, first)

      within(5.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(3))
        }
      }

      // we need the UniqueAddress
      val memberProbe = TestProbe()
      cluster.subscribe(memberProbe.ref, initialStateMode = InitialStateAsEvents, classOf[MemberUp])
      val thirdUniqueAddress = {
        val member = memberProbe
          .fishForMessage(3.seconds) {
            case MemberUp(m) if m.address == node(third).address => true
            case _                                               => false
          }
          .asInstanceOf[MemberUp]
          .member
        member.uniqueAddress
      }

      replicator ! Update(KeyA, GCounter(), WriteAll(timeout))(_ :+ 3)
      expectMsg(UpdateSuccess(KeyA, None))

      replicator ! Update(KeyB, ORSet(), WriteAll(timeout))(_ :+ "a" :+ "b" :+ "c")
      expectMsg(UpdateSuccess(KeyB, None))

      replicator ! Update(KeyC, PNCounterMap.empty[String], WriteAll(timeout)) {
        _.incrementBy("x", 1).incrementBy("y", 1)
      }
      expectMsg(UpdateSuccess(KeyC, None))

      replicator ! Update(KeyD, ORMultiMap.empty[String, String], WriteAll(timeout)) { _ :+ ("a" -> Set("A")) }
      expectMsg(UpdateSuccess(KeyD, None))

      replicator ! Update(KeyE, ORMap.empty[String, GSet[String]], WriteAll(timeout)) {
        _ :+ ("a" -> GSet.empty[String].add("A"))
      }
      expectMsg(UpdateSuccess(KeyE, None))

      enterBarrier("updates-done")

      replicator ! Get(KeyA, ReadLocal)
      val oldCounter = expectMsgType[GetSuccess[GCounter]].dataValue
      oldCounter.value should be(9)

      replicator ! Get(KeyB, ReadLocal)
      val oldSet = expectMsgType[GetSuccess[ORSet[String]]].dataValue
      oldSet.elements should be(Set("a", "b", "c"))

      replicator ! Get(KeyC, ReadLocal)
      val oldMap = expectMsgType[GetSuccess[PNCounterMap[String]]].dataValue
      oldMap.get("x") should be(Some(3))
      oldMap.get("y") should be(Some(3))

      replicator ! Get(KeyD, ReadLocal)
      val oldMultiMap = expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue
      oldMultiMap.get("a") should be(Some(Set("A")))

      replicator ! Get(KeyE, ReadLocal)
      val oldORMap = expectMsgType[GetSuccess[ORMap[String, GSet[String]]]].dataValue
      val GSet(d) = oldORMap.entries("a")
      d should be(Set("A"))

      enterBarrier("get-old")

      runOn(third) {
        replicator ! Update(KeyE, ORMap.empty[String, GSet[String]], WriteLocal) { _.remove("a") }
        expectMsg(UpdateSuccess(KeyE, None))
      }

      enterBarrier("remove-element")

      runOn(first) {
        cluster.leave(node(third).address)
      }

      runOn(first, second) {
        within(15.seconds) {
          awaitAssert {
            replicator ! GetReplicaCount
            expectMsg(ReplicaCount(2))
          }
        }
      }
      enterBarrier("third-removed")

      runOn(first, second) {
        within(15.seconds) {
          var values = Set.empty[Int]
          awaitAssert {
            replicator ! Get(KeyA, ReadLocal)
            expectMsgPF() {
              case g @ GetSuccess(KeyA, _) =>
                val value = g.get(KeyA).value.toInt
                values += value
                value should be(9)
                g.get(KeyA).needPruningFrom(thirdUniqueAddress) should be(false)
            }
          }
          values should ===(Set(9))
        }
        within(5.seconds) {
          awaitAssert {
            replicator ! Get(KeyB, ReadLocal)
            expectMsgPF() {
              case g @ GetSuccess(KeyB, _) =>
                g.get(KeyB).elements should be(Set("a", "b", "c"))
                g.get(KeyB).needPruningFrom(thirdUniqueAddress) should be(false)
            }
          }
        }
        within(5.seconds) {
          awaitAssert {
            replicator ! Get(KeyC, ReadLocal)
            expectMsgPF() {
              case g @ GetSuccess(KeyC, _) =>
                g.get(KeyC).entries should be(Map("x" -> 3L, "y" -> 3L))
                g.get(KeyC).needPruningFrom(thirdUniqueAddress) should be(false)
            }
          }
        }
        within(5.seconds) {
          awaitAssert {
            replicator ! Get(KeyD, ReadLocal)
            expectMsgPF() {
              case g @ GetSuccess(KeyD, _) =>
                g.get(KeyD).entries("a") should be(Set("A"))
                g.get(KeyD).needPruningFrom(thirdUniqueAddress) should be(false)
            }
          }
        }
        within(5.seconds) {
          awaitAssert {
            replicator ! Get(KeyE, ReadLocal)
            expectMsgPF() {
              case g @ GetSuccess(KeyE, _) =>
                g.get(KeyE).needPruningFrom(thirdUniqueAddress) should be(false)
            }
          }
        }
      }
      enterBarrier("pruning-done")

      // after pruning performed we should not be able to update with data from removed node
      def updateAfterPruning(expectedValue: Int): Unit = {
        replicator ! Update(KeyA, GCounter(), WriteAll(timeout), None) { existing =>
          // inject data from removed node to simulate bad data
          existing.merge(oldCounter) :+ 1
        }
        expectMsgPF() {
          case UpdateSuccess(KeyA, _) =>
            replicator ! Get(KeyA, ReadLocal)
            val retrieved = expectMsgType[GetSuccess[GCounter]].dataValue
            retrieved.value should be(expectedValue)
        }
      }
      def checkValuePropagated(expectedValue: Int): Unit =
        awaitAssert {
          replicator ! Get(KeyA, ReadLocal)
          expectMsgPF() {
            case g @ GetSuccess(KeyA, _) =>
              g.get(KeyA).value.toInt should be(expectedValue)
          }
        }

      runOn(first) {
        updateAfterPruning(expectedValue = 10)
      }
      runOn(second) {
        checkValuePropagated(10)
      }

      enterBarrier("update-first-after-pruning")

      runOn(second) {
        updateAfterPruning(expectedValue = 11)
      }
      enterBarrier("update-second-after-pruning")

      // after full replication should still not be able to update with data from removed node
      // but it would not work after removal of the PruningPerformed markers
      expectNoMessage(maxPruningDissemination + 3.seconds)

      runOn(first) {
        updateAfterPruning(expectedValue = 12)
      }
      runOn(second) {
        checkValuePropagated(12)
      }

      enterBarrier("update-first-after-dissemination")

      runOn(second) {
        updateAfterPruning(expectedValue = 13)
      }
      enterBarrier("update-second-after-dissemination")

      enterBarrier("after-1")
    }
  }

}
