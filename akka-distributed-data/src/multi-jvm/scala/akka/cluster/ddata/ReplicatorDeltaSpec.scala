/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.serialization.jackson.CborSerializable
import akka.testkit._

object ReplicatorDeltaSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.log-dead-letters-during-shutdown = off
    """))

  testTransport(on = true)

  case class Highest(n: Int, delta: Option[Highest] = None)
      extends DeltaReplicatedData
      with RequiresCausalDeliveryOfDeltas
      with ReplicatedDelta
      with CborSerializable {
    type T = Highest
    type D = Highest

    override def merge(other: Highest): Highest =
      if (n >= other.n) this else other

    override def mergeDelta(other: Highest): Highest = merge(other)

    override def zero: Highest = Highest(0)

    override def resetDelta: Highest = Highest(n)

    def incr(i: Int): Highest = Highest(n + i, Some(Highest(n + i)))

    def incrNoDelta(i: Int): Highest = Highest(n + i, None)
  }

  sealed trait Op
  final case class Delay(n: Int) extends Op
  final case class Incr(key: PNCounterKey, n: Int, consistency: WriteConsistency) extends Op
  final case class Decr(key: PNCounterKey, n: Int, consistency: WriteConsistency) extends Op
  final case class Add(key: ORSetKey[String], elem: String, consistency: WriteConsistency) extends Op
  final case class Remove(key: ORSetKey[String], elem: String, consistency: WriteConsistency) extends Op

  val timeout = 5.seconds
  val writeTwo = WriteTo(2, timeout)
  val writeMajority = WriteMajority(timeout)

  val KeyHigh = new Key[Highest]("High") {}
  val KeyA = PNCounterKey("A")
  val KeyB = PNCounterKey("B")
  val KeyC = PNCounterKey("C")
  val KeyD = ORSetKey[String]("D")
  val KeyE = ORSetKey[String]("E")
  val KeyF = ORSetKey[String]("F")

  def generateOperations(onNode: RoleName): Vector[Op] = {
    val rnd = ThreadLocalRandom.current()

    def consistency(): WriteConsistency = {
      rnd.nextInt(100) match {
        case n if n < 90  => WriteLocal
        case n if n < 95  => writeTwo
        case n if n < 100 => writeMajority
      }
    }

    def rndPnCounterkey(): PNCounterKey = {
      rnd.nextInt(3) match {
        case 0 => KeyA
        case 1 => KeyB
        case 2 => KeyC
      }
    }

    def rndOrSetkey(): ORSetKey[String] = {
      rnd.nextInt(3) match {
        case 0 => KeyD
        case 1 => KeyE
        case 2 => KeyF
      }
    }

    var availableForRemove = Set.empty[String]

    def rndAddElement(): String = {
      // lower case a - j
      val s = (97 + rnd.nextInt(10)).toChar.toString
      availableForRemove += s
      s
    }

    def rndRemoveElement(): String = {
      if (availableForRemove.isEmpty)
        "a"
      else
        availableForRemove.toVector(rnd.nextInt(availableForRemove.size))
    }

    (0 to (30 + rnd.nextInt(10))).map { _ =>
      rnd.nextInt(4) match {
        case 0 => Delay(rnd.nextInt(500))
        case 1 => Incr(rndPnCounterkey(), rnd.nextInt(100), consistency())
        case 2 => Decr(rndPnCounterkey(), rnd.nextInt(10), consistency())
        case 3 =>
          // ORSet
          val key = rndOrSetkey()
          // only removals for KeyF on node first
          if (key == KeyF && onNode == first && rnd.nextBoolean())
            Remove(key, rndRemoveElement(), consistency())
          else
            Add(key, rndAddElement(), consistency())
      }
    }.toVector
  }

}

class ReplicatorDeltaSpecMultiJvmNode1 extends ReplicatorDeltaSpec
class ReplicatorDeltaSpecMultiJvmNode2 extends ReplicatorDeltaSpec
class ReplicatorDeltaSpecMultiJvmNode3 extends ReplicatorDeltaSpec
class ReplicatorDeltaSpecMultiJvmNode4 extends ReplicatorDeltaSpec

class ReplicatorDeltaSpec extends MultiNodeSpec(ReplicatorDeltaSpec) with STMultiNodeSpec with ImplicitSender {
  import Replicator._
  import ReplicatorDeltaSpec._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)
  implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  val fullStateReplicator = system.actorOf(
    Replicator.props(ReplicatorSettings(system).withGossipInterval(1.second).withDeltaCrdtEnabled(false)),
    "fullStateReplicator")
  val deltaReplicator = {
    val r = system.actorOf(Replicator.props(ReplicatorSettings(system)), "deltaReplicator")
    r ! Replicator.Internal.TestFullStateGossip(enabled = false)
    r
  }

  val writeAll = WriteAll(5.seconds)

  var afterCounter = 0
  def enterBarrierAfterTestStep(): Unit = {
    afterCounter += 1
    enterBarrier("after-" + afterCounter)
  }

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "delta-CRDT" must {
    "join cluster" in {
      join(first, first)
      join(second, first)
      join(third, first)
      join(fourth, first)

      within(15.seconds) {
        awaitAssert {
          fullStateReplicator ! GetReplicaCount
          expectMsg(ReplicaCount(4))
        }
      }

      enterBarrierAfterTestStep()
    }

    "propagate delta" in {
      join(first, first)
      join(second, first)
      join(third, first)
      join(fourth, first)

      within(15.seconds) {
        awaitAssert {
          fullStateReplicator ! GetReplicaCount
          expectMsg(ReplicaCount(4))
        }
      }
      enterBarrier("ready")

      runOn(first) {
        // by setting something for each key we don't have to worry about NotFound
        List(KeyA, KeyB, KeyC).foreach { key =>
          fullStateReplicator ! Update(key, PNCounter.empty, WriteLocal)(_ :+ 1)
          deltaReplicator ! Update(key, PNCounter.empty, WriteLocal)(_ :+ 1)
        }
        List(KeyD, KeyE, KeyF).foreach { key =>
          fullStateReplicator ! Update(key, ORSet.empty[String], WriteLocal)(_ :+ "a")
          deltaReplicator ! Update(key, ORSet.empty[String], WriteLocal)(_ :+ "a")
        }
      }
      enterBarrier("updated-1")

      within(5.seconds) {
        awaitAssert {
          val p = TestProbe()
          List(KeyA, KeyB, KeyC).foreach { key =>
            fullStateReplicator.tell(Get(key, ReadLocal), p.ref)
            p.expectMsgType[GetSuccess[PNCounter]].dataValue.getValue.intValue should be(1)
          }
        }
        awaitAssert {
          val p = TestProbe()
          List(KeyD, KeyE, KeyF).foreach { key =>
            deltaReplicator.tell(Get(key, ReadLocal), p.ref)
            p.expectMsgType[GetSuccess[ORSet[String]]].dataValue.elements should ===(Set("a"))
          }
        }
      }

      enterBarrierAfterTestStep()
    }

    "work with write consistency" in {
      runOn(first) {
        val p1 = TestProbe()
        fullStateReplicator.tell(Update(KeyD, ORSet.empty[String], writeAll)(_ :+ "A"), p1.ref)
        deltaReplicator.tell(Update(KeyD, ORSet.empty[String], writeAll)(_ :+ "A"), p1.ref)
        p1.expectMsgType[UpdateSuccess[_]]
        p1.expectMsgType[UpdateSuccess[_]]
      }
      enterBarrier("write-1")

      val p = TestProbe()
      deltaReplicator.tell(Get(KeyD, ReadLocal), p.ref)
      p.expectMsgType[GetSuccess[ORSet[String]]].dataValue.elements should ===(Set("a", "A"))
      enterBarrier("read-1")

      // and also when doing several at the same time (deltas may be reordered) and then we
      // retry with full state to sort it out
      runOn(first) {
        val p1 = TestProbe()
        deltaReplicator.tell(Update(KeyD, ORSet.empty[String], writeAll)(_ :+ "B"), p1.ref)
        deltaReplicator.tell(Update(KeyD, ORSet.empty[String], writeAll)(_ :+ "C"), p1.ref)
        deltaReplicator.tell(Update(KeyD, ORSet.empty[String], writeAll)(_ :+ "D"), p1.ref)
        p1.expectMsgType[UpdateSuccess[_]]
        p1.expectMsgType[UpdateSuccess[_]]
        p1.expectMsgType[UpdateSuccess[_]]
      }
      enterBarrier("write-2")
      deltaReplicator.tell(Get(KeyD, ReadLocal), p.ref)
      p.expectMsgType[GetSuccess[ORSet[String]]].dataValue.elements should ===(Set("a", "A", "B", "C", "D"))

      // add same to the fullStateReplicator so they are in sync
      runOn(first) {
        val p1 = TestProbe()
        fullStateReplicator.tell(Update(KeyD, ORSet.empty[String], writeAll)(_ :+ "A" :+ "B" :+ "C" :+ "D"), p1.ref)
        p1.expectMsgType[UpdateSuccess[_]]
      }
      enterBarrier("write-3")
      fullStateReplicator.tell(Get(KeyD, ReadLocal), p.ref)
      p.expectMsgType[GetSuccess[ORSet[String]]].dataValue.elements should ===(Set("a", "A", "B", "C", "D"))

      enterBarrierAfterTestStep()
    }

    "preserve causal consistency for None delta" in {
      runOn(first) {
        val p1 = TestProbe()
        deltaReplicator.tell(Update(KeyHigh, Highest(0), WriteLocal)(_.incr(1)), p1.ref)
        p1.expectMsgType[UpdateSuccess[_]]
      }
      enterBarrier("write-1")

      runOn(first) {
        val p = TestProbe()
        deltaReplicator.tell(Get(KeyHigh, ReadLocal), p.ref)
        p.expectMsgType[GetSuccess[Highest]].dataValue.n should ===(1)
      }
      runOn(second, third, fourth) {
        within(5.seconds) {
          awaitAssert {
            val p = TestProbe()
            deltaReplicator.tell(Get(KeyHigh, ReadLocal), p.ref)
            p.expectMsgType[GetSuccess[Highest]].dataValue.n should ===(1)
          }
        }
      }
      enterBarrier("read-1")

      runOn(first) {
        val p1 = TestProbe()
        deltaReplicator.tell(Update(KeyHigh, Highest(0), writeAll)(_.incr(2)), p1.ref)
        p1.expectMsgType[UpdateSuccess[_]]
        deltaReplicator.tell(Update(KeyHigh, Highest(0), WriteLocal)(_.incrNoDelta(5)), p1.ref)
        deltaReplicator.tell(Update(KeyHigh, Highest(0), WriteLocal)(_.incr(10)), p1.ref)
        p1.expectMsgType[UpdateSuccess[_]]
        p1.expectMsgType[UpdateSuccess[_]]
      }
      enterBarrier("write-2")

      runOn(first) {
        val p = TestProbe()
        deltaReplicator.tell(Get(KeyHigh, ReadLocal), p.ref)
        p.expectMsgType[GetSuccess[Highest]].dataValue.n should ===(18)
      }
      runOn(second, third, fourth) {
        within(5.seconds) {
          awaitAssert {
            val p = TestProbe()
            deltaReplicator.tell(Get(KeyHigh, ReadLocal), p.ref)
            // the incrNoDelta(5) is not propagated as delta, and then incr(10) is also skipped
            p.expectMsgType[GetSuccess[Highest]].dataValue.n should ===(3)
          }
        }
      }
      enterBarrier("read-2")

      runOn(first) {
        val p1 = TestProbe()
        // WriteAll will send full state when delta can't be applied and thereby syncing the
        // delta versions again. Same would happen via full state gossip.
        // Thereafter delta can be propagated and applied again.
        deltaReplicator.tell(Update(KeyHigh, Highest(0), writeAll)(_.incr(100)), p1.ref)
        p1.expectMsgType[UpdateSuccess[_]]
        // Flush the deltaPropagation buffer, otherwise it will contain
        // NoDeltaPlaceholder from previous updates and the incr(4) delta will also
        // be folded into NoDeltaPlaceholder and not propagated as delta. A few DeltaPropagationTick
        // are needed to send to all and flush buffer.
        roles.foreach { _ =>
          deltaReplicator ! Replicator.Internal.DeltaPropagationTick
        }
        deltaReplicator.tell(Update(KeyHigh, Highest(0), WriteLocal)(_.incr(4)), p1.ref)
        p1.expectMsgType[UpdateSuccess[_]]
      }
      enterBarrier("write-3")

      within(5.seconds) {
        awaitAssert {
          val p = TestProbe()
          deltaReplicator.tell(Get(KeyHigh, ReadLocal), p.ref)
          p.expectMsgType[GetSuccess[Highest]].dataValue.n should ===(122)
        }
      }

      enterBarrierAfterTestStep()
    }

    "be eventually consistent" in {
      val operations = generateOperations(onNode = myself)
      log.debug(s"random operations on [${myself.name}]: ${operations.mkString(", ")}")
      try {
        // perform random operations with both delta and full-state replicators
        // and compare that the end result is the same

        for (op <- operations) {
          log.debug("operation: {}", op)
          op match {
            case Delay(d) => Thread.sleep(d)
            case Incr(key, n, consistency) =>
              fullStateReplicator ! Update(key, PNCounter.empty, consistency)(_ :+ n)
              deltaReplicator ! Update(key, PNCounter.empty, consistency)(_ :+ n)
            case Decr(key, n, consistency) =>
              fullStateReplicator ! Update(key, PNCounter.empty, consistency)(_.decrement(n))
              deltaReplicator ! Update(key, PNCounter.empty, consistency)(_.decrement(n))
            case Add(key, elem, consistency) =>
              // to have an deterministic result when mixing add/remove we can only perform
              // the ORSet operations from one node
              runOn((if (key == KeyF) List(first) else List(first, second, third)): _*) {
                fullStateReplicator ! Update(key, ORSet.empty[String], consistency)(_ :+ elem)
                deltaReplicator ! Update(key, ORSet.empty[String], consistency)(_ :+ elem)
              }
            case Remove(key, elem, consistency) =>
              runOn(first) {
                fullStateReplicator ! Update(key, ORSet.empty[String], consistency)(_.remove(elem))
                deltaReplicator ! Update(key, ORSet.empty[String], consistency)(_.remove(elem))
              }
          }
        }

        enterBarrier("updated-2")

        List(KeyA, KeyB, KeyC).foreach { key =>
          within(5.seconds) {
            awaitAssert {
              val p = TestProbe()
              fullStateReplicator.tell(Get(key, ReadLocal), p.ref)
              val fullStateValue = p.expectMsgType[GetSuccess[PNCounter]].dataValue
              deltaReplicator.tell(Get(key, ReadLocal), p.ref)
              val deltaValue = p.expectMsgType[GetSuccess[PNCounter]].dataValue
              deltaValue should ===(fullStateValue)
            }
          }
        }

        List(KeyD, KeyE, KeyF).foreach { key =>
          within(5.seconds) {
            awaitAssert {
              val p = TestProbe()
              fullStateReplicator.tell(Get(key, ReadLocal), p.ref)
              val fullStateValue = p.expectMsgType[GetSuccess[ORSet[String]]].dataValue.elements
              deltaReplicator.tell(Get(key, ReadLocal), p.ref)
              val deltaValue = p.expectMsgType[GetSuccess[ORSet[String]]].dataValue.elements
              deltaValue should ===(fullStateValue)
            }
          }
        }

        enterBarrierAfterTestStep()
      } catch {
        case e: Throwable =>
          info(s"random operations on [${myself.name}]: ${operations.mkString(", ")}")
          throw e
      }
    }
  }

}
