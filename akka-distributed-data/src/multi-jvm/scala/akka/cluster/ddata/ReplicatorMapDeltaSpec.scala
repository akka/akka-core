/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.event.Logging.Error
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._

object ReplicatorMapDeltaSpec extends MultiNodeConfig {
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

  sealed trait Op
  final case class Delay(n: Int) extends Op
  final case class Incr(ki: (PNCounterMapKey[String], String), n: Int, consistency: WriteConsistency) extends Op
  final case class Decr(ki: (PNCounterMapKey[String], String), n: Int, consistency: WriteConsistency) extends Op
  // AddVD and RemoveVD for variant of ORMultiMap with Value Deltas, NoVD - for the vanilla ORMultiMap
  final case class AddVD(ki: (ORMultiMapKey[String, String], String), elem: String, consistency: WriteConsistency)
      extends Op
  final case class RemoveVD(ki: (ORMultiMapKey[String, String], String), elem: String, consistency: WriteConsistency)
      extends Op
  final case class AddNoVD(ki: (ORMultiMapKey[String, String], String), elem: String, consistency: WriteConsistency)
      extends Op
  final case class RemoveNoVD(ki: (ORMultiMapKey[String, String], String), elem: String, consistency: WriteConsistency)
      extends Op
  // AddOM and RemoveOM for Vanilla ORMap holding ORSet inside
  final case class AddOM(ki: (ORMapKey[String, ORSet[String]], String), elem: String, consistency: WriteConsistency)
      extends Op
  final case class RemoveOM(ki: (ORMapKey[String, ORSet[String]], String), elem: String, consistency: WriteConsistency)
      extends Op

  val timeout = 5.seconds
  val writeTwo = WriteTo(2, timeout)
  val writeMajority = WriteMajority(timeout)

  val KeyPN = PNCounterMapKey[String]("A")
  // VD and NoVD as above
  val KeyMMVD = ORMultiMapKey[String, String]("D")
  val KeyMMNoVD = ORMultiMapKey[String, String]("G")
  // OM as above
  val KeyOM = ORMapKey[String, ORSet[String]]("J")

  val KeyA: (PNCounterMapKey[String], String) = (KeyPN, "a")
  val KeyB: (PNCounterMapKey[String], String) = (KeyPN, "b")
  val KeyC: (PNCounterMapKey[String], String) = (KeyPN, "c")
  val KeyD: (ORMultiMapKey[String, String], String) = (KeyMMVD, "d")
  val KeyE: (ORMultiMapKey[String, String], String) = (KeyMMVD, "e")
  val KeyF: (ORMultiMapKey[String, String], String) = (KeyMMVD, "f")
  val KeyG: (ORMultiMapKey[String, String], String) = (KeyMMNoVD, "g")
  val KeyH: (ORMultiMapKey[String, String], String) = (KeyMMNoVD, "h")
  val KeyI: (ORMultiMapKey[String, String], String) = (KeyMMNoVD, "i")
  val KeyJ: (ORMapKey[String, ORSet[String]], String) = (KeyOM, "j")
  val KeyK: (ORMapKey[String, ORSet[String]], String) = (KeyOM, "k")
  val KeyL: (ORMapKey[String, ORSet[String]], String) = (KeyOM, "l")

  def generateOperations(onNode: RoleName): Vector[Op] = {
    val rnd = ThreadLocalRandom.current()

    def consistency(): WriteConsistency = {
      rnd.nextInt(100) match {
        case n if n < 90  => WriteLocal
        case n if n < 95  => writeTwo
        case n if n < 100 => writeMajority
      }
    }

    def rndPnCounterkey(): (PNCounterMapKey[String], String) = {
      rnd.nextInt(3) match {
        case 0 => KeyA
        case 1 => KeyB
        case 2 => KeyC
      }
    }

    def rndOrSetkeyVD(): (ORMultiMapKey[String, String], String) = {
      rnd.nextInt(3) match {
        case 0 => KeyD
        case 1 => KeyE
        case 2 => KeyF
      }
    }

    def rndOrSetkeyNoVD(): (ORMultiMapKey[String, String], String) = {
      rnd.nextInt(3) match {
        case 0 => KeyG
        case 1 => KeyH
        case 2 => KeyI
      }
    }

    def rndOrSetkeyOM(): (ORMapKey[String, ORSet[String]], String) = {
      rnd.nextInt(3) match {
        case 0 => KeyJ
        case 1 => KeyK
        case 2 => KeyL
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

    (0 to (50 + rnd.nextInt(10))).map { _ =>
      rnd.nextInt(6) match {
        case 0 => Delay(rnd.nextInt(500))
        case 1 => Incr(rndPnCounterkey(), rnd.nextInt(100), consistency())
        case 2 => Decr(rndPnCounterkey(), rnd.nextInt(10), consistency())
        case 3 =>
          // ORMultiMap.withValueDeltas
          val key = rndOrSetkeyVD()
          // only removals for KeyF on node first
          if (key == KeyF && onNode == first && rnd.nextBoolean())
            RemoveVD(key, rndRemoveElement(), consistency())
          else
            AddVD(key, rndAddElement(), consistency())
        case 4 =>
          // ORMultiMap - vanilla variant - without Value Deltas
          val key = rndOrSetkeyNoVD()
          // only removals for KeyI on node first
          if (key == KeyI && onNode == first && rnd.nextBoolean())
            RemoveNoVD(key, rndRemoveElement(), consistency())
          else
            AddNoVD(key, rndAddElement(), consistency())
        case 5 =>
          // Vanilla ORMap - with ORSet inside
          val key = rndOrSetkeyOM()
          // only removals for KeyL on node first
          if (key == KeyL && onNode == first && rnd.nextBoolean())
            RemoveOM(key, rndRemoveElement(), consistency())
          else
            AddOM(key, rndAddElement(), consistency())
      }
    }.toVector
  }

  def addElementToORMap(om: ORMap[String, ORSet[String]], key: String, element: String)(
      implicit node: SelfUniqueAddress) =
    om.updated(node, key, ORSet.empty[String])(_ :+ element)

  def removeElementFromORMap(om: ORMap[String, ORSet[String]], key: String, element: String)(
      implicit node: SelfUniqueAddress) =
    om.updated(node, key, ORSet.empty[String])(_.remove(element))
}

class ReplicatorMapDeltaSpecMultiJvmNode1 extends ReplicatorMapDeltaSpec
class ReplicatorMapDeltaSpecMultiJvmNode2 extends ReplicatorMapDeltaSpec
class ReplicatorMapDeltaSpecMultiJvmNode3 extends ReplicatorMapDeltaSpec
class ReplicatorMapDeltaSpecMultiJvmNode4 extends ReplicatorMapDeltaSpec

class ReplicatorMapDeltaSpec extends MultiNodeSpec(ReplicatorMapDeltaSpec) with STMultiNodeSpec with ImplicitSender {
  import Replicator._
  import ReplicatorMapDeltaSpec._

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
  // both deltas and full state
  val ordinaryReplicator =
    system.actorOf(Replicator.props(ReplicatorSettings(system).withGossipInterval(1.second)), "ordinaryReplicator")

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
          fullStateReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_.incrementBy(key._2, 1))
          deltaReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_.incrementBy(key._2, 1))
        }
        List(KeyD, KeyE, KeyF).foreach { key =>
          fullStateReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(
            _ :+ (key._2 -> Set("a")))
          deltaReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(
            _ :+ (key._2 -> Set("a")))
        }
        List(KeyG, KeyH, KeyI).foreach { key =>
          fullStateReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(_ :+ (key._2 -> Set("a")))
          deltaReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(_ :+ (key._2 -> Set("a")))
        }
        List(KeyJ, KeyK, KeyL).foreach { key =>
          fullStateReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(
            _ :+ (key._2 -> (ORSet.empty :+ "a")))
          deltaReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(
            _ :+ (key._2 -> (ORSet.empty :+ "a")))
        }
      }
      enterBarrier("updated-1")

      within(5.seconds) {
        awaitAssert {
          val p = TestProbe()
          List(KeyA, KeyB, KeyC).foreach { key =>
            fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
            p.expectMsgType[GetSuccess[PNCounterMap[String]]].dataValue.get(key._2).get.intValue should be(1)
          }
        }
        awaitAssert {
          val p = TestProbe()
          List(KeyD, KeyE, KeyF).foreach { key =>
            fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
            p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2) should ===(Some(Set("a")))
          }
        }
        awaitAssert {
          val p = TestProbe()
          List(KeyG, KeyH, KeyI).foreach { key =>
            fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
            p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2) should ===(Some(Set("a")))
          }
        }
        awaitAssert {
          val p = TestProbe()
          List(KeyJ, KeyK, KeyL).foreach { key =>
            fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
            val res = p.expectMsgType[GetSuccess[ORMap[String, ORSet[String]]]].dataValue.get(key._2)
            res.map(_.elements) should ===(Some(Set("a")))
          }
        }
      }

      enterBarrierAfterTestStep()
    }

    "replicate high throughput changes without OversizedPayloadException" in {
      val N = 1000
      val errorLogProbe = TestProbe()
      system.eventStream.subscribe(errorLogProbe.ref, classOf[Error])
      runOn(first) {
        for (_ <- 1 to N; key <- List(KeyA, KeyB)) {
          ordinaryReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_.incrementBy(key._2, 1))
        }
      }
      enterBarrier("updated-2")

      within(5.seconds) {
        awaitAssert {
          val p = TestProbe()
          List(KeyA, KeyB).foreach { key =>
            ordinaryReplicator.tell(Get(key._1, ReadLocal), p.ref)
            p.expectMsgType[GetSuccess[PNCounterMap[String]]].dataValue.get(key._2).get.intValue should be(N)
          }
        }
      }

      enterBarrier("replicated-2")
      // no OversizedPayloadException logging
      errorLogProbe.expectNoMessage()

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
            case Incr(key, n, _) =>
              fullStateReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_.incrementBy(key._2, n))
              deltaReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_.incrementBy(key._2, n))
            case Decr(key, n, _) =>
              fullStateReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_.decrementBy(key._2, n))
              deltaReplicator ! Update(key._1, PNCounterMap.empty[String], WriteLocal)(_.decrementBy(key._2, n))
            case AddVD(key, elem, _) =>
              // to have an deterministic result when mixing add/remove we can only perform
              // the ORSet operations from one node
              runOn((if (key == KeyF) List(first) else List(first, second, third)): _*) {
                fullStateReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(
                  _.addBindingBy(key._2, elem))
                deltaReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(
                  _.addBindingBy(key._2, elem))
              }
            case RemoveVD(key, elem, _) =>
              runOn(first) {
                fullStateReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(
                  _.removeBindingBy(key._2, elem))
                deltaReplicator ! Update(key._1, ORMultiMap.emptyWithValueDeltas[String, String], WriteLocal)(
                  _.removeBindingBy(key._2, elem))
              }
            case AddNoVD(key, elem, _) =>
              // to have an deterministic result when mixing add/remove we can only perform
              // the ORSet operations from one node
              runOn((if (key == KeyI) List(first) else List(first, second, third)): _*) {
                fullStateReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(
                  _.addBindingBy(key._2, elem))
                deltaReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(
                  _.addBindingBy(key._2, elem))
              }
            case RemoveNoVD(key, elem, _) =>
              runOn(first) {
                fullStateReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(
                  _.removeBindingBy(key._2, elem))
                deltaReplicator ! Update(key._1, ORMultiMap.empty[String, String], WriteLocal)(
                  _.removeBindingBy(key._2, elem))
              }
            case AddOM(key, elem, _) =>
              // to have an deterministic result when mixing add/remove we can only perform
              // the ORSet operations from one node
              runOn((if (key == KeyL) List(first) else List(first, second, third)): _*) {
                fullStateReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(om =>
                  addElementToORMap(om, key._2, elem))
                deltaReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(om =>
                  addElementToORMap(om, key._2, elem))
              }
            case RemoveOM(key, elem, _) =>
              runOn(first) {
                fullStateReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(om =>
                  removeElementFromORMap(om, key._2, elem))
                deltaReplicator ! Update(key._1, ORMap.empty[String, ORSet[String]], WriteLocal)(om =>
                  removeElementFromORMap(om, key._2, elem))
              }
          }
        }

        enterBarrier("updated-3")

        List(KeyA, KeyB, KeyC).foreach { key =>
          within(5.seconds) {
            awaitAssert {
              val p = TestProbe()
              fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val fullStateValue = p.expectMsgType[GetSuccess[PNCounterMap[String]]].dataValue.get(key._2).get.intValue
              deltaReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val deltaValue = p.expectMsgType[GetSuccess[PNCounterMap[String]]].dataValue.get(key._2).get.intValue
              deltaValue should ===(fullStateValue)
            }
          }
        }

        List(KeyD, KeyE, KeyF).foreach { key =>
          within(5.seconds) {
            awaitAssert {
              val p = TestProbe()
              fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val fullStateValue = p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2)
              deltaReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val deltaValue = p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2)
              deltaValue should ===(fullStateValue)
            }
          }
        }

        List(KeyG, KeyH, KeyI).foreach { key =>
          within(5.seconds) {
            awaitAssert {
              val p = TestProbe()
              fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val fullStateValue = p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2)
              deltaReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val deltaValue = p.expectMsgType[GetSuccess[ORMultiMap[String, String]]].dataValue.get(key._2)
              deltaValue should ===(fullStateValue)
            }
          }
        }

        List(KeyJ, KeyK, KeyL).foreach { key =>
          within(5.seconds) {
            awaitAssert {
              val p = TestProbe()
              fullStateReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val fullStateValue = p.expectMsgType[GetSuccess[ORMap[String, ORSet[String]]]].dataValue.get(key._2)
              deltaReplicator.tell(Get(key._1, ReadLocal), p.ref)
              val deltaValue = p.expectMsgType[GetSuccess[ORMap[String, ORSet[String]]]].dataValue.get(key._2)
              deltaValue.map(_.elements) should ===(fullStateValue.map(_.elements))
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
