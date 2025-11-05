/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata.protobuf

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.actor.Identify
import akka.actor.Props
import akka.actor.RootActorPath
import akka.cluster.Cluster
import akka.cluster.UniqueAddress
import akka.cluster.ddata._
import akka.cluster.ddata.Replicator.Internal._
import akka.testkit.TestActors
import akka.testkit.TestKit

class ReplicatedDataSerializerSpec
    extends TestKit(
      ActorSystem(
        "ReplicatedDataSerializerSpec",
        ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.actor.provider=cluster
    akka.remote.artery.canonical.port = 0
    """)))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val serializer = new ReplicatedDataSerializer(system.asInstanceOf[ExtendedActorSystem])

  val Protocol = "akka"

  val address1 = UniqueAddress(Address(Protocol, system.name, "some.host.org", 4711), 1L)
  val address2 = UniqueAddress(Address(Protocol, system.name, "other.host.org", 4711), 2L)
  val address3 = UniqueAddress(Address(Protocol, system.name, "some.host.org", 4712), 3L)

  val ref1 = system.actorOf(Props.empty, "ref1")
  val ref2 = system.actorOf(Props.empty, "ref2")
  val ref3 = system.actorOf(Props.empty, "ref3")

  override def afterAll(): Unit = {
    shutdown()
  }

  def checkSerialization(obj: AnyRef): Int = {
    val blob = serializer.toBinary(obj)
    val ref = serializer.fromBinary(blob, serializer.manifest(obj))
    ref should be(obj)
    blob.length
  }

  def checkSameContent(a: AnyRef, b: AnyRef): Unit = {
    a should be(b)
    val blobA = serializer.toBinary(a)
    val blobB = serializer.toBinary(b)
    blobA.toSeq should be(blobB.toSeq)
  }

  "ReplicatedDataSerializer" must {

    "serialize GSet" in {
      checkSerialization(GSet())
      checkSerialization(GSet() + "a")
      checkSerialization(GSet() + "a" + "b")

      checkSerialization(GSet() + 1 + 2 + 3)
      checkSerialization(GSet() + ref1 + ref2)

      checkSerialization(GSet() + 1L + "2" + 3 + ref1)

      checkSameContent(GSet() + "a" + "b", GSet() + "a" + "b")
      checkSameContent(GSet() + "a" + "b", GSet() + "b" + "a")
      checkSameContent(GSet() + ref1 + ref2 + ref3, GSet() + ref2 + ref1 + ref3)
      checkSameContent(GSet() + ref1 + ref2 + ref3, GSet() + ref3 + ref2 + ref1)

      checkSerialization(GSetKey[String]("id"))
    }

    "serialize ORSet" in {
      checkSerialization(ORSet())
      checkSerialization(ORSet().add(address1, "a"))
      checkSerialization(ORSet().add(address1, "a").add(address2, "a"))
      checkSerialization(ORSet().add(address1, "a").remove(address2, "a"))
      checkSerialization(ORSet().add(address1, "a").add(address2, "b").remove(address1, "a"))
      checkSerialization(ORSet().add(address1, 1).add(address2, 2))
      checkSerialization(ORSet().add(address1, 1L).add(address2, 2L))
      checkSerialization(ORSet().add(address1, "a").add(address2, 2).add(address3, 3L).add(address3, ref3))

      val s1 = ORSet().add(address1, "a").add(address2, "b")
      val s2 = ORSet().add(address2, "b").add(address1, "a")

      checkSameContent(s1.merge(s2), s2.merge(s1))

      val s3 = ORSet().add(address1, "a").add(address2, 17).remove(address3, 17)
      val s4 = ORSet().add(address2, 17).remove(address3, 17).add(address1, "a")
      checkSameContent(s3.merge(s4), s4.merge(s3))

      // ORSet with ActorRef
      checkSerialization(ORSet().add(address1, ref1))
      checkSerialization(ORSet().add(address1, ref1).add(address1, ref2))
      checkSerialization(ORSet().add(address1, ref1).add(address1, "a").add(address2, ref2).add(address2, "b"))

      val s5 = ORSet().add(address1, "a").add(address2, ref1)
      val s6 = ORSet().add(address2, ref1).add(address1, "a")
      checkSameContent(s5.merge(s6), s6.merge(s5))

      checkSerialization(ORSetKey[String]("id"))
    }

    "serialize ORSet with ActorRef message sent between two systems" in {
      val system2 = ActorSystem(system.name, system.settings.config)
      try {
        val echo1 = system.actorOf(TestActors.echoActorProps, "echo1")
        system2.actorOf(TestActors.echoActorProps, "echo2")

        system
          .actorSelection(RootActorPath(Cluster(system2).selfAddress) / "user" / "echo2")
          .tell(Identify("2"), testActor)
        val echo2 = expectMsgType[ActorIdentity].ref.get

        val msg = ORSet
          .empty[ActorRef]
          .add(Cluster(system).selfUniqueAddress, echo1)
          .add(Cluster(system).selfUniqueAddress, echo2)
        echo2.tell(msg, testActor)
        val reply = expectMsgType[ORSet[ActorRef]]
        reply.elements should ===(Set(echo1, echo2))

      } finally {
        shutdown(system2)
      }
    }

    "serialize ORSet delta" in {
      checkSerialization(ORSet().add(address1, "a").delta.get)
      checkSerialization(ORSet().add(address1, "a").resetDelta.remove(address2, "a").delta.get)
      checkSerialization(ORSet().add(address1, "a").remove(address2, "a").delta.get)
      checkSerialization(ORSet().add(address1, "a").resetDelta.clear().delta.get)
      checkSerialization(ORSet().add(address1, "a").clear().delta.get)
    }

    "serialize large GSet" in {
      val largeSet = (10000 until 20000).foldLeft(GSet.empty[String]) {
        case (acc, n) => acc.resetDelta.add(n.toString)
      }
      val numberOfBytes = checkSerialization(largeSet)
      info(s"size of GSet with ${largeSet.size} elements: $numberOfBytes bytes")
      numberOfBytes should be <= (80000)
    }

    "serialize large ORSet" in {
      val largeSet = (10000 until 20000).foldLeft(ORSet.empty[String]) {
        case (acc, n) =>
          val address = (n % 3) match {
            case 0 => address1
            case 1 => address2
            case 2 => address3
          }
          acc.resetDelta.add(address, n.toString)
      }
      val numberOfBytes = checkSerialization(largeSet)
      // note that ORSet is compressed, and therefore smaller than GSet
      info(s"size of ORSet with ${largeSet.size} elements: $numberOfBytes bytes")
      numberOfBytes should be <= (50000)
    }

    "serialize Flag" in {
      checkSerialization(Flag())
      checkSerialization(Flag().switchOn)
      checkSerialization(FlagKey("id"))
    }

    "serialize LWWRegister" in {
      checkSerialization(LWWRegister(address1, "value1", LWWRegister.defaultClock[String]))
      checkSerialization(
        LWWRegister(address1, "value2", LWWRegister.defaultClock[String])
          .withValue(address2, "value3", LWWRegister.defaultClock[String]))
      checkSerialization(LWWRegisterKey[String]("id"))
    }

    "serialize GCounter" in {
      checkSerialization(GCounter())
      checkSerialization(GCounter().increment(address1, 3))
      checkSerialization(GCounter().increment(address1, 2).increment(address2, 5))

      checkSameContent(
        GCounter().increment(address1, 2).increment(address2, 5),
        GCounter().increment(address2, 5).increment(address1, 1).increment(address1, 1))
      checkSameContent(
        GCounter().increment(address1, 2).increment(address3, 5),
        GCounter().increment(address3, 5).increment(address1, 2))

      checkSerialization(GCounterKey("id"))
    }

    "serialize PNCounter" in {
      checkSerialization(PNCounter())
      checkSerialization(PNCounter().increment(address1, 3))
      checkSerialization(PNCounter().increment(address1, 3).decrement(address1, 1))
      checkSerialization(PNCounter().increment(address1, 2).increment(address2, 5))
      checkSerialization(PNCounter().increment(address1, 2).increment(address2, 5).decrement(address1, 1))

      checkSameContent(
        PNCounter().increment(address1, 2).increment(address2, 5),
        PNCounter().increment(address2, 5).increment(address1, 1).increment(address1, 1))
      checkSameContent(
        PNCounter().increment(address1, 2).increment(address3, 5),
        PNCounter().increment(address3, 5).increment(address1, 2))
      checkSameContent(
        PNCounter().increment(address1, 2).decrement(address1, 1).increment(address3, 5),
        PNCounter().increment(address3, 5).increment(address1, 2).decrement(address1, 1))

      checkSerialization(PNCounterKey("id"))
    }

    "serialize ORMap" in {
      checkSerialization(ORMap().put(address1, "a", GSet() + "A").put(address2, "b", GSet() + "B"))
      checkSerialization(ORMap().put(address1, 1, GSet() + "A"))
      checkSerialization(ORMap().put(address1, 1L, GSet() + "A"))
      // use Flag for this test as object key because it is serializable
      checkSerialization(ORMap().put(address1, Flag(), GSet() + "A"))

      checkSerialization(ORMapKey[UniqueAddress, GSet[String]]("id"))
    }

    "serialize ORMap delta" in {
      checkSerialization(ORMap().put(address1, "a", GSet() + "A").put(address2, "b", GSet() + "B").delta.get)
      checkSerialization(ORMap().put(address1, "a", GSet() + "A").resetDelta.remove(address2, "a").delta.get)
      checkSerialization(ORMap().put(address1, "a", GSet() + "A").remove(address2, "a").delta.get)
      checkSerialization(ORMap().put(address1, 1, GSet() + "A").delta.get)
      checkSerialization(ORMap().put(address1, 1L, GSet() + "A").delta.get)
      checkSerialization(
        ORMap
          .empty[String, ORSet[String]]
          .put(address1, "a", ORSet.empty[String].add(address1, "A"))
          .put(address2, "b", ORSet.empty[String].add(address2, "B"))
          .updated(address1, "a", ORSet.empty[String])(_.add(address1, "C"))
          .delta
          .get)
      checkSerialization(
        ORMap
          .empty[String, ORSet[String]]
          .resetDelta
          .updated(address1, "a", ORSet.empty[String])(_.add(address1, "C"))
          .delta
          .get)
      // use Flag for this test as object key because it is serializable
      checkSerialization(ORMap().put(address1, Flag(), GSet() + "A").delta.get)
    }

    "serialize LWWMap" in {
      checkSerialization(LWWMap())
      checkSerialization(LWWMap().put(address1, "a", "value1", LWWRegister.defaultClock[Any]))
      checkSerialization(LWWMap().put(address1, 1, "value1", LWWRegister.defaultClock[Any]))
      checkSerialization(LWWMap().put(address1, 1L, "value1", LWWRegister.defaultClock[Any]))
      checkSerialization(LWWMap().put(address1, Flag(), "value1", LWWRegister.defaultClock[Any]))
      checkSerialization(
        LWWMap()
          .put(address1, "a", "value1", LWWRegister.defaultClock[Any])
          .put(address2, "b", 17, LWWRegister.defaultClock[Any]))

      checkSerialization(LWWMapKey[UniqueAddress, String]("id"))
    }

    "serialize PNCounterMap" in {
      checkSerialization(PNCounterMap())
      checkSerialization(PNCounterMap().increment(address1, "a", 3))
      checkSerialization(PNCounterMap().increment(address1, 1, 3))
      checkSerialization(PNCounterMap().increment(address1, 1L, 3))
      checkSerialization(PNCounterMap().increment(address1, Flag(), 3))
      checkSerialization(
        PNCounterMap().increment(address1, "a", 3).decrement(address2, "a", 2).increment(address2, "b", 5))

      checkSerialization(PNCounterMapKey[String]("id"))
    }

    "serialize ORMultiMap" in {
      checkSerialization(ORMultiMap())
      checkSerialization(ORMultiMap().addBinding(address1, "a", "A"))
      checkSerialization(ORMultiMap().addBinding(address1, 1, "A"))
      checkSerialization(ORMultiMap().addBinding(address1, 1L, "A"))
      checkSerialization(ORMultiMap().addBinding(address1, Flag(), "A"))
      checkSerialization(
        ORMultiMap
          .empty[String, String]
          .addBinding(address1, "a", "A1")
          .put(address2, "b", Set("B1", "B2", "B3"))
          .addBinding(address2, "a", "A2"))

      val m1 = ORMultiMap.empty[String, String].addBinding(address1, "a", "A1").addBinding(address2, "a", "A2")
      val m2 = ORMultiMap.empty[String, String].put(address2, "b", Set("B1", "B2", "B3"))
      checkSameContent(m1.merge(m2), m2.merge(m1))
      checkSerialization(
        ORMultiMap.empty[String, String].addBinding(address1, "a", "A1").addBinding(address1, "a", "A2").delta.get)
      val m3 = ORMultiMap.empty[String, String].addBinding(address1, "a", "A1")
      val d3 = m3.resetDelta.addBinding(address1, "a", "A2").addBinding(address1, "a", "A3").delta.get
      checkSerialization(d3)

      checkSerialization(ORMultiMapKey[String, String]("id"))
    }

    "serialize ORMultiMap withValueDeltas" in {
      checkSerialization(ORMultiMap._emptyWithValueDeltas)
      checkSerialization(ORMultiMap._emptyWithValueDeltas.addBinding(address1, "a", "A"))
      checkSerialization(ORMultiMap._emptyWithValueDeltas.addBinding(address1, 1, "A"))
      checkSerialization(ORMultiMap._emptyWithValueDeltas.addBinding(address1, 1L, "A"))
      checkSerialization(ORMultiMap._emptyWithValueDeltas.addBinding(address1, Flag(), "A"))
      checkSerialization(
        ORMultiMap.emptyWithValueDeltas[String, String].addBinding(address1, "a", "A").remove(address1, "a").delta.get)
      checkSerialization(
        ORMultiMap
          .emptyWithValueDeltas[String, String]
          .addBinding(address1, "a", "A1")
          .put(address2, "b", Set("B1", "B2", "B3"))
          .addBinding(address2, "a", "A2"))

      val m1 =
        ORMultiMap.emptyWithValueDeltas[String, String].addBinding(address1, "a", "A1").addBinding(address2, "a", "A2")
      val m2 = ORMultiMap.emptyWithValueDeltas[String, String].put(address2, "b", Set("B1", "B2", "B3"))
      checkSameContent(m1.merge(m2), m2.merge(m1))
    }

    "serialize DeletedData" in {
      checkSerialization(DeletedData)
    }

    "serialize VersionVector" in {
      checkSerialization(VersionVector())
      checkSerialization(VersionVector().increment(address1))
      checkSerialization(VersionVector().increment(address1).increment(address2))

      val v1 = VersionVector().increment(address1).increment(address1)
      val v2 = VersionVector().increment(address2)
      checkSameContent(v1.merge(v2), v2.merge(v1))
    }

    "serialize UnspecificKey" in {
      checkSerialization(Key.UnspecificKey("id"))
    }

  }
}
