/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.ddata

import scala.concurrent.duration._
import akka.actor.Actor
import akka.cluster.ddata._
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe
import akka.actor.ActorRef
import akka.serialization.SerializationExtension
import jdocs.ddata

import scala.annotation.nowarn

object DistributedDataDocSpec {

  val config =
    """
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.port = 0

    #//#serializer-config
    akka.actor {
      serializers {
        two-phase-set = "docs.ddata.protobuf.TwoPhaseSetSerializer"
      }
      serialization-bindings {
        "docs.ddata.TwoPhaseSet" = two-phase-set
      }
    }
    #//#serializer-config

    #//#japi-serializer-config
    akka.actor {
      serializers {
        twophaseset = "jdocs.ddata.protobuf.TwoPhaseSetSerializer"
      }
      serialization-bindings {
        "jdocs.ddata.TwoPhaseSet" = twophaseset
      }
    }
    #//#japi-serializer-config
    """

  //#data-bot
  import java.util.concurrent.ThreadLocalRandom
  import akka.actor.Actor
  import akka.actor.ActorLogging
  import akka.cluster.ddata.DistributedData
  import akka.cluster.ddata.ORSet
  import akka.cluster.ddata.ORSetKey
  import akka.cluster.ddata.Replicator._

  object DataBot {
    private case object Tick
  }

  class DataBot extends Actor with ActorLogging {
    import DataBot._

    val replicator = DistributedData(context.system).replicator
    implicit val node: SelfUniqueAddress = DistributedData(context.system).selfUniqueAddress

    import context.dispatcher
    val tickTask = context.system.scheduler.scheduleWithFixedDelay(5.seconds, 5.seconds, self, Tick)

    val DataKey = ORSetKey[String]("key")

    replicator ! Subscribe(DataKey, self)

    def receive = {
      case Tick =>
        val s = ThreadLocalRandom.current().nextInt(97, 123).toChar.toString
        if (ThreadLocalRandom.current().nextBoolean()) {
          // add
          log.info("Adding: {}", s)
          replicator ! Update(DataKey, ORSet.empty[String], WriteLocal)(_ :+ s)
        } else {
          // remove
          log.info("Removing: {}", s)
          replicator ! Update(DataKey, ORSet.empty[String], WriteLocal)(_.remove(s))
        }

      case _: UpdateResponse[_] => // ignore

      case c @ Changed(DataKey) =>
        val data = c.get(DataKey)
        log.info("Current elements: {}", data.elements)
    }

    override def postStop(): Unit = tickTask.cancel()

  }
  //#data-bot

}

@nowarn("msg=never used") // sample snippets
class DistributedDataDocSpec extends AkkaSpec(DistributedDataDocSpec.config) {
  import Replicator._

  "demonstrate update" in {
    val probe = TestProbe()
    implicit val self: ActorRef = probe.ref

    //#update
    implicit val node: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
    val replicator = DistributedData(system).replicator

    val Counter1Key = PNCounterKey("counter1")
    val Set1Key = GSetKey[String]("set1")
    val Set2Key = ORSetKey[String]("set2")
    val ActiveFlagKey = FlagKey("active")

    replicator ! Update(Counter1Key, PNCounter(), WriteLocal)(_ :+ 1)

    val writeTo3 = WriteTo(n = 3, timeout = 1.second)
    replicator ! Update(Set1Key, GSet.empty[String], writeTo3)(_ + "hello")

    val writeMajority = WriteMajority(timeout = 5.seconds)
    replicator ! Update(Set2Key, ORSet.empty[String], writeMajority)(_ :+ "hello")

    val writeAll = WriteAll(timeout = 5.seconds)
    replicator ! Update(ActiveFlagKey, Flag.Disabled, writeAll)(_.switchOn)
    //#update

    probe.expectMsgType[UpdateResponse[_]] match {
      //#update-response1
      case UpdateSuccess(Counter1Key, req) => // ok
      //#update-response1
      case unexpected => fail("Unexpected response: " + unexpected)
    }

    probe.expectMsgType[UpdateResponse[_]] match {
      //#update-response2
      case UpdateSuccess(Set1Key, req) => // ok
      case UpdateTimeout(Set1Key, req) =>
      // write to 3 nodes failed within 1.second
      //#update-response2
      case UpdateSuccess(Set2Key, None) =>
      case unexpected                   => fail("Unexpected response: " + unexpected)
    }
  }

  "demonstrate update with request context" in {
    import Actor.Receive
    val probe = TestProbe()
    implicit val self: ActorRef = probe.ref
    def sender() = self

    //#update-request-context
    implicit val node = DistributedData(system).selfUniqueAddress
    val replicator = DistributedData(system).replicator
    val writeTwo = WriteTo(n = 2, timeout = 3.second)
    val Counter1Key = PNCounterKey("counter1")

    def receive: Receive = {
      case "increment" =>
        // incoming command to increase the counter
        val upd = Update(Counter1Key, PNCounter(), writeTwo, request = Some(sender()))(_ :+ 1)
        replicator ! upd

      case UpdateSuccess(Counter1Key, Some(replyTo: ActorRef)) =>
        replyTo ! "ack"
      case UpdateTimeout(Counter1Key, Some(replyTo: ActorRef)) =>
        replyTo ! "nack"
    }
    //#update-request-context
  }

  "demonstrate get" in {
    val probe = TestProbe()
    implicit val self: ActorRef = probe.ref

    //#get
    val replicator = DistributedData(system).replicator
    val Counter1Key = PNCounterKey("counter1")
    val Set1Key = GSetKey[String]("set1")
    val Set2Key = ORSetKey[String]("set2")
    val ActiveFlagKey = FlagKey("active")

    replicator ! Get(Counter1Key, ReadLocal)

    val readFrom3 = ReadFrom(n = 3, timeout = 1.second)
    replicator ! Get(Set1Key, readFrom3)

    val readMajority = ReadMajority(timeout = 5.seconds)
    replicator ! Get(Set2Key, readMajority)

    val readAll = ReadAll(timeout = 5.seconds)
    replicator ! Get(ActiveFlagKey, readAll)
    //#get

    probe.expectMsgType[GetResponse[_]] match {
      //#get-response1
      case g @ GetSuccess(Counter1Key, req) =>
        val value = g.get(Counter1Key).value
      case NotFound(Counter1Key, req) => // key counter1 does not exist
      //#get-response1
      case unexpected => fail("Unexpected response: " + unexpected)
    }

    probe.expectMsgType[GetResponse[_]] match {
      //#get-response2
      case g @ GetSuccess(Set1Key, req) =>
        val elements = g.get(Set1Key).elements
      case GetFailure(Set1Key, req) =>
      // read from 3 nodes failed within 1.second
      case NotFound(Set1Key, req) => // key set1 does not exist
      //#get-response2
      case g @ GetSuccess(Set2Key, None) =>
        val elements = g.get(Set2Key).elements
      case unexpected => fail("Unexpected response: " + unexpected)
    }
  }

  "demonstrate get with request context" in {
    import Actor.Receive
    val probe = TestProbe()
    implicit val self: ActorRef = probe.ref
    def sender() = self

    //#get-request-context
    implicit val node = DistributedData(system).selfUniqueAddress
    val replicator = DistributedData(system).replicator
    val readTwo = ReadFrom(n = 2, timeout = 3.second)
    val Counter1Key = PNCounterKey("counter1")

    def receive: Receive = {
      case "get-count" =>
        // incoming request to retrieve current value of the counter
        replicator ! Get(Counter1Key, readTwo, request = Some(sender()))

      case g @ GetSuccess(Counter1Key, Some(replyTo: ActorRef)) =>
        val value = g.get(Counter1Key).value.longValue
        replyTo ! value
      case GetFailure(Counter1Key, Some(replyTo: ActorRef)) =>
        replyTo ! -1L
      case NotFound(Counter1Key, Some(replyTo: ActorRef)) =>
        replyTo ! 0L
    }
    //#get-request-context
  }

  "demonstrate subscribe" in {
    import Actor.Receive
    val probe = TestProbe()
    implicit val self: ActorRef = probe.ref
    def sender() = self

    //#subscribe
    val replicator = DistributedData(system).replicator
    val Counter1Key = PNCounterKey("counter1")
    // subscribe to changes of the Counter1Key value
    replicator ! Subscribe(Counter1Key, self)
    var currentValue = BigInt(0)

    def receive: Receive = {
      case c @ Changed(Counter1Key) =>
        currentValue = c.get(Counter1Key).value
      case "get-count" =>
        // incoming request to retrieve current value of the counter
        sender() ! currentValue
    }
    //#subscribe
  }

  "demonstrate delete" in {
    val probe = TestProbe()
    implicit val self: ActorRef = probe.ref

    //#delete
    val replicator = DistributedData(system).replicator
    val Counter1Key = PNCounterKey("counter1")
    val Set2Key = ORSetKey[String]("set2")

    replicator ! Delete(Counter1Key, WriteLocal)

    val writeMajority = WriteMajority(timeout = 5.seconds)
    replicator ! Delete(Set2Key, writeMajority)
    //#delete
  }

  "demonstrate PNCounter" in {
    def println(o: Any): Unit = ()
    //#pncounter
    implicit val node = DistributedData(system).selfUniqueAddress

    val c0 = PNCounter.empty
    val c1 = c0 :+ 1
    val c2 = c1 :+ 7
    val c3: PNCounter = c2.decrement(2)
    println(c3.value) // 6
    //#pncounter
  }

  "demonstrate PNCounterMap" in {
    def println(o: Any): Unit = ()
    //#pncountermap
    implicit val node = DistributedData(system).selfUniqueAddress
    val m0 = PNCounterMap.empty[String]
    val m1 = m0.increment(node, "a", 7)
    val m2 = m1.decrement(node, "a", 2)
    val m3 = m2.increment(node, "b", 1)
    println(m3.get("a")) // 5
    m3.entries.foreach { case (key, value) => println(s"$key -> $value") }
    //#pncountermap
  }

  "demonstrate GSet" in {
    def println(o: Any): Unit = ()
    //#gset
    val s0 = GSet.empty[String]
    val s1 = s0 + "a"
    val s2 = s1 + "b" + "c"
    if (s2.contains("a"))
      println(s2.elements) // a, b, c
    //#gset
  }

  "demonstrate ORSet" in {
    def println(o: Any): Unit = ()
    //#orset
    implicit val node = DistributedData(system).selfUniqueAddress
    val s0 = ORSet.empty[String]
    val s1 = s0 :+ "a"
    val s2 = s1 :+ "b"
    val s3 = s2.remove("a")
    println(s3.elements) // b
    //#orset
  }

  "demonstrate ORMultiMap" in {
    def println(o: Any): Unit = ()
    //#ormultimap
    implicit val node = DistributedData(system).selfUniqueAddress
    val m0 = ORMultiMap.empty[String, Int]
    val m1 = m0 :+ ("a" -> Set(1, 2, 3))
    val m2 = m1.addBinding(node, "a", 4)
    val m3 = m2.removeBinding(node, "a", 2)
    val m4 = m3.addBinding(node, "b", 1)
    println(m4.entries)
    //#ormultimap
  }

  "demonstrate Flag" in {
    def println(o: Any): Unit = ()
    //#flag
    val f0 = Flag.Disabled
    val f1 = f0.switchOn
    println(f1.enabled)
    //#flag
  }

  "demonstrate LWWRegister" in {
    def println(o: Any): Unit = ()
    //#lwwregister
    implicit val node = DistributedData(system).selfUniqueAddress
    val r1 = LWWRegister.create("Hello")
    val r2 = r1.withValueOf("Hi")
    println(s"${r1.value} by ${r1.updatedBy} at ${r1.timestamp}")
    //#lwwregister
    r2.value should be("Hi")
  }

  "demonstrate LWWRegister with custom clock" in {
    def println(o: Any): Unit = ()
    //#lwwregister-custom-clock
    case class Record(version: Int, name: String, address: String)

    implicit val node = DistributedData(system).selfUniqueAddress
    implicit val recordClock: LWWRegister.Clock[Record] = new LWWRegister.Clock[Record] {
      override def apply(currentTimestamp: Long, value: Record): Long =
        value.version
    }

    val record1 = Record(version = 1, "Alice", "Union Square")
    val r1 = LWWRegister(node, record1, recordClock)

    val record2 = Record(version = 2, "Alice", "Madison Square")
    val r2 = LWWRegister(node, record2, recordClock)

    val r3 = r1.merge(r2)
    println(r3.value)
    //#lwwregister-custom-clock

    r3.value.address should be("Madison Square")
  }

  "test TwoPhaseSetSerializer" in {
    val s1 = TwoPhaseSet().add("a").add("b").add("c").remove("b")
    s1.elements should be(Set("a", "c"))
    val serializer = SerializationExtension(system).findSerializerFor(s1)
    val blob = serializer.toBinary(s1)
    val s2 = serializer.fromBinary(blob, None)
    s1 should be(s2)
  }

  "test japi.TwoPhaseSetSerializer" in {
    import scala.jdk.CollectionConverters._
    val s1 = ddata.TwoPhaseSet.create().add("a").add("b").add("c").remove("b")
    s1.getElements.asScala should be(Set("a", "c"))
    val serializer = SerializationExtension(system).findSerializerFor(s1)
    val blob = serializer.toBinary(s1)
    val s2 = serializer.fromBinary(blob, None).asInstanceOf[ddata.TwoPhaseSet]
    s1.getElements should be(s2.getElements) // no equals on ddata.TwoPhaseSet
  }

}
