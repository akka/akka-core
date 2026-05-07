/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.serialization

import java.io.NotSerializableException
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import com.typesafe.config._
import org.apache.commons.codec.binary.Hex.{ decodeHex, encodeHex }

import akka.actor._
import akka.persistence._
import akka.persistence.AtLeastOnceDelivery.{ AtLeastOnceDeliverySnapshot, UnconfirmedDelivery }
import akka.serialization._
import akka.testkit._
import akka.util.ByteString.UTF_8

object SerializerSpecConfigs {
  val customSerializers =
    ConfigFactory.parseString("""
      akka.actor {
        serializers {
          my-payload = "akka.persistence.serialization.MyPayloadSerializer"
          my-payload2 = "akka.persistence.serialization.MyPayload2Serializer"
          my-snapshot = "akka.persistence.serialization.MySnapshotSerializer"
          my-snapshot2 = "akka.persistence.serialization.MySnapshotSerializer2"
          old-payload = "akka.persistence.serialization.OldPayloadSerializer"
        }
        serialization-bindings {
          "akka.persistence.serialization.MyPayload" = my-payload
          "akka.persistence.serialization.MyPayload2" = my-payload2
          "akka.persistence.serialization.MySnapshot" = my-snapshot
          "akka.persistence.serialization.MySnapshot2" = my-snapshot2
          # this entry was used when creating the data for the test
          # "deserialize data when class is removed"
          #"akka.persistence.serialization.OldPayload" = old-payload
        }
      }
    """)

  val remote = ConfigFactory.parseString("""
      akka {
        actor {
          provider = remote
        }
        remote {
          artery.canonical {
            hostname = "127.0.0.1"
            port = 0
          }
        }
        loglevel = ERROR
        log-dead-letters = 0
        log-dead-letters-during-shutdown = off
      }
    """)

  def config(configs: String*): Config =
    configs.foldLeft(ConfigFactory.empty)((r, c) => r.withFallback(ConfigFactory.parseString(c)))

}

import akka.persistence.serialization.SerializerSpecConfigs._

class SnapshotSerializerPersistenceSpec extends AkkaSpec(customSerializers) {
  val serialization = SerializationExtension(system)

  "A snapshot serializer" must {
    "handle custom snapshot Serialization" in {
      val wrapped = Snapshot(MySnapshot("a"))
      val serializer = serialization.findSerializerFor(wrapped)

      val bytes = serializer.toBinary(wrapped)
      val deserialized = serializer.fromBinary(bytes, None)

      deserialized should ===(Snapshot(MySnapshot(".a.")))
    }

    "handle custom snapshot Serialization with string manifest" in {
      val wrapped = Snapshot(MySnapshot2("a"))
      val serializer = serialization.findSerializerFor(wrapped)

      val bytes = serializer.toBinary(wrapped)
      val deserialized = serializer.fromBinary(bytes, None)

      deserialized should ===(Snapshot(MySnapshot2(".a.")))
    }

    "throw error when reads snapshot created with akka 2.3.6 and Scala 2.10" in {
      val dataStr = "abc"
      val snapshot = Snapshot(dataStr.getBytes(UTF_8))
      val serializer = serialization.findSerializerFor(snapshot)

      // the oldSnapshot was created with Akka 2.3.6 and it is using JavaSerialization
      // for the SnapshotHeader. See issue #16009.
      // It was created with:
      // println(s"encoded snapshot: " + String.valueOf(encodeHex(serializer.toBinary(snapshot))))
      val oldSnapshot = // 32 bytes per line
        "a8000000aced00057372002d616b6b612e70657273697374656e63652e736572" +
        "69616c697a6174696f6e2e536e617073686f7448656164657200000000000000" +
        "0102000249000c73657269616c697a657249644c00086d616e69666573747400" +
        "0e4c7363616c612f4f7074696f6e3b7870000000047372000b7363616c612e4e" +
        "6f6e6524465024f653ca94ac0200007872000c7363616c612e4f7074696f6ee3" +
        "6024a8328a45e90200007870616263"

      val bytes = decodeHex(oldSnapshot.toCharArray)
      val cause = intercept[NotSerializableException] {
        serializer.fromBinary(bytes, None).asInstanceOf[Snapshot]
      }
      cause.getMessage should startWith("Replaying snapshot from akka 2.3.x")
    }

    "throw error when reads snapshot created with akka 2.3.6 and Scala 2.11" in {
      val dataStr = "abc"
      val snapshot = Snapshot(dataStr.getBytes(UTF_8))
      val serializer = serialization.findSerializerFor(snapshot)

      // the oldSnapshot was created with Akka 2.3.6 and it is using JavaSerialization
      // for the SnapshotHeader. See issue #16009.
      // It was created with:
      // println(s"encoded snapshot: " + String.valueOf(encodeHex(serializer.toBinary(snapshot))))
      val oldSnapshot = // 32 bytes per line
        "a8000000aced00057372002d616b6b612e70657273697374656e63652e736572" +
        "69616c697a6174696f6e2e536e617073686f7448656164657200000000000000" +
        "0102000249000c73657269616c697a657249644c00086d616e69666573747400" +
        "0e4c7363616c612f4f7074696f6e3b7870000000047372000b7363616c612e4e" +
        "6f6e6524465024f653ca94ac0200007872000c7363616c612e4f7074696f6efe" +
        "6937fddb0e66740200007870616263"

      val bytes = decodeHex(oldSnapshot.toCharArray)
      val cause = intercept[NotSerializableException] {
        serializer.fromBinary(bytes, None).asInstanceOf[Snapshot]
      }
      cause.getMessage should startWith("Replaying snapshot from akka 2.3.x")
    }

    "be able to write and read snapshot created with akka 2.4.17 and Scala 2.11" in {
      val dataStr = "abc"
      val snapshot = Snapshot(dataStr.getBytes(UTF_8))
      val serializer = serialization.findSerializerFor(snapshot)

      // the oldSnapshot was created with Akka 2.4.17
      // It was created with:
      // import org.apache.commons.codec.binary.Hex.encodeHex
      // println(s"encoded snapshot: " + String.valueOf(encodeHex(serializer.toBinary(snapshot))))
      val oldSnapshot = "0400000004000000616263"

      oldSnapshot should ===(String.valueOf(encodeHex(serializer.toBinary(snapshot))))

      val bytes = decodeHex(oldSnapshot.toCharArray)
      val deserialized = serializer.fromBinary(bytes, None).asInstanceOf[Snapshot]
      val deserializedDataStr = new String(deserialized.data.asInstanceOf[Array[Byte]], UTF_8)

      dataStr should ===(deserializedDataStr)
    }
  }
}

class MessageSerializerPersistenceSpec extends AkkaSpec(customSerializers) {
  val serialization = SerializationExtension(system)

  "A message serializer" when {
    "not given a manifest" must {
      "handle custom Persistent message serialization" in {
        val persistent = PersistentRepr(MyPayload("a"), 13, "p1", "", writerUuid = UUID.randomUUID().toString)
        val serializer = serialization.findSerializerFor(persistent)

        val bytes = serializer.toBinary(persistent)
        val deserialized = serializer.fromBinary(bytes, None)

        deserialized should ===(persistent.withPayload(MyPayload(".a.")))
      }
    }

    "given a PersistentRepr manifest" must {
      "handle custom Persistent message serialization" in {
        val persistent = PersistentRepr(MyPayload("b"), 13, "p1", "", writerUuid = UUID.randomUUID().toString)
        val serializer = serialization.findSerializerFor(persistent)

        val bytes = serializer.toBinary(persistent)
        val deserialized = serializer.fromBinary(bytes, Some(classOf[PersistentRepr]))

        deserialized should ===(persistent.withPayload(MyPayload(".b.")))
      }
    }

    "given payload serializer with string manifest" must {
      "handle serialization" in {
        val persistent = PersistentRepr(MyPayload2("a", 17), 13, "p1", "", writerUuid = UUID.randomUUID().toString)
        val serializer = serialization.findSerializerFor(persistent)

        val bytes = serializer.toBinary(persistent)
        val deserialized = serializer.fromBinary(bytes, None)

        deserialized should ===(persistent.withPayload(MyPayload2(".a.", 17)))
      }

      "be able to evolve the data types" in {
        val oldEvent = MyPayload("a")
        val serializer1 = serialization.findSerializerFor(oldEvent)
        val bytes = serializer1.toBinary(oldEvent)

        // now the system is updated to version 2 with new class MyPayload2
        // and MyPayload2Serializer that handles migration from old MyPayload
        val serializer2 = serialization.serializerFor(classOf[MyPayload2])
        val deserialized = serializer2.fromBinary(bytes, Some(oldEvent.getClass))

        deserialized should be(MyPayload2(".a.", 0))
      }

      "be able to deserialize data when class is removed" in {
        val serializer = serialization.findSerializerFor(PersistentRepr("x", 13, "p1", ""))

        // It was created with:
        // val old = PersistentRepr(OldPayload('A'), 13, "p1", true, testActor)
        // import org.apache.commons.codec.binary.Hex._
        // println(s"encoded OldPayload: " + String.valueOf(encodeHex(serializer.toBinary(old))))
        //
        val oldData =
          "0a3e08c7da04120d4f6c645061796c6f61642841291a2" +
          "9616b6b612e70657273697374656e63652e7365726961" +
          "6c697a6174696f6e2e4f6c645061796c6f6164100d1a0" +
          "2703120015a45616b6b613a2f2f4d6573736167655365" +
          "7269616c697a657250657273697374656e63655370656" +
          "32f73797374656d2f746573744163746f722d31233133" +
          "3137373931343033"

        // now the system is updated, OldPayload is replaced by MyPayload, and the
        // OldPayloadSerializer is adjusted to migrate OldPayload
        val bytes = decodeHex(oldData.toCharArray)

        val deserialized = serializer.fromBinary(bytes, None).asInstanceOf[PersistentRepr]

        deserialized.payload should be(MyPayload("OldPayload(A)"))
      }
    }

    "given PersistentRepr serialized with Akka 2.3.11 Scala 2.10" must {
      "be able to deserialize with latest version" in {
        // It was created with:
        // val old = PersistentRepr(MyPayload("a"), 13, "p1", true, 3, List("c1", "c2"), confirmable = true, DeliveredByChannel("p2", "c2", 14), testActor, testActor)
        // import org.apache.commons.codec.binary.Hex._
        // println(s"encoded persistent: " + String.valueOf(encodeHex(serializer.toBinary(persistent))))
        val oldData =
          "0a3208c3da0412022e611a28616b6b612e70657273697374656e63652e73657269616c697a" +
          "6174696f6e2e4d795061796c6f6164100d1a027031200130033a0263313a02633240014a0c" +
          "0a02703212026332180e20005244616b6b613a2f2f4d65737361676553657269616c697a65" +
          "7250657273697374656e6365537065632f73797374656d2f746573744163746f7232232d34" +
          "34373233313933375a44616b6b613a2f2f4d65737361676553657269616c697a6572506572" +
          "73697374656e6365537065632f73797374656d2f746573744163746f7232232d3434373233" +
          "31393337"

        val bytes = decodeHex(oldData.toCharArray)
        val expected = PersistentRepr(MyPayload(".a."), 13, "p1", "", true, Actor.noSender)
        val serializer = serialization.findSerializerFor(expected)
        val deserialized = serializer.fromBinary(bytes, None).asInstanceOf[PersistentRepr]
        deserialized.sender should not be null
        val deserializedWithoutSender = deserialized.update(sender = Actor.noSender)
        deserializedWithoutSender should be(expected)
      }
    }

    "given AtLeastOnceDeliverySnapshot" must {
      "handle empty unconfirmed" in {
        val unconfirmed = Vector.empty
        val snap = AtLeastOnceDeliverySnapshot(13, unconfirmed)
        val serializer = serialization.findSerializerFor(snap)

        val bytes = serializer.toBinary(snap)
        val deserialized = serializer.fromBinary(bytes, Some(classOf[AtLeastOnceDeliverySnapshot]))

        deserialized should ===(snap)
      }

      "handle a few unconfirmed" in {
        val unconfirmed = Vector(
          UnconfirmedDelivery(deliveryId = 1, destination = testActor.path, "a"),
          UnconfirmedDelivery(deliveryId = 2, destination = testActor.path, "b"),
          UnconfirmedDelivery(deliveryId = 3, destination = testActor.path, 42))
        val snap = AtLeastOnceDeliverySnapshot(17, unconfirmed)
        val serializer = serialization.findSerializerFor(snap)

        val bytes = serializer.toBinary(snap)
        val deserialized = serializer.fromBinary(bytes, Some(classOf[AtLeastOnceDeliverySnapshot]))

        deserialized should ===(snap)
      }
    }

  }
}

object MessageSerializerRemotingSpec {
  class LocalActor(port: Int) extends Actor {
    def receive = {
      case m => context.actorSelection(s"akka://remote@127.0.0.1:${port}/user/remote").tell(m, Actor.noSender)
    }
  }

  class RemoteActor extends Actor {
    def receive = {
      case p @ PersistentRepr(MyPayload(data), _) => p.sender ! s"p${data}"
      case a: AtomicWrite =>
        a.payload.foreach {
          case p @ PersistentRepr(MyPayload(data), _) => p.sender ! s"p${data}"
          case x                                      => throw new RuntimeException(s"Unexpected payload: $x")
        }
    }
  }

  def port(system: ActorSystem) =
    address(system).port.get

  def address(system: ActorSystem) =
    system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
}

class MessageSerializerRemotingSpec extends AkkaSpec(remote.withFallback(customSerializers)) with DefaultTimeout {
  import MessageSerializerRemotingSpec._

  val remoteSystem = ActorSystem("remote", remote.withFallback(customSerializers))
  val localActor = system.actorOf(Props(classOf[LocalActor], port(remoteSystem)), "local")

  val serialization = SerializationExtension(system)

  override protected def atStartup(): Unit = {
    remoteSystem.actorOf(Props[RemoteActor](), "remote")
  }

  override def afterTermination(): Unit = {
    Await.ready(remoteSystem.terminate(), Duration.Inf)
  }

  "A message serializer" must {
    "custom-serialize PersistentRepr messages during remoting" in {
      // this also verifies serialization of PersistentRepr.sender,
      // because the RemoteActor will reply to the PersistentRepr.sender
      // is kept intact
      localActor ! PersistentRepr(MyPayload("a"), sender = testActor)
      expectMsg("p.a.")
    }

    "custom-serialize AtomicWrite messages during remoting" in {
      val p1 = PersistentRepr(MyPayload("a"), sender = testActor)
      val p2 = PersistentRepr(MyPayload("b"), sender = testActor)
      localActor ! AtomicWrite(List(p1, p2))
      expectMsg("p.a.")
      expectMsg("p.b.")
    }

    "serialize manifest provided by EventAdapter" in {
      val p1 = PersistentRepr(MyPayload("a"), sender = testActor).withManifest("manifest")
      val bytes = serialization.serialize(p1).get
      val back = serialization.deserialize(bytes, classOf[PersistentRepr]).get
      require(p1.manifest == back.manifest)
    }
  }
}

final case class MyPayload(data: String)
final case class MyPayload2(data: String, n: Int)
final case class MySnapshot(data: String)
final case class MySnapshot2(data: String)

// this class was used when creating the data for the test
// "deserialize data when class is removed"
//final case class OldPayload(c: Char)

class MyPayloadSerializer extends Serializer {
  val MyPayloadClass = classOf[MyPayload]

  def identifier: Int = 77123
  def includeManifest: Boolean = true

  def toBinary(o: AnyRef): Array[Byte] = o match {
    case MyPayload(data) => s".${data}".getBytes(UTF_8)
    case x               => throw new NotSerializableException(s"Unexpected object: $x")
  }

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case Some(MyPayloadClass) => MyPayload(s"${new String(bytes, UTF_8)}.")
    case Some(c)              => throw new Exception(s"unexpected manifest ${c}")
    case None                 => throw new Exception("no manifest")
  }
}

class MyPayload2Serializer extends SerializerWithStringManifest {
  val MyPayload2Class = classOf[MyPayload]

  val ManifestV1 = classOf[MyPayload].getName
  val ManifestV2 = "MyPayload-V2"

  def identifier: Int = 77125

  def manifest(o: AnyRef): String = ManifestV2

  def toBinary(o: AnyRef): Array[Byte] = o match {
    case MyPayload2(data, n) => s".$data:$n".getBytes(UTF_8)
    case x                   => throw new NotSerializableException(s"Unexpected object: $x")
  }

  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case ManifestV2 =>
      val parts = new String(bytes, UTF_8).split(":")
      MyPayload2(data = parts(0) + ".", n = parts(1).toInt)
    case ManifestV1 =>
      MyPayload2(data = s"${new String(bytes, UTF_8)}.", n = 0)
    case other =>
      throw new Exception(s"unexpected manifest [$other]")
  }
}

class MySnapshotSerializer extends Serializer {
  val MySnapshotClass = classOf[MySnapshot]

  def identifier: Int = 77124
  def includeManifest: Boolean = true

  def toBinary(o: AnyRef): Array[Byte] = o match {
    case MySnapshot(data) => s".${data}".getBytes(UTF_8)
    case x                => throw new NotSerializableException(s"Unexpected object: $x")
  }

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case Some(MySnapshotClass) => MySnapshot(s"${new String(bytes, UTF_8)}.")
    case Some(c)               => throw new Exception(s"unexpected manifest ${c}")
    case None                  => throw new Exception("no manifest")
  }
}

class MySnapshotSerializer2 extends SerializerWithStringManifest {
  val CurrentManifest = "MySnapshot-V2"
  val OldManifest = classOf[MySnapshot].getName

  def identifier: Int = 77126

  def manifest(o: AnyRef): String = CurrentManifest

  def toBinary(o: AnyRef): Array[Byte] = o match {
    case MySnapshot2(data) => s".${data}".getBytes(UTF_8)
    case unexpected        => throw new NotSerializableException(s"Unexpected: $unexpected")
  }

  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case CurrentManifest | OldManifest =>
      MySnapshot2(s"${new String(bytes, UTF_8)}.")
    case other =>
      throw new Exception(s"unexpected manifest [$other]")
  }
}

class OldPayloadSerializer extends SerializerWithStringManifest {

  def identifier: Int = 77127
  val OldPayloadClassName = "akka.persistence.serialization.OldPayload"
  val MyPayloadClassName = classOf[MyPayload].getName

  def manifest(o: AnyRef): String = o.getClass.getName

  def toBinary(o: AnyRef): Array[Byte] = o match {
    case MyPayload(data) => s".${data}".getBytes(UTF_8)
    case old if old.getClass.getName == OldPayloadClassName =>
      o.toString.getBytes(UTF_8)
    case x => throw new NotSerializableException(s"Unexpected object: $x")
  }

  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case OldPayloadClassName =>
      MyPayload(new String(bytes, UTF_8))
    case MyPayloadClassName => MyPayload(s"${new String(bytes, UTF_8)}.")
    case other =>
      throw new Exception(s"unexpected manifest [$other]")
  }
}
