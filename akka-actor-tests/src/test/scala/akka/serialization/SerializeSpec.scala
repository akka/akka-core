/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization

import java.io._
import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import SerializationTests._
import com.typesafe.config._
import language.postfixOps
import test.akka.serialization.NoVerification

import akka.actor._
import akka.actor.dungeon.SerializationCheckFailedException
import akka.pattern.ask
import akka.testkit.{ AkkaSpec, EventFilter }
import akka.util.Timeout
import akka.util.ByteString

object SerializationTests {

  val serializeConf = s"""
    akka {
      actor {
        serializers {
          test = "akka.serialization.NoopSerializer"
          test2 = "akka.serialization.NoopSerializer2"
          other = "other.SerializerOutsideAkkaPackage"

          constructor1 = "akka.serialization.Constructor1Serializer"
          constructor2 = "akka.serialization.Constructor2Serializer"
          constructor3 = "akka.serialization.Constructor3Serializer"
          constructor4 = "akka.serialization.Constructor4Serializer"
          constructor5 = "akka.serialization.Constructor5Serializer"
          constructor6 = "akka.serialization.Constructor6Serializer"
          constructor7 = "akka.serialization.Constructor7Serializer"
        }

        serialization-bindings {
          "akka.serialization.SerializationTests$$Person" = java
          "akka.serialization.SerializationTests$$Address" = java
          "akka.serialization.SerializationTests$$Marker" = test
          "akka.serialization.SerializationTests$$PlainMessage" = test
          "akka.serialization.SerializationTests$$A" = java
          "akka.serialization.SerializationTests$$B" = test
          "akka.serialization.SerializationTests$$D" = test
          "akka.serialization.SerializationTests$$Marker2" = test2
          "akka.serialization.SerializationTests$$AbstractOther" = other

          "akka.serialization.ConstructorSerializer$$No1" = constructor1
          "akka.serialization.ConstructorSerializer$$No2" = constructor2
          "akka.serialization.ConstructorSerializer$$No3" = constructor3
          "akka.serialization.ConstructorSerializer$$No4" = constructor4
          "akka.serialization.ConstructorSerializer$$No5" = constructor5
          "akka.serialization.ConstructorSerializer$$No6" = constructor6
          "akka.serialization.ConstructorSerializer$$No7" = constructor7
        }
      }
    }
  """

  final case class Address(no: String, street: String, city: String, zip: String) { def this() = this("", "", "", "") }

  final case class Person(name: String, age: Int, address: Address) { def this() = this("", 0, null) }

  final case class Record(id: Int, person: Person)

  protected[akka] trait Marker
  protected[akka] trait Marker2
  @nowarn // can't use unused otherwise case class below gets a deprecated
  class SimpleMessage(s: String) extends Marker

  @nowarn
  class ExtendedSimpleMessage(s: String, i: Int) extends SimpleMessage(s)

  trait AnotherInterface extends Marker

  class AnotherMessage extends AnotherInterface

  class ExtendedAnotherMessage extends AnotherMessage

  class PlainMessage

  class ExtendedPlainMessage extends PlainMessage

  class BothTestSerializableAndJavaSerializable(s: String) extends SimpleMessage(s) with Serializable

  class BothTestSerializableAndTestSerializable2(@nowarn("msg=never used") s: String) extends Marker with Marker2

  trait A
  trait B
  class C extends B with A
  class D extends A
  class E extends D

  abstract class AbstractOther

  final class Other extends AbstractOther {
    override def toString: String = "Other"
  }

  val verifySerializabilityConf = """
    akka {
      actor {
        serialize-messages = on
        serialize-creators = on
        allow-java-serialization = on
      }
    }
  """

  class FooActor extends Actor {
    def receive = {
      case msg => sender() ! msg
    }
  }

  class FooAbstractActor extends AbstractActor {
    override def createReceive(): AbstractActor.Receive =
      receiveBuilder().build()
  }

  class NonSerializableActor(@nowarn("msg=never used") arg: AnyRef) extends Actor {
    def receive = {
      case s: String => sender() ! s
    }
  }

  def mostlyReferenceSystem: ActorSystem = {
    val referenceConf = ConfigFactory.defaultReference()
    val mostlyReferenceConf = AkkaSpec.testConf.withFallback(referenceConf)
    ActorSystem("SerializationSystem", mostlyReferenceConf)
  }

  def allowJavaSerializationSystem: ActorSystem = {
    val referenceConf = ConfigFactory.defaultReference()
    val conf = ConfigFactory
      .parseString("""
      akka.actor.warn-about-java-serializer-usage = on
      akka.actor.allow-java-serialization = on
      """)
      .withFallback(ConfigFactory.parseString(serializeConf))
      .withFallback(AkkaSpec.testConf.withFallback(referenceConf))
    ActorSystem("SerializationSystem", conf)
  }

  val systemMessageMultiSerializerConf = """
    akka {
      actor {
        serializers {
          test = "akka.serialization.NoopSerializer"
        }

        serialization-bindings {
          "akka.dispatch.sysmsg.SystemMessage" = test
        }
      }
    }
  """

}

class SerializeSpec extends AkkaSpec(SerializationTests.serializeConf) {

  val ser = SerializationExtension(system)

  val address = SerializationTests.Address("120", "Monroe Street", "Santa Clara", "95050")

  "Serialization" must {

    "have correct bindings" in {
      ser.bindings.collectFirst { case (c, s) if c == address.getClass => s.getClass } should ===(
        Some(classOf[DisabledJavaSerializer]))
      ser.bindings.collectFirst { case (c, s) if c == classOf[PlainMessage] => s.getClass } should ===(
        Some(classOf[NoopSerializer]))
    }

    "not serialize ActorCell" in {
      val a = system.actorOf(Props(new Actor {
        def receive = {
          case o: ObjectOutputStream =>
            try o.writeObject(this)
            catch { case _: NotSerializableException => testActor ! "pass" }
        }
      }))
      a ! new ObjectOutputStream(new ByteArrayOutputStream())
      expectMsg("pass")
      system.stop(a)
    }

    "resolve serializer by direct interface" in {
      ser.serializerFor(classOf[SimpleMessage]).getClass should ===(classOf[NoopSerializer])
    }

    "resolve serializer by interface implemented by super class" in {
      ser.serializerFor(classOf[ExtendedSimpleMessage]).getClass should ===(classOf[NoopSerializer])
    }

    "resolve serializer by indirect interface" in {
      ser.serializerFor(classOf[AnotherMessage]).getClass should ===(classOf[NoopSerializer])
    }

    "resolve serializer by indirect interface implemented by super class" in {
      ser.serializerFor(classOf[ExtendedAnotherMessage]).getClass should ===(classOf[NoopSerializer])
    }

    "resolve serializer for message with binding" in {
      ser.serializerFor(classOf[PlainMessage]).getClass should ===(classOf[NoopSerializer])
    }

    "resolve serializer for message extending class with with binding" in {
      ser.serializerFor(classOf[ExtendedPlainMessage]).getClass should ===(classOf[NoopSerializer])
    }

    "give JavaSerializer lower priority for message with several bindings" in {
      ser.serializerFor(classOf[BothTestSerializableAndJavaSerializable]).getClass should ===(classOf[NoopSerializer])
    }

    "give warning for message with several bindings" in {
      EventFilter.warning(start = "Multiple serializers found", occurrences = 1).intercept {
        ser.serializerFor(classOf[BothTestSerializableAndTestSerializable2]).getClass should be(classOf[NoopSerializer])
          .or(be(classOf[NoopSerializer2]))
      }
    }

    "resolve serializer in the order of the bindings" in {
      ser.serializerFor(classOf[A]).getClass should ===(classOf[DisabledJavaSerializer])
      ser.serializerFor(classOf[B]).getClass should ===(classOf[NoopSerializer])
      // JavaSerializer lower prio when multiple found
      ser.serializerFor(classOf[C]).getClass should ===(classOf[NoopSerializer])
    }

    "resolve serializer in the order of most specific binding first" in {
      ser.serializerFor(classOf[A]).getClass should ===(classOf[DisabledJavaSerializer])
      ser.serializerFor(classOf[D]).getClass should ===(classOf[NoopSerializer])
      ser.serializerFor(classOf[E]).getClass should ===(classOf[NoopSerializer])
    }

    "throw java.io.NotSerializableException when no binding" in {
      intercept[java.io.NotSerializableException] {
        ser.serializerFor(classOf[Actor])
      }
    }

    "use ByteArraySerializer for byte arrays" in {
      val byteSerializer = ser.serializerFor(classOf[Array[Byte]])
      (byteSerializer.getClass should be).theSameInstanceAs(classOf[ByteArraySerializer])

      for (a <- Seq("foo".getBytes("UTF-8"), null: Array[Byte], Array[Byte]()))
        (byteSerializer.fromBinary(byteSerializer.toBinary(a)) should be).theSameInstanceAs(a)

      intercept[IllegalArgumentException] {
        byteSerializer.toBinary("pigdog")
      }.getMessage should ===(
        s"${classOf[ByteArraySerializer].getName} only serializes byte arrays, not [java.lang.String]")
    }

    "support ByteBuffer serialization for byte arrays" in {
      val byteSerializer = ser.serializerFor(classOf[Array[Byte]]).asInstanceOf[ByteBufferSerializer]

      val byteBuffer = ByteBuffer.allocate(128).order(ByteOrder.LITTLE_ENDIAN)
      val str = "abcdef"
      val payload = str.getBytes("UTF-8")
      byteSerializer.toBinary(payload, byteBuffer)
      byteBuffer.position() should ===(payload.length)
      byteBuffer.flip()
      val deserialized = byteSerializer.fromBinary(byteBuffer, "").asInstanceOf[Array[Byte]]
      byteBuffer.remaining() should ===(0)
      new String(deserialized, "UTF-8") should ===(str)

      intercept[IllegalArgumentException] {
        byteSerializer.toBinary("pigdog", byteBuffer)
      }.getMessage should ===(
        s"${classOf[ByteArraySerializer].getName} only serializes byte arrays, not [java.lang.String]")
    }

    "log warning if non-Akka serializer is configured for Akka message" in {
      EventFilter.warning(pattern = ".*not implemented by Akka.*", occurrences = 1).intercept {
        ser.serialize(new Other).get
      }
    }

    "detect duplicate serializer ids" in {
      (intercept[IllegalArgumentException] {
        val sys = ActorSystem(
          "SerializeSpec",
          ConfigFactory.parseString(s"""
          akka {
            actor {
              serializers {
                test = "akka.serialization.NoopSerializer"
                test-same = "akka.serialization.NoopSerializerSameId"
              }
      
              serialization-bindings {
                "akka.serialization.SerializationTests$$Person" = test
                "akka.serialization.SerializationTests$$Address" = test-same
              }
            }
          }
          """))
        shutdown(sys)
      }.getMessage should include).regex("Serializer identifier \\[9999\\].*is not unique")
    }

    "look for various constructors" in {
      ser.serializerFor(classOf[ConstructorSerializer.No1]).getClass should ===(classOf[Constructor1Serializer])
      ser.serializerFor(classOf[ConstructorSerializer.No2]).getClass should ===(classOf[Constructor2Serializer])
      ser.serializerFor(classOf[ConstructorSerializer.No3]).getClass should ===(classOf[Constructor3Serializer])
      ser.serializerFor(classOf[ConstructorSerializer.No4]).getClass should ===(classOf[Constructor4Serializer])
      ser.serializerFor(classOf[ConstructorSerializer.No5]).getClass should ===(classOf[Constructor5Serializer])
      ser.serializerFor(classOf[ConstructorSerializer.No6]).getClass should ===(classOf[Constructor6Serializer])
      ser.serializerFor(classOf[ConstructorSerializer.No7]).getClass should ===(classOf[Constructor7Serializer])
    }
  }
}

class VerifySerializabilitySpec extends AkkaSpec(SerializationTests.verifySerializabilityConf) {
  implicit val timeout: Timeout = Timeout(5 seconds)

  "verify config" in {
    system.settings.SerializeAllCreators should ===(true)
    system.settings.SerializeAllMessages should ===(true)
  }

  "verify creators" in {
    val a = system.actorOf(Props[FooActor]())
    system.stop(a)

    val b = system.actorOf(Props(new FooAbstractActor))
    system.stop(b)

    intercept[IllegalArgumentException] {
      system.actorOf(Props(classOf[NonSerializableActor], new AnyRef))
    }

  }

  "not verify akka creators" in {
    EventFilter.warning(start = "ok", occurrences = 1).intercept {
      // ActorSystem is not possible to serialize, but ok since it starts with "akka."
      val a = system.actorOf(Props(classOf[NonSerializableActor], system))
      // to verify that nothing is logged
      system.log.warning("ok")
      system.stop(a)
    }
  }

  "verify messages" in {
    val a = system.actorOf(Props[FooActor]())
    Await.result(a ? "pigdog", timeout.duration) should ===("pigdog")

    EventFilter[SerializationCheckFailedException](
      start = "Failed to serialize and deserialize message of type java.lang.Object",
      occurrences = 1).intercept {
      a ! new AnyRef
    }
    system.stop(a)
  }

  "not verify akka messages" in {
    val a = system.actorOf(Props[FooActor]())
    EventFilter.warning(start = "ok", occurrences = 1).intercept {
      // ActorSystem is not possible to serialize, but ok since it starts with "akka."
      val message = system
      Await.result(a ? message, timeout.duration) should ===(message)
      // to verify that nothing is logged
      system.log.warning("ok")
    }
    system.stop(a)
  }
}

class ReferenceSerializationSpec extends AkkaSpec(SerializationTests.mostlyReferenceSystem) {

  val ser = SerializationExtension(system)
  def serializerMustBe(toSerialize: Class[_], expectedSerializer: Class[_]) =
    ser.serializerFor(toSerialize).getClass should ===(expectedSerializer)

  "Serialization settings from reference.conf" must {

    "declare Serializable classes to be use DisabledJavaSerializer" in {
      serializerMustBe(classOf[Serializable], classOf[DisabledJavaSerializer])
    }

    "declare Array[Byte] to use ByteArraySerializer" in {
      serializerMustBe(classOf[Array[Byte]], classOf[ByteArraySerializer])
    }

    "declare Long, Int, String, ByteString to use primitive serializers" in {
      serializerMustBe(classOf[java.lang.Long], classOf[LongSerializer])
      serializerMustBe(classOf[java.lang.Integer], classOf[IntSerializer])
      serializerMustBe(classOf[String], classOf[StringSerializer])
      serializerMustBe(classOf[ByteString.ByteString1], classOf[ByteStringSerializer])
      serializerMustBe(classOf[ByteString.ByteString1C], classOf[ByteStringSerializer])
      serializerMustBe(classOf[ByteString.ByteStrings], classOf[ByteStringSerializer])

    }

    "not support serialization for other classes" in {
      intercept[NotSerializableException] { ser.serializerFor(classOf[Object]) }
    }

    "not allow serialize function" in {
      val f = (i: Int) => i + 1
      serializerMustBe(f.getClass, classOf[DisabledJavaSerializer])
    }

  }
}

class AllowJavaSerializationSpec extends AkkaSpec(SerializationTests.allowJavaSerializationSystem) {

  val ser = SerializationExtension(system)
  def serializerMustBe(toSerialize: Class[_], expectedSerializer: Class[_]) =
    ser.serializerFor(toSerialize).getClass should ===(expectedSerializer)

  val address = SerializationTests.Address("120", "Monroe Street", "Santa Clara", "95050")
  val person = SerializationTests.Person(
    "debasish ghosh",
    25,
    SerializationTests.Address("120", "Monroe Street", "Santa Clara", "95050"))

  val messagePrefix = "Using the Java serializer for class"

  "Serialization settings with allow-java-serialization = on" must {

    "declare Serializable classes to be use JavaSerializer" in {
      serializerMustBe(classOf[Serializable], classOf[JavaSerializer])
    }

    "declare Array[Byte] to use ByteArraySerializer" in {
      serializerMustBe(classOf[Array[Byte]], classOf[ByteArraySerializer])
    }

    "declare Long, Int, String, ByteString to use primitive serializers" in {
      serializerMustBe(classOf[java.lang.Long], classOf[LongSerializer])
      serializerMustBe(classOf[java.lang.Integer], classOf[IntSerializer])
      serializerMustBe(classOf[String], classOf[StringSerializer])
      serializerMustBe(classOf[ByteString.ByteString1], classOf[ByteStringSerializer])
    }

    "not support serialization for other classes" in {
      intercept[NotSerializableException] { ser.serializerFor(classOf[Object]) }
    }

    "serialize function with JavaSerializer" in {
      val f = (i: Int) => i + 1
      val serializer = ser.serializerFor(f.getClass)
      serializer.getClass should ===(classOf[JavaSerializer])
      val bytes = ser.serialize(f).get
      val f2 = ser.deserialize(bytes, serializer.identifier, "").get.asInstanceOf[Function1[Int, Int]]
      f2(3) should ===(4)
    }

    "log a warning when serializing classes outside of java.lang package" in {
      EventFilter.warning(start = messagePrefix, occurrences = 1).intercept {
        ser.serializerFor(classOf[java.math.BigDecimal])
      }
    }

    "not log warning when serializing classes from java.lang package" in {
      EventFilter.warning(start = messagePrefix, occurrences = 0).intercept {
        ser.serializerFor(classOf[java.lang.String])
      }
    }

    "have correct bindings" in {
      ser.bindings.collectFirst { case (c, s) if c == address.getClass => s.getClass } should ===(
        Some(classOf[JavaSerializer]))
      ser.bindings.collectFirst { case (c, s) if c == classOf[PlainMessage] => s.getClass } should ===(
        Some(classOf[NoopSerializer]))
    }

    "serialize Address" in {
      assert(ser.deserialize(ser.serialize(address).get, classOf[SerializationTests.Address]).get === address)
    }

    "serialize Person" in {
      assert(ser.deserialize(ser.serialize(person).get, classOf[Person]).get === person)
    }

    "serialize record with Java serializer" in {
      val r = Record(100, person)
      assert(ser.deserialize(ser.serialize(r).get, classOf[Record]).get === r)
    }

    "not serialize ActorCell" in {
      val a = system.actorOf(Props(new Actor {
        def receive = {
          case o: ObjectOutputStream =>
            try o.writeObject(this)
            catch { case _: NotSerializableException => testActor ! "pass" }
        }
      }))
      a ! new ObjectOutputStream(new ByteArrayOutputStream())
      expectMsg("pass")
      system.stop(a)
    }

    "serialize DeadLetterActorRef" in {
      val outbuf = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(outbuf)
      val a = ActorSystem("SerializeDeadLeterActorRef", AkkaSpec.testConf)
      try {
        out.writeObject(a.deadLetters)
        out.flush()
        out.close()

        val in = new ObjectInputStream(new ByteArrayInputStream(outbuf.toByteArray))
        JavaSerializer.currentSystem.withValue(a.asInstanceOf[ActorSystemImpl]) {
          val deadLetters = in.readObject().asInstanceOf[DeadLetterActorRef]
          (deadLetters eq a.deadLetters) should ===(true)
        }
      } finally {
        shutdown(a)
      }
    }

  }
}

class NoVerificationWarningSpec
    extends AkkaSpec(ConfigFactory.parseString("""
        akka.actor.allow-java-serialization = on
        akka.actor.warn-about-java-serializer-usage = on
        akka.actor.warn-on-no-serialization-verification = on
        """)) {

  val ser = SerializationExtension(system)
  val messagePrefix = "Using the Java serializer for class"

  "When warn-on-no-serialization-verification = on, using the Java serializer" must {

    "log a warning on classes without extending NoSerializationVerificationNeeded" in {
      EventFilter.warning(start = messagePrefix, occurrences = 1).intercept {
        ser.serializerFor(classOf[java.math.BigDecimal])
      }
    }

    "still log warning on classes extending NoSerializationVerificationNeeded" in {
      EventFilter.warning(start = messagePrefix, occurrences = 1).intercept {
        ser.serializerFor(classOf[NoVerification])
      }
    }
  }
}

class NoVerificationWarningOffSpec
    extends AkkaSpec(ConfigFactory.parseString("""
        akka.actor.allow-java-serialization = on
        akka.actor.warn-about-java-serializer-usage = on
        akka.actor.warn-on-no-serialization-verification = off
        """)) {

  val ser = SerializationExtension(system)
  val messagePrefix = "Using the Java serializer for class"

  "When warn-on-no-serialization-verification = off, using the Java serializer" must {

    "log a warning on classes without extending NoSerializationVerificationNeeded" in {
      EventFilter.warning(start = messagePrefix, occurrences = 1).intercept {
        ser.serializerFor(classOf[java.math.BigDecimal])
      }
    }

    "not log warning on classes extending NoSerializationVerificationNeeded" in {
      EventFilter.warning(start = messagePrefix, occurrences = 0).intercept {
        ser.serializerFor(classOf[NoVerification])
      }
    }
  }
}

class SerializerDeadlockSpec extends AkkaSpec {

  "SerializationExtension" must {

    "not be accessed from constructor of serializer" in {
      intercept[IllegalStateException] {
        val sys = ActorSystem(
          "SerializerDeadlockSpec",
          ConfigFactory.parseString("""
          akka {
            actor {
              creation-timeout = 1s
              serializers {
                test = "akka.serialization.DeadlockSerializer"
              }
            }
          }
          """))
        shutdown(sys)
      }.getMessage should include("SerializationExtension from its constructor")
    }
  }
}

protected[akka] class NoopSerializer extends Serializer {
  def includeManifest: Boolean = false

  def identifier = 9999

  def toBinary(o: AnyRef): Array[Byte] = {
    Array.empty[Byte]
  }

  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = null
}

protected[akka] class NoopSerializer2 extends Serializer {
  def includeManifest: Boolean = false

  def identifier = 10000

  def toBinary(o: AnyRef): Array[Byte] = {
    Array.empty[Byte]
  }

  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = null
}

protected[akka] class NoopSerializerSameId extends NoopSerializer

@SerialVersionUID(1)
protected[akka] final case class FakeThrowable(msg: String) extends Throwable(msg) with Serializable {
  override def fillInStackTrace = null
}

class DeadlockSerializer(system: ExtendedActorSystem) extends Serializer {

  // not allowed
  SerializationExtension(system)

  def includeManifest: Boolean = false

  def identifier = 9999

  def toBinary(o: AnyRef): Array[Byte] = Array.empty[Byte]

  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = null
}

object ConstructorSerializer {
  class No1
  class No2
  class No3
  class No4
  class No5
  class No6
  class No7
}

private[akka] abstract class ConstructorSerializer extends SerializerWithStringManifest {

  def toBinary(o: AnyRef): Array[Byte] = {
    Array.empty[Byte]
  }

  override def manifest(o: AnyRef): String = "test"

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    "Test"
}

private[akka] class Constructor1Serializer(@nowarn("msg=never used") system: ExtendedActorSystem)
    extends ConstructorSerializer {
  override def identifier = 100001
}

private[akka] class Constructor2Serializer(@nowarn("msg=never used") system: ActorSystem)
    extends ConstructorSerializer {
  override def identifier = 100002
}

private[akka] class Constructor3Serializer(@nowarn("msg=never used") system: ClassicActorSystemProvider)
    extends ConstructorSerializer {
  override def identifier = 100003
}

private[akka] class Constructor4Serializer extends ConstructorSerializer {
  override def identifier = 100004
}

private[akka] class Constructor5Serializer(
    @nowarn("msg=never used") system: ExtendedActorSystem,
    @nowarn("msg=never used") binding: String)
    extends ConstructorSerializer {
  override def identifier = 100005
}

private[akka] class Constructor6Serializer(
    @nowarn("msg=never used") system: ActorSystem,
    @nowarn("msg=never used") binding: String)
    extends ConstructorSerializer {
  override def identifier = 100006
}

private[akka] class Constructor7Serializer(
    @nowarn("msg=never used") system: ClassicActorSystemProvider,
    @nowarn("msg=never used") binding: String)
    extends ConstructorSerializer {
  override def identifier = 100007
}
