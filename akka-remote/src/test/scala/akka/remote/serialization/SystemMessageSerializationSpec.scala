/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import com.typesafe.config.ConfigFactory

import akka.actor.{ ActorInitializationException, ExtendedActorSystem, InternalActorRef }
import akka.dispatch.sysmsg._
import akka.serialization.SerializationExtension
import akka.testkit.{ AkkaSpec, TestProbe }
import akka.testkit.JavaSerializable

object SystemMessageSerializationSpec {
  val serializationTestOverrides =
    """
    """

  val testConfig = ConfigFactory.parseString(serializationTestOverrides).withFallback(AkkaSpec.testConf)

  class TestException(msg: String) extends RuntimeException(msg) with JavaSerializable {
    override def equals(other: Any): Boolean = other match {
      case e: TestException => e.getMessage == getMessage
      case _                => false
    }
  }
}

class SystemMessageSerializationSpec extends AkkaSpec(PrimitivesSerializationSpec.testConfig) {
  import SystemMessageSerializationSpec._

  val testRef = TestProbe().ref.asInstanceOf[InternalActorRef]
  val testRef2 = TestProbe().ref.asInstanceOf[InternalActorRef]

  "SystemMessageSerializer" must {
    Seq(
      "Create(None)" -> Create(None),
      "Recreate(ex)" -> Recreate(new TestException("test2")),
      "Suspend()" -> Suspend(),
      "Resume(ex)" -> Resume(new TestException("test3")),
      "Terminate()" -> Terminate(),
      "Supervise(ref, async)" -> Supervise(testRef, async = true),
      "Watch(ref, ref)" -> Watch(testRef, testRef2),
      "Unwatch(ref, ref)" -> Unwatch(testRef, testRef2),
      "Failed(ref, ex, uid)" -> Failed(testRef, new TestException("test4"), 42),
      "DeathWatchNotification(ref, confimed, addressTerminated)" ->
      DeathWatchNotification(testRef, existenceConfirmed = true, addressTerminated = true)).foreach {
      case (scenario, item) =>
        s"resolve serializer for [$scenario]" in {
          val serializer = SerializationExtension(system)
          serializer.serializerFor(item.getClass).getClass should ===(classOf[SystemMessageSerializer])
        }

        s"serialize and de-serialize [$scenario]" in {
          verifySerialization(item)
        }
    }

    def verifySerialization(msg: AnyRef): Unit = {
      val serializer = new SystemMessageSerializer(system.asInstanceOf[ExtendedActorSystem])
      serializer.fromBinary(serializer.toBinary(msg), None) should ===(msg)
    }

    // ActorInitializationException has no proper equality
    "serialize and de-serialize Create(Some(exc))" in {
      // TestException is JavaSerializable
      val aiex = ActorInitializationException(testRef, "test", new TestException("test5"))
      val createMsg = Create(Some(aiex))
      val serializer = new SystemMessageSerializer(system.asInstanceOf[ExtendedActorSystem])
      val deserialized = serializer.fromBinary(serializer.toBinary(createMsg), None).asInstanceOf[Create]

      deserialized.failure.get.getCause should ===(aiex.getCause)
      deserialized.failure.get.getMessage should ===(aiex.getMessage)
      deserialized.failure.get.getActor should ===(aiex.getActor)
    }

    "serialize and de-serialize Create(Some(exc)) when exc not serializable" in {
      val aiex = ActorInitializationException(testRef, "test", new IllegalStateException("test6"))
      val createMsg = Create(Some(aiex))
      val serializer = new SystemMessageSerializer(system.asInstanceOf[ExtendedActorSystem])
      val deserialized = serializer.fromBinary(serializer.toBinary(createMsg), None).asInstanceOf[Create]

      deserialized.failure.get.getCause.getClass should ===(classOf[ThrowableNotSerializableException])
      val cause = deserialized.failure.get.getCause.asInstanceOf[ThrowableNotSerializableException]
      cause.originalMessage should ===("test6")
      cause.originalClassName should ===(aiex.getCause.getClass.getName)
    }

    "serialize and de-serialize Failed" in {
      // TestException is JavaSerializable
      val testExc = new TestException("test7")
      val createMsg = Failed(testRef, testExc, 1234)
      val serializer = new SystemMessageSerializer(system.asInstanceOf[ExtendedActorSystem])
      val deserialized = serializer.fromBinary(serializer.toBinary(createMsg), None).asInstanceOf[Failed]

      deserialized.cause should ===(testExc)
      deserialized.child should ===(testRef)
      deserialized.uid should ===(1234)
    }

    "serialize and de-serialize Failed  when exc not serializable" in {
      val createMsg = Failed(testRef, new IllegalStateException("test8"), 1234)
      val serializer = new SystemMessageSerializer(system.asInstanceOf[ExtendedActorSystem])
      val deserialized = serializer.fromBinary(serializer.toBinary(createMsg), None).asInstanceOf[Failed]

      deserialized.cause.getClass should ===(classOf[ThrowableNotSerializableException])
      val cause = deserialized.cause.asInstanceOf[ThrowableNotSerializableException]
      cause.originalMessage should ===("test8")
      cause.originalClassName should ===(classOf[IllegalStateException].getName)
    }

  }

}
