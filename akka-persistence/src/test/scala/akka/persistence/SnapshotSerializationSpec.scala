/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.io._

import akka.actor.{ ActorRef, Props }
import akka.serialization.Serializer
import akka.testkit.ImplicitSender

object SnapshotSerializationSpec {
  trait SerializationMarker

  // The FQN of the MySnapshot class needs to be long so that the total snapshot header length
  // is bigger than 255 bytes (this happens to be 269)
  object XXXXXXXXXXXXXXXXXXXX {
    class MySnapshot(val id: String) extends SerializationMarker {
      override def equals(obj: scala.Any) = obj match {
        case s: MySnapshot => s.id.equals(id)
        case _             => false
      }
    }
  }

  import XXXXXXXXXXXXXXXXXXXX._

  class MySerializer extends Serializer {
    def includeManifest: Boolean = true
    def identifier = 5177

    def toBinary(obj: AnyRef): Array[Byte] = {
      val bStream = new ByteArrayOutputStream()
      val pStream = new PrintStream(bStream)
      val msg: String = obj match {
        case s: MySnapshot => s.id
        case _             => "unknown"
      }
      pStream.println(msg)
      bStream.toByteArray
    }

    def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
      val bStream = new ByteArrayInputStream(bytes)
      val reader = new BufferedReader(new InputStreamReader(bStream))
      new MySnapshot(reader.readLine())
    }
  }

  class TestPersistentActor(name: String, probe: ActorRef) extends NamedPersistentActor(name) {

    override def receiveRecover: Receive = {
      case SnapshotOffer(md, s) => probe ! ((md, s))
      case RecoveryCompleted    => // ignore
      case other                => probe ! other
    }

    override def receiveCommand = {
      case s: String               => saveSnapshot(new MySnapshot(s))
      case SaveSnapshotSuccess(md) => probe ! md.sequenceNr
      case other                   => probe ! other
    }
  }

}

class SnapshotSerializationSpec
    extends PersistenceSpec(
      PersistenceSpec.config(
        "inmem",
        "SnapshotSerializationSpec",
        serialization = "off",
        extraConfig = Some("""
    akka.actor {
      serializers {
        my-snapshot = "akka.persistence.SnapshotSerializationSpec$MySerializer"
      }
      serialization-bindings {
        "akka.persistence.SnapshotSerializationSpec$SerializationMarker" = my-snapshot
      }
    }
  """)))
    with ImplicitSender {

  import SnapshotSerializationSpec._
  import SnapshotSerializationSpec.XXXXXXXXXXXXXXXXXXXX._

  "A PersistentActor with custom Serializer" must {
    "be able to handle serialization header of more than 255 bytes" in {
      val sPersistentActor = system.actorOf(Props(classOf[TestPersistentActor], name, testActor))
      val persistenceId = name

      sPersistentActor ! "blahonga"
      expectMsg(0)
      system.actorOf(Props(classOf[TestPersistentActor], name, testActor))
      expectMsgPF() {
        case (SnapshotMetadata(`persistenceId`, 0, timestamp), state) =>
          state should ===(new MySnapshot("blahonga"))
          timestamp should be > (0L)
      }
    }
  }
}
