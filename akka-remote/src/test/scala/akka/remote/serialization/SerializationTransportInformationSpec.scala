/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory

import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Identify
import akka.actor.RootActorPath
import akka.remote.RARP
import akka.serialization.Serialization
import akka.serialization.SerializerWithStringManifest
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.JavaSerializable
import akka.testkit.TestActors

object SerializationTransportInformationSpec {

  final case class TestMessage(from: ActorRef, to: ActorRef)
  final case class JavaSerTestMessage(from: ActorRef, to: ActorRef) extends JavaSerializable

  class TestSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
    def identifier: Int = 666
    def manifest(o: AnyRef): String = o match {
      case _: TestMessage => "A"
      case _              => throw new NotSerializableException()
    }
    def toBinary(o: AnyRef): Array[Byte] = o match {
      case TestMessage(from, to) =>
        verifyTransportInfo()
        val fromStr = Serialization.serializedActorPath(from)
        val toStr = Serialization.serializedActorPath(to)
        s"$fromStr,$toStr".getBytes(StandardCharsets.UTF_8)
      case _ => throw new NotSerializableException()
    }
    def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
      verifyTransportInfo()
      manifest match {
        case "A" =>
          val parts = new String(bytes, StandardCharsets.UTF_8).split(',')
          val fromStr = parts(0)
          val toStr = parts(1)
          val from = system.provider.resolveActorRef(fromStr)
          val to = system.provider.resolveActorRef(toStr)
          TestMessage(from, to)
        case _ => throw new NotSerializableException()
      }
    }

    private def verifyTransportInfo(): Unit = {
      Serialization.currentTransportInformation.value match {
        case null =>
          throw new IllegalStateException("currentTransportInformation was not set")
        case t =>
          if (t.system ne system)
            throw new IllegalStateException(s"wrong system in currentTransportInformation, ${t.system} != $system")
          if (t.address != system.provider.getDefaultAddress)
            throw new IllegalStateException(
              s"wrong address in currentTransportInformation, ${t.address} != ${system.provider.getDefaultAddress}")
      }
    }
  }
}

class SerializationTransportInformationSpec
    extends AkkaSpec(
      ConfigFactory.parseString("""
    akka {
      loglevel = info
      actor {
        provider = remote
        warn-about-java-serializer-usage = off
        serializers {
          test = "akka.remote.serialization.SerializationTransportInformationSpec$TestSerializer"
        }
        serialization-bindings {
          "akka.remote.serialization.SerializationTransportInformationSpec$TestMessage" = test
        }
      }
    }
    akka.remote.artery.canonical.port = 0
  """))
    with ImplicitSender {

  import SerializationTransportInformationSpec._

  val port = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.port.get
  val sysName = system.name
  val protocol = "akka"

  val system2 = ActorSystem(system.name, system.settings.config)
  val system2Address = RARP(system2).provider.getDefaultAddress

  "Serialization of ActorRef in remote message" must {

    "resolve address" in {
      system2.actorOf(TestActors.echoActorProps, "echo")

      val echoSel = system.actorSelection(RootActorPath(system2Address) / "user" / "echo")
      echoSel ! Identify(1)
      val echo = expectMsgType[ActorIdentity].ref.get

      echo ! TestMessage(testActor, echo)
      expectMsg(TestMessage(testActor, echo))

      echo ! JavaSerTestMessage(testActor, echo)
      expectMsg(JavaSerTestMessage(testActor, echo))

      echo ! testActor
      expectMsg(testActor)

      echo ! echo
      expectMsg(echo)

    }
  }

  override def afterTermination(): Unit = {
    shutdown(system2)
  }
}
