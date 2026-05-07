/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.io.NotSerializableException

import scala.annotation.nowarn

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.RootActorPath
import akka.remote.RARP
import akka.serialization.SerializerWithStringManifest
import akka.testkit.AkkaSpec
import akka.testkit.TestActors
import akka.testkit.TestKit

object TransientSerializationErrorSpec {
  object ManifestNotSerializable
  object ManifestIllegal
  object ToBinaryNotSerializable
  object ToBinaryIllegal
  object NotDeserializable
  object IllegalOnDeserialize

  class TestSerializer(@nowarn("msg=never used") system: ExtendedActorSystem) extends SerializerWithStringManifest {
    def identifier: Int = 666
    def manifest(o: AnyRef): String = o match {
      case ManifestNotSerializable => throw new NotSerializableException()
      case ManifestIllegal         => throw new IllegalArgumentException()
      case ToBinaryNotSerializable => "TBNS"
      case ToBinaryIllegal         => "TI"
      case NotDeserializable       => "ND"
      case IllegalOnDeserialize    => "IOD"
      case _                       => throw new NotSerializableException()
    }
    def toBinary(o: AnyRef): Array[Byte] = o match {
      case ToBinaryNotSerializable => throw new NotSerializableException()
      case ToBinaryIllegal         => throw new IllegalArgumentException()
      case _                       => Array.emptyByteArray
    }
    def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
      manifest match {
        case "ND"  => throw new NotSerializableException() // Not sure this applies here
        case "IOD" => throw new IllegalArgumentException()
        case _     => throw new NotSerializableException()
      }
    }
  }
}

class TransientSerializationErrorSpec
    extends AkkaSpec(
      ArterySpecSupport.defaultConfig.withFallback(
        ConfigFactory.parseString("""
    akka {
      loglevel = info
      actor {
        provider = remote
        warn-about-java-serializer-usage = off
        serializers {
          test = "akka.remote.artery.TransientSerializationErrorSpec$TestSerializer"
        }
        serialization-bindings {
          "akka.remote.artery.TransientSerializationErrorSpec$ManifestNotSerializable$" = test
          "akka.remote.artery.TransientSerializationErrorSpec$ManifestIllegal$" = test
          "akka.remote.artery.TransientSerializationErrorSpec$ToBinaryNotSerializable$" = test
          "akka.remote.artery.TransientSerializationErrorSpec$ToBinaryIllegal$" = test
          "akka.remote.artery.TransientSerializationErrorSpec$NotDeserializable$" = test
          "akka.remote.artery.TransientSerializationErrorSpec$IllegalOnDeserialize$" = test
        }
      }
    }
  """))) {

  import TransientSerializationErrorSpec._

  val port = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.port.get
  val sysName = system.name
  val protocol = "akka"

  val system2 = ActorSystem(system.name, system.settings.config)
  val system2Address = RARP(system2).provider.getDefaultAddress

  "The transport" must {

    "stay alive after a transient exception from the serializer" in {
      system2.actorOf(TestActors.echoActorProps, "echo")

      val selection = system.actorSelection(RootActorPath(system2Address) / "user" / "echo")

      selection.tell("ping", this.testActor)
      expectMsg("ping")

      // none of these should tear down the connection
      List[AnyRef](
        ManifestIllegal,
        ManifestNotSerializable,
        ToBinaryIllegal,
        ToBinaryNotSerializable,
        NotDeserializable,
        IllegalOnDeserialize).foreach(msg => selection.tell(msg, this.testActor))

      // make sure we still have a connection
      selection.tell("ping", this.testActor)
      expectMsg("ping")

    }
  }

  override def afterTermination(): Unit = {
    TestKit.shutdownActorSystem(system2)
  }
}
