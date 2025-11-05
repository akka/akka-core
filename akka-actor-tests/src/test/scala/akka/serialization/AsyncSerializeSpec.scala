/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.serialization

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

import scala.concurrent.Future

import com.typesafe.config.ConfigFactory

import akka.actor.ExtendedActorSystem
import akka.testkit.{ AkkaSpec, EventFilter }

object AsyncSerializeSpec {

  case class Message1(str: String)
  case class Message2(str: String)
  case class Message3(str: String)
  case class Message4(str: String)

  val config = ConfigFactory.parseString(s"""
       akka {
        actor {
          serializers {
            async = "akka.serialization.AsyncSerializeSpec$$TestAsyncSerializer"
            asyncCS = "akka.serialization.AsyncSerializeSpec$$TestAsyncSerializerCS"
          }

          serialization-bindings = {
            "akka.serialization.AsyncSerializeSpec$$Message1" = async
            "akka.serialization.AsyncSerializeSpec$$Message2" = async
            "akka.serialization.AsyncSerializeSpec$$Message3" = asyncCS
            "akka.serialization.AsyncSerializeSpec$$Message4" = asyncCS
          }
        }
       }
    """)

  class TestAsyncSerializer(system: ExtendedActorSystem) extends AsyncSerializerWithStringManifest(system) {

    override def toBinaryAsync(o: AnyRef): Future[Array[Byte]] = {
      o match {
        case Message1(msg) => Future.successful(msg.getBytes)
        case Message2(msg) => Future.successful(msg.getBytes)
        case _             => throw new IllegalArgumentException(s"Unknown type $o")
      }
    }

    override def fromBinaryAsync(bytes: Array[Byte], manifest: String): Future[AnyRef] = {
      manifest match {
        case "1" => Future.successful(Message1(new String(bytes)))
        case "2" => Future.successful(Message2(new String(bytes)))
        case _   => throw new IllegalArgumentException(s"Unknown manifest $manifest")
      }
    }

    override def identifier: Int = 9000

    override def manifest(o: AnyRef): String = o match {
      case _: Message1 => "1"
      case _: Message2 => "2"
      case _           => throw new IllegalArgumentException(s"Unknown type $o")
    }
  }

  class TestAsyncSerializerCS(system: ExtendedActorSystem) extends AsyncSerializerWithStringManifestCS(system) {

    override def toBinaryAsyncCS(o: AnyRef): CompletionStage[Array[Byte]] = {
      o match {
        case Message3(msg) => CompletableFuture.completedFuture(msg.getBytes)
        case Message4(msg) => CompletableFuture.completedFuture(msg.getBytes)
        case _             => throw new IllegalArgumentException(s"Unknown type $o")
      }
    }

    override def fromBinaryAsyncCS(bytes: Array[Byte], manifest: String): CompletionStage[AnyRef] = {
      manifest match {
        case "1" => CompletableFuture.completedFuture(Message3(new String(bytes)))
        case "2" => CompletableFuture.completedFuture(Message4(new String(bytes)))
        case _   => throw new IllegalArgumentException(s"Unknown manifest $manifest")
      }
    }

    override def identifier: Int = 9001

    override def manifest(o: AnyRef): String = o match {
      case _: Message3 => "1"
      case _: Message4 => "2"
      case _           => throw new IllegalArgumentException(s"Unknown type $o")
    }
  }

}

class AsyncSerializeSpec extends AkkaSpec(AsyncSerializeSpec.config) {
  import AsyncSerializeSpec._

  val ser = SerializationExtension(system)

  "SerializationExtension" must {
    "find async serializers" in {
      val o1 = Message1("to async")
      val serializer: Serializer = ser.findSerializerFor(o1)
      serializer.getClass shouldEqual classOf[TestAsyncSerializer]
      val asyncSerializer = serializer.asInstanceOf[SerializerWithStringManifest with AsyncSerializer]
      val binary = asyncSerializer.toBinaryAsync(o1).futureValue
      val back = asyncSerializer.fromBinaryAsync(binary, asyncSerializer.manifest(o1)).futureValue
      o1 shouldEqual back
    }

    "logs warning if sync methods called" in {
      EventFilter.warning(start = "Async serializer called synchronously", occurrences = 1).intercept {
        ser.serialize(Message1("to async"))
      }
    }

    "have Java API for async serializers that delegate to the CS methods" in {
      val msg3 = Message3("to async")

      val serializer = ser.findSerializerFor(msg3).asInstanceOf[TestAsyncSerializerCS]

      EventFilter.warning(start = "Async serializer called synchronously", occurrences = 2).intercept {
        val binary = ser.serialize(msg3).get
        val back = ser.deserialize(binary, serializer.identifier, serializer.manifest(msg3)).get
        back shouldEqual msg3
      }
    }
  }
}
