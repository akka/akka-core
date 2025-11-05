/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.jackson

import com.fasterxml.jackson.databind.annotation.{ JsonDeserialize, JsonSerialize }
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.{ LogCapturing, ScalaTestWithActorTestKit, SerializationTestKit }
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.crdt.{ Counter, LwwTime, ORSet }
import akka.persistence.typed.jackson.ReplicatedEventSourcingJacksonSpec.{ WithCounter, WithLwwTime, WithOrSet }
import akka.serialization.jackson.{ AkkaSerializationDeserializer, AkkaSerializationSerializer, JsonSerializable }

object ReplicatedEventSourcingJacksonSpec {
  final case class WithLwwTime(lwwTime: LwwTime) extends JsonSerializable
  final case class WithOrSet(
      @JsonDeserialize(`using` = classOf[AkkaSerializationDeserializer])
      @JsonSerialize(`using` = classOf[AkkaSerializationSerializer])
      orSet: ORSet[String])
      extends JsonSerializable
  final case class WithCounter(
      @JsonDeserialize(`using` = classOf[AkkaSerializationDeserializer])
      @JsonSerialize(`using` = classOf[AkkaSerializationSerializer])
      counter: Counter)
      extends JsonSerializable

}

class ReplicatedEventSourcingJacksonSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  private val serializationTestkit = new SerializationTestKit(system)

  "RES jackson" should {
    "serialize LwwTime" in {
      val obj = WithLwwTime(LwwTime(5, ReplicaId("A")))
      serializationTestkit.verifySerialization(obj)
    }
    "serialize ORSet" in {
      val emptyOrSet = WithOrSet(ORSet.empty[String](ReplicaId("A")))
      serializationTestkit.verifySerialization(emptyOrSet)
    }
    "serialize Counter" in {
      val counter = WithCounter(Counter.empty)
      serializationTestkit.verifySerialization(counter)
    }
  }
}
