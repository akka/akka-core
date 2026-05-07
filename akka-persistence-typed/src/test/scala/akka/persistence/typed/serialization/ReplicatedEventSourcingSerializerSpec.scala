/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.serialization

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.crdt.Counter
import akka.persistence.typed.crdt.ORSet
import akka.persistence.typed.internal.PublishedEventImpl
import akka.persistence.typed.internal.ReplicatedPublishedEventMetaData
import akka.persistence.typed.internal.VersionVector

class ReplicatedEventSourcingSerializerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "ReplicatedEventSourcingSerializer" should {
    "serializer" in {
      serializationTestKit.verifySerialization(ORSet.empty(ReplicaId("R1")))
      serializationTestKit.verifySerialization(ORSet.empty(ReplicaId("R1")).add("cat"))
      serializationTestKit.verifySerialization(ORSet.empty(ReplicaId("R1")).remove("cat"))
      serializationTestKit.verifySerialization(ORSet.empty(ReplicaId("R1")).addAll(Set("one", "two")))
      serializationTestKit.verifySerialization(ORSet.empty(ReplicaId("R1")).removeAll(Set("one", "two")))

      serializationTestKit.verifySerialization(Counter.empty)
      serializationTestKit.verifySerialization(Counter.Updated(BigInt(10)))
      serializationTestKit.verifySerialization(Counter.empty.applyOperation(Counter.Updated(BigInt(12))))

      serializationTestKit.verifySerialization(VersionVector.empty)
      serializationTestKit.verifySerialization(VersionVector.empty.updated("a", 10))

      serializationTestKit.verifySerialization(
        PublishedEventImpl(
          PersistenceId.ofUniqueId("cat"),
          10,
          "payload",
          1,
          Some(new ReplicatedPublishedEventMetaData(ReplicaId("R1"), VersionVector.empty, Some("meta"))),
          Some(system.deadLetters)),
        assertEquality = true)

      serializationTestKit.verifySerialization(
        PublishedEventImpl(PersistenceId.ofUniqueId("cat"), 10, "payload", 1, None, None),
        assertEquality = true)
    }
  }

}
