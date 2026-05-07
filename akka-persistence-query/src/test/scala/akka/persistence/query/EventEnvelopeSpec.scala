/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant
import java.util.Optional
import scala.annotation.nowarn

@nowarn("msg=deprecated")
class EventEnvelopeSpec extends AnyWordSpecLike with Matchers {
  "EventEnvelope" must {
    "support single event metadata" in {
      val env = new EventEnvelope(
        offset = NoOffset,
        persistenceId = "pid",
        sequenceNr = 1L,
        event = "evt",
        System.currentTimeMillis(),
        _eventMetadata = Some("meta"))

      env.metadata[String] shouldBe Some("meta")
      env.metadata[java.time.Instant] shouldBe None
      env.metadata[AnyRef] shouldBe None
      env.eventMetadata shouldBe Some("meta") // deprecated
      env.removeMetadata[String].metadata[String] shouldBe None
      env.removeMetadata[AnyRef] shouldBe theSameInstanceAs(env)

      // Java API
      env.getMetadata(classOf[String]) shouldBe Optional.of("meta")
      env.getMetadata(classOf[java.time.Instant]) shouldBe Optional.empty
      env.getMetadata(classOf[AnyRef]) shouldBe Optional.empty
      env.getEventMetaData() shouldBe Optional.of("meta") // deprecated
      env.removeMetadata(classOf[String]).getMetadata(classOf[String]) shouldBe Optional.empty()
      env.removeMetadata(classOf[AnyRef]) shouldBe theSameInstanceAs(env)
    }

    "support composite event metadata" in {
      val env =
        EventEnvelope(offset = NoOffset, persistenceId = "pid", sequenceNr = 1L, "evt", System.currentTimeMillis())

      env.metadata[String] shouldBe None
      env.eventMetadata shouldBe None // deprecated

      val env2 = env.withMetadata("meta")
      env2.metadata[String] shouldBe Some("meta")
      env2.metadata[java.time.Instant] shouldBe None
      env2.eventMetadata shouldBe Some("meta") // deprecated

      val instant = Instant.now()
      val env3 = env2.withMetadata(instant)
      env3.metadata[String] shouldBe Some("meta")
      env3.metadata[java.time.Instant] shouldBe Some(instant)
      env3.metadata[AnyRef] shouldBe None
      // For backwards compatibility this will use the metadata that was added last (ReplicatedEventMetaData)
      env3.eventMetadata shouldBe Some(instant) // deprecated

      // in case same class is added again the last will be used
      val instant2 = instant.plusSeconds(1)
      val env4 = env3.withMetadata(instant2)
      env4.metadata[String] shouldBe Some("meta")
      env4.metadata[java.time.Instant] shouldBe Some(instant2)
      env4.eventMetadata shouldBe Some(instant2) // deprecated

      env4.removeMetadata[String].metadata[String] shouldBe None
      env4.removeMetadata[AnyRef] shouldBe theSameInstanceAs(env4)

      // Java API
      env.getMetadata(classOf[String]) shouldBe Optional.empty
      env.getEventMetaData() shouldBe Optional.empty // deprecated

      env2.getMetadata(classOf[String]) shouldBe Optional.of("meta")
      env2.getMetadata(classOf[java.time.Instant]) shouldBe Optional.empty
      env2.getEventMetaData() shouldBe Optional.of("meta") // deprecated

      env3.getMetadata(classOf[String]) shouldBe Optional.of("meta")
      env3.getMetadata(classOf[java.time.Instant]) shouldBe Optional.of(instant)
      env3.getMetadata(classOf[AnyRef]) shouldBe Optional.empty
      env3.getEventMetaData() shouldBe Optional.of(instant) // deprecated

      env4.getMetadata(classOf[String]) shouldBe Optional.of("meta")
      env4.getMetadata(classOf[java.time.Instant]) shouldBe Optional.of(instant2)
      env4.getEventMetaData() shouldBe Optional.of(instant2) // deprecated

      env4.removeMetadata(classOf[String]).getMetadata(classOf[String]) shouldBe Optional.empty()
      env4.removeMetadata(classOf[AnyRef]) shouldBe theSameInstanceAs(env4)
    }
  }

}
