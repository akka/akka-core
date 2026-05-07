/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.pubsub

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class PubSubSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  private val pubSub = PubSub(system)

  "The PubSub registry extension" should {
    "return the same topic for the same name" in {
      val topic1 = pubSub.topic[String]("fruit1")
      val topic2 = pubSub.topic[String]("fruit1")
      topic1 shouldBe theSameInstanceAs(topic2)
    }
    "throw if same name is used with different message types" in {
      val topic1 = pubSub.topic[String]("fruit2")
      intercept[IllegalArgumentException](pubSub.topic[Long]("fruit1"))
      // but it should not break any further use
      val topic2 = pubSub.topic[String]("fruit2")
      topic1 shouldBe theSameInstanceAs(topic2)
    }
    "return the same topic, but warn if subsequent request is with ttl" in {
      val topic1 = pubSub.topic[String]("fruit3")
      val topic2 = LoggingTestKit
        .warn("Asked for topic [fruit3] with TTL [1.000 min] but already existing topic has a different TTL [none]")
        .expect {
          pubSub.topic[String]("fruit3", 1.minute)
        }
      // we still get the same topic back
      topic1 shouldBe theSameInstanceAs(topic2)

      // same in the other direction
      val topic3 = pubSub.topic[String]("fruit4", 2.minutes)
      val topic4 = LoggingTestKit
        .warn(
          "Asked for topic [fruit4] with TTL [1.000 min] but already existing topic has a different TTL [2.000 min]")
        .expect {
          pubSub.topic[String]("fruit4", 1.minute)
        }
      // we still get the same topic back
      topic3 shouldBe theSameInstanceAs(topic4)
    }
    "allow inspection of all current topics" in {
      // Note depends on previous tests since same actor system
      pubSub.currentTopics should ===(Set("fruit1", "fruit2", "fruit3", "fruit4"))
    }
    "remove topics once they terminate" in {
      val topic1 = pubSub.topic[String]("fruit5", 100.millis)
      Thread.sleep(300)
      val probe = testKit.createTestProbe()
      probe.expectTerminated(topic1)
      pubSub.currentTopics should not contain ("fruit5")
      // new started topic back now
      val topic2 = pubSub.topic[String]("fruit5", 100.millis)
      topic2 should not be theSameInstanceAs(topic1)
    }
  }

}
