/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.actor.typed.pubsub

import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit

class LocalPubSubSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "A pub-sub topic running locally" must {

    "publish to all local subscriber actors of a topic" in {
      val fruitTopic = testKit.spawn(Topic[String]("fruit1"))

      try {
        val probe1 = testKit.createTestProbe[String]()
        val probe2 = testKit.createTestProbe[String]()
        val probe3 = testKit.createTestProbe[String]()

        fruitTopic ! Topic.Subscribe(probe1.ref)
        fruitTopic ! Topic.Subscribe(probe2.ref)
        fruitTopic ! Topic.Subscribe(probe3.ref)

        // the subscriber registration of all 3 completed
        val statsProbe = testKit.createTestProbe[Topic.TopicStats]()
        statsProbe.awaitAssert {
          fruitTopic ! Topic.GetTopicStats(statsProbe.ref)
          val stats = statsProbe.receiveMessage()
          stats.localSubscriberCount should ===(3)
          stats.topicInstanceCount should ===(1)
        }

        fruitTopic ! Topic.Publish("banana")
        probe1.expectMessage("banana")
        probe2.expectMessage("banana")
        probe3.expectMessage("banana")

      } finally {
        testKit.stop(fruitTopic)
      }
    }

    "publish to all subscriber actors across several instances of the same topic" in {
      // using different topic name than in the previous test to avoid interference
      val fruitTopic1 = testKit.spawn(Topic[String]("fruit2"))
      val fruitTopic2 = testKit.spawn(Topic[String]("fruit2"))

      try {
        val probe1 = testKit.createTestProbe[String]()
        val probe2 = testKit.createTestProbe[String]()
        val probe3 = testKit.createTestProbe[String]()

        fruitTopic2 ! Topic.Subscribe(probe1.ref)
        fruitTopic2 ! Topic.Subscribe(probe2.ref)
        fruitTopic2 ! Topic.Subscribe(probe3.ref)

        // the subscriber registration of all 3 completed
        val statsProbe = testKit.createTestProbe[Any]()
        statsProbe.awaitAssert {
          fruitTopic2 ! Topic.GetTopicStats(statsProbe.ref)
          statsProbe.expectMessageType[Topic.TopicStats].localSubscriberCount should ===(3)
        }
        // and the other topic knows about it
        statsProbe.awaitAssert {
          fruitTopic1 ! Topic.GetTopicStats(statsProbe.ref)
          statsProbe.expectMessageType[Topic.TopicStats].topicInstanceCount should ===(1)
        }

        fruitTopic1 ! Topic.Publish("apple")
        probe1.expectMessage("apple")
        probe2.expectMessage("apple")
        probe3.expectMessage("apple")

      } finally {
        testKit.stop(fruitTopic1)
        testKit.stop(fruitTopic2)
      }
    }

    "doesn't publish across topics unsubscribe" in {
      val fruitTopic =
        testKit.spawn(Topic[String]("fruit3"))

      val veggieTopic =
        testKit.spawn(Topic[String]("veggies"))

      try {
        val probe1 = testKit.createTestProbe[String]()

        fruitTopic ! Topic.Subscribe(probe1.ref)
        // the subscriber registration of all 3 completed
        val statsProbe = testKit.createTestProbe[Any]()
        statsProbe.awaitAssert {
          fruitTopic ! Topic.GetTopicStats(statsProbe.ref)
          statsProbe.expectMessageType[Topic.TopicStats].localSubscriberCount should ===(1)
        }

        // the other topic should not know about it
        statsProbe.awaitAssert {
          veggieTopic ! Topic.GetTopicStats(statsProbe.ref)
          statsProbe.expectMessageType[Topic.TopicStats].topicInstanceCount should ===(0)
        }

        veggieTopic ! Topic.Publish("carrot")
        probe1.expectNoMessage(200.millis)

      } finally {
        testKit.stop(fruitTopic)
      }
    }

    "doesn't publish after unsubscribe" in {
      val fruitTopic = testKit.spawn(Topic[String]("fruit4"))

      try {
        val probe1 = testKit.createTestProbe[String]()

        fruitTopic ! Topic.Subscribe(probe1.ref)
        // the subscriber registration of completed
        val statsProbe = testKit.createTestProbe[Any]()
        statsProbe.awaitAssert {
          fruitTopic ! Topic.GetTopicStats(statsProbe.ref)
          statsProbe.expectMessageType[Topic.TopicStats].localSubscriberCount should ===(1)
        }

        fruitTopic ! Topic.Unsubscribe(probe1.ref)
        statsProbe.awaitAssert {
          fruitTopic ! Topic.GetTopicStats(statsProbe.ref)
          statsProbe.expectMessageType[Topic.TopicStats].localSubscriberCount should ===(0)
        }

        fruitTopic ! Topic.Publish("orange")
        probe1.expectNoMessage(200.millis)

      } finally {
        testKit.stop(fruitTopic)
      }
    }

  }
}
