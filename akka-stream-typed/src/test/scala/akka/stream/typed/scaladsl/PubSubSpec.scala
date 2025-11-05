/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.typed.scaladsl

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.internal.pubsub.TopicImpl
import akka.actor.typed.pubsub.Topic
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink

class PubSubSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "PubSub.source" should {

    "emit messages from the topic" in {
      val topic = testKit.spawn(Topic[String]("my-topic-1"))

      val source = PubSub.source(topic, 100, OverflowStrategy.fail)
      val sourceProbe = source.runWith(TestSink())
      sourceProbe.ensureSubscription()

      // wait until subscription has been seen
      val probe = testKit.createTestProbe[TopicImpl.TopicStats]()
      probe.awaitAssert {
        topic ! TopicImpl.GetTopicStats(probe.ref)
        probe.expectMessageType[TopicImpl.TopicStats].localSubscriberCount should ===(1)
      }

      topic ! Topic.Publish("published")
      sourceProbe.requestNext("published")
      sourceProbe.cancel()
    }

  }

  "PubSub.sink" should {
    "publish messages" in {
      val topic = testKit.spawn(Topic[String]("my-topic-2"))

      val subscriberProbe = testKit.createTestProbe[String]()
      topic ! Topic.Subscribe(subscriberProbe.ref)

      // wait until subscription has been seen
      val probe = testKit.createTestProbe[TopicImpl.TopicStats]()
      probe.awaitAssert {
        topic ! TopicImpl.GetTopicStats(probe.ref)
        probe.expectMessageType[TopicImpl.TopicStats].localSubscriberCount should ===(1)
      }

      Source.single("published").runWith(PubSub.sink(topic))

      subscriberProbe.expectMessage("published")
    }
  }

}
