/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.pubsub

import akka.actor.Dropped
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.internal.pubsub.TopicImpl
import akka.testkit.TimingTest
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class LocalTopicSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

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

        // since we defer publish until there is a listing from the receptionist,
        // we can count on being able to publish immediately
        val fruitTopic3 = testKit.spawn(Topic[String]("fruit2"))
        fruitTopic3 ! Topic.Publish("kiwi")

        probe1.expectMessage("kiwi")
        probe2.expectMessage("kiwi")
        probe3.expectMessage("kiwi")

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

    "same topic name different message types are different topics" in {
      val fruitTopic = testKit.spawn(Topic[String]("fruit5"))
      val numberTopic = testKit.spawn(Topic[Int]("fruit5"))

      try {
        val probe1 = testKit.createTestProbe[String]()

        fruitTopic ! Topic.Subscribe(probe1.ref)
        // the subscriber registration of completed
        val statsProbe = testKit.createTestProbe[Any]()
        statsProbe.awaitAssert {
          fruitTopic ! Topic.GetTopicStats(statsProbe.ref)
          statsProbe.expectMessageType[Topic.TopicStats].localSubscriberCount should ===(1)
        }

        val probe2 = testKit.createTestProbe[Int]()
        numberTopic ! Topic.Subscribe(probe2.ref)
        statsProbe.awaitAssert {
          numberTopic ! Topic.GetTopicStats(statsProbe.ref)
          statsProbe.expectMessageType[Topic.TopicStats].localSubscriberCount should ===(1)
        }

        // both subscribed
        fruitTopic ! Topic.Publish("dragonfruit")
        probe1.expectMessage("dragonfruit")
        probe2.expectNoMessage()

      } finally {
        testKit.stop(fruitTopic)
        testKit.stop(numberTopic)
      }
    }

    "shut down topic after ttl" taggedAs (TimingTest) in {
      def createTopic() =
        testKit.spawn(TopicImpl[String]("fruit6", Some(300.millis)))
      val topic = createTopic()
      val probe = testKit.createTestProbe()
      probe.expectTerminated(topic)
    }

    "keep topic with ttl alive with publishing" taggedAs (TimingTest) in {
      def createTopic() =
        testKit.spawn(TopicImpl[String]("fruit7", Some(500.millis)))
      val deadLetters = testKit.createDeadLetterProbe()
      val topic = createTopic()
      // no subscribers, only local publish
      (0 to 4).foreach { _ =>
        Thread.sleep(200)
        topic ! Topic.Publish("durian")
      }
      // Verify it isn't dead yet through no dead letters from the topic being dead (but it does drop because no subscribers)
      deadLetters
        .receiveMessages(4)
        .forall(_.message match {
          case Dropped(_, reason, _, _) => reason == "No topic subscribers known"
          case _                        => false
        }) should ===(true)

      // then it ttl-outs
      Thread.sleep(600)
      val probe = testKit.createTestProbe()
      probe.expectTerminated(topic)
    }

    "keep topic with ttl alive with subscriber" taggedAs (TimingTest) in {
      def createTopic() =
        testKit.spawn(TopicImpl[String]("fruit8", Some(300.millis)))
      val topic = createTopic()
      val subscriber = testKit.createTestProbe[String]()
      topic ! Topic.Subscribe(subscriber.ref)
      Thread.sleep(500)
      topic ! Topic.Publish("cherimoya")
      subscriber.expectMessage("cherimoya")
      topic ! Topic.Unsubscribe(subscriber.ref)
      Thread.sleep(500)
      // now it should have ttl-outed
      val probe = testKit.createTestProbe()
      probe.expectTerminated(topic)
    }

  }
}
