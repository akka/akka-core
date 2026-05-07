/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed.pubsub

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

import scala.annotation.nowarn
import scala.concurrent.duration.DurationInt

@nowarn("msg=never used")
object PubSubExample {

  case class Message(text: String)

  def example: Behavior[Any] = {
    // #start-topic
    import akka.actor.typed.pubsub.Topic

    Behaviors.setup { context =>
      val topic = context.spawn(Topic[Message]("my-topic"), "MyTopic")
      // #start-topic

      Behaviors.empty
    }
  }

  def registry(): Behavior[Any] = {
    // #lookup-topic
    import akka.actor.typed.pubsub.Topic
    import akka.actor.typed.pubsub.PubSub

    Behaviors.setup { context =>
      val pubSub = PubSub(context.system)

      val topic: ActorRef[Topic.Command[Message]] = pubSub.topic[Message]("my-topic")
      // #lookup-topic

      // #ttl
      val topicWithTtl = pubSub.topic[Message]("my-topic", 3.minutes)
      // #ttl

      val subscriberActor: ActorRef[Message] = ???
      // #subscribe
      topic ! Topic.Subscribe(subscriberActor)

      topic ! Topic.Unsubscribe(subscriberActor)
      // #subscribe

      // #publish
      topic ! Topic.Publish(Message("Hello Subscribers!"))
      // #publish

      Behaviors.empty
    }

  }

}
