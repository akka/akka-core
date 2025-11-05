/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.typed.pubsub

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object PubSubExample {

  case class Message(text: String)

  def example: Behavior[Any] = {
    // #start-topic
    import akka.actor.typed.pubsub.Topic

    Behaviors.setup { context =>
      val topic = context.spawn(Topic[Message]("my-topic"), "MyTopic")
      // #start-topic

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
