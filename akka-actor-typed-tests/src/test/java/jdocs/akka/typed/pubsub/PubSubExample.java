/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed.pubsub;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

// #start-topic
import akka.actor.typed.pubsub.Topic;

// #start-topic

public class PubSubExample {

  static class Message {
    public final String text;

    public Message(String text) {
      this.text = text;
    }
  }

  private Behavior<?> behavior =
      // #start-topic
      Behaviors.setup(
          context -> {
            ActorRef<Topic.Command<Message>> topic =
                context.spawn(Topic.create(Message.class, "my-topic"), "MyTopic");
            // #start-topic

            ActorRef<Message> subscriberActor = null;
            // #subscribe
            topic.tell(Topic.subscribe(subscriberActor));

            topic.tell(Topic.unsubscribe(subscriberActor));
            // #subscribe

            // #publish
            topic.tell(Topic.publish(new Message("Hello Subscribers!")));
            // #publish

            return Behaviors.empty();
          });
}
