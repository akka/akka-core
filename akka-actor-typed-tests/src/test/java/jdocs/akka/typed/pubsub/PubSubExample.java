/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed.pubsub;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;

// #lookup-topic
import akka.actor.typed.pubsub.PubSub;
// #start-topic
import akka.actor.typed.pubsub.Topic;

import java.time.Duration;

// #start-topic
// #lookup-topic

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

            return Behaviors.empty();
          });

  private Behavior<?> registry =
      // #lookup-topic
      Behaviors.setup(
          context -> {
            PubSub pubSub = PubSub.get(context.getSystem());

            ActorRef<Topic.Command<Message>> topic =
                pubSub.topic(Message.class, "my-topic");
            // #lookup-topic

            // #ttl
            ActorRef<Topic.Command<Message>> topicWithTtl =
                pubSub.topic(Message.class, "my-ttl-topic", Duration.ofMinutes(3));
            // #ttl


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
