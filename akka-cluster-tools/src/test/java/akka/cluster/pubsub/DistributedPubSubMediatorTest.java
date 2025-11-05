/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.pubsub;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.testkit.AkkaJUnitActorSystemResource;
import com.typesafe.config.ConfigFactory;
import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class DistributedPubSubMediatorTest extends JUnitSuite {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource(
          "DistributedPubSubMediatorTest",
          ConfigFactory.parseString(
              "akka.actor.provider = \"cluster\"\n" + "akka.remote.artery.canonical.port=0"));

  private final ActorSystem system = actorSystemResource.getSystem();

  @Test
  public void demonstratePublishUsage() {
    // #start-subscribers
    system.actorOf(Props.create(Subscriber.class), "subscriber1");
    // another node
    system.actorOf(Props.create(Subscriber.class), "subscriber2");
    system.actorOf(Props.create(Subscriber.class), "subscriber3");
    // #start-subscribers

    // #publish-message
    // somewhere else
    ActorRef publisher = system.actorOf(Props.create(Publisher.class), "publisher");
    // after a while the subscriptions are replicated
    publisher.tell("hello", null);
    // #publish-message
  }

  public void demonstrateSendUsage() {
    // #start-send-destinations
    system.actorOf(Props.create(Destination.class), "destination");
    // another node
    system.actorOf(Props.create(Destination.class), "destination");
    // #start-send-destinations

    // #send-message
    // somewhere else
    ActorRef sender = system.actorOf(Props.create(Publisher.class), "sender");
    // after a while the destinations are replicated
    sender.tell("hello", null);
    // #send-message
  }

  public // #subscriber
  static class Subscriber extends AbstractActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public Subscriber() {
      ActorRef mediator = DistributedPubSub.get(getContext().system()).mediator();
      // subscribe to the topic named "content"
      mediator.tell(new DistributedPubSubMediator.Subscribe("content", getSelf()), getSelf());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(String.class, msg -> log.info("Got: {}", msg))
          .match(DistributedPubSubMediator.SubscribeAck.class, msg -> log.info("subscribed"))
          .build();
    }
  }

  // #subscriber

  public // #publisher
  static class Publisher extends AbstractActor {

    // activate the extension
    ActorRef mediator = DistributedPubSub.get(getContext().system()).mediator();

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              String.class,
              in -> {
                String out = in.toUpperCase();
                mediator.tell(new DistributedPubSubMediator.Publish("content", out), getSelf());
              })
          .build();
    }
  }
  // #publisher

  public // #send-destination
  static class Destination extends AbstractActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public Destination() {
      ActorRef mediator = DistributedPubSub.get(getContext().system()).mediator();
      // register to the path
      mediator.tell(new DistributedPubSubMediator.Put(getSelf()), getSelf());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder().match(String.class, msg -> log.info("Got: {}", msg)).build();
    }
  }

  // #send-destination

  public // #sender
  static class Sender extends AbstractActor {

    // activate the extension
    ActorRef mediator = DistributedPubSub.get(getContext().system()).mediator();

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              String.class,
              in -> {
                String out = in.toUpperCase();
                boolean localAffinity = true;
                mediator.tell(
                    new DistributedPubSubMediator.Send("/user/destination", out, localAffinity),
                    getSelf());
              })
          .build();
    }
  }
  // #sender
}
