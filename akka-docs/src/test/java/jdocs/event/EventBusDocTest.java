/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.event;

import akka.event.japi.EventBus;

import java.time.Duration;

import jdocs.AbstractJavaTest;
import akka.testkit.javadsl.TestKit;
import org.junit.ClassRule;
import org.junit.Test;

import akka.actor.ActorSystem;
import akka.actor.ActorRef;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.util.Subclassification;

// #lookup-bus
import akka.event.japi.LookupEventBus;

// #lookup-bus

// #subchannel-bus
import akka.event.japi.SubchannelEventBus;

// #subchannel-bus

// #scanning-bus
import akka.event.japi.ScanningEventBus;

// #scanning-bus

// #actor-bus
import akka.event.japi.ManagedActorEventBus;

// #actor-bus

public class EventBusDocTest extends AbstractJavaTest {

  public static class Event {}

  public static class Subscriber {}

  public static class Classifier {}

  public static interface EventBusApi extends EventBus<Event, Subscriber, Classifier> {

    @Override
    // #event-bus-api
    /**
     * Attempts to register the subscriber to the specified Classifier
     *
     * @return true if successful and false if not (because it was already subscribed to that
     *     Classifier, or otherwise)
     */
    public boolean subscribe(Subscriber subscriber, Classifier to);

    // #event-bus-api

    @Override
    // #event-bus-api
    /**
     * Attempts to deregister the subscriber from the specified Classifier
     *
     * @return true if successful and false if not (because it wasn't subscribed to that Classifier,
     *     or otherwise)
     */
    public boolean unsubscribe(Subscriber subscriber, Classifier from);

    // #event-bus-api

    @Override
    // #event-bus-api
    /** Attempts to deregister the subscriber from all Classifiers it may be subscribed to */
    public void unsubscribe(Subscriber subscriber);

    // #event-bus-api

    @Override
    // #event-bus-api
    /** Publishes the specified Event to this bus */
    public void publish(Event event);

    // #event-bus-api

  }

  public
  // #lookup-bus
  static class MsgEnvelope {
    public final String topic;
    public final Object payload;

    public MsgEnvelope(String topic, Object payload) {
      this.topic = topic;
      this.payload = payload;
    }
  }

  // #lookup-bus
  public
  // #lookup-bus
  /**
   * Publishes the payload of the MsgEnvelope when the topic of the MsgEnvelope equals the String
   * specified when subscribing.
   */
  static class LookupBusImpl extends LookupEventBus<MsgEnvelope, ActorRef, String> {

    // is used for extracting the classifier from the incoming events
    @Override
    public String classify(MsgEnvelope event) {
      return event.topic;
    }

    // will be invoked for each event for all subscribers which registered themselves
    // for the event’s classifier
    @Override
    public void publish(MsgEnvelope event, ActorRef subscriber) {
      subscriber.tell(event.payload, ActorRef.noSender());
    }

    // must define a full order over the subscribers, expressed as expected from
    // `java.lang.Comparable.compare`
    @Override
    public int compareSubscribers(ActorRef a, ActorRef b) {
      return a.compareTo(b);
    }

    // determines the initial size of the index data structure
    // used internally (i.e. the expected number of different classifiers)
    @Override
    public int mapSize() {
      return 128;
    }
  }
  // #lookup-bus

  public
  // #subchannel-bus
  static class StartsWithSubclassification implements Subclassification<String> {
    @Override
    public boolean isEqual(String x, String y) {
      return x.equals(y);
    }

    @Override
    public boolean isSubclass(String x, String y) {
      return x.startsWith(y);
    }
  }

  // #subchannel-bus

  public
  // #subchannel-bus
  /**
   * Publishes the payload of the MsgEnvelope when the topic of the MsgEnvelope starts with the
   * String specified when subscribing.
   */
  static class SubchannelBusImpl extends SubchannelEventBus<MsgEnvelope, ActorRef, String> {

    // Subclassification is an object providing `isEqual` and `isSubclass`
    // to be consumed by the other methods of this classifier
    @Override
    public Subclassification<String> subclassification() {
      return new StartsWithSubclassification();
    }

    // is used for extracting the classifier from the incoming events
    @Override
    public String classify(MsgEnvelope event) {
      return event.topic;
    }

    // will be invoked for each event for all subscribers which registered themselves
    // for the event’s classifier
    @Override
    public void publish(MsgEnvelope event, ActorRef subscriber) {
      subscriber.tell(event.payload, ActorRef.noSender());
    }
  }
  // #subchannel-bus

  public
  // #scanning-bus
  /**
   * Publishes String messages with length less than or equal to the length specified when
   * subscribing.
   */
  static class ScanningBusImpl extends ScanningEventBus<String, ActorRef, Integer> {

    // is needed for determining matching classifiers and storing them in an
    // ordered collection
    @Override
    public int compareClassifiers(Integer a, Integer b) {
      return a.compareTo(b);
    }

    // is needed for storing subscribers in an ordered collection
    @Override
    public int compareSubscribers(ActorRef a, ActorRef b) {
      return a.compareTo(b);
    }

    // determines whether a given classifier shall match a given event; it is invoked
    // for each subscription for all received events, hence the name of the classifier
    @Override
    public boolean matches(Integer classifier, String event) {
      return event.length() <= classifier;
    }

    // will be invoked for each event for all subscribers which registered themselves
    // for the event’s classifier
    @Override
    public void publish(String event, ActorRef subscriber) {
      subscriber.tell(event, ActorRef.noSender());
    }
  }
  // #scanning-bus

  public
  // #actor-bus
  static class Notification {
    public final ActorRef ref;
    public final int id;

    public Notification(ActorRef ref, int id) {
      this.ref = ref;
      this.id = id;
    }
  }

  // #actor-bus

  public
  // #actor-bus
  static class ActorBusImpl extends ManagedActorEventBus<Notification> {

    // the ActorSystem will be used for book-keeping operations, such as subscribers terminating
    public ActorBusImpl(ActorSystem system) {
      super(system);
    }

    // is used for extracting the classifier from the incoming events
    @Override
    public ActorRef classify(Notification event) {
      return event.ref;
    }

    // determines the initial size of the index data structure
    // used internally (i.e. the expected number of different classifiers)
    @Override
    public int mapSize() {
      return 128;
    }
  }
  // #actor-bus

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("EventBusDocTest");

  private final ActorSystem system = actorSystemResource.getSystem();

  @Test
  public void demonstrateLookupClassification() {
    new TestKit(system) {
      {
        // #lookup-bus-test
        LookupBusImpl lookupBus = new LookupBusImpl();
        lookupBus.subscribe(getTestActor(), "greetings");
        lookupBus.publish(new MsgEnvelope("time", System.currentTimeMillis()));
        lookupBus.publish(new MsgEnvelope("greetings", "hello"));
        expectMsgEquals("hello");
        // #lookup-bus-test
      }
    };
  }

  @Test
  public void demonstrateSubchannelClassification() {
    new TestKit(system) {
      {
        // #subchannel-bus-test
        SubchannelBusImpl subchannelBus = new SubchannelBusImpl();
        subchannelBus.subscribe(getTestActor(), "abc");
        subchannelBus.publish(new MsgEnvelope("xyzabc", "x"));
        subchannelBus.publish(new MsgEnvelope("bcdef", "b"));
        subchannelBus.publish(new MsgEnvelope("abc", "c"));
        expectMsgEquals("c");
        subchannelBus.publish(new MsgEnvelope("abcdef", "d"));
        expectMsgEquals("d");
        // #subchannel-bus-test
      }
    };
  }

  @Test
  public void demonstrateScanningClassification() {
    new TestKit(system) {
      {
        // #scanning-bus-test
        ScanningBusImpl scanningBus = new ScanningBusImpl();
        scanningBus.subscribe(getTestActor(), 3);
        scanningBus.publish("xyzabc");
        scanningBus.publish("ab");
        expectMsgEquals("ab");
        scanningBus.publish("abc");
        expectMsgEquals("abc");
        // #scanning-bus-test
      }
    };
  }

  @Test
  public void demonstrateManagedActorClassification() {
    // #actor-bus-test
    ActorRef observer1 = new TestKit(system).getRef();
    ActorRef observer2 = new TestKit(system).getRef();
    TestKit probe1 = new TestKit(system);
    TestKit probe2 = new TestKit(system);
    ActorRef subscriber1 = probe1.getRef();
    ActorRef subscriber2 = probe2.getRef();
    ActorBusImpl actorBus = new ActorBusImpl(system);
    actorBus.subscribe(subscriber1, observer1);
    actorBus.subscribe(subscriber2, observer1);
    actorBus.subscribe(subscriber2, observer2);
    Notification n1 = new Notification(observer1, 100);
    actorBus.publish(n1);
    probe1.expectMsgEquals(n1);
    probe2.expectMsgEquals(n1);
    Notification n2 = new Notification(observer2, 101);
    actorBus.publish(n2);
    probe2.expectMsgEquals(n2);
    probe1.expectNoMessage(Duration.ofMillis(500));
    // #actor-bus-test
  }
}
