/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.event

import scala.concurrent.duration._
import akka.testkit.AkkaSpec
import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.TestProbe

object EventBusDocSpec {

  //#lookup-bus
  import akka.event.EventBus
  import akka.event.LookupClassification

  final case class MsgEnvelope(topic: String, payload: Any)

  /**
   * Publishes the payload of the MsgEnvelope when the topic of the
   * MsgEnvelope equals the String specified when subscribing.
   */
  class LookupBusImpl extends EventBus with LookupClassification {
    type Event = MsgEnvelope
    type Classifier = String
    type Subscriber = ActorRef

    // is used for extracting the classifier from the incoming events
    override protected def classify(event: Event): Classifier = event.topic

    // will be invoked for each event for all subscribers which registered themselves
    // for the event’s classifier
    override protected def publish(event: Event, subscriber: Subscriber): Unit = {
      subscriber ! event.payload
    }

    // must define a full order over the subscribers, expressed as expected from
    // `java.lang.Comparable.compare`
    override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int =
      a.compareTo(b)

    // determines the initial size of the index data structure
    // used internally (i.e. the expected number of different classifiers)
    override protected def mapSize(): Int = 128

  }

  //#lookup-bus

  //#subchannel-bus
  import akka.util.Subclassification

  class StartsWithSubclassification extends Subclassification[String] {
    override def isEqual(x: String, y: String): Boolean =
      x == y

    override def isSubclass(x: String, y: String): Boolean =
      x.startsWith(y)
  }

  import akka.event.SubchannelClassification

  /**
   * Publishes the payload of the MsgEnvelope when the topic of the
   * MsgEnvelope starts with the String specified when subscribing.
   */
  class SubchannelBusImpl extends EventBus with SubchannelClassification {
    type Event = MsgEnvelope
    type Classifier = String
    type Subscriber = ActorRef

    // Subclassification is an object providing `isEqual` and `isSubclass`
    // to be consumed by the other methods of this classifier
    override protected val subclassification: Subclassification[Classifier] =
      new StartsWithSubclassification

    // is used for extracting the classifier from the incoming events
    override protected def classify(event: Event): Classifier = event.topic

    // will be invoked for each event for all subscribers which registered
    // themselves for the event’s classifier
    override protected def publish(event: Event, subscriber: Subscriber): Unit = {
      subscriber ! event.payload
    }
  }
  //#subchannel-bus

  //#scanning-bus
  import akka.event.ScanningClassification

  /**
   * Publishes String messages with length less than or equal to the length
   * specified when subscribing.
   */
  class ScanningBusImpl extends EventBus with ScanningClassification {
    type Event = String
    type Classifier = Int
    type Subscriber = ActorRef

    // is needed for determining matching classifiers and storing them in an
    // ordered collection
    override protected def compareClassifiers(a: Classifier, b: Classifier): Int =
      if (a < b) -1 else if (a == b) 0 else 1

    // is needed for storing subscribers in an ordered collection
    override protected def compareSubscribers(a: Subscriber, b: Subscriber): Int =
      a.compareTo(b)

    // determines whether a given classifier shall match a given event; it is invoked
    // for each subscription for all received events, hence the name of the classifier
    override protected def matches(classifier: Classifier, event: Event): Boolean =
      event.length <= classifier

    // will be invoked for each event for all subscribers which registered themselves
    // for a classifier matching this event
    override protected def publish(event: Event, subscriber: Subscriber): Unit = {
      subscriber ! event
    }
  }
  //#scanning-bus

  //#actor-bus
  import akka.event.ActorEventBus
  import akka.event.ManagedActorClassification
  import akka.event.ActorClassifier

  final case class Notification(ref: ActorRef, id: Int)

  class ActorBusImpl(val system: ActorSystem)
      extends ActorEventBus
      with ActorClassifier
      with ManagedActorClassification {
    type Event = Notification

    // is used for extracting the classifier from the incoming events
    override protected def classify(event: Event): ActorRef = event.ref

    // determines the initial size of the index data structure
    // used internally (i.e. the expected number of different classifiers)
    override protected def mapSize: Int = 128
  }
  //#actor-bus

}

class EventBusDocSpec extends AkkaSpec {
  import EventBusDocSpec._

  "demonstrate LookupClassification" in {
    //#lookup-bus-test
    val lookupBus = new LookupBusImpl
    lookupBus.subscribe(testActor, "greetings")
    lookupBus.publish(MsgEnvelope("time", System.currentTimeMillis()))
    lookupBus.publish(MsgEnvelope("greetings", "hello"))
    expectMsg("hello")
    //#lookup-bus-test
  }

  "demonstrate SubchannelClassification" in {
    //#subchannel-bus-test
    val subchannelBus = new SubchannelBusImpl
    subchannelBus.subscribe(testActor, "abc")
    subchannelBus.publish(MsgEnvelope("xyzabc", "x"))
    subchannelBus.publish(MsgEnvelope("bcdef", "b"))
    subchannelBus.publish(MsgEnvelope("abc", "c"))
    expectMsg("c")
    subchannelBus.publish(MsgEnvelope("abcdef", "d"))
    expectMsg("d")
    //#subchannel-bus-test
  }

  "demonstrate ScanningClassification" in {
    //#scanning-bus-test
    val scanningBus = new ScanningBusImpl
    scanningBus.subscribe(testActor, 3)
    scanningBus.publish("xyzabc")
    scanningBus.publish("ab")
    expectMsg("ab")
    scanningBus.publish("abc")
    expectMsg("abc")
    //#scanning-bus-test
  }

  "demonstrate ManagedActorClassification" in {
    //#actor-bus-test
    val observer1 = TestProbe().ref
    val observer2 = TestProbe().ref
    val probe1 = TestProbe()
    val probe2 = TestProbe()
    val subscriber1 = probe1.ref
    val subscriber2 = probe2.ref
    val actorBus = new ActorBusImpl(system)
    actorBus.subscribe(subscriber1, observer1)
    actorBus.subscribe(subscriber2, observer1)
    actorBus.subscribe(subscriber2, observer2)
    actorBus.publish(Notification(observer1, 100))
    probe1.expectMsg(Notification(observer1, 100))
    probe2.expectMsg(Notification(observer1, 100))
    actorBus.publish(Notification(observer2, 101))
    probe2.expectMsg(Notification(observer2, 101))
    probe1.expectNoMessage(500.millis)
    //#actor-bus-test
  }
}
