/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event.japi

import akka.actor.{ ActorRef, ActorSystem }
import akka.util.Subclassification

/**
 * Java API: See documentation for [[akka.event.EventBus]]
 * E is the Event type
 * S is the Subscriber type
 * C is the Classifier type
 */
trait EventBus[E, S, C] {

  /**
   * Attempts to register the subscriber to the specified Classifier
   * @return true if successful and false if not (because it was already subscribed to that Classifier, or otherwise)
   */
  def subscribe(subscriber: S, to: C): Boolean

  /**
   * Attempts to deregister the subscriber from the specified Classifier
   * @return true if successful and false if not (because it wasn't subscribed to that Classifier, or otherwise)
   */
  def unsubscribe(subscriber: S, from: C): Boolean

  /**
   * Attempts to deregister the subscriber from all Classifiers it may be subscribed to
   */
  def unsubscribe(subscriber: S): Unit

  /**
   * Publishes the specified Event to this bus
   */
  def publish(event: E): Unit
}

/**
 * Java API: See documentation for [[akka.event.LookupClassification]]
 * E is the Event type
 * S is the Subscriber type
 * C is the Classifier type
 */
abstract class LookupEventBus[E, S, C] extends EventBus[E, S, C] {
  private val bus = new akka.event.EventBus with akka.event.LookupClassification {
    type Event = E
    type Subscriber = S
    type Classifier = C

    override protected def mapSize(): Int = LookupEventBus.this.mapSize()

    override protected def compareSubscribers(a: S, b: S): Int =
      LookupEventBus.this.compareSubscribers(a, b)

    override protected def classify(event: E): C =
      LookupEventBus.this.classify(event)

    override protected def publish(event: E, subscriber: S): Unit =
      LookupEventBus.this.publish(event, subscriber)
  }

  /**
   * This is a size hint for the number of Classifiers you expect to have (use powers of 2)
   */
  protected def mapSize(): Int

  /**
   * Provides a total ordering of Subscribers (think java.util.Comparator.compare)
   */
  protected def compareSubscribers(a: S, b: S): Int

  /**
   * Returns the Classifier associated with the given Event
   */
  protected def classify(event: E): C

  /**
   * Publishes the given Event to the given Subscriber
   */
  protected def publish(event: E, subscriber: S): Unit

  override def subscribe(subscriber: S, to: C): Boolean = bus.subscribe(subscriber, to)
  override def unsubscribe(subscriber: S, from: C): Boolean = bus.unsubscribe(subscriber, from)
  override def unsubscribe(subscriber: S): Unit = bus.unsubscribe(subscriber)
  override def publish(event: E): Unit = bus.publish(event)

}

/**
 * Java API: See documentation for [[akka.event.SubchannelClassification]]
 * E is the Event type
 * S is the Subscriber type
 * C is the Classifier type
 */
abstract class SubchannelEventBus[E, S, C] extends EventBus[E, S, C] {
  private val bus = new akka.event.EventBus with akka.event.SubchannelClassification {
    type Event = E
    type Subscriber = S
    type Classifier = C

    override protected def subclassification: Subclassification[Classifier] =
      SubchannelEventBus.this.subclassification

    override protected def classify(event: Event): Classifier =
      SubchannelEventBus.this.classify(event)

    override protected def publish(event: Event, subscriber: Subscriber): Unit =
      SubchannelEventBus.this.publish(event, subscriber)
  }

  /**
   * The logic to form sub-class hierarchy
   */
  def subclassification: Subclassification[C]

  /**
   * Returns the Classifier associated with the given Event
   */
  protected def classify(event: E): C

  /**
   * Publishes the given Event to the given Subscriber
   */
  protected def publish(event: E, subscriber: S): Unit

  override def subscribe(subscriber: S, to: C): Boolean = bus.subscribe(subscriber, to)
  override def unsubscribe(subscriber: S, from: C): Boolean = bus.unsubscribe(subscriber, from)
  override def unsubscribe(subscriber: S): Unit = bus.unsubscribe(subscriber)
  override def publish(event: E): Unit = bus.publish(event)

}

/**
 * Java API: See documentation for [[akka.event.ScanningClassification]]
 * E is the Event type
 * S is the Subscriber type
 * C is the Classifier type
 */
abstract class ScanningEventBus[E, S, C] extends EventBus[E, S, C] {
  private val bus = new akka.event.EventBus with akka.event.ScanningClassification {
    type Event = E
    type Subscriber = S
    type Classifier = C

    override protected def compareClassifiers(a: C, b: C): Int =
      ScanningEventBus.this.compareClassifiers(a, b)

    override protected def compareSubscribers(a: S, b: S): Int =
      ScanningEventBus.this.compareSubscribers(a, b)

    override protected def matches(classifier: C, event: E): Boolean =
      ScanningEventBus.this.matches(classifier, event)

    override protected def publish(event: E, subscriber: S): Unit =
      ScanningEventBus.this.publish(event, subscriber)
  }

  /**
   * Provides a total ordering of Classifiers (think java.util.Comparator.compare)
   */
  protected def compareClassifiers(a: C, b: C): Int

  /**
   * Provides a total ordering of Subscribers (think java.util.Comparator.compare)
   */
  protected def compareSubscribers(a: S, b: S): Int

  /**
   * Returns whether the specified Classifier matches the specified Event
   */
  protected def matches(classifier: C, event: E): Boolean

  /**
   * Publishes the specified Event to the specified Subscriber
   */
  protected def publish(event: E, subscriber: S): Unit

  override def subscribe(subscriber: S, to: C): Boolean = bus.subscribe(subscriber, to)
  override def unsubscribe(subscriber: S, from: C): Boolean = bus.unsubscribe(subscriber, from)
  override def unsubscribe(subscriber: S): Unit = bus.unsubscribe(subscriber)
  override def publish(event: E): Unit = bus.publish(event)
}

/**
 * Java API: See documentation for [[akka.event.ManagedActorClassification]]
 * An EventBus where the Subscribers are ActorRefs and the Classifier is ActorRef
 * Means that ActorRefs "listen" to other ActorRefs
 * E is the Event type
 */
abstract class ManagedActorEventBus[E](system: ActorSystem) extends EventBus[E, ActorRef, ActorRef] {
  private val bus = new akka.event.ActorEventBus with akka.event.ManagedActorClassification
  with akka.event.ActorClassifier {
    type Event = E

    override val system = ManagedActorEventBus.this.system

    override protected def mapSize: Int = ManagedActorEventBus.this.mapSize()

    override protected def classify(event: E): ActorRef =
      ManagedActorEventBus.this.classify(event)
  }

  /**
   * This is a size hint for the number of Classifiers you expect to have (use powers of 2)
   */
  protected def mapSize(): Int

  /**
   * Returns the Classifier associated with the given Event
   */
  protected def classify(event: E): ActorRef

  override def subscribe(subscriber: ActorRef, to: ActorRef): Boolean = bus.subscribe(subscriber, to)
  override def unsubscribe(subscriber: ActorRef, from: ActorRef): Boolean = bus.unsubscribe(subscriber, from)
  override def unsubscribe(subscriber: ActorRef): Unit = bus.unsubscribe(subscriber)
  override def publish(event: E): Unit = bus.publish(event)
}
