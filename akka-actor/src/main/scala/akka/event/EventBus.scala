/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import java.util.Comparator
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.atomic.AtomicReference

import scala.collection.immutable

import akka.actor.{ ActorRef, ActorSystem }
import akka.util.{ Subclassification, SubclassifiedIndex }
import akka.util.Index

/**
 * Represents the base type for EventBuses
 * Internally has an Event type, a Classifier type and a Subscriber type
 *
 * For the Java API, see akka.event.japi.*
 */
trait EventBus {
  type Event
  type Classifier
  type Subscriber

  //#event-bus-api
  /**
   * Attempts to register the subscriber to the specified Classifier
   * @return true if successful and false if not (because it was already
   *   subscribed to that Classifier, or otherwise)
   */
  def subscribe(subscriber: Subscriber, to: Classifier): Boolean

  /**
   * Attempts to deregister the subscriber from the specified Classifier
   * @return true if successful and false if not (because it wasn't subscribed
   *   to that Classifier, or otherwise)
   */
  def unsubscribe(subscriber: Subscriber, from: Classifier): Boolean

  /**
   * Attempts to deregister the subscriber from all Classifiers it may be subscribed to
   */
  def unsubscribe(subscriber: Subscriber): Unit

  /**
   * Publishes the specified Event to this bus
   */
  def publish(event: Event): Unit
  //#event-bus-api
}

/**
 * Represents an EventBus where the Subscriber type is ActorRef
 */
trait ActorEventBus extends EventBus {
  type Subscriber = ActorRef
  protected def compareSubscribers(a: ActorRef, b: ActorRef) = a.compareTo(b)
}

/**
 * Can be mixed into an EventBus to specify that the Classifier type is ActorRef
 */
trait ActorClassifier { this: EventBus =>
  type Classifier = ActorRef
}

/**
 * Can be mixed into an EventBus to specify that the Classifier type is a Function from Event to Boolean (predicate)
 */
trait PredicateClassifier { this: EventBus =>
  type Classifier = Event => Boolean
}

/**
 * Maps Subscribers to Classifiers using equality on Classifier to store a Set of Subscribers (hence the need for compareSubscribers)
 * Maps Events to Classifiers through the classify-method (so it knows who to publish to)
 *
 * The compareSubscribers need to provide a total ordering of the Subscribers
 */
trait LookupClassification { this: EventBus =>

  protected final val subscribers = new Index[Classifier, Subscriber](mapSize(), new Comparator[Subscriber] {
    def compare(a: Subscriber, b: Subscriber): Int = compareSubscribers(a, b)
  })

  /**
   * This is a size hint for the number of Classifiers you expect to have (use powers of 2)
   */
  protected def mapSize(): Int

  /**
   * Provides a total ordering of Subscribers (think java.util.Comparator.compare)
   */
  protected def compareSubscribers(a: Subscriber, b: Subscriber): Int

  /**
   * Returns the Classifier associated with the given Event
   */
  protected def classify(event: Event): Classifier

  /**
   * Publishes the given Event to the given Subscriber
   */
  protected def publish(event: Event, subscriber: Subscriber): Unit

  def subscribe(subscriber: Subscriber, to: Classifier): Boolean = subscribers.put(to, subscriber)

  def unsubscribe(subscriber: Subscriber, from: Classifier): Boolean = subscribers.remove(from, subscriber)

  def unsubscribe(subscriber: Subscriber): Unit = subscribers.removeValue(subscriber)

  def publish(event: Event): Unit = {
    val i = subscribers.valueIterator(classify(event))
    while (i.hasNext) publish(event, i.next())
  }
}

/**
 * Classification which respects relationships between channels: subscribing
 * to one channel automatically and idempotently subscribes to all sub-channels.
 */
trait SubchannelClassification { this: EventBus =>

  /**
   * The logic to form sub-class hierarchy
   */
  protected implicit def subclassification: Subclassification[Classifier]

  // must be lazy to avoid initialization order problem with subclassification
  private lazy val subscriptions = new SubclassifiedIndex[Classifier, Subscriber]()

  @volatile
  private var cache = Map.empty[Classifier, Set[Subscriber]]

  /**
   * Returns the Classifier associated with the given Event
   */
  protected def classify(event: Event): Classifier

  /**
   * Publishes the given Event to the given Subscriber
   */
  protected def publish(event: Event, subscriber: Subscriber): Unit

  def subscribe(subscriber: Subscriber, to: Classifier): Boolean = subscriptions.synchronized {
    val diff = subscriptions.addValue(to, subscriber)
    addToCache(diff)
    diff.nonEmpty
  }

  def unsubscribe(subscriber: Subscriber, from: Classifier): Boolean = subscriptions.synchronized {
    val diff = subscriptions.removeValue(from, subscriber)
    // removeValue(K, V) does not return the diff to remove from or add to the cache
    // but instead the whole set of keys and values that should be updated in the cache
    cache ++= diff
    diff.nonEmpty
  }

  def unsubscribe(subscriber: Subscriber): Unit = subscriptions.synchronized {
    removeFromCache(subscriptions.removeValue(subscriber))
  }

  def publish(event: Event): Unit = {
    val c = classify(event)
    val recv =
      if (cache contains c) cache(c) // c will never be removed from cache
      else
        subscriptions.synchronized {
          if (cache contains c) cache(c)
          else {
            addToCache(subscriptions.addKey(c))
            cache(c)
          }
        }
    recv.foreach(publish(event, _))
  }

  /**
   * INTERNAL API
   * Expensive call! Avoid calling directly from event bus subscribe / unsubscribe.
   */
  private[akka] def hasSubscriptions(subscriber: Subscriber): Boolean =
    // FIXME binary incompatible, but I think it is safe to filter out this problem,
    //       since it is only called from new functionality in EventStreamUnsubscriber
    cache.values.exists { _ contains subscriber }

  private def removeFromCache(changes: immutable.Seq[(Classifier, Set[Subscriber])]): Unit =
    cache = changes.foldLeft(cache) {
      case (m, (c, cs)) => m.updated(c, m.getOrElse(c, Set.empty[Subscriber]).diff(cs))
    }

  private def addToCache(changes: immutable.Seq[(Classifier, Set[Subscriber])]): Unit =
    cache = changes.foldLeft(cache) {
      case (m, (c, cs)) => m.updated(c, m.getOrElse(c, Set.empty[Subscriber]).union(cs))
    }

}

/**
 * Maps Classifiers to Subscribers and selects which Subscriber should receive which publication through scanning through all Subscribers
 * through the matches(classifier, event) method
 *
 * Note: the compareClassifiers and compareSubscribers must together form an absolute ordering (think java.util.Comparator.compare)
 */
trait ScanningClassification { self: EventBus =>
  protected final val subscribers =
    new ConcurrentSkipListSet[(Classifier, Subscriber)](new Comparator[(Classifier, Subscriber)] {
      def compare(a: (Classifier, Subscriber), b: (Classifier, Subscriber)): Int =
        compareClassifiers(a._1, b._1) match {
          case 0     => compareSubscribers(a._2, b._2)
          case other => other
        }
    })

  /**
   * Provides a total ordering of Classifiers (think java.util.Comparator.compare)
   */
  protected def compareClassifiers(a: Classifier, b: Classifier): Int

  /**
   * Provides a total ordering of Subscribers (think java.util.Comparator.compare)
   */
  protected def compareSubscribers(a: Subscriber, b: Subscriber): Int

  /**
   * Returns whether the specified Classifier matches the specified Event
   */
  protected def matches(classifier: Classifier, event: Event): Boolean

  /**
   * Publishes the specified Event to the specified Subscriber
   */
  protected def publish(event: Event, subscriber: Subscriber): Unit

  def subscribe(subscriber: Subscriber, to: Classifier): Boolean = subscribers.add((to, subscriber))

  def unsubscribe(subscriber: Subscriber, from: Classifier): Boolean = subscribers.remove((from, subscriber))

  def unsubscribe(subscriber: Subscriber): Unit = {
    val i = subscribers.iterator()
    while (i.hasNext) {
      val e = i.next()
      if (compareSubscribers(subscriber, e._2) == 0) i.remove()
    }
  }

  def publish(event: Event): Unit = {
    val currentSubscribers = subscribers.iterator()
    while (currentSubscribers.hasNext) {
      val (classifier, subscriber) = currentSubscribers.next()
      if (matches(classifier, event))
        publish(event, subscriber)
    }
  }
}

/**
 * Maps ActorRefs to ActorRefs to form an EventBus where ActorRefs can listen to other ActorRefs.
 *
 * All subscribers will be watched by an `akka.event.ActorClassificationUnsubscriber` and unsubscribed when they terminate.
 * The unsubscriber actor will not be stopped automatically, and if you want to stop using the bus you should stop it yourself.
 */
trait ManagedActorClassification { this: ActorEventBus with ActorClassifier =>
  import scala.annotation.tailrec

  protected def system: ActorSystem

  private class ManagedActorClassificationMappings(
      val seqNr: Int,
      val backing: Map[ActorRef, immutable.TreeSet[ActorRef]]) {

    def get(monitored: ActorRef): immutable.TreeSet[ActorRef] = backing.getOrElse(monitored, empty)

    def add(monitored: ActorRef, monitor: ActorRef) = {
      val watchers = backing.get(monitored).getOrElse(empty) + monitor
      new ManagedActorClassificationMappings(seqNr + 1, backing.updated(monitored, watchers))
    }

    def remove(monitored: ActorRef, monitor: ActorRef) = {
      val monitors = backing.get(monitored).getOrElse(empty) - monitor
      new ManagedActorClassificationMappings(seqNr + 1, backing.updated(monitored, monitors))
    }

    def remove(monitored: ActorRef) = {
      val v = backing - monitored
      new ManagedActorClassificationMappings(seqNr + 1, v)
    }
  }

  private val mappings = new AtomicReference[ManagedActorClassificationMappings](
    new ManagedActorClassificationMappings(0, Map.empty[ActorRef, immutable.TreeSet[ActorRef]]))

  private val empty = immutable.TreeSet.empty[ActorRef]

  /** The unsubscriber takes care of unsubscribing actors, which have terminated. */
  protected lazy val unsubscriber =
    ActorClassificationUnsubscriber.start(system, this.toString(), (this.unsubscribe: ActorRef => Unit))

  @tailrec
  protected final def associate(monitored: ActorRef, monitor: ActorRef): Boolean = {
    val current = mappings.get

    current.backing.get(monitored) match {
      case None =>
        val added = current.add(monitored, monitor)

        if (mappings.compareAndSet(current, added)) registerWithUnsubscriber(monitor, added.seqNr)
        else associate(monitored, monitor)

      case Some(monitors) =>
        if (monitors.contains(monitored)) false
        else {
          val added = current.add(monitored, monitor)
          val noChange = current.backing == added.backing

          if (noChange) false
          else if (mappings.compareAndSet(current, added)) registerWithUnsubscriber(monitor, added.seqNr)
          else associate(monitored, monitor)
        }
    }
  }

  protected final def dissociate(actor: ActorRef): Unit = {
    @tailrec
    def dissociateAsMonitored(monitored: ActorRef): Unit = {
      val current = mappings.get
      if (current.backing.contains(monitored)) {
        val removed = current.remove(monitored)
        if (!mappings.compareAndSet(current, removed))
          dissociateAsMonitored(monitored)
      }
    }

    def dissociateAsMonitor(monitor: ActorRef): Unit = {
      val current = mappings.get
      val i = current.backing.iterator
      while (i.hasNext) {
        val (key, value) = i.next()
        value match {
          case null =>
          // do nothing

          case monitors =>
            if (monitors.contains(monitor))
              dissociate(key, monitor)
        }
      }
    }

    try {
      dissociateAsMonitored(actor)
    } finally {
      dissociateAsMonitor(actor)
    }
  }

  @tailrec
  protected final def dissociate(monitored: ActorRef, monitor: ActorRef): Boolean = {
    val current = mappings.get

    current.backing.get(monitored) match {
      case None => false
      case Some(monitors) =>
        val removed = current.remove(monitored, monitor)
        val removedMonitors = removed.get(monitored)

        if (monitors.isEmpty || monitors == removedMonitors) {
          false
        } else {
          if (mappings.compareAndSet(current, removed)) unregisterFromUnsubscriber(monitor, removed.seqNr)
          else dissociate(monitored, monitor)
        }
    }
  }

  /**
   * Returns the Classifier associated with the specified Event
   */
  protected def classify(event: Event): Classifier

  /**
   * This is a size hint for the number of Classifiers you expect to have (use powers of 2)
   */
  protected def mapSize: Int

  def publish(event: Event): Unit = {
    mappings.get.backing.get(classify(event)) match {
      case None       => ()
      case Some(refs) => refs.foreach { _ ! event }
    }
  }

  def subscribe(subscriber: Subscriber, to: Classifier): Boolean =
    if (subscriber eq null) throw new IllegalArgumentException("Subscriber is null")
    else if (to eq null) throw new IllegalArgumentException("Classifier is null")
    else associate(to, subscriber)

  def unsubscribe(subscriber: Subscriber, from: Classifier): Boolean =
    if (subscriber eq null) throw new IllegalArgumentException("Subscriber is null")
    else if (from eq null) throw new IllegalArgumentException("Classifier is null")
    else dissociate(from, subscriber)

  def unsubscribe(subscriber: Subscriber): Unit =
    if (subscriber eq null) throw new IllegalArgumentException("Subscriber is null")
    else dissociate(subscriber)

  /**
   * INTERNAL API
   */
  private[akka] def registerWithUnsubscriber(subscriber: ActorRef, seqNr: Int): Boolean = {
    unsubscriber ! ActorClassificationUnsubscriber.Register(subscriber, seqNr)
    true
  }

  /**
   * INTERNAL API
   */
  private[akka] def unregisterFromUnsubscriber(subscriber: ActorRef, seqNr: Int): Boolean = {
    unsubscriber ! ActorClassificationUnsubscriber.Unregister(subscriber, seqNr)
    true
  }
}
