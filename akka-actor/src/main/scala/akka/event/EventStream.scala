/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec

import akka.actor.{ ActorRef, ActorSystem }
import akka.event.Logging.simpleName
import akka.util.Subclassification

/**
 * An Akka EventStream is a pub-sub stream of events both system and user generated,
 * where subscribers are ActorRefs and the channels are Classes and Events are any java.lang.Object.
 * EventStreams employ SubchannelClassification, which means that if you listen to a Class,
 * you'll receive any message that is of that type or a subtype.
 *
 * The debug flag in the constructor toggles if operations on this EventStream should also be published
 * as Debug-Events
 */
class EventStream(sys: ActorSystem, private val debug: Boolean) extends LoggingBus with SubchannelClassification {

  def this(sys: ActorSystem) = this(sys, debug = false)

  type Event = Any
  type Classifier = Class[_]

  /** Either the list of subscribed actors, or a ref to an [[akka.event.EventStreamUnsubscriber]] */
  private val initiallySubscribedOrUnsubscriber = new AtomicReference[Either[Set[ActorRef], ActorRef]](Left(Set.empty))

  protected implicit val subclassification: Subclassification[Classifier] = new Subclassification[Class[_]] {
    def isEqual(x: Class[_], y: Class[_]) = x == y
    def isSubclass(x: Class[_], y: Class[_]) = y.isAssignableFrom(x)
  }

  protected def classify(event: Any): Class[_] = event.getClass

  protected def publish(event: Any, subscriber: ActorRef) = {
    if (sys == null && subscriber.isTerminated) unsubscribe(subscriber)
    else subscriber ! event
  }

  /**
   * Subscribe an actor to listen for types and subtypes by passing Class to channel argument.
   * The same actor can create multiple subscriptions for different Class.
   */
  override def subscribe(subscriber: ActorRef, channel: Class[_]): Boolean = {
    if (subscriber eq null) throw new IllegalArgumentException("subscriber is null")
    if (debug)
      publish(Logging.Debug(simpleName(this), this.getClass, "subscribing " + subscriber + " to channel " + channel))
    registerWithUnsubscriber(subscriber)
    super.subscribe(subscriber, channel)
  }

  /**
   * Unsubscribe specific types subscriptions created by this actor from the event stream.
   */
  override def unsubscribe(subscriber: ActorRef, channel: Class[_]): Boolean = {
    if (subscriber eq null) throw new IllegalArgumentException("subscriber is null")
    val ret = super.unsubscribe(subscriber, channel)
    unregisterIfNoMoreSubscribedChannels(subscriber)
    if (debug)
      publish(
        Logging.Debug(simpleName(this), this.getClass, "unsubscribing " + subscriber + " from channel " + channel))
    ret
  }

  /**
   * Unsubscribe all subscriptions created by this actor from the event stream.
   */
  override def unsubscribe(subscriber: ActorRef): Unit = {
    if (subscriber eq null) throw new IllegalArgumentException("subscriber is null")
    super.unsubscribe(subscriber)
    unregisterIfNoMoreSubscribedChannels(subscriber)
    if (debug)
      publish(Logging.Debug(simpleName(this), this.getClass, "unsubscribing " + subscriber + " from all channels"))
  }

  /**
   * ''Must'' be called after actor system is "ready".
   * Starts system actor that takes care of unsubscribing subscribers that have terminated.
   */
  def startUnsubscriber(): Unit =
    // sys may be null for backwards compatibility reasons
    if (sys ne null) EventStreamUnsubscriber.start(sys, this)

  /**
   * INTERNAL API
   */
  @tailrec
  final private[akka] def initUnsubscriber(unsubscriber: ActorRef): Boolean = {
    // sys may be null for backwards compatibility reasons
    if (sys eq null) false
    else
      initiallySubscribedOrUnsubscriber.get match {
        case value @ Left(subscribers) =>
          if (initiallySubscribedOrUnsubscriber.compareAndSet(value, Right(unsubscriber))) {
            if (debug)
              publish(
                Logging.Debug(
                  simpleName(this),
                  this.getClass,
                  "initialized unsubscriber to: " + unsubscriber + ", registering " + subscribers.size + " initial subscribers with it"))
            subscribers.foreach(registerWithUnsubscriber)
            true
          } else {
            // recurse, because either new subscribers have been registered since `get` (retry Left case),
            // or another thread has succeeded in setting it's unsubscriber (end on Right case)
            initUnsubscriber(unsubscriber)
          }

        case Right(presentUnsubscriber) =>
          if (debug)
            publish(
              Logging.Debug(
                simpleName(this),
                this.getClass,
                s"not using unsubscriber $unsubscriber, because already initialized with $presentUnsubscriber"))
          false
      }
  }

  /**
   * INTERNAL API
   */
  @tailrec
  private def registerWithUnsubscriber(subscriber: ActorRef): Unit = {
    // sys may be null for backwards compatibility reasons
    if (sys ne null) initiallySubscribedOrUnsubscriber.get match {
      case value @ Left(subscribers) =>
        if (!initiallySubscribedOrUnsubscriber.compareAndSet(value, Left(subscribers + subscriber)))
          registerWithUnsubscriber(subscriber)

      case Right(unsubscriber) =>
        unsubscriber ! EventStreamUnsubscriber.Register(subscriber)
    }
  }

  /**
   * INTERNAL API
   *
   * The actual check if the subscriber still has subscriptions is performed by the `EventStreamUnsubscriber`,
   * because it's an expensive operation, and we don want to block client-code for that long, the Actor will eventually
   * catch up and perform the appropriate operation.
   */
  @tailrec
  private def unregisterIfNoMoreSubscribedChannels(subscriber: ActorRef): Unit = {
    // sys may be null for backwards compatibility reasons
    if (sys ne null) initiallySubscribedOrUnsubscriber.get match {
      case value @ Left(subscribers) =>
        if (!initiallySubscribedOrUnsubscriber.compareAndSet(value, Left(subscribers - subscriber)))
          unregisterIfNoMoreSubscribedChannels(subscriber)

      case Right(unsubscriber) =>
        unsubscriber ! EventStreamUnsubscriber.UnregisterIfNoMoreSubscribedChannels(subscriber)
    }
  }

}
