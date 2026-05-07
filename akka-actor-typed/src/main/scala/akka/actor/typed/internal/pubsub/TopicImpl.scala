/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.pubsub

import akka.actor.Dropped
import akka.actor.InvalidMessageException
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.pubsub.Topic
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.StashBuffer
import akka.actor.typed.scaladsl.TimerScheduler
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.util.PrettyDuration.PrettyPrintableDuration
import akka.util.WallClock

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object TopicImpl {

  trait Command[T]

  // actual public messages but internal to ease bincomp evolution
  final case class Publish[T](message: T) extends Topic.Command[T] {
    if (message == null)
      throw InvalidMessageException("[null] is not an allowed message")
  }
  final case class Subscribe[T](subscriber: ActorRef[T]) extends Topic.Command[T]
  final case class Unsubscribe[T](subscriber: ActorRef[T]) extends Topic.Command[T]

  // internal messages, note that the protobuf serializer for those sent remotely is defined in akka-cluster-typed
  final case class GetTopicStats[T](replyTo: ActorRef[TopicStats]) extends Topic.Command[T]
  final case class TopicStats(localSubscriberCount: Int, topicInstanceCount: Int) extends Topic.TopicStats
  final case class TopicInstancesUpdated[T](topics: Set[ActorRef[TopicImpl.Command[T]]]) extends Command[T]
  final case class MessagePublished[T](message: T) extends Command[T]
  final case class SubscriberTerminated[T](subscriber: ActorRef[T]) extends Command[T]

  def apply[T](topicName: String, ttl: Option[FiniteDuration])(implicit classTag: ClassTag[T]): Behavior[Command[T]] = {
    ttl match {
      case None =>
        Behaviors.setup[TopicImpl.Command[T]](context => new InitialTopicImpl[T](topicName, context, None)).narrow
      case Some(definedTtl) =>
        Behaviors
          .setup[TopicImpl.Command[T]](context =>
            Behaviors.withTimers[TopicImpl.Command[T]](timers =>
              new InitialTopicImpl[T](topicName, context, Some((definedTtl, timers, WallClock.AlwaysIncreasingClock)))))
          .narrow
    }
  }
}

/**
 * INTERNAL API
 *
 * Starting behavior for a topic before it got a first subscriber listing back from the receptionist
 */
@InternalApi
private final class InitialTopicImpl[T](
    topicName: String,
    context: ActorContext[TopicImpl.Command[T]],
    ttlAndTimers: Option[(FiniteDuration, TimerScheduler[TopicImpl.Command[T]], WallClock)])(
    implicit classTag: ClassTag[T])
    extends AbstractBehavior[TopicImpl.Command[T]](context) {
  import TopicImpl._

  private val stash = StashBuffer[Command[T]](context, capacity = 10000)
  private val topicServiceKey = ServiceKey[TopicImpl.Command[T]](topicName)
  if (context.log.isDebugEnabled())
    context.log.debug(
      "Starting up pub-sub topic [{}] for messages of type [{}]{}",
      topicName,
      classTag.runtimeClass.getName,
      ttlAndTimers.map { case (ttl, _, _) => s" (ttl: ${ttl.pretty})" }.getOrElse(""))

  private def receptionist = context.system.receptionist
  private val receptionistAdapter = context.messageAdapter[Receptionist.Listing] {
    case topicServiceKey.Listing(topics) => TopicInstancesUpdated(topics)
    case _                               => throw new IllegalArgumentException() // compiler completeness check pleaser
  }
  receptionist ! Receptionist.Subscribe(topicServiceKey, receptionistAdapter)

  def onMessage(msg: Command[T]): Behavior[Command[T]] = msg match {
    case TopicInstancesUpdated(initialTopicInstances) =>
      context.log.debug("Initial topic instance listing received for pub-sub topic [{}], starting", topicName)
      val initializedTopicImpl =
        new TopicImpl[T](topicName, context, topicServiceKey, ttlAndTimers, initialTopicInstances)
      stash.unstashAll(initializedTopicImpl)

    case msg: Command[T @unchecked] =>
      import akka.actor.typed.scaladsl.adapter._
      if (!stash.isFull) stash.stash(msg)
      else
        context.system.eventStream ! EventStream.Publish(Dropped(
          msg,
          s"Stash is full in topic [$topicServiceKey]",
          context.self.toClassic)) // don't fail on full stash
      this
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class TopicImpl[T](
    topicName: String,
    context: ActorContext[TopicImpl.Command[T]],
    topicServiceKey: ServiceKey[TopicImpl.Command[T]],
    ttlAndTimers: Option[(FiniteDuration, TimerScheduler[TopicImpl.Command[T]], WallClock)],
    initialTopicInstances: Set[ActorRef[TopicImpl.Command[T]]])(implicit classTag: ClassTag[T])
    extends AbstractBehavior[TopicImpl.Command[T]](context) {

  private def receptionist = context.system.receptionist
  private case object TtlTick extends TopicImpl.Command[T]

  /*
   * The topic actor keeps a local set of subscribers, whenever that is non-empty it registers itself for
   * a topic service key, and when it becomes empty again it deregisters from the service key. Published
   * messages go to all currently known topics registered for the topic service key and the individual topic
   * instances then broadcast the message to all local subscribers. This achieves deduplication for nodes
   * with multiple subscribers and avoids sending to nodes without any subscribers.
   */

  import TopicImpl._

  private var topicInstances = initialTopicInstances
  private var localSubscribers = Set.empty[ActorRef[T]]
  // Note: timestamp when last publish or when the subscriber list became empty because of last unsubscribe
  private var lastActivityForTtl: Long = Long.MinValue

  ttlAndTimers match {
    case Some((ttl, timers, wallClock)) =>
      if (topicInstances.isEmpty) lastActivityForTtl = wallClock.currentTimeMillis()
      timers.startTimerWithFixedDelay(TtlTick, ttl / 2L)
    case _ =>
  }

  override def onMessage(msg: TopicImpl.Command[T]): Behavior[TopicImpl.Command[T]] = msg match {

    case Publish(message) =>
      if (topicInstances.isEmpty) {
        if (localSubscribers.isEmpty) {
          context.log.trace("Publishing message of type [{}] but no subscribers, dropping", msg.getClass)
          if (context.system.settings.PubSubDeadLettersWhenNoSubscribers)
            context.system.deadLetters[Dropped] ! Dropped(message, "No topic subscribers known", context.self.toClassic)
        } else {
          context.log.trace(
            "Publishing message of type [{}] to local subscribers only (topic listing not seen yet)",
            msg.getClass)
          localSubscribers.foreach(_ ! message)
        }
      } else {
        context.log.trace("Publishing message of type [{}]", msg.getClass)
        val pub = MessagePublished(message)
        topicInstances.foreach(_ ! pub)
      }
      activity()
      this

    case MessagePublished(msg) =>
      context.log.trace("Message of type [{}] published", msg.getClass)
      if (classTag.runtimeClass.isAssignableFrom(msg.getClass))
        localSubscribers.foreach(_ ! msg)
      this

    case Subscribe(subscriber) =>
      if (!localSubscribers.contains(subscriber)) {
        context.watchWith(subscriber, SubscriberTerminated(subscriber))
        localSubscribers = localSubscribers + subscriber
        if (localSubscribers.size == 1) {
          context.log.debug(
            "Local subscriber [{}] added, went from no subscribers to one, subscribing to receptionist",
            subscriber)
          // we went from no subscribers to one, register to the receptionist
          receptionist ! Receptionist.Register(topicServiceKey, context.self)
        } else {
          context.log.debug("Local subscriber [{}] added", subscriber)
        }
      } else {
        context.log.debug("Local subscriber [{}] already subscribed, ignoring Subscribe command", subscriber)
      }
      this

    case Unsubscribe(subscriber) =>
      context.unwatch(subscriber)
      localSubscribers = localSubscribers.filterNot(_ == subscriber)
      if (localSubscribers.isEmpty) {
        context.log.debug("Last local subscriber [{}] unsubscribed, deregistering from receptionist", subscriber)
        // that was the lost subscriber, deregister from the receptionist
        receptionist ! Receptionist.Deregister(topicServiceKey, context.self)
        activity()
      } else {
        context.log.debug("Local subscriber [{}] unsubscribed", subscriber)
      }
      this

    case SubscriberTerminated(subscriber) =>
      localSubscribers -= subscriber
      if (localSubscribers.isEmpty) {
        context.log.debug("Last local subscriber [{}] terminated, deregistering from receptionist", subscriber)
        // that was the last subscriber, deregister from the receptionist
        receptionist ! Receptionist.Deregister(topicServiceKey, context.self)
      } else {
        context.log.debug("Local subscriber [{}] terminated, removing from subscriber list", subscriber)
      }
      this

    case TopicInstancesUpdated(newTopics) =>
      context.log.debug("Topic list updated [{}]", newTopics)
      topicInstances = newTopics
      if (lastActivityForTtl == Long.MinValue) {
        ttlAndTimers.foreach {
          case (ttl, timers, _) =>
            timers.startTimerWithFixedDelay(TtlTick.asInstanceOf[Command[T]], ttl / 2L)
        }
      }
      this

    case GetTopicStats(replyTo) =>
      replyTo ! TopicStats(localSubscribers.size, topicInstances.size)
      this

    case TtlTick =>
      if (localSubscribers.isEmpty) {
        // only ever arrives if ttl is defined
        ttlAndTimers match {
          case Some((ttl, _, clock)) =>
            val limit = clock.currentTimeMillis() - ttl.toMillis
            if (lastActivityForTtl < limit) {
              context.log.debug("Topic [{}] reached TTL [{}] without activity, terminating", topicName, ttl.pretty)
              Behaviors.stopped
            } else {
              this
            }
          case _ => this
        }
      } else this

    case other =>
      // can't do exhaustiveness check correctly because of protocol internal/public design
      throw new IllegalArgumentException(s"Unexpected command type ${other.getClass}")
  }

  private def activity(): Unit = ttlAndTimers match {
    case Some((_, _, clock)) =>
      lastActivityForTtl = clock.currentTimeMillis()
    case _ =>
  }
}
