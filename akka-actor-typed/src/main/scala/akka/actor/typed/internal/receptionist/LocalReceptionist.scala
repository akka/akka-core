/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.receptionist

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.Receptionist._
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.util.TypedMultiMap

/**
 * Marker interface to use with dynamic access
 *
 * INTERNAL API
 */
@InternalApi
private[akka] trait ReceptionistBehaviorProvider {
  def name: String
  def behavior: Behavior[Command]
}

// just to provide a log class
/** INTERNAL API */
@InternalApi
private[akka] final class LocalReceptionist

/** INTERNAL API */
@InternalApi
private[akka] object LocalReceptionist extends ReceptionistBehaviorProvider {

  override val name = "localReceptionist"

  private type Service[K <: AbstractServiceKey] = Platform.Service[K]
  private type Subscriber[K <: AbstractServiceKey] = Platform.Subscriber[K]

  private sealed trait InternalCommand
  private final case class RegisteredActorTerminated[T](ref: ActorRef[T]) extends InternalCommand
  private final case class SubscriberTerminated[T](ref: ActorRef[ReceptionistMessages.Listing[T]])
      extends InternalCommand

  private object State {
    def empty =
      State(
        TypedMultiMap.empty[AbstractServiceKey, Service],
        Map.empty,
        TypedMultiMap.empty[AbstractServiceKey, Subscriber],
        Map.empty)
  }

  /**
   * @param services current registered services per key
   * @param servicesPerActor current registered service keys per actor (needed for unregistration since an actor can implement several services)
   * @param subscriptions current subscriptions per service key
   * @param subscriptionsPerActor current subscriptions per subscriber (needed since a subscriber can subscribe to several keys) FIXME is it really needed?
   */
  private final case class State(
      services: TypedMultiMap[AbstractServiceKey, Service],
      servicesPerActor: Map[ActorRef[_], Set[AbstractServiceKey]],
      subscriptions: TypedMultiMap[AbstractServiceKey, Subscriber],
      subscriptionsPerActor: Map[ActorRef[_], Set[AbstractServiceKey]]) {

    def serviceInstanceAdded[Key <: AbstractServiceKey](key: Key)(serviceInstance: ActorRef[key.Protocol]): State = {
      val newServices = services.inserted(key)(serviceInstance)
      val newServicePerActor =
        servicesPerActor.updated(
          serviceInstance,
          servicesPerActor.getOrElse(serviceInstance, Set.empty) + key.asServiceKey)
      copy(services = newServices, servicesPerActor = newServicePerActor)
    }

    def serviceInstanceRemoved[Key <: AbstractServiceKey](key: Key)(serviceInstance: ActorRef[key.Protocol]): State = {
      val newServices = services.removed(key)(serviceInstance)
      val newServicePerActor =
        servicesPerActor.get(serviceInstance) match {
          case Some(keys) =>
            val newKeys = keys - key.asServiceKey
            // only/last service this actor was registered for
            if (newKeys.isEmpty) {
              servicesPerActor - serviceInstance
            } else servicesPerActor.updated(serviceInstance, newKeys)
          case None =>
            // no services actually registered for actor
            servicesPerActor
        }
      copy(services = newServices, servicesPerActor = newServicePerActor)
    }

    def serviceInstanceRemoved(serviceInstance: ActorRef[_]): State = {
      val keys = servicesPerActor.getOrElse(serviceInstance, Set.empty)
      val newServices =
        if (keys.isEmpty) services
        else
          keys.foldLeft(services)((acc, key) =>
            acc.removed(key.asServiceKey)(serviceInstance.asInstanceOf[ActorRef[key.Protocol]]))
      val newServicesPerActor = servicesPerActor - serviceInstance
      copy(services = newServices, servicesPerActor = newServicesPerActor)
    }

    def subscriberAdded[Key <: AbstractServiceKey](key: Key)(subscriber: Subscriber[key.type]): State = {
      val newSubscriptions = subscriptions.inserted(key)(subscriber)
      val newSubscriptionsPerActor =
        subscriptionsPerActor.updated(
          subscriber,
          subscriptionsPerActor.getOrElse(subscriber, Set.empty) + key.asServiceKey)

      copy(subscriptions = newSubscriptions, subscriptionsPerActor = newSubscriptionsPerActor)
    }

    def subscriptionRemoved[Key <: AbstractServiceKey](key: Key)(subscriber: Subscriber[key.type]): State = {
      val newSubscriptions = subscriptions.removed(key)(subscriber)
      val newSubscriptionsPerActor =
        subscriptionsPerActor.get(subscriber) match {
          case Some(keys) =>
            val newKeys = keys - key.asServiceKey
            if (newKeys.isEmpty) {
              subscriptionsPerActor - subscriber
            } else {
              subscriptionsPerActor.updated(subscriber, newKeys)
            }
          case None =>
            // no subscriptions actually exist for actor
            subscriptionsPerActor
        }
      copy(subscriptions = newSubscriptions, subscriptionsPerActor = newSubscriptionsPerActor)
    }

    def subscriberRemoved(subscriber: ActorRef[_]): State = {
      val keys = subscriptionsPerActor.getOrElse(subscriber, Set.empty)
      if (keys.isEmpty) this
      else {
        val newSubscriptions = keys.foldLeft(subscriptions) { (subscriptions, key) =>
          val serviceKey = key.asServiceKey
          subscriptions.removed(serviceKey)(subscriber.asInstanceOf[Subscriber[serviceKey.type]])
        }
        val newSubscriptionsPerActor = subscriptionsPerActor - subscriber
        copy(subscriptions = newSubscriptions, subscriptionsPerActor = newSubscriptionsPerActor)
      }
    }
  }

  override def behavior: Behavior[Command] = Behaviors.setup { ctx =>
    ctx.setLoggerName(classOf[LocalReceptionist])
    behavior(State.empty).narrow[Command]
  }

  private def behavior(state: State): Behavior[Any] = {
    // Helper that makes sure that subscribers are notified when an entry is changed
    def updateServices(changedKeysHint: Set[AbstractServiceKey], f: State => State): Behavior[Any] = {
      val newState = f(state)

      def notifySubscribersFor[T](key: AbstractServiceKey): Unit = {
        val newListing = newState.services.get(key)
        val listing =
          ReceptionistMessages.Listing(key.asServiceKey, newListing, newListing, servicesWereAddedOrRemoved = true)
        newState.subscriptions.get(key).foreach(_ ! listing)
      }

      changedKeysHint.foreach(notifySubscribersFor)
      behavior(newState)
    }

    def replyWithListing[T](key: ServiceKey[T], replyTo: ActorRef[Listing]): Unit = {
      val listing = state.services.get(key)
      replyTo ! ReceptionistMessages.Listing(key, listing, listing, servicesWereAddedOrRemoved = true)
    }

    def onCommand(ctx: ActorContext[Any], cmd: Command): Behavior[Any] = cmd match {

      case ReceptionistMessages.Register(key, serviceInstance, maybeReplyTo) =>
        ctx.log.debug("Actor was registered: {} {}", key, serviceInstance)
        if (!state.servicesPerActor.contains(serviceInstance))
          ctx.watchWith(serviceInstance, RegisteredActorTerminated(serviceInstance))
        maybeReplyTo match {
          case Some(replyTo) => replyTo ! ReceptionistMessages.Registered(key, serviceInstance)
          case None          =>
        }
        updateServices(Set(key), _.serviceInstanceAdded(key)(serviceInstance))

      case ReceptionistMessages.Deregister(key, serviceInstance, maybeReplyTo) =>
        val servicesForActor = state.servicesPerActor.getOrElse(serviceInstance, Set.empty)
        if (servicesForActor.isEmpty) {
          // actor deregistered but we saw a terminate message before we got the deregistration
          Behaviors.same
        } else {
          ctx.log.debug("Actor was deregistered: {} {}", key, serviceInstance)
          if ((servicesForActor - key).isEmpty)
            ctx.unwatch(serviceInstance)

          maybeReplyTo match {
            case Some(replyTo) => replyTo ! ReceptionistMessages.Deregistered(key, serviceInstance)
            case None          =>
          }

          updateServices(Set(key), { state =>
            val newState = state.serviceInstanceRemoved(key)(serviceInstance)
            if (state.servicesPerActor.getOrElse(serviceInstance, Set.empty).isEmpty)
              ctx.unwatch(serviceInstance)
            newState
          })

        }

      case ReceptionistMessages.Find(key, replyTo) =>
        replyWithListing(key, replyTo)
        Behaviors.same

      case ReceptionistMessages.Subscribe(key, subscriber) =>
        if (!state.subscriptionsPerActor.contains(subscriber))
          ctx.watchWith(subscriber, SubscriberTerminated(subscriber))

        // immediately reply with initial listings to the new subscriber
        replyWithListing(key, subscriber)

        behavior(state.subscriberAdded(key)(subscriber))

      case other =>
        // compiler does not know about our division into public and internal commands
        throw new IllegalArgumentException(s"Unexpected command type ${other.getClass}")
    }

    def onInternal(ctx: ActorContext[Any], cmd: InternalCommand): Behavior[Any] = cmd match {
      case RegisteredActorTerminated(serviceInstance) =>
        val keys = state.servicesPerActor.getOrElse(serviceInstance, Set.empty)
        if (keys.isEmpty) {
          // actor terminated but had deregistered all registrations before we could process the termination
          Behaviors.same
        } else {
          ctx.log.debug("Registered actor terminated: [{}] {}", keys.mkString(","), serviceInstance)
          updateServices(keys, _.serviceInstanceRemoved(serviceInstance))
        }
      case SubscriberTerminated(subscriber) =>
        if (ctx.log.isDebugEnabled) {
          val keys = state.subscriptionsPerActor.getOrElse(subscriber, Set.empty)
          ctx.log.debug("Subscribed actor terminated: [{}] {}", keys.mkString(","), subscriber)
        }
        behavior(state.subscriberRemoved(subscriber))
    }

    Behaviors.receive[Any] { (ctx, msg) =>
      msg match {
        case cmd: Command         => onCommand(ctx, cmd)
        case cmd: InternalCommand => onInternal(ctx, cmd)
        case _                    => Behaviors.unhandled
      }
    }
  }
}
