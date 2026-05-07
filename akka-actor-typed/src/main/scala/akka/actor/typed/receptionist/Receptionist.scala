/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.receptionist

import scala.reflect.ClassTag

import akka.actor.typed.{ ActorRef, ActorSystem, Extension, ExtensionId, ExtensionSetup }
import akka.actor.typed.internal.receptionist._
import akka.annotation.DoNotInherit
import scala.jdk.CollectionConverters._

/**
 * Register and discover actors that implement a service with a protocol defined by a [[ServiceKey]].
 *
 * This class is not intended for user extension other than for test purposes (e.g.
 * stub implementation). More methods may be added in the future and that may break
 * such implementations.
 */
@DoNotInherit
abstract class Receptionist extends Extension {
  def ref: ActorRef[Receptionist.Command]
}

object ServiceKey {

  /**
   * Scala API: Creates a service key. The given ID should uniquely define a service with a given protocol.
   */
  def apply[T](id: String)(implicit classTag: ClassTag[T]): ServiceKey[T] =
    DefaultServiceKey(id, classTag.runtimeClass.getName)

  /**
   * Java API: Creates a service key. The given ID should uniquely define a service with a given protocol.
   */
  def create[T](clazz: Class[T], id: String): ServiceKey[T] =
    DefaultServiceKey(id, clazz.getName)

}

/**
 * A service key is an object that implements this trait for a given protocol
 * T, meaning that it signifies that the type T is the entry point into the
 * protocol spoken by that service (think of it as the set of first messages
 * that a client could send).
 *
 * Not for user extension, see factories in companion object: [[ServiceKey#create]] and [[ServiceKey#apply]]
 */
@DoNotInherit
abstract class ServiceKey[T] extends AbstractServiceKey { key =>
  type Protocol = T
  def id: String
  def asServiceKey: ServiceKey[T] = this

  /**
   * Scala API: Provides a type safe pattern match for listings.
   *
   * Using it for pattern match like this will return the reachable service instances:
   *
   * ```
   *   case MyServiceKey.Listing(reachable) =>
   * ```
   *
   * In a non-clustered `ActorSystem` this will always be all registered instances
   * for a service key. For a clustered environment services on nodes that have
   * been observed unreachable are not among these (note that they could have
   * become unreachable between this message being sent and the receiving actor
   * processing it).
   */
  object Listing {
    def unapply(l: Receptionist.Listing): Option[Set[ActorRef[T]]] =
      if (l.isForKey(key)) Some(l.serviceInstances(key))
      else None
  }

  /**
   * Scala API: Provides a type safe pattern match for registration acks
   */
  object Registered {
    def unapply(l: Receptionist.Registered): Option[ActorRef[T]] =
      if (l.isForKey(key)) Some(l.serviceInstance(key))
      else None
  }
}

/**
 * A Receptionist is an entry point into an Actor hierarchy where select Actors
 * publish their identity together with the protocols that they implement. Other
 * Actors need only know the Receptionist’s identity in order to be able to use
 * the services of the registered Actors.
 *
 * These are the messages (and the extension) for interacting with the receptionist.
 * The receptionist is easiest accessed through the system: [[ActorSystem.receptionist]]
 */
object Receptionist extends ExtensionId[Receptionist] {
  def createExtension(system: ActorSystem[_]): Receptionist = new ReceptionistImpl(system)
  def get(system: ActorSystem[_]): Receptionist = apply(system)

  /**
   * The set of commands accepted by a Receptionist.
   *
   * Not for user Extension
   */
  @DoNotInherit abstract class Command

  /**
   * `Register` message. Associate the given [[akka.actor.typed.ActorRef]] with the given [[ServiceKey]]
   * by sending this command to the [[Receptionist.ref]].
   *
   * Multiple registrations can be made for the same key. De-registration is implied by
   * the end of the referenced Actor’s lifecycle, but it can also be explicitly deregistered before termination.
   *
   * Registration will be acknowledged with the [[Registered]] message to the given replyTo actor
   * if there is one.
   */
  object Register {

    /**
     * Create a Register without Ack that the service was registered
     */
    def apply[T](key: ServiceKey[T], service: ActorRef[T]): Command =
      new ReceptionistMessages.Register[T](key, service, None)

    /**
     * Create a Register with an actor that will get an ack that the service was registered
     */
    def apply[T](key: ServiceKey[T], service: ActorRef[T], replyTo: ActorRef[Registered]): Command =
      new ReceptionistMessages.Register[T](key, service, Some(replyTo))
  }

  /**
   * Java API: A Register message without Ack that the service was registered.
   * Associate the given [[akka.actor.typed.ActorRef]] with the given [[ServiceKey]]
   * by sending this command to the [[Receptionist.ref]].
   *
   * Multiple registrations can be made for the same key. De-registration is implied by
   * the end of the referenced Actor’s lifecycle, but it can also be explicitly deregistered before termination.
   */
  def register[T](key: ServiceKey[T], service: ActorRef[T]): Command = Register(key, service)

  /**
   * Java API: A `Register` message with Ack that the service was registered.
   * Associate the given [[akka.actor.typed.ActorRef]] with the given [[ServiceKey]]
   * by sending this command to the [[Receptionist.ref]].
   *
   * Multiple registrations can be made for the same key. De-registration is implied by
   * the end of the referenced Actor’s lifecycle, but it can also be explicitly deregistered before termination.
   *
   * Registration will be acknowledged with the [[Registered]] message to the given replyTo actor.
   */
  def register[T](key: ServiceKey[T], service: ActorRef[T], replyTo: ActorRef[Registered]): Command =
    Register(key, service, replyTo)

  /**
   * Confirmation that the given [[akka.actor.typed.ActorRef]] has been associated with the [[ServiceKey]].
   *
   * You can use `key.Registered` for type-safe pattern matching.
   *
   * Not for user extension
   */
  @DoNotInherit
  trait Registered {

    def isForKey(key: ServiceKey[_]): Boolean

    /** Scala API */
    def key: ServiceKey[_]

    /** Java API */
    def getKey: ServiceKey[_] = key

    /**
     * Scala API
     *
     * Also, see [[ServiceKey.Listing]] for more convenient pattern matching
     */
    def serviceInstance[T](key: ServiceKey[T]): ActorRef[T]

    /** Java API */
    def getServiceInstance[T](key: ServiceKey[T]): ActorRef[T]
  }

  /**
   * Sent by the receptionist, available here for easier testing
   */
  object Registered {

    /**
     * Scala API
     */
    def apply[T](key: ServiceKey[T], serviceInstance: ActorRef[T]): Registered =
      new ReceptionistMessages.Registered(key, serviceInstance)

  }

  /**
   * Java API: Sent by the receptionist, available here for easier testing
   */
  def registered[T](key: ServiceKey[T], serviceInstance: ActorRef[T]): Registered =
    Registered(key, serviceInstance)

  /**
   * Remove association between the given [[akka.actor.typed.ActorRef]] and the given [[ServiceKey]].
   *
   * Deregistration can be acknowledged with the [[Deregistered]] message if the deregister message is created with a
   * `replyTo` actor.
   *
   * Note that getting the [[Deregistered]] confirmation does not mean all service key subscribers has seen the fact
   * that the actor has been deregistered yet (especially in a clustered context) so it will be possible that messages
   * sent to the actor in the role of service provider arrive even after getting the confirmation.
   */
  object Deregister {

    /**
     * Create a Deregister without Ack that the service was deregistered
     */
    def apply[T](key: ServiceKey[T], service: ActorRef[T]): Command =
      new ReceptionistMessages.Deregister[T](key, service, None)

    /**
     * Create a Deregister with an actor that will get an ack that the service was unregistered
     */
    def apply[T](key: ServiceKey[T], service: ActorRef[T], replyTo: ActorRef[Deregistered]): Command =
      new ReceptionistMessages.Deregister[T](key, service, Some(replyTo))
  }

  /**
   * Java API: A Deregister message without Ack that the service was unregistered
   */
  def deregister[T](key: ServiceKey[T], service: ActorRef[T]): Command = Deregister(key, service)

  /**
   * Java API: A Deregister message with an actor that will get an ack that the service was unregistered
   */
  def deregister[T](key: ServiceKey[T], service: ActorRef[T], replyTo: ActorRef[Deregistered]): Command =
    Deregister(key, service, replyTo)

  /**
   * Confirmation that the given [[akka.actor.typed.ActorRef]] no more associated with the [[ServiceKey]] in the local receptionist.
   * Note that this does not guarantee that subscribers has yet seen that the service is deregistered.
   *
   * Not for user extension
   */
  @DoNotInherit
  trait Deregistered {

    def isForKey(key: ServiceKey[_]): Boolean

    /** Scala API */
    def key: ServiceKey[_]

    /** Java API */
    def getKey: ServiceKey[_] = key

    /**
     * Scala API
     *
     * Also, see [[ServiceKey.Listing]] for more convenient pattern matching
     */
    def serviceInstance[T](key: ServiceKey[T]): ActorRef[T]

    /** Java API */
    def getServiceInstance[T](key: ServiceKey[T]): ActorRef[T]
  }

  /**
   * Sent by the receptionist, available here for easier testing
   */
  object Deregistered {

    /**
     * Scala API
     */
    def apply[T](key: ServiceKey[T], serviceInstance: ActorRef[T]): Deregistered =
      new ReceptionistMessages.Deregistered(key, serviceInstance)
  }

  /**
   * Java API: Sent by the receptionist, available here for easier testing
   */
  def deregistered[T](key: ServiceKey[T], serviceInstance: ActorRef[T]): Deregistered =
    Deregistered(key, serviceInstance)

  /**
   * `Subscribe` message. The given actor will subscribe to service updates when this command is sent to
   * the [[Receptionist.ref]]. When the set of instances registered for the given key changes
   * the subscriber will be sent a [[Listing]] with the new set of instances for that service.
   *
   * The subscription will be acknowledged by sending out a first [[Listing]]. The subscription automatically ends
   * with the termination of the subscriber.
   */
  object Subscribe {

    /**
     * Scala API:
     */
    def apply[T](key: ServiceKey[T], subscriber: ActorRef[Listing]): Command =
      new ReceptionistMessages.Subscribe(key, subscriber)

  }

  /**
   * Java API: `Subscribe` message. The given actor to service updates when this command is sent to
   * the [[Receptionist.ref]]. When the set of instances registered for the given key changes
   * the subscriber will be sent a [[Listing]] with the new set of instances for that service.
   *
   * The subscription will be acknowledged by sending out a first [[Listing]]. The subscription automatically ends
   * with the termination of the subscriber.
   */
  def subscribe[T](key: ServiceKey[T], subscriber: ActorRef[Listing]): Command = Subscribe(key, subscriber)

  /**
   * `Find` message. Query the Receptionist for a list of all Actors implementing the given
   * protocol at one point in time by sending this command to the [[Receptionist.ref]].
   */
  object Find {

    /** Scala API: */
    def apply[T](key: ServiceKey[T], replyTo: ActorRef[Listing]): Command =
      new ReceptionistMessages.Find(key, replyTo)

    /**
     * Special factory to make using Find with ask easier
     */
    def apply[T](key: ServiceKey[T]): ActorRef[Listing] => Command = ref => new ReceptionistMessages.Find(key, ref)
  }

  /**
   * Java API: `Find` message. Query the Receptionist for a list of all Actors implementing the given
   * protocol at one point in time by sending this command to the [[Receptionist.ref]].
   */
  def find[T](key: ServiceKey[T], replyTo: ActorRef[Listing]): Command =
    Find(key, replyTo)

  /**
   * Current listing of all Actors that implement the protocol given by the [[ServiceKey]].
   *
   * You can use `key.Listing` for type-safe pattern matching.
   *
   * Not for user extension.
   */
  @DoNotInherit
  trait Listing {

    /** Scala API */
    def key: ServiceKey[_]

    /** Java API */
    def getKey: ServiceKey[_] = key

    def isForKey(key: ServiceKey[_]): Boolean

    /**
     * Scala API: Return the reachable service instances.
     *
     * In a non-clustered `ActorSystem` this will always be all registered instances
     * for a service key.
     *
     * For a clustered `ActorSystem` it only contain services on nodes that
     * are not seen as unreachable (note that they could have still have become
     * unreachable between this message being sent and the receiving actor processing it).
     *
     * For a list including both reachable and unreachable instances see [[#allServiceInstances]]
     *
     * Also, see [[ServiceKey.Listing]] for more convenient pattern matching
     */
    def serviceInstances[T](key: ServiceKey[T]): Set[ActorRef[T]]

    /**
     * Java API: Return the reachable service instances.
     *
     * In a non-clustered `ActorSystem` this will always be all registered instances
     * for a service key.
     *
     * For a clustered `ActorSystem` it only contain services on nodes that has
     * are not seen as unreachable (note that they could have still have become
     * unreachable between this message being sent and the receiving actor processing it).
     *
     * For a list including both reachable and unreachable instances see [[#getAllServiceInstances]]
     */
    def getServiceInstances[T](key: ServiceKey[T]): java.util.Set[ActorRef[T]]

    /**
     * Scala API: Return both the reachable and the unreachable service instances.
     *
     * In a non-clustered `ActorSystem` this will always be the same as [[#serviceInstances]].
     *
     * For a clustered `ActorSystem` this include both services on nodes that are reachable
     * and nodes that are unreachable.
     */
    def allServiceInstances[T](key: ServiceKey[T]): Set[ActorRef[T]]

    /**
     * Java API: Return both the reachable and the unreachable service instances.
     *
     * In a non-clustered `ActorSystem` this will always be the same as [[#getServiceInstances]].
     *
     * For a clustered `ActorSystem` this include both services on nodes that are reachable
     * and nodes that are unreachable.
     */
    def getAllServiceInstances[T](key: ServiceKey[T]): java.util.Set[ActorRef[T]]

    /**
     * Returns `true` only if this `Listing` was sent triggered by new actors added or removed to the receptionist.
     * When `false` the event is only about reachability changes - meaning that the full set of actors
     * ([[#allServiceInstances]] or [[#getAllServiceInstances]]) is the same as the previous `Listing`.
     *
     * knowing this is useful for subscribers only concerned with [[#allServiceInstances]] or [[#getAllServiceInstances]]
     * that can then ignore `Listing`s related to reachability.
     *
     * In a non-clustered `ActorSystem` this will be `true` for all listings.
     * For `Find` queries and the initial listing for a `Subscribe` this will always be `true`.
     */
    def servicesWereAddedOrRemoved: Boolean

  }

  /**
   * Sent by the receptionist, available here for easier testing
   */
  object Listing {

    /** Scala API: */
    def apply[T](key: ServiceKey[T], serviceInstances: Set[ActorRef[T]]): Listing =
      apply(key, serviceInstances, serviceInstances, servicesWereAddedOrRemoved = true)

    /** Scala API: */
    def apply[T](
        key: ServiceKey[T],
        serviceInstances: Set[ActorRef[T]],
        allServiceInstances: Set[ActorRef[T]],
        servicesWereAddedOrRemoved: Boolean): Listing =
      new ReceptionistMessages.Listing[T](key, serviceInstances, allServiceInstances, servicesWereAddedOrRemoved)
  }

  /**
   * Java API: Sent by the receptionist, available here for easier testing
   */
  def listing[T](key: ServiceKey[T], serviceInstances: java.util.Set[ActorRef[T]]): Listing =
    Listing(key, serviceInstances.asScala.toSet)

  /**
   * Java API: Sent by the receptionist, available here for easier testing
   */
  def listing[T](
      key: ServiceKey[T],
      serviceInstances: java.util.Set[ActorRef[T]],
      allServiceInstances: java.util.Set[ActorRef[T]],
      servicesWereAddedOrRemoved: Boolean): Listing =
    Listing(key, serviceInstances.asScala.toSet, allServiceInstances.asScala.toSet, servicesWereAddedOrRemoved)

}

object ReceptionistSetup {
  def apply[T <: Extension](createExtension: ActorSystem[_] => Receptionist): ReceptionistSetup =
    new ReceptionistSetup(createExtension(_))

}

/**
 * Can be used in [[akka.actor.setup.ActorSystemSetup]] when starting the [[ActorSystem]]
 * to replace the default implementation of the [[Receptionist]] extension. Intended
 * for tests that need to replace extension with stub/mock implementations.
 */
final class ReceptionistSetup(createExtension: java.util.function.Function[ActorSystem[_], Receptionist])
    extends ExtensionSetup[Receptionist](Receptionist, createExtension)
