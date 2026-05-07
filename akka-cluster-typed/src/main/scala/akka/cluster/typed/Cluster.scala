/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import java.util.concurrent.CompletionStage

import scala.collection.immutable
import scala.concurrent.Future

import akka.actor.Address
import akka.actor.typed.{ ActorRef, ActorSystem, Extension, ExtensionId }
import akka.actor.typed.ExtensionSetup
import akka.annotation.DoNotInherit
import akka.cluster._
import akka.cluster.ClusterEvent.{ ClusterDomainEvent, CurrentClusterState }
import akka.cluster.typed.internal.AdapterClusterImpl
import akka.japi.Util
import akka.util.Version

/**
 * Messages for subscribing to changes in the cluster state
 *
 * Not intended for user extension.
 */
@DoNotInherit
sealed trait ClusterStateSubscription

/**
 * Subscribe to cluster state changes. The initial state of the cluster will be sent as
 * a "replay" of the subscribed events.
 *
 * @param subscriber A subscriber that will receive events until it is unsubscribed or stops
 * @param eventClass The type of events to subscribe to, can be individual event types such as
 *                   `ReachabilityEvent` or one of the common supertypes, such as `MemberEvent` to get
 *                   all the subtypes of events.
 */
final case class Subscribe[A <: ClusterDomainEvent](subscriber: ActorRef[A], eventClass: Class[A])
    extends ClusterStateSubscription

object Subscribe {

  /**
   * Java API
   */
  def create[A <: ClusterDomainEvent](subscriber: ActorRef[A], eventClass: Class[A]): Subscribe[A] =
    Subscribe(subscriber, eventClass)
}

/**
 * Subscribe to this node being up, after sending this event the subscription is automatically
 * cancelled. If the node is already up the event is also sent to the subscriber. If the node was up
 * but is no more because it left or is leaving the cluster, no event is sent and the subscription
 * request is ignored.
 *
 * Note: Only emitted for the typed cluster.
 */
final case class SelfUp(currentClusterState: CurrentClusterState) extends ClusterDomainEvent

/**
 * Subscribe to this node being removed from the cluster. If the node was already removed from the cluster
 * when this subscription is created it will be responded to immediately from the subscriptions actor.
 *
 * Note: Only emitted for the typed cluster.
 */
final case class SelfRemoved(previousStatus: MemberStatus) extends ClusterDomainEvent

final case class Unsubscribe[T](subscriber: ActorRef[T]) extends ClusterStateSubscription
final case class GetCurrentState(recipient: ActorRef[CurrentClusterState]) extends ClusterStateSubscription

/**
 * Not intended for user extension.
 */
@DoNotInherit
sealed trait ClusterCommand

/**
 * Try to join this cluster node with the node specified by 'address'.
 *
 * An actor system can only join a cluster once. Additional attempts will be ignored.
 * When it has successfully joined it must be restarted to be able to join another
 * cluster or to join the same cluster again.
 *
 * The name of the [[akka.actor.ActorSystem]] must be the same for all members of a
 * cluster.
 */
final case class Join(address: Address) extends ClusterCommand {
  address.checkHostCharacters()
}

object Join {

  /**
   * Java API
   */
  def create(address: Address): Join = Join(address)
}

/**
 * Scala API: Join the specified seed nodes without defining them in config.
 * Especially useful from tests when Addresses are unknown before startup time.
 *
 * An actor system can only join a cluster once. Additional attempts will be ignored.
 * When it has successfully joined it must be restarted to be able to join another
 * cluster or to join the same cluster again.
 */
final case class JoinSeedNodes(seedNodes: immutable.Seq[Address]) extends ClusterCommand {
  seedNodes.foreach(_.checkHostCharacters())

  /**
   * Java API: Join the specified seed nodes without defining them in config.
   * Especially useful from tests when Addresses are unknown before startup time.
   *
   * An actor system can only join a cluster once. Additional attempts will be ignored.
   * When it has successfully joined it must be restarted to be able to join another
   * cluster or to join the same cluster again.
   *
   * Creates a defensive copy of the list to ensure immutability.
   */
  def this(seedNodes: java.util.List[Address]) = this(Util.immutableSeq(seedNodes))

}

/**
 * Scala API: If the `appVersion` is read from an external system (e.g. Kubernetes) it can be defined after
 * system startup but before joining by completing the `appVersion` `Future`. In that case,
 * `SetAppVersionLater` should be sent before [[Join]] or [[JoinSeedNodes]] It's fine to send
 * `Join` or `JoinSeedNodes` immediately afterwards (before the `Future` is completed. The join will
 * then wait for the `appVersion` to be completed.
 */
final case class SetAppVersionLater(appVersion: Future[Version]) extends ClusterCommand

object SetAppVersionLater {

  /**
   * Java API: If the `appVersion` is read from an external system (e.g. Kubernetes) it can be defined after
   * system startup but before joining by completing the `appVersion` `CompletionStage`. In that case,
   * `SetAppVersionLater` should be sent before [[Join]] or [[JoinSeedNodes]] It's fine to send
   * `Join` or `JoinSeedNodes` immediately afterwards (before the `CompletionStage` is completed. The join will
   * then wait for the `appVersion` to be completed.
   */
  def create(appVersion: CompletionStage[Version]): SetAppVersionLater = {
    import scala.jdk.FutureConverters._
    SetAppVersionLater(appVersion.asScala)
  }
}

/**
 * Send command to issue state transition to LEAVING for the node specified by 'address'.
 * The member will go through the status changes [[MemberStatus]] `Leaving` (not published to
 * subscribers) followed by [[MemberStatus]] `Exiting` and finally [[MemberStatus]] `Removed`.
 *
 * Note that this command can be issued to any member in the cluster, not necessarily the
 * one that is leaving. The cluster extension, but not the actor system or JVM, of the
 * leaving member will be shutdown after the leader has changed status of the member to
 * Exiting. Thereafter the member will be removed from the cluster. Normally this is
 * handled automatically, but in case of network failures during this process it might
 * still be necessary to set the node’s status to Down in order to complete the removal.
 */
final case class Leave(address: Address) extends ClusterCommand

object Leave {

  /**
   * Java API
   */
  def create(address: Address): Leave = Leave(address)
}

/**
 * Send command to DOWN the node specified by 'address'.
 *
 * When a member is considered by the failure detector to be unreachable the leader is not
 * allowed to perform its duties, such as changing status of new joining members to 'Up'.
 * The status of the unreachable member must be changed to 'Down', which can be done with
 * this method.
 */
final case class Down(address: Address) extends ClusterCommand

/**
 * Initiate a full cluster shutdown. This stops:
 * - New members joining the cluster
 * - New rebalances in Cluster Sharding
 * - Singleton handovers
 *
 * However, it does not stop the nodes. That is expected to be signalled externally.
 *
 * Not for user extension
 */
@DoNotInherit sealed trait PrepareForFullClusterShutdown extends ClusterCommand

case object PrepareForFullClusterShutdown extends PrepareForFullClusterShutdown {

  /**
   * Java API
   */
  def prepareForFullClusterShutdown(): PrepareForFullClusterShutdown = this
}

/**
 * Akka Typed Cluster API entry point
 */
object Cluster extends ExtensionId[Cluster] {

  def createExtension(system: ActorSystem[_]): Cluster = new AdapterClusterImpl(system)

  /**
   * Java API
   */
  def get(system: ActorSystem[_]): Cluster = apply(system)
}

/**
 * This class is not intended for user extension other than for test purposes (e.g.
 * stub implementation). More methods may be added in the future and that may break
 * such implementations.
 */
@DoNotInherit
abstract class Cluster extends Extension {

  /** Details about this cluster node itself */
  def selfMember: Member

  /** Returns true if this cluster instance has be shutdown. */
  def isTerminated: Boolean

  /** Current snapshot state of the cluster. */
  def state: CurrentClusterState

  /**
   * @return an actor that allows for subscribing to messages when the cluster state changes
   */
  def subscriptions: ActorRef[ClusterStateSubscription]

  /**
   * @return an actor that accepts commands to join, leave and down nodes in a cluster
   */
  def manager: ActorRef[ClusterCommand]

}

object ClusterSetup {
  def apply[T <: Extension](createExtension: ActorSystem[_] => Cluster): ClusterSetup =
    new ClusterSetup(createExtension(_))

}

/**
 * Can be used in [[akka.actor.setup.ActorSystemSetup]] when starting the [[ActorSystem]]
 * to replace the default implementation of the [[Cluster]] extension. Intended
 * for tests that need to replace extension with stub/mock implementations.
 */
final class ClusterSetup(createExtension: java.util.function.Function[ActorSystem[_], Cluster])
    extends ExtensionSetup[Cluster](Cluster, createExtension)
