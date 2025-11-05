/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.javadsl

import java.util.Optional
import java.util.function.{ Function => JFunction }
import java.util.function.IntFunction

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.typed.ShardedDaemonProcessCommand
import akka.cluster.sharding.typed.ShardedDaemonProcessContext
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings

object ShardedDaemonProcess {
  def get(system: ActorSystem[_]): ShardedDaemonProcess =
    akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess(system).asJava
}

/**
 * This extension runs a pre set number of actors in a cluster.
 *
 * The typical use case is when you have a task that can be divided in a number of workers, each doing a
 * sharded part of the work, for example consuming the read side events from Akka Persistence through
 * tagged events where each tag decides which consumer that should consume the event.
 *
 * Each named set needs to be started on all the nodes of the cluster on start up.
 *
 * The processes are spread out across the cluster, when the cluster topology changes the processes may be stopped
 * and started anew on a new node to rebalance them.
 *
 * Not for user extension.
 */
@DoNotInherit
abstract class ShardedDaemonProcess {

  /**
   * Start a specific number of actors that is then kept alive in the cluster.
   * @param behaviorFactory Given a unique id of `0` until `numberOfInstance` create the behavior for that actor.
   */
  def init[T](
      messageClass: Class[T],
      name: String,
      numberOfInstances: Int,
      behaviorFactory: IntFunction[Behavior[T]]): Unit

  /**
   * Start a specific number of actors that is then kept alive in the cluster.
   *
   * @param behaviorFactory Given a unique id of `0` until `numberOfInstance` create the behavior for that actor.
   * @param stopMessage sent to the actors when they need to stop because of a rebalance across the nodes of the cluster
   *                    or cluster shutdown.
   */
  def init[T](
      messageClass: Class[T],
      name: String,
      numberOfInstances: Int,
      behaviorFactory: IntFunction[Behavior[T]],
      stopMessage: T): Unit

  /**
   * Start a specific number of actors, each with a unique numeric id in the set, that is then kept alive in the cluster.
   * @param behaviorFactory Given a unique id of `0` until `numberOfInstance` create the behavior for that actor.
   * @param stopMessage if defined sent to the actors when they need to stop because of a rebalance across the nodes of the cluster
   *                    or cluster shutdown.
   */
  def init[T](
      messageClass: Class[T],
      name: String,
      numberOfInstances: Int,
      behaviorFactory: IntFunction[Behavior[T]],
      settings: ShardedDaemonProcessSettings,
      stopMessage: Optional[T]): Unit

  /**
   * Start a specific number of actors, each with a unique numeric id in the set, that is then kept alive in the cluster.
   * @param behaviorFactory Given a unique id of `0` until `numberOfInstance` create the behavior for that actor.
   * @param stopMessage if defined sent to the actors when they need to stop because of a rebalance across the nodes of the cluster,
   *                    rescale or cluster shutdown.
   * @param shardAllocationStrategy if defined used by entities to control the shard allocation
   */
  def init[T](
      messageClass: Class[T],
      name: String,
      numberOfInstances: Int,
      behaviorFactory: IntFunction[Behavior[T]],
      settings: ShardedDaemonProcessSettings,
      stopMessage: Optional[T],
      shardAllocationStrategy: Optional[ShardAllocationStrategy]): Unit

  /**
   * Start a specific number of actors, each with a unique numeric id in the set, that is then kept alive in the cluster.
   * The number of processing actors can be rescaled by interacting with the returned actor.
   *
   * @param behaviorFactory Given a unique id of `0` until `numberOfInstance` and total number of processes, create the behavior for that actor.
   */
  @ApiMayChange
  def initWithContext[T](
      messageClass: Class[T],
      name: String,
      initialNumberOfInstances: Int,
      behaviorFactory: JFunction[ShardedDaemonProcessContext, Behavior[T]]): ActorRef[ShardedDaemonProcessCommand]

  /**
   * Start a specific number of actors, each with a unique numeric id in the set, that is then kept alive in the cluster.
   * The number of processing actors can be rescaled by interacting with the returned actor.
   *
   * @param behaviorFactory Given a unique id of `0` until `numberOfInstance` and total number of processes, create the behavior for that actor.
   * @param stopMessage     Sent to the actors when they need to stop because of a rebalance across the nodes of the cluster
   *                        or cluster shutdown.
   */
  @ApiMayChange
  def initWithContext[T](
      messageClass: Class[T],
      name: String,
      initialNumberOfInstances: Int,
      behaviorFactory: JFunction[ShardedDaemonProcessContext, Behavior[T]],
      settings: ShardedDaemonProcessSettings,
      stopMessage: Optional[T]): ActorRef[ShardedDaemonProcessCommand]

  /**
   * Start a specific number of actors, each with a unique numeric id in the set, that is then kept alive in the cluster.
   *
   * @param behaviorFactory         Given a unique sharded daemon process context containing the total number of workers and the id
   *                                the specific worker being started, create the behavior for that actor.
   * @param stopMessage             If defined: sent to the actors when they need to stop because of a rebalance across the nodes of the cluster,
   *                                rescale or cluster shutdown.
   * @param shardAllocationStrategy If defined: used by entities to control the shard allocation
   *
   */
  @ApiMayChange
  def initWithContext[T](
      messageClass: Class[T],
      name: String,
      numberOfInstances: Int,
      behaviorFactory: JFunction[ShardedDaemonProcessContext, Behavior[T]],
      settings: ShardedDaemonProcessSettings,
      stopMessage: Optional[T],
      shardAllocationStrategy: Optional[ShardAllocationStrategy]): ActorRef[ShardedDaemonProcessCommand]
}
