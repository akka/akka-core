/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl

import java.util.function.Predicate

import akka.actor.typed.{ Behavior, Props }
import akka.actor.typed.internal.BehaviorImpl.DeferredBehavior
import akka.actor.typed.internal.routing.{ GroupRouterBuilder, PoolRouterBuilder }
import akka.actor.typed.receptionist.ServiceKey
import akka.annotation.DoNotInherit

object Routers {

  /**
   * A router that will keep track of the available routees registered to the [[akka.actor.typed.receptionist.Receptionist]]
   * and route over those by random selection.
   *
   * In a clustered app this means the routees could live on any node in the cluster.
   * The current impl does not try to avoid sending messages to unreachable cluster nodes.
   *
   * Note that there is a delay between a routee stopping and this being detected by the receptionist, and another
   * before the group detects this, therefore it is best to deregister routees from the receptionist and not stop
   * until the deregistration is complete if you want to minimize the risk of lost messages.
   */
  def group[T](key: ServiceKey[T]): GroupRouter[T] =
    new GroupRouterBuilder[T](key)

  /**
   * Spawn `poolSize` children with the given `behavior` and forward messages to them using round robin.
   * If a child is stopped it is removed from the pool, to have children restart on failure use supervision.
   * When all children are stopped the pool stops itself. To stop the pool from the outside, use `ActorContext.stop`
   * from the parent actor.
   *
   * Note that if a child stops there is a slight chance that messages still get delivered to it, and get lost,
   * before the pool sees that the child stopped. Therefore it is best to _not_ stop children arbitrarily.
   */
  def pool[T](poolSize: Int)(behavior: Behavior[T]): PoolRouter[T] =
    new PoolRouterBuilder[T](poolSize, behavior)

}

/**
 * Provides builder style configuration options for group routers
 *
 * Not for user extension. Use [[Routers#group]] to create
 */
@DoNotInherit
abstract class GroupRouter[T] extends DeferredBehavior[T] {

  /**
   * Route messages by randomly selecting the routee from the available routees.
   *
   * This is the default for group routers.
   */
  def withRandomRouting(): GroupRouter[T]

  /**
   * Route messages by randomly selecting the routee from the available routees.
   *
   * This is the default for group routers.
   *
   * @param preferLocalRoutees if the value is false, all reachable routees will be used;
   *                           if the value is true and there are local routees, only local routees will be used.
   *                           if the value is true and there is no local routees, remote routees will be used.
   */
  def withRandomRouting(preferLocalRoutees: Boolean): GroupRouter[T]

  /**
   * Route messages by using round robin.
   *
   * Round robin gives fair routing where every available routee gets the same amount of messages as long as the set
   * of routees stays relatively stable, but may be unfair if the set of routees changes a lot.
   */
  def withRoundRobinRouting(): GroupRouter[T]

  /**
   * Route messages by using round robin.
   *
   * Round robin gives fair routing where every available routee gets the same amount of messages as long as the set
   * of routees stays relatively stable, but may be unfair if the set of routees changes a lot.
   *
   * @param preferLocalRoutees if the value is false, all reachable routees will be used;
   *                           if the value is true and there are local routees, only local routees will be used.
   *                           if the value is true and there is no local routees, remote routees will be used.
   */
  def withRoundRobinRouting(preferLocalRoutees: Boolean): GroupRouter[T]

  /**
   * Route messages by using consistent hashing.
   *
   * From wikipedia: Consistent hashing is based on mapping each object to a point on a circle
   * (or equivalently, mapping each object to a real angle). The system maps each available machine
   * (or other storage bucket) to many pseudo-randomly distributed points on the same circle.
   *
   * @param virtualNodesFactor This factor has to be greater or equal to 1. Assuming that the reader
   *                           knows what consistent hashing is
   *                           (if not, please refer: https://www.tom-e-white.com/2007/11/consistent-hashing.html or wiki).
   *                           This number is responsible for creating additional,
   *                           virtual addresses for a provided set of routees,
   *                           so that in the total number of points on hashing ring
   *                           will be equal to numberOfRoutees * virtualNodesFactor
   *                           (if virtualNodesFactor is equal to 1, then no additional points will be created).
   *
   *                           Those virtual nodes are being created by additionally rehashing routees
   *                           to evenly distribute them across hashing ring.
   *                           Consider increasing this number when you have a small number of routees.
   *                           For bigger loads one can aim in having around 100-200 total addresses.
   *
   *                           Please also note that setting this number to a too big value will cause
   *                           reasonable overhead when new routees will be added or old one removed.
   *
   * @param mapping            Hash key extractor. This function will be used in consistent hashing process.
   *                           Result of this operation should possibly uniquely distinguish messages.
   */
  def withConsistentHashingRouting(
      virtualNodesFactor: Int,
      mapping: java.util.function.Function[T, String]): GroupRouter[T]

}

/**
 * Provides builder style configuration options for pool routers
 *
 * Not for user extension. Use [[Routers#pool]] to create
 */
@DoNotInherit
abstract class PoolRouter[T] extends DeferredBehavior[T] {

  /**
   * Route messages by randomly selecting the routee from the available routees.
   *
   * Random routing makes it less likely that every `poolsize` message from a single producer ends up in the same
   * mailbox of a slow actor.
   */
  def withRandomRouting(): PoolRouter[T]

  /**
   * Route messages through round robin, providing a fair distribution of messages across the routees.
   *
   * Round robin gives fair routing where every available routee gets the same amount of messages
   *
   * This is the default for pool routers.
   */
  def withRoundRobinRouting(): PoolRouter[T]

  /**
   * Route messages by using consistent hashing.
   *
   * From wikipedia: Consistent hashing is based on mapping each object to a point on a circle
   * (or equivalently, mapping each object to a real angle). The system maps each available machine
   * (or other storage bucket) to many pseudo-randomly distributed points on the same circle.
   *
   * @param virtualNodesFactor This factor has to be greater or equal to 1. Assuming that the reader
   *                           knows what consistent hashing is
   *                           (if not, please refer: https://www.tom-e-white.com/2007/11/consistent-hashing.html or wiki).
   *                           This number is responsible for creating additional,
   *                           virtual addresses for a provided set of routees,
   *                           so that in the total number of points on hashing ring
   *                           will be equal to numberOfRoutees * virtualNodesFactor
   *                           (if virtualNodesFactor is equal to 1, then no additional points will be created).
   *
   *                           Those virtual nodes are being created by additionally rehashing routees
   *                           to evenly distribute them across hashing ring.
   *                           Consider increasing this number when you have a small number of routees.
   *                           For bigger loads one can aim in having around 100-200 total addresses.
   *
   * @param mapping            Hash key extractor. This function will be used in consistent hashing process.
   *                           Result of this operation should possibly uniquely distinguish messages.
   */
  def withConsistentHashingRouting(
      virtualNodesFactor: Int,
      mapping: java.util.function.Function[T, String]): PoolRouter[T]

  /**
   * Set a new pool size from the one set at construction
   */
  def withPoolSize(poolSize: Int): PoolRouter[T]

  /**
   * Set the props used to spawn the pool's routees
   */
  def withRouteeProps(routeeProps: Props): PoolRouter[T]

  /**
   * Any message that the predicate returns true for will be broadcast to all routees.
   */
  def withBroadcastPredicate(pred: Predicate[T]): PoolRouter[T]
}
