/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata.typed.scaladsl

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import org.slf4j.LoggerFactory

import akka.actor.ExtendedActorSystem
import akka.actor.typed.{ ActorRef, ActorSystem, Extension, ExtensionId, Props }
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.cluster.{ ddata => dd }
import akka.cluster.Cluster
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.SelfUniqueAddress

object DistributedData extends ExtensionId[DistributedData] {
  def get(system: ActorSystem[_]): DistributedData = apply(system)

  override def createExtension(system: ActorSystem[_]): DistributedData =
    new DistributedData(system)

  /**
   * When interacting with the [[DistributedData.replicator]] from an actor the [[ReplicatorMessageAdapter]]
   * provides convenient methods that adapts the response messages to the requesting actor's message protocol.
   *
   * One `ReplicatorMessageAdapter` instance can be used for a given `ReplicatedData` type,
   * e.g. an `OrSet[String]`. Interaction with several [[akka.cluster.ddata.Key]]s can be used via the same adapter
   * but they must all be of the same `ReplicatedData` type. For interaction with several different
   * `ReplicatedData` types, e.g. an `OrSet[String]` and a `GCounter`, an adapter can be created
   * for each type.
   *
   * *Warning*: A `ReplicatorMessageAdapter` instance is not thread-safe and must only be used from a single actor
   * It must not be accessed from threads other than the ordinary actor message processing thread, such as
   * [[scala.concurrent.Future]] callbacks. It must not be shared between several actor instances.
   *
   * @param factory Factory of the `Behavior` for the actor that is using the `ReplicatorMessageAdapter`
   *
   * @tparam A Message type of the requesting actor.
   * @tparam B Type of the [[ReplicatedData]].
   */
  def withReplicatorMessageAdapter[A, B <: ReplicatedData](
      factory: ReplicatorMessageAdapter[A, B] => Behavior[A]): Behavior[A] = {
    Behaviors.setup[A] { context =>
      val distributedData = DistributedData(context.system)
      val replicatorAdapter =
        new ReplicatorMessageAdapter[A, B](context, distributedData.replicator, distributedData.unexpectedAskTimeout)
      factory(replicatorAdapter)
    }
  }

}

/**
 * Akka extension for convenient configuration and use of the
 * [[Replicator]]. Configuration settings are defined in the
 * `akka.cluster.ddata` section, see `reference.conf`.
 *
 * This is using the same underlying `Replicator` instance as
 * [[akka.cluster.ddata.DistributedData]] and that means that typed
 * and classic actors can share the same data.
 */
class DistributedData(system: ActorSystem[_]) extends Extension {
  import akka.actor.typed.scaladsl.adapter._

  private val settings: ReplicatorSettings = ReplicatorSettings(system)

  /** INTERNAL API */
  @InternalApi private[akka] val unexpectedAskTimeout: FiniteDuration =
    system.settings.config
      .getDuration("akka.cluster.ddata.typed.replicator-message-adapter-unexpected-ask-timeout")
      .toScala

  private val classicSystem = system.toClassic.asInstanceOf[ExtendedActorSystem]

  implicit val selfUniqueAddress: SelfUniqueAddress = dd.DistributedData(classicSystem).selfUniqueAddress

  /**
   * `ActorRef` of the [[Replicator]].
   *
   * @see [[DistributedData.withReplicatorMessageAdapter]]
   */
  val replicator: ActorRef[Replicator.Command] =
    if (isTerminated) {
      val log = LoggerFactory.getLogger(getClass)
      if (Cluster(classicSystem).isTerminated)
        log.warn("Replicator points to dead letters, because Cluster is terminated.")
      else
        log.warn(
          "Replicator points to dead letters. Make sure the cluster node has the proper role. " +
          "Node has roles [{}], Distributed Data is configured for roles [{}].",
          Cluster(classicSystem).selfRoles.mkString(","),
          settings.roles.mkString(","))
      system.deadLetters
    } else {
      val underlyingReplicator = dd.DistributedData(classicSystem).replicator
      val replicatorBehavior = Replicator.behavior(settings, underlyingReplicator)

      system.internalSystemActorOf(
        replicatorBehavior,
        ReplicatorSettings.name(system),
        Props.empty.withDispatcherFromConfig(settings.dispatcher))
    }

  /**
   * Returns true if this member is not tagged with the role configured for the replicas.
   */
  private def isTerminated: Boolean = dd.DistributedData(system.toClassic).isTerminated

}
