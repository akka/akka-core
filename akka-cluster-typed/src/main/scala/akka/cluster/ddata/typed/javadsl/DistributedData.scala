/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata.typed.javadsl

import java.util.function.{ Function => JFunction }

import scala.jdk.DurationConverters._

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.ExtensionSetup
import akka.actor.typed.javadsl.Behaviors
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.SelfUniqueAddress

object DistributedData extends ExtensionId[DistributedData] {
  def get(system: ActorSystem[_]): DistributedData = apply(system)

  override def createExtension(system: ActorSystem[_]): DistributedData =
    new DistributedDataImpl(system)

  /**
   * When interacting with the [[DistributedData.replicator]] from an actor the [[ReplicatorMessageAdapter]]
   * provides convenient methods that adapts the response messages to the requesting actor's message protocol.
   *
   * One `ReplicatorMessageAdapter` instance can be used for a given `ReplicatedData` type,
   * e.g. an `OrSet<String>`. Interaction with several [[akka.cluster.ddata.Key]]s can be used via the same adapter
   * but they must all be of the same `ReplicatedData` type. For interaction with several different
   * `ReplicatedData` types, e.g. an `OrSet<String>` and a `GCounter`, an adapter can be created
   * for each type.
   *
   * *Warning*: A `ReplicatorMessageAdapter` instance is not thread-safe and must only be used from a single actor
   * It must not be accessed from threads other than the ordinary actor message processing thread, such as
   * [[java.util.concurrent.CompletionStage]] callbacks. It must not be shared between several actor instances.
   *
   * @param factory Factory of the `Behavior` for the actor that is using the `ReplicatorMessageAdapter`
   *
   * @tparam A Message type of the requesting actor.
   * @tparam B Type of the [[ReplicatedData]].
   */
  def withReplicatorMessageAdapter[A, B <: ReplicatedData](
      factory: JFunction[ReplicatorMessageAdapter[A, B], Behavior[A]]): Behavior[A] = {
    Behaviors.setup[A] { context =>
      val distributedData = akka.cluster.ddata.typed.scaladsl.DistributedData(context.getSystem)
      val replicatorAdapter =
        new ReplicatorMessageAdapter[A, B](
          context,
          distributedData.replicator,
          distributedData.unexpectedAskTimeout.toJava)
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
 *
 * This class is not intended for user extension other than for test purposes (e.g.
 * stub implementation). More methods may be added in the future and that may break
 * such implementations.
 */
@DoNotInherit
abstract class DistributedData extends Extension {

  /**
   * `ActorRef` of the [[Replicator]].
   *
   * @see [[DistributedData.withReplicatorMessageAdapter]]
   */
  def replicator: ActorRef[Replicator.Command]

  def selfUniqueAddress: SelfUniqueAddress
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class DistributedDataImpl(system: ActorSystem[_]) extends DistributedData {

  override val replicator: ActorRef[Replicator.Command] =
    akka.cluster.ddata.typed.scaladsl.DistributedData(system).replicator.narrow[Replicator.Command]

  override val selfUniqueAddress: SelfUniqueAddress =
    akka.cluster.ddata.typed.scaladsl.DistributedData(system).selfUniqueAddress

}

object DistributedDataSetup {
  def apply[T <: Extension](createExtension: ActorSystem[_] => DistributedData): DistributedDataSetup =
    new DistributedDataSetup(createExtension(_))

}

/**
 * Can be used in [[akka.actor.setup.ActorSystemSetup]] when starting the [[ActorSystem]]
 * to replace the default implementation of the [[DistributedData]] extension. Intended
 * for tests that need to replace extension with stub/mock implementations.
 */
final class DistributedDataSetup(createExtension: java.util.function.Function[ActorSystem[_], DistributedData])
    extends ExtensionSetup[DistributedData](DistributedData, createExtension)
