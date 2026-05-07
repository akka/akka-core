/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata.typed.scaladsl

import com.typesafe.config.Config

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.cluster.{ ddata => dd }

/**
 * @see [[akka.cluster.ddata.ReplicatorSettings]].
 */
object ReplicatorSettings {

  /**
   * Create settings from the default configuration
   * `akka.cluster.distributed-data`.
   */
  def apply(system: ActorSystem[_]): ReplicatorSettings =
    dd.ReplicatorSettings(system.toClassic)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.distributed-data`.
   */
  def apply(config: Config): ReplicatorSettings =
    dd.ReplicatorSettings(config)

  /**
   * INTERNAL API
   * The name of the actor used in DistributedData extensions.
   */
  @InternalApi private[akka] def name(system: ActorSystem[_]): String =
    dd.ReplicatorSettings.name(system.toClassic, Some("typed"))
}
