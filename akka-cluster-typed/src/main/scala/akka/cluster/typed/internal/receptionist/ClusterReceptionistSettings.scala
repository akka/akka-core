/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed.internal.receptionist

import scala.concurrent.duration._
import scala.concurrent.duration.{ FiniteDuration, MILLISECONDS }

import com.typesafe.config.Config

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.cluster.ddata.Key.KeyId
import akka.cluster.ddata.Replicator
import akka.cluster.ddata.Replicator.WriteConsistency
import akka.cluster.ddata.ReplicatorSettings
import akka.util.Helpers.toRootLowerCase

/**
 * Internal API
 */
@InternalApi
private[akka] object ClusterReceptionistSettings {
  def apply(system: ActorSystem[_]): ClusterReceptionistSettings =
    apply(system.settings.config.getConfig("akka.cluster.typed.receptionist"))

  def apply(config: Config): ClusterReceptionistSettings = {
    val writeTimeout = 5.seconds // the timeout is not important
    val writeConsistency = {
      val key = "write-consistency"
      toRootLowerCase(config.getString(key)) match {
        case "local"    => Replicator.WriteLocal
        case "majority" => Replicator.WriteMajority(writeTimeout)
        case "all"      => Replicator.WriteAll(writeTimeout)
        case _          => Replicator.WriteTo(config.getInt(key), writeTimeout)
      }
    }

    val replicatorSettings = ReplicatorSettings(config.getConfig("distributed-data"))

    // Having durable store of entries does not make sense for receptionist, as registered
    // services does not survive a full cluster stop, make sure that it is disabled
    val replicatorSettingsWithoutDurableStore = replicatorSettings.withDurableKeys(Set.empty[KeyId])

    ClusterReceptionistSettings(
      writeConsistency,
      pruningInterval = config.getDuration("pruning-interval", MILLISECONDS).millis,
      pruneRemovedOlderThan = config.getDuration("prune-removed-older-than", MILLISECONDS).millis,
      config.getInt("distributed-key-count"),
      replicatorSettingsWithoutDurableStore)
  }
}

/**
 * Internal API
 */
@InternalApi
private[akka] case class ClusterReceptionistSettings(
    writeConsistency: WriteConsistency,
    pruningInterval: FiniteDuration,
    pruneRemovedOlderThan: FiniteDuration,
    distributedKeyCount: Int,
    replicatorSettings: ReplicatorSettings)
