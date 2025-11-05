/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import com.typesafe.config.Config

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence

/**
 * INTERNAL API
 */
@InternalApi private[akka] object EventSourcedSettings {

  def apply(system: ActorSystem[_], journalPluginId: String, snapshotPluginId: String): EventSourcedSettings =
    apply(system.settings.config, journalPluginId, snapshotPluginId, None)

  def apply(
      system: ActorSystem[_],
      journalPluginId: String,
      snapshotPluginId: String,
      customStashCapacity: Option[Int]): EventSourcedSettings =
    apply(system.settings.config, journalPluginId, snapshotPluginId, customStashCapacity)

  def apply(
      config: Config,
      journalPluginId: String,
      snapshotPluginId: String,
      customStashCapacity: Option[Int]): EventSourcedSettings = {
    val typedConfig = config.getConfig("akka.persistence.typed")

    val stashOverflowStrategy = typedConfig.getString("stash-overflow-strategy").toLowerCase match {
      case "drop" => StashOverflowStrategy.Drop
      case "fail" => StashOverflowStrategy.Fail
      case unknown =>
        throw new IllegalArgumentException(s"Unknown value for stash-overflow-strategy: [$unknown]")
    }

    val stashCapacity = customStashCapacity.getOrElse(typedConfig.getInt("stash-capacity"))
    require(stashCapacity > 0, "stash-capacity MUST be > 0, unbounded buffering is not supported.")

    val logOnStashing = typedConfig.getBoolean("log-stashing")

    val journalConfig = journalConfigFor(config, journalPluginId)
    val recoveryEventTimeout: FiniteDuration =
      journalConfig.getDuration("recovery-event-timeout", TimeUnit.MILLISECONDS).millis

    val useContextLoggerForInternalLogging = typedConfig.getBoolean("use-context-logger-for-internal-logging")

    Persistence.verifyPluginConfigExists(config, snapshotPluginId, "Snapshot store")

    EventSourcedSettings(
      stashCapacity = stashCapacity,
      stashOverflowStrategy,
      logOnStashing = logOnStashing,
      recoveryEventTimeout,
      journalPluginId,
      snapshotPluginId,
      useContextLoggerForInternalLogging)
  }

  private def journalConfigFor(config: Config, journalPluginId: String): Config = {
    def defaultJournalPluginId = {
      val configPath = config.getString("akka.persistence.journal.plugin")
      Persistence.verifyPluginConfigIsDefined(configPath, "Default journal")
      configPath
    }

    val configPath = if (journalPluginId == "") defaultJournalPluginId else journalPluginId
    Persistence.verifyPluginConfigExists(config, configPath, "Journal")
    config.getConfig(configPath).withFallback(config.getConfig(Persistence.JournalFallbackConfigPath))
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class EventSourcedSettings(
    stashCapacity: Int,
    stashOverflowStrategy: StashOverflowStrategy,
    logOnStashing: Boolean,
    recoveryEventTimeout: FiniteDuration,
    journalPluginId: String,
    snapshotPluginId: String,
    useContextLoggerForInternalLogging: Boolean) {

  require(journalPluginId != null, "journal plugin id must not be null; use empty string for 'default' journal")
  require(
    snapshotPluginId != null,
    "snapshot plugin id must not be null; use empty string for 'default' snapshot store")

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] sealed trait StashOverflowStrategy

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object StashOverflowStrategy {
  case object Drop extends StashOverflowStrategy
  case object Fail extends StashOverflowStrategy
}
