/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.internal

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence

/**
 * INTERNAL API
 */
@InternalApi private[akka] object DurableStateSettings {

  def apply(
      system: ActorSystem[_],
      durableStateStorePluginId: String,
      customStashCapacity: Option[Int]): DurableStateSettings =
    apply(system.settings.config, durableStateStorePluginId, customStashCapacity)

  def apply(
      config: Config,
      durableStateStorePluginId: String,
      customStashCapacity: Option[Int]): DurableStateSettings = {
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

    val durableStateStoreConfig = durableStateStoreConfigFor(config, durableStateStorePluginId)
    val recoveryTimeout: FiniteDuration =
      durableStateStoreConfig.getDuration("recovery-timeout", TimeUnit.MILLISECONDS).millis

    val useContextLoggerForInternalLogging = typedConfig.getBoolean("use-context-logger-for-internal-logging")

    DurableStateSettings(
      stashCapacity = stashCapacity,
      stashOverflowStrategy,
      logOnStashing = logOnStashing,
      recoveryTimeout,
      durableStateStorePluginId,
      useContextLoggerForInternalLogging)
  }

  private def durableStateStoreConfigFor(config: Config, pluginId: String): Config = {
    def defaultPluginId = {
      val configPath = config.getString("akka.persistence.state.plugin")
      Persistence.verifyPluginConfigIsDefined(configPath, "Default DurableStateStore")
      configPath
    }

    val configPath = if (pluginId == "") defaultPluginId else pluginId
    Persistence.verifyPluginConfigExists(config, configPath, "DurableStateStore")
    config.getConfig(configPath).withFallback(config.getConfig("akka.persistence.state-plugin-fallback"))
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class DurableStateSettings(
    stashCapacity: Int,
    stashOverflowStrategy: StashOverflowStrategy,
    logOnStashing: Boolean,
    recoveryTimeout: FiniteDuration,
    durableStateStorePluginId: String,
    useContextLoggerForInternalLogging: Boolean) {

  require(
    durableStateStorePluginId != null,
    "DurableStateBehavior plugin id must not be null; use empty string for 'default' state store")
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
