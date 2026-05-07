/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import com.typesafe.config.Config

import akka.actor.ActorSystem
import akka.annotation.DoNotInherit
import akka.stream.impl.streamref.StreamRefSettingsImpl

object StreamRefSettings {

  /** Java API */
  @deprecated(
    "Use attributes on the Runnable graph or change the defaults in configuration, see migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    since = "2.6.0")
  def create(system: ActorSystem): StreamRefSettings = apply(system)

  /** Scala API */
  @deprecated(
    "Use attributes on the Runnable graph or change the defaults in configuration, see migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    since = "2.6.0")
  def apply(system: ActorSystem): StreamRefSettings = {
    apply(system.settings.config.getConfig("akka.stream.materializer.stream-ref"))
  }

  /** Java API */
  @deprecated(
    "Use attributes on the Runnable graph or change the defaults in configuration, see migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    since = "2.6.0")
  def create(c: Config): StreamRefSettings = apply(c)

  /** Scala API */
  @deprecated(
    "Use attributes on the Runnable graph or change the defaults in configuration, see migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    since = "2.6.0")
  def apply(c: Config): StreamRefSettings = {
    StreamRefSettingsImpl(
      bufferCapacity = c.getInt("buffer-capacity"),
      demandRedeliveryInterval = c.getDuration("demand-redelivery-interval", TimeUnit.MILLISECONDS).millis,
      subscriptionTimeout = c.getDuration("subscription-timeout", TimeUnit.MILLISECONDS).millis,
      finalTerminationSignalDeadline = c.getDuration("final-termination-signal-deadline", TimeUnit.MILLISECONDS).millis)
  }
}

/**
 * Settings specific to [[SourceRef]] and [[SinkRef]].
 * More detailed documentation about each of the settings is available in `reference.conf`.
 */
@DoNotInherit
trait StreamRefSettings {
  @deprecated("Use attribute 'StreamRefAttributes.BufferCapacity' to read the concrete setting value", "2.6.0")
  def bufferCapacity: Int
  @deprecated(
    "Use attribute 'StreamRefAttributes.DemandRedeliveryInterval' to read the concrete setting value",
    "2.6.0")
  def demandRedeliveryInterval: FiniteDuration
  @deprecated("Use attribute 'StreamRefAttributes.SubscriptionTimeout' to read the concrete setting value", "2.6.0")
  def subscriptionTimeout: FiniteDuration
  @deprecated(
    "Use attribute 'StreamRefAttributes.FinalTerminationSignalDeadline' to read the concrete setting value",
    "2.6.0")
  def finalTerminationSignalDeadline: FiniteDuration

  // --- with... methods ---

  def withBufferCapacity(value: Int): StreamRefSettings
  def withDemandRedeliveryInterval(value: FiniteDuration): StreamRefSettings
  def withSubscriptionTimeout(value: FiniteDuration): StreamRefSettings
  def withTerminationReceivedBeforeCompletionLeeway(value: FiniteDuration): StreamRefSettings
}
