/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.{ DurationLong, MILLISECONDS }
import akka.actor.{
  ActorSystem,
  ClassicActorSystemProvider,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider
}
import akka.annotation.InternalApi
import akka.pattern.internal.CircuitBreakerTelemetryProvider
import com.typesafe.config.Config

import scala.jdk.CollectionConverters._

/**
 * Companion object providing factory methods for Circuit Breaker which runs callbacks in caller's thread
 */
object CircuitBreakersRegistry extends ExtensionId[CircuitBreakersRegistry] with ExtensionIdProvider {

  /**
   * Is used by Akka to instantiate the Extension identified by this ExtensionId,
   * internal use only.
   */
  override def createExtension(system: ExtendedActorSystem): CircuitBreakersRegistry =
    new CircuitBreakersRegistry(system)

  /**
   * Returns the canonical ExtensionId for this Extension
   */
  override def lookup: ExtensionId[_ <: Extension] = CircuitBreakersRegistry

  /**
   * Returns an instance of the extension identified by this ExtensionId instance.
   * Java API
   */
  override def get(system: ActorSystem): CircuitBreakersRegistry = super.get(system)

  /**
   * Returns an instance of the extension identified by this ExtensionId instance.
   * Java API
   */
  override def get(system: ClassicActorSystemProvider): CircuitBreakersRegistry = super.get(system)
}

/**
 * A CircuitBreakersPanel is a central point collecting all circuit breakers in Akka.
 */
final class CircuitBreakersRegistry(system: ExtendedActorSystem) extends Extension {

  private val breakers = new ConcurrentHashMap[String, CircuitBreaker]

  private val config = system.settings.config.getConfig("akka.circuit-breaker")
  private val defaultBreakerConfig = config.getConfig("default")

  private def createCircuitBreaker(id: String): CircuitBreaker = {
    val breakerConfig =
      if (config.hasPath(id)) config.getConfig(id).withFallback(defaultBreakerConfig)
      else defaultBreakerConfig

    breakerFromConfig(id, breakerConfig)
  }

  /** INTERNAL API */
  @InternalApi
  private[akka] def getOrCreate(id: String, config: Config): CircuitBreaker =
    breakers.computeIfAbsent(id, _ => breakerFromConfig(id, config))

  private[akka] def get(id: String): CircuitBreaker =
    breakers.computeIfAbsent(id, createCircuitBreaker)

  private def breakerFromConfig(id: String, config: Config): CircuitBreaker = {
    val maxFailures = config.getInt("max-failures")
    val callTimeout = config.getDuration("call-timeout", MILLISECONDS).millis
    val resetTimeout = config.getDuration("reset-timeout", MILLISECONDS).millis
    val maxResetTimeout = config.getDuration("max-reset-timeout", MILLISECONDS).millis
    val exponentialBackoffFactor = config.getDouble("exponential-backoff")
    val randomFactor = config.getDouble("random-factor")

    val allowExceptions: Set[String] = config.getStringList("exception-allowlist").asScala.toSet

    val telemetry = CircuitBreakerTelemetryProvider.start(id, system)
    new CircuitBreaker(
      system.scheduler,
      maxFailures,
      callTimeout,
      resetTimeout,
      maxResetTimeout,
      exponentialBackoffFactor,
      randomFactor,
      allowExceptions,
      telemetry)(system.dispatcher)
  }
}
