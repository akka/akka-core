/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern.internal

import java.util.{ List => JList }

import akka.actor.ExtendedActorSystem
import akka.annotation.{ InternalApi, InternalStableApi }
import scala.jdk.CollectionConverters._

/**
 * Service Provider Interface (SPI) for collecting metrics from Circuit Breaker.
 *
 * Implementations must include a single constructor with two arguments: Circuit Breaker id
 * and [[ExtendedActorSystem]]. To setup your implementation, add a setting in your `application.conf`:
 *
 * {{{
 * akka.circuit-breaker.telemetry.implementations += com.example.MyMetrics
 * }}}
 */
@InternalStableApi
trait CircuitBreakerTelemetry {

  /**
   * Invoked when the circuit breaker transitions to the open state.
   */
  def onOpen(): Unit

  /**
   * Invoked when the circuit breaker transitions to the close state.
   */
  def onClose(): Unit

  /**
   * Invoked when the circuit breaker transitions to the half-open state after reset timeout.
   */
  def onHalfOpen(): Unit

  /**
   * Invoked for each successful call.
   *
   * @param elapsedNanos the elapsed duration of the call in nanoseconds
   */
  def onCallSuccess(elapsedNanos: Long): Unit

  /**
   * Invoked for each call when the future is completed with exception, except for
   * [[scala.concurrent.TimeoutException]] and [[akka.pattern.CircuitBreakerOpenException]]
   * that are handled by separate methods.
   *
   * @param elapsedNanos the elapsed duration of the call in nanoseconds
   */
  def onCallFailure(elapsedNanos: Long): Unit

  /**
   * Invoked for each call when the future is completed with `java.util.concurrent.TimeoutException`
   *
   * @param elapsedNanos the elapsed duration of the call in nanoseconds
   */
  def onCallTimeoutFailure(elapsedNanos: Long): Unit

  /**
   * Invoked for each call when the future is completed with
   * `akka.pattern.CircuitBreakerOpenException`
   */
  def onCallBreakerOpenFailure(): Unit

  /**
   * Called when the circuit breaker is removed, e.g. expired due to inactivity. It is also called
   * if the circuit breaker is re-configured, before calling [[CircuitBreakerTelemetryProvider#start]].
   */
  def stopped(): Unit
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CircuitBreakerTelemetryProvider {
  def start(breakerId: String, system: ExtendedActorSystem): CircuitBreakerTelemetry = {
    val configPath = "akka.circuit-breaker.telemetry.implementations"
    if (!system.settings.config.hasPath(configPath)) {
      CircuitBreakerNoopTelemetry
    } else {
      val telemetryFqcns: JList[String] = system.settings.config.getStringList(configPath)

      telemetryFqcns.size() match {
        case 0 =>
          CircuitBreakerNoopTelemetry
        case 1 =>
          val fqcn = telemetryFqcns.get(0)
          create(breakerId, system, fqcn)
        case _ =>
          new CircuitBreakerEnsembleTelemetry(telemetryFqcns.asScala.toSeq, breakerId, system)
      }
    }
  }

  def create(breakerId: String, system: ExtendedActorSystem, fqcn: String): CircuitBreakerTelemetry = {
    system.dynamicAccess
      .createInstanceFor[CircuitBreakerTelemetry](
        fqcn,
        List(classOf[String] -> breakerId, classOf[ExtendedActorSystem] -> system))
      .get
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CircuitBreakerNoopTelemetry extends CircuitBreakerTelemetry {
  override def onOpen(): Unit = ()

  override def onClose(): Unit = ()

  override def onHalfOpen(): Unit = ()

  override def onCallSuccess(elapsedNanos: Long): Unit = ()

  override def onCallFailure(elapsedNanos: Long): Unit = ()

  override def onCallTimeoutFailure(elapsedNanos: Long): Unit = ()

  override def onCallBreakerOpenFailure(): Unit = ()

  override def stopped(): Unit = ()
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CircuitBreakerEnsembleTelemetry(
    telemetryFqcns: Seq[String],
    breakerId: String,
    system: ExtendedActorSystem)
    extends CircuitBreakerTelemetry {

  private val telemetries = telemetryFqcns.map(fqcn => CircuitBreakerTelemetryProvider.create(breakerId, system, fqcn))

  override def onOpen(): Unit = telemetries.foreach(_.onOpen())

  override def onClose(): Unit = telemetries.foreach(_.onClose())

  override def onHalfOpen(): Unit = telemetries.foreach(_.onHalfOpen())

  override def onCallSuccess(elapsedNanos: Long): Unit = telemetries.foreach(_.onCallSuccess(elapsedNanos))

  override def onCallFailure(elapsedNanos: Long): Unit = telemetries.foreach(_.onCallFailure(elapsedNanos))

  override def onCallTimeoutFailure(elapsedNanos: Long): Unit =
    telemetries.foreach(_.onCallTimeoutFailure(elapsedNanos))

  override def onCallBreakerOpenFailure(): Unit = telemetries.foreach(_.onCallBreakerOpenFailure())

  override def stopped(): Unit = telemetries.foreach(_.stopped())
}
