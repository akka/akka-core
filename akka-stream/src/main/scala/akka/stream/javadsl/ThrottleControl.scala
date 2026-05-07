/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import scala.jdk.DurationConverters._

import akka.annotation.InternalApi
import akka.stream.ThrottleMode
import akka.stream.impl.Throttle
import akka.stream.scaladsl

/**
 * Control the throttle rate from the outside of the stream, or share a common throttle rate
 * across several streams.
 */
final class ThrottleControl private[akka] (
    cost: Int,
    per: java.time.Duration,
    maximumBurst: Int,
    val mode: ThrottleMode) {
  def this(cost: Int, per: java.time.Duration) =
    this(cost, per, Throttle.AutomaticMaximumBurst, ThrottleMode.Shaping)

  private val delegate = new scaladsl.ThrottleControl(cost, per.toScala, maximumBurst, mode, shared = true)

  def update(cost: Int, per: java.time.Duration): Unit =
    delegate.update(cost, per.toScala)

  /**
   * Speed is limited to `cost/per`. This is the current cost.
   */
  def getCost(): Int =
    delegate.getCost()

  /**
   * Speed is limited to `cost/per`. This is the current per duration.
   */
  def getPer(): java.time.Duration =
    delegate.getPer().toJava

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def asScala: scaladsl.ThrottleControl =
    delegate

}
