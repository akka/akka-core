/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

import java.time.{ Duration => JDuration }
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TestClock extends Clock {

  @volatile private var _instant = roundToMillis(Instant.now())

  override def getZone: ZoneId = ZoneOffset.UTC

  override def withZone(zone: ZoneId): Clock =
    throw new UnsupportedOperationException("withZone not supported")

  override def instant(): Instant =
    _instant

  def setInstant(newInstant: Instant): Unit =
    _instant = roundToMillis(newInstant)

  def tick(duration: JDuration): Instant = {
    val newInstant = roundToMillis(_instant.plus(duration))
    _instant = newInstant
    newInstant
  }

  def tick(duration: FiniteDuration): Instant =
    tick(JDuration.ofMillis(duration.toMillis))

  private def roundToMillis(i: Instant): Instant = {
    // algo taken from java.time.Clock.tick
    val epochMilli = i.toEpochMilli
    Instant.ofEpochMilli(epochMilli - Math.floorMod(epochMilli, 1L))
  }

}
