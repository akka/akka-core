/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.concurrent.duration.FiniteDuration

/**
 * Controlled clock for testing recency windows.
 * Durations are always in seconds.
 */
class TestClock extends Clock {
  private var time = 0L

  def tick(): Unit = time += 1

  override def currentTime(): Long = time

  override def earlierTime(duration: FiniteDuration): Long = currentTime() - duration.toSeconds
}
