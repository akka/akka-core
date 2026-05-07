/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.atomic.AtomicLong
import java.util.function.LongUnaryOperator

import akka.annotation.ApiMayChange
import akka.annotation.InternalApi

/**
 * A time source.
 */
@ApiMayChange
trait WallClock {
  def currentTimeMillis(): Long
}

object WallClock {

  /**
   * Always increasing time source. Based on `System.currentTimeMillis()` but
   * guaranteed to always increase for each invocation.
   */
  val AlwaysIncreasingClock: WallClock = new AlwaysIncreasingClock()
}

/**
 * INTERNAL API: Always increasing wall clock time.
 */
@InternalApi
private[akka] final class AlwaysIncreasingClock() extends AtomicLong with WallClock {

  override def currentTimeMillis(): Long = {
    val currentSystemTime = System.currentTimeMillis()
    updateAndGet {
      new LongUnaryOperator {
        override def applyAsLong(time: Long): Long = {
          if (currentSystemTime <= time) time + 1
          else currentSystemTime
        }
      }
    }
  }
}
