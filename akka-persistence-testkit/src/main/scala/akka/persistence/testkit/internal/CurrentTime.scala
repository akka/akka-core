/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.internal

import java.util.concurrent.atomic.AtomicLong

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CurrentTime {
  private val previous = new AtomicLong(System.currentTimeMillis())

  /**
   * `System.currentTimeMillis` but always increasing.
   */
  def now(): Long = {
    val current = System.currentTimeMillis()
    val prev = previous.get()
    if (current > prev) {
      previous.compareAndSet(prev, current)
      current
    } else {
      previous.compareAndSet(prev, prev + 1)
      prev + 1
    }
  }
}
