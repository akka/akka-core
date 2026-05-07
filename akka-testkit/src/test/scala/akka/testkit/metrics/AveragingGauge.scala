/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit.metrics

import java.util.concurrent.atomic.LongAdder

import com.codahale.metrics.Gauge

/**
 * Gauge which exposes the Arithmetic Mean of values given to it.
 *
 * Can be used to expose average of a series of values to `com.codahale.metrics.ScheduledReporter`.
 */
class AveragingGauge extends Gauge[Double] {

  private val sum = new LongAdder
  private val count = new LongAdder

  def add(n: Long): Unit = {
    count.increment()
    sum.add(n)
  }

  def add(ns: Seq[Long]): Unit = {
    // takes a mutable Seq on order to allow use with Array's
    count.add(ns.length)
    sum.add(ns.sum)
  }

  override def getValue: Double = sum.sum().toDouble / count.sum()
}
