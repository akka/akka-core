/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit.metrics

import com.codahale.metrics._
import com.codahale.metrics.jvm

private[akka] trait MemoryUsageSnapshotting extends MetricsPrefix {
  this: jvm.MemoryUsageGaugeSet =>

  // accessing metrics in order to not to duplicate mxBean access too much

  def getHeapSnapshot = {
    val metrics = getMetrics
    HeapMemoryUsage(
      metrics.get(key("heap-init")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("heap-used")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("heap-max")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("heap-committed")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("heap-usage")).asInstanceOf[RatioGauge].getValue)
  }

  def getTotalSnapshot = {
    val metrics = getMetrics
    TotalMemoryUsage(
      metrics.get(key("total-init")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("total-used")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("total-max")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("total-committed")).asInstanceOf[Gauge[Long]].getValue)
  }

  def getNonHeapSnapshot = {
    val metrics = getMetrics
    NonHeapMemoryUsage(
      metrics.get(key("non-heap-init")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("non-heap-used")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("non-heap-max")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("non-heap-committed")).asInstanceOf[Gauge[Long]].getValue,
      metrics.get(key("non-heap-usage")).asInstanceOf[RatioGauge].getValue)
  }

  private def key(k: String) = prefix.toString + "." + k

}

private[akka] case class TotalMemoryUsage(init: Long, used: Long, max: Long, committed: Long) {

  def diff(other: TotalMemoryUsage): TotalMemoryUsage =
    TotalMemoryUsage(
      this.init - other.init,
      this.used - other.used,
      this.max - other.max,
      this.committed - other.committed)

}

private[akka] case class HeapMemoryUsage(init: Long, used: Long, max: Long, committed: Long, usage: Double) {

  def diff(other: HeapMemoryUsage): HeapMemoryUsage =
    HeapMemoryUsage(
      this.init - other.init,
      this.used - other.used,
      this.max - other.max,
      this.committed - other.committed,
      this.usage - other.usage)
}

private[akka] case class NonHeapMemoryUsage(init: Long, used: Long, max: Long, committed: Long, usage: Double) {

  def diff(other: NonHeapMemoryUsage): NonHeapMemoryUsage =
    NonHeapMemoryUsage(
      this.init - other.init,
      this.used - other.used,
      this.max - other.max,
      this.committed - other.committed,
      this.usage - other.usage)
}
