/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit.metrics

import java.util

import com.codahale.metrics._
import com.codahale.metrics.jvm
import com.codahale.metrics.jvm.MemoryUsageGaugeSet
import org.scalatest.Notifying

/**
 * User Land operations provided by the [[MetricsKit]].
 *
 * Extracted to give easy overview of user-API detached from MetricsKit internals.
 */
private[akka] trait MetricsKitOps extends MetricKeyDSL {
  this: MetricsKit with Notifying =>

  type MetricKeyType = MetricKeyDSL#MetricKey

  /** Simple thread-safe counter, backed by `java.util.concurrent.LongAdder` so can pretty efficiently work even when hit by multiple threads */
  def counter(key: MetricKeyType): Counter = registry.counter(key.toString)

  /** Simple averaging Gauge, which exposes an arithmetic mean of the values added to it. */
  def averageGauge(key: MetricKeyType): AveragingGauge = getOrRegister(key.toString, new AveragingGauge)

  /**
   * Used to measure timing of known number of operations over time.
   * While not being the most precise, it allows to measure a coarse op/s without injecting counters to the measured operation (potentially hot-loop).
   *
   * Do not use for short running pieces of code.
   */
  def timedWithKnownOps[T](key: MetricKeyType, ops: Long)(run: => T): T = {
    val c = getOrRegister(key.toString, new KnownOpsInTimespanTimer(expectedOps = ops))
    try run
    finally c.stop()
  }

  /**
   * Use when measuring for 9x'th percentiles as well as min / max / mean values.
   *
   * Backed by [[HdrHistogram]].
   *
   * @param unitString just for human readable output, during console printing
   */
  def hdrHistogram(
      key: MetricKeyType,
      highestTrackableValue: Long,
      numberOfSignificantValueDigits: Int,
      unitString: String = ""): HdrHistogram =
    getOrRegister(
      (key / "hdr-histogram").toString,
      new HdrHistogram(highestTrackableValue, numberOfSignificantValueDigits, unitString))

  /**
   * Use when measuring for 9x'th percentiles as well as min / max / mean values.
   *
   * Backed by codahale `ExponentiallyDecayingReservoir`.
   */
  def histogram(key: MetricKeyType): Histogram = {
    registry.histogram((key / "histogram").toString)
  }

  def forceGcEnabled: Boolean = true

  /** Yet another delegate to `System.gc()` */
  def gc(): Unit = {
    if (forceGcEnabled)
      System.gc()
  }

  /**
   * Enable memory measurements - will be logged by `ScheduledReporter`s if enabled.
   * Must not be triggered multiple times - pass around the `MemoryUsageSnapshotting` if you need to measure different points.
   *
   * Also allows to `MemoryUsageSnapshotting.getHeapSnapshot` to obtain memory usage numbers at given point in time.
   */
  def measureMemory(key: MetricKeyType): MemoryUsageGaugeSet with MemoryUsageSnapshotting = {
    val gaugeSet = new jvm.MemoryUsageGaugeSet() with MemoryUsageSnapshotting {
      val prefix = key / "mem"
    }

    registry.registerAll(gaugeSet)
    gaugeSet
  }

  /** Enable GC measurements */
  def measureGc(key: MetricKeyType) =
    registry.registerAll(new jvm.GarbageCollectorMetricSet() with MetricsPrefix { val prefix = key / "gc" })

  /** Enable File Descriptor measurements */
  def measureFileDescriptors(key: MetricKeyType) =
    registry.registerAll(new FileDescriptorMetricSet() with MetricsPrefix { val prefix = key / "file-descriptors" })

}

private[metrics] trait MetricsPrefix extends MetricSet {
  def prefix: MetricKeyDSL#MetricKey

  abstract override def getMetrics: util.Map[String, Metric] = {
    // does not have to be fast, is only called once during registering registry
    import scala.jdk.CollectionConverters._
    (super.getMetrics.asScala.map { case (k, v) => (prefix / k).toString -> v }).asJava
  }
}
