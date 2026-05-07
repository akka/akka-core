/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit.metrics

import java.lang.management.{ ManagementFactory, OperatingSystemMXBean }
import java.util

import com.codahale.metrics.{ Gauge, Metric, MetricSet }
import com.codahale.metrics.MetricRegistry._
import com.codahale.metrics.jvm.FileDescriptorRatioGauge

import scala.jdk.CollectionConverters._

/**
 * MetricSet exposing number of open and maximum file descriptors used by the JVM process.
 */
private[akka] class FileDescriptorMetricSet(os: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean)
    extends MetricSet {

  override def getMetrics: util.Map[String, Metric] = {
    Map[String, Metric](name("file-descriptors", "open") -> new Gauge[Long] {
      override def getValue: Long = invoke("getOpenFileDescriptorCount")
    }, name("file-descriptors", "max") -> new Gauge[Long] {
      override def getValue: Long = invoke("getMaxFileDescriptorCount")
    }, name("file-descriptors", "ratio") -> new FileDescriptorRatioGauge(os)).asJava
  }

  private def invoke(name: String): Long = {
    val method = os.getClass.getDeclaredMethod(name)
    method.setAccessible(true)
    method.invoke(os).asInstanceOf[Long]
  }
}
