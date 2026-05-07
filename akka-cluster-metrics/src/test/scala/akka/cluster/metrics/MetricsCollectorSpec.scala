/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

import akka.cluster.metrics.StandardMetrics._
import akka.testkit._

class MetricsCollectorSpec
    extends AkkaSpec(MetricsConfig.defaultEnabled)
    with ImplicitSender
    with MetricsCollectorFactory {

  val collector = createMetricsCollector

  "Metric must" must {

    "merge 2 metrics that are tracking the same metric" in {
      for (_ <- 1 to 20) {
        val sample1 = collector.sample().metrics
        val sample2 = collector.sample().metrics
        sample2.flatMap(latest =>
          sample1.collect {
            case peer if latest.sameAs(peer) =>
              val m = peer :+ latest
              m.value should ===(latest.value)
              m.isSmooth should ===(peer.isSmooth || latest.isSmooth)
              m
          })

        val sample3 = collector.sample().metrics
        val sample4 = collector.sample().metrics
        sample4.flatMap(latest =>
          sample3.collect {
            case peer if latest.sameAs(peer) =>
              val m = peer :+ latest
              m.value should ===(latest.value)
              m.isSmooth should ===(peer.isSmooth || latest.isSmooth)
              m
          })
      }
    }
  }

  "MetricsCollector" must {

    "not raise errors when attempting reflective code in apply" in {
      Try(createMetricsCollector).get should not be null
    }

    "collect accurate metrics for a node" in {
      val sample = collector.sample()
      val metrics = sample.metrics.collect { case m                        => (m.name, m.value) }
      val used = metrics.collectFirst { case (HeapMemoryUsed, b)           => b }
      val committed = metrics.collectFirst { case (HeapMemoryCommitted, b) => b }
      metrics.foreach {
        case (SystemLoadAverage, b)   => b.doubleValue should be >= 0.0
        case (Processors, b)          => b.intValue should be >= 0
        case (HeapMemoryUsed, b)      => b.longValue should be >= 0L
        case (HeapMemoryCommitted, b) => b.longValue should be > 0L
        case (HeapMemoryMax, b) =>
          b.longValue should be > 0L
          used.get.longValue should be <= b.longValue
          committed.get.longValue should be <= b.longValue
        case (CpuCombined, b) =>
          b.doubleValue should be <= 1.0
          b.doubleValue should be >= 0.0
        case (CpuStolen, b) =>
          b.doubleValue should be <= 1.0
          b.doubleValue should be >= 0.0
        case unexpected =>
          fail(s"Unexpected metric type $unexpected")

      }
    }

    "collect JMX metrics" in {
      // heap max may be undefined depending on the OS
      // systemLoadAverage is JMX when SIGAR not present, but
      // it's not present on all platforms
      val c = collector.asInstanceOf[JmxMetricsCollector]
      val heap = c.heapMemoryUsage
      c.heapUsed(heap).isDefined should ===(true)
      c.heapCommitted(heap).isDefined should ===(true)
      c.processors.isDefined should ===(true)
    }

    "collect 50 node metrics samples in an acceptable duration" taggedAs LongRunningTest in within(10 seconds) {
      (1 to 50).foreach { _ =>
        val sample = collector.sample()
        sample.metrics.size should be >= 3
        Thread.sleep(100)
      }
    }
  }
}
