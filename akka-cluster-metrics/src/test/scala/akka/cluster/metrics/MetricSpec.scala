/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import java.lang.System.{ currentTimeMillis => newTimestamp }

import scala.annotation.nowarn
import scala.util.Failure

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.cluster.metrics.StandardMetrics._
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender

@nowarn
class MetricNumericConverterSpec extends AnyWordSpec with Matchers with MetricNumericConverter {

  "MetricNumericConverter" must {

    "convert" in {
      convertNumber(0).isLeft should ===(true)
      convertNumber(1).left.get should ===(1)
      convertNumber(1L).isLeft should ===(true)
      convertNumber(0.0).isRight should ===(true)
    }

    "define a new metric" in {
      val metric = Metric.create(HeapMemoryUsed, 256L, decayFactor = Some(0.18)).get
      metric.name should ===(HeapMemoryUsed)
      metric.value should ===(256L)
      metric.isSmooth should ===(true)
      metric.smoothValue should ===(256.0 +- 0.0001)
    }

    "define an undefined value with a None " in {
      Metric.create("x", -1, None).isDefined should ===(false)
      Metric.create("x", java.lang.Double.NaN, None).isDefined should ===(false)
      Metric.create("x", Failure(new RuntimeException), None).isDefined should ===(false)
    }

    "recognize whether a metric value is defined" in {
      defined(0) should ===(true)
      defined(0.0) should ===(true)
    }

    "recognize whether a metric value is not defined" in {
      defined(-1) should ===(false)
      defined(-1.0) should ===(false)
      defined(Double.NaN) should ===(false)
    }
  }
}

class NodeMetricsSpec extends AnyWordSpec with Matchers {

  val node1 = Address("akka", "sys", "a", 2554)
  val node2 = Address("akka", "sys", "a", 2555)

  "NodeMetrics must" must {

    "return correct result for 2 'same' nodes" in {
      (NodeMetrics(node1, 0).sameAs(NodeMetrics(node1, 0))) should ===(true)
    }

    "return correct result for 2 not 'same' nodes" in {
      (NodeMetrics(node1, 0).sameAs(NodeMetrics(node2, 0))) should ===(false)
    }

    "merge 2 NodeMetrics by most recent" in {
      val sample1 = NodeMetrics(node1, 1, Set(Metric.create("a", 10, None), Metric.create("b", 20, None)).flatten)
      val sample2 = NodeMetrics(node1, 2, Set(Metric.create("a", 11, None), Metric.create("c", 30, None)).flatten)

      val merged = sample1.merge(sample2)
      merged.timestamp should ===(sample2.timestamp)
      merged.metric("a").map(_.value) should ===(Some(11))
      merged.metric("b").map(_.value) should ===(Some(20))
      merged.metric("c").map(_.value) should ===(Some(30))
    }

    "not merge 2 NodeMetrics if master is more recent" in {
      val sample1 = NodeMetrics(node1, 1, Set(Metric.create("a", 10, None), Metric.create("b", 20, None)).flatten)
      val sample2 = NodeMetrics(node1, 0, Set(Metric.create("a", 11, None), Metric.create("c", 30, None)).flatten)

      val merged = sample1.merge(sample2) // older and not same
      merged.timestamp should ===(sample1.timestamp)
      merged.metrics should ===(sample1.metrics)
    }

    "update 2 NodeMetrics by most recent" in {
      val sample1 = NodeMetrics(node1, 1, Set(Metric.create("a", 10, None), Metric.create("b", 20, None)).flatten)
      val sample2 = NodeMetrics(node1, 2, Set(Metric.create("a", 11, None), Metric.create("c", 30, None)).flatten)

      val updated = sample1.update(sample2)

      updated.metrics.size should ===(3)
      updated.timestamp should ===(sample2.timestamp)
      updated.metric("a").map(_.value) should ===(Some(11))
      updated.metric("b").map(_.value) should ===(Some(20))
      updated.metric("c").map(_.value) should ===(Some(30))
    }

    "update 3 NodeMetrics with ewma applied" in {
      import MetricsConfig._

      val decay = Some(defaultDecayFactor)
      val epsilon = 0.001

      val sample1 = NodeMetrics(node1, 1, Set(Metric.create("a", 1, decay), Metric.create("b", 4, decay)).flatten)
      val sample2 = NodeMetrics(node1, 2, Set(Metric.create("a", 2, decay), Metric.create("c", 5, decay)).flatten)
      val sample3 = NodeMetrics(node1, 3, Set(Metric.create("a", 3, decay), Metric.create("d", 6, decay)).flatten)

      val updated = sample1.update(sample2).update(sample3)

      updated.metrics.size should ===(4)
      updated.timestamp should ===(sample3.timestamp)

      updated.metric("a").map(_.value).get should ===(3)
      updated.metric("b").map(_.value).get should ===(4)
      updated.metric("c").map(_.value).get should ===(5)
      updated.metric("d").map(_.value).get should ===(6)

      updated.metric("a").map(_.smoothValue).get should ===(1.512 +- epsilon)
      updated.metric("b").map(_.smoothValue).get should ===(4.000 +- epsilon)
      updated.metric("c").map(_.smoothValue).get should ===(5.000 +- epsilon)
      updated.metric("d").map(_.smoothValue).get should ===(6.000 +- epsilon)
    }

  }
}

class MetricsGossipSpec
    extends AkkaSpec(MetricsConfig.defaultEnabled)
    with ImplicitSender
    with MetricsCollectorFactory {

  val collector = createMetricsCollector

  /**
   * sometimes Sigar will not be able to return a valid value (NaN and such) so must ensure they
   * have the same Metric types
   */
  def newSample(previousSample: Set[Metric]): Set[Metric] = {
    // Metric.equals is based on name equality
    collector.sample().metrics.filter(previousSample.contains) ++ previousSample
  }

  "A MetricsGossip" must {
    "add new NodeMetrics" in {
      val m1 = NodeMetrics(Address("akka", "sys", "a", 2554), newTimestamp, collector.sample().metrics)
      val m2 = NodeMetrics(Address("akka", "sys", "a", 2555), newTimestamp, collector.sample().metrics)

      m1.metrics.size should be > 3
      m2.metrics.size should be > 3

      val g1 = MetricsGossip.empty :+ m1
      g1.nodes.size should ===(1)
      g1.nodeMetricsFor(m1.address).map(_.metrics) should ===(Some(m1.metrics))

      val g2 = g1 :+ m2
      g2.nodes.size should ===(2)
      g2.nodeMetricsFor(m1.address).map(_.metrics) should ===(Some(m1.metrics))
      g2.nodeMetricsFor(m2.address).map(_.metrics) should ===(Some(m2.metrics))
    }

    "merge peer metrics" in {
      val m1 = NodeMetrics(Address("akka", "sys", "a", 2554), newTimestamp, collector.sample().metrics)
      val m2 = NodeMetrics(Address("akka", "sys", "a", 2555), newTimestamp, collector.sample().metrics)

      val g1 = MetricsGossip.empty :+ m1 :+ m2
      g1.nodes.size should ===(2)

      val m2Updated = m2.copy(metrics = newSample(m2.metrics), timestamp = m2.timestamp + 1000)
      val g2 = g1 :+ m2Updated // merge peers
      g2.nodes.size should ===(2)
      g2.nodeMetricsFor(m1.address).map(_.metrics) should ===(Some(m1.metrics))
      g2.nodeMetricsFor(m2.address).map(_.metrics) should ===(Some(m2Updated.metrics))
      g2.nodes.collect { case peer if peer.address == m2.address => peer.timestamp should ===(m2Updated.timestamp) }
    }

    "merge an existing metric set for a node and update node ring" in {
      val m1 = NodeMetrics(Address("akka", "sys", "a", 2554), newTimestamp, collector.sample().metrics)
      val m2 = NodeMetrics(Address("akka", "sys", "a", 2555), newTimestamp, collector.sample().metrics)
      val m3 = NodeMetrics(Address("akka", "sys", "a", 2556), newTimestamp, collector.sample().metrics)
      val m2Updated = m2.copy(metrics = newSample(m2.metrics), timestamp = m2.timestamp + 1000)

      val g1 = MetricsGossip.empty :+ m1 :+ m2
      val g2 = MetricsGossip.empty :+ m3 :+ m2Updated

      g1.nodes.map(_.address) should ===(Set(m1.address, m2.address))

      // should contain nodes 1,3, and the most recent version of 2
      val mergedGossip = g1.merge(g2)
      mergedGossip.nodes.map(_.address) should ===(Set(m1.address, m2.address, m3.address))
      mergedGossip.nodeMetricsFor(m1.address).map(_.metrics) should ===(Some(m1.metrics))
      mergedGossip.nodeMetricsFor(m2.address).map(_.metrics) should ===(Some(m2Updated.metrics))
      mergedGossip.nodeMetricsFor(m3.address).map(_.metrics) should ===(Some(m3.metrics))
      mergedGossip.nodes.foreach(_.metrics.size should be > 3)
      mergedGossip.nodeMetricsFor(m2.address).map(_.timestamp) should ===(Some(m2Updated.timestamp))
    }

    "get the current NodeMetrics if it exists in the local nodes" in {
      val m1 = NodeMetrics(Address("akka", "sys", "a", 2554), newTimestamp, collector.sample().metrics)
      val g1 = MetricsGossip.empty :+ m1
      g1.nodeMetricsFor(m1.address).map(_.metrics) should ===(Some(m1.metrics))
    }

    "remove a node if it is no longer Up" in {
      val m1 = NodeMetrics(Address("akka", "sys", "a", 2554), newTimestamp, collector.sample().metrics)
      val m2 = NodeMetrics(Address("akka", "sys", "a", 2555), newTimestamp, collector.sample().metrics)

      val g1 = MetricsGossip.empty :+ m1 :+ m2
      g1.nodes.size should ===(2)
      val g2 = g1.remove(m1.address)
      g2.nodes.size should ===(1)
      g2.nodes.exists(_.address == m1.address) should ===(false)
      g2.nodeMetricsFor(m1.address) should ===(None)
      g2.nodeMetricsFor(m2.address).map(_.metrics) should ===(Some(m2.metrics))
    }

    "filter nodes" in {
      val m1 = NodeMetrics(Address("akka", "sys", "a", 2554), newTimestamp, collector.sample().metrics)
      val m2 = NodeMetrics(Address("akka", "sys", "a", 2555), newTimestamp, collector.sample().metrics)

      val g1 = MetricsGossip.empty :+ m1 :+ m2
      g1.nodes.size should ===(2)
      val g2 = g1.filter(Set(m2.address))
      g2.nodes.size should ===(1)
      g2.nodes.exists(_.address == m1.address) should ===(false)
      g2.nodeMetricsFor(m1.address) should ===(None)
      g2.nodeMetricsFor(m2.address).map(_.metrics) should ===(Some(m2.metrics))
    }
  }
}

class MetricValuesSpec extends AkkaSpec(MetricsConfig.defaultEnabled) with MetricsCollectorFactory {
  import akka.cluster.metrics.StandardMetrics._

  val collector = createMetricsCollector

  val node1 = NodeMetrics(Address("akka", "sys", "a", 2554), 1, collector.sample().metrics)
  val node2 = NodeMetrics(Address("akka", "sys", "a", 2555), 1, collector.sample().metrics)

  val nodes: Seq[NodeMetrics] = {
    (1 to 100).foldLeft(List(node1, node2)) { (nodes, _) =>
      nodes.map { n =>
        n.copy(
          metrics = collector
            .sample()
            .metrics
            .flatMap(latest =>
              n.metrics.collect {
                case streaming if latest.sameAs(streaming) => streaming :+ latest
              }))
      }
    }
  }

  "NodeMetrics.MetricValues" must {
    "extract expected metrics for load balancing" in {
      val stream1 = node2.metric(HeapMemoryCommitted).get.value.longValue
      val stream2 = node1.metric(HeapMemoryUsed).get.value.longValue
      stream1 should be >= stream2
    }

    "extract expected MetricValue types for load balancing" in {
      nodes.foreach { node =>
        node match {
          case HeapMemory(_, _, used, committed, _) =>
            used should be > 0L
            committed should be >= used
            // Documentation java.lang.management.MemoryUsage says that committed <= max,
            // but in practice that is not always true (we have seen it happen). Therefore
            // we don't check the heap max value in this test.
            // extract is the java api
            StandardMetrics.extractHeapMemory(node) should not be null
          case other =>
            fail(other.toString)
        }

        node match {
          case Cpu(_, _, systemLoadAverageOption, cpuCombinedOption, cpuStolenOption, processors) =>
            processors should be > 0
            if (systemLoadAverageOption.isDefined)
              systemLoadAverageOption.get should be >= 0.0
            if (cpuCombinedOption.isDefined) {
              cpuCombinedOption.get should be <= 1.0
              cpuCombinedOption.get should be >= 0.0
            }
            if (cpuStolenOption.isDefined) {
              cpuStolenOption.get should be <= 1.0
              cpuStolenOption.get should be >= 0.0
            }
            // extract is the java api
            StandardMetrics.extractCpu(node) should not be null
          case other =>
            fail(other.toString)
        }
      }
    }
  }

}
