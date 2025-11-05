/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics.protobuf

import akka.actor.{ Address, ExtendedActorSystem }
import akka.cluster.MemberStatus
import akka.cluster.TestMember
import akka.cluster.metrics._
import akka.testkit.AkkaSpec

class MessageSerializerSpec extends AkkaSpec("""
     akka.actor.provider = cluster
     akka.remote.artery.canonical.port = 0
  """) {

  val serializer = new MessageSerializer(system.asInstanceOf[ExtendedActorSystem])

  def checkSerialization(obj: AnyRef): Unit = {
    val blob = serializer.toBinary(obj)
    val ref = serializer.fromBinary(blob, serializer.manifest(obj))
    obj match {
      case _ =>
        ref should ===(obj)
    }

  }

  import MemberStatus._

  val a1 = TestMember(Address("akka", "sys", "a", 2552), Joining, Set.empty)
  val b1 = TestMember(Address("akka", "sys", "b", 2552), Up, Set("r1"))
  val c1 = TestMember(Address("akka", "sys", "c", 2552), Leaving, Set("r2"))
  val d1 = TestMember(Address("akka", "sys", "d", 2552), Exiting, Set("r1", "r2"))
  val e1 = TestMember(Address("akka", "sys", "e", 2552), Down, Set("r3"))
  val f1 = TestMember(Address("akka", "sys", "f", 2552), Removed, Set("r2", "r3"))

  "ClusterMessages" must {

    "be serializable" in {

      val metricsGossip = MetricsGossip(
        Set(
          NodeMetrics(a1.address, 4711, Set(Metric("foo", 1.2, None))),
          NodeMetrics(
            b1.address,
            4712,
            Set(
              Metric("foo", 2.1, Some(EWMA(value = 100.0, alpha = 0.18))),
              Metric("bar1", Double.MinPositiveValue, None),
              Metric("bar2", Float.MaxValue, None),
              Metric("bar3", Int.MaxValue, None),
              Metric("bar4", Long.MaxValue, None),
              Metric("bar5", BigInt(Long.MaxValue), None)))))

      checkSerialization(MetricsGossipEnvelope(a1.address, metricsGossip, true))

    }
  }

  "AdaptiveLoadBalancingPool" must {
    "be serializable" in {
      val simplePool = AdaptiveLoadBalancingPool()
      checkSerialization(simplePool)

      val complicatedPool = AdaptiveLoadBalancingPool(
        metricsSelector =
          MixMetricsSelector(Vector(CpuMetricsSelector, HeapMetricsSelector, SystemLoadAverageMetricsSelector)),
        nrOfInstances = 7,
        routerDispatcher = "my-dispatcher",
        usePoolDispatcher = true)
      checkSerialization(complicatedPool)
    }
  }
}
