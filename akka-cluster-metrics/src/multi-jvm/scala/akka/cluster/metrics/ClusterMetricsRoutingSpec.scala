/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import java.lang.management.ManagementFactory

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.MultiNodeClusterSpec
import akka.cluster.routing.ClusterRouterPool
import akka.cluster.routing.ClusterRouterPoolSettings
import akka.pattern.ask
import akka.remote.testkit.MultiNodeConfig
import akka.routing.ActorRefRoutee
import akka.routing.FromConfig
import akka.routing.GetRoutees
import akka.routing.Routees
import akka.serialization.jackson.CborSerializable
import akka.testkit.{ DefaultTimeout, ImplicitSender, LongRunningTest }
import akka.testkit.GHExcludeTest

object AdaptiveLoadBalancingRouterConfig extends MultiNodeConfig {

  class Echo extends Actor {
    def receive = {
      case _ => sender() ! Reply(Cluster(context.system).selfAddress)
    }
  }

  class Memory extends Actor with ActorLogging {
    var usedMemory: Array[Array[Int]] = _
    def receive = {
      case AllocateMemory =>
        val heap = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage
        // getMax can be undefined (-1)
        val max = math.max(heap.getMax, heap.getCommitted)
        val used = heap.getUsed
        log.info("used heap before: [{}] bytes, of max [{}]", used, heap.getMax)
        // allocate 70% of free space
        val allocateBytes = (0.7 * (max - used)).toInt
        val numberOfArrays = allocateBytes / 1024
        usedMemory = Array.ofDim(numberOfArrays, 248) // each 248 element Int array will use ~ 1 kB
        log.info("used heap after: [{}] bytes", ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed)
        sender() ! "done"
    }
  }

  case object AllocateMemory
  final case class Reply(address: Address) extends CborSerializable

  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")

  def nodeList = Seq(node1, node2, node3)

  // Extract individual sigar library for every node.
  nodeList.foreach { role =>
    nodeConfig(role) {
      ConfigFactory.parseString(
        s"akka.cluster.metrics.native-library-extract-folder=$${user.dir}/target/native/" + role.name)
    }
  }

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      # Enable metrics estension.
      akka.extensions=["akka.cluster.metrics.ClusterMetricsExtension"]

      akka.cluster.failure-detector.acceptable-heartbeat-pause = 10s

      # Use rapid metrics collection.
      akka.cluster.metrics {
        collector {
          sample-interval = 1s
          gossip-interval = 1s
          moving-average-half-life = 2s
        }
      }

      # Use metrics extension routing.
      akka.actor.deployment {
        /router3 = {
          router = cluster-metrics-adaptive-pool
          metrics-selector = cpu
          nr-of-instances = 9
        }
        /router4 = {
          router = cluster-metrics-adaptive-pool
          metrics-selector = "akka.cluster.metrics.TestCustomMetricsSelector"
          nr-of-instances = 10
          cluster {
            enabled = on
            max-nr-of-instances-per-node = 2
          }
        }
      }
    """))
      .withFallback(MultiNodeClusterSpec.clusterConfig))

}

class TestCustomMetricsSelector(@nowarn("msg=never used") config: Config) extends MetricsSelector {
  override def weights(nodeMetrics: Set[NodeMetrics]): Map[Address, Int] = Map.empty
}

class AdaptiveLoadBalancingRouterMultiJvmNode1 extends AdaptiveLoadBalancingRouterSpec
class AdaptiveLoadBalancingRouterMultiJvmNode2 extends AdaptiveLoadBalancingRouterSpec
class AdaptiveLoadBalancingRouterMultiJvmNode3 extends AdaptiveLoadBalancingRouterSpec

@nowarn
abstract class AdaptiveLoadBalancingRouterSpec
    extends MultiNodeClusterSpec(AdaptiveLoadBalancingRouterConfig)
    with RedirectLogging
    with ImplicitSender
    with DefaultTimeout {
  import AdaptiveLoadBalancingRouterConfig._

  def currentRoutees(router: ActorRef) =
    Await.result(router ? GetRoutees, timeout.duration).asInstanceOf[Routees].routees

  def receiveReplies(expectedReplies: Int): Map[Address, Int] = {
    val zero = Map.empty[Address, Int] ++ roles.map(address(_) -> 0)
    (receiveWhile(5 seconds, messages = expectedReplies) {
      case Reply(address) => address
    }).foldLeft(zero) {
      case (replyMap, address) => replyMap + (address -> (replyMap(address) + 1))
    }
  }

  /**
   * Fills in self address for local ActorRef
   */
  def fullAddress(actorRef: ActorRef): Address = actorRef.path.address match {
    case Address(_, _, None, None) => cluster.selfAddress
    case a                         => a
  }

  def startRouter(name: String): ActorRef = {
    val router = system.actorOf(
      ClusterRouterPool(
        local = AdaptiveLoadBalancingPool(HeapMetricsSelector),
        settings = ClusterRouterPoolSettings(totalInstances = 10, maxInstancesPerNode = 1, allowLocalRoutees = true))
        .props(Props[Echo]()),
      name)
    // it may take some time until router receives cluster member events
    awaitAssert { currentRoutees(router).size should ===(roles.size) }
    val routees = currentRoutees(router)
    routees.collect { case ActorRefRoutee(ref) => fullAddress(ref) }.toSet should ===(roles.map(address).toSet)
    router
  }

  val metricsSettings = ClusterMetricsSettings(cluster.system.settings.config)

  def metricsAwait(factor: Int = 10): Unit = Thread.sleep(metricsSettings.CollectorSampleInterval.toMillis * factor)

  "A cluster with a AdaptiveLoadBalancingRouter" must {
    "start cluster nodes" taggedAs LongRunningTest in {
      awaitClusterUp(roles: _*)
      enterBarrier("after-1")
    }

    "use all nodes in the cluster when not overloaded" taggedAs LongRunningTest in {
      runOn(node1) {
        val router1 = startRouter("router1")

        // collect some metrics before we start
        metricsAwait()

        val iterationCount = 100
        (1 to iterationCount).foreach { _ =>
          router1 ! "hit"
          // wait a while between each message, since metrics is collected periodically
          Thread.sleep(10)
        }

        val replies = receiveReplies(iterationCount)

        replies(node1) should be > (0)
        replies(node2) should be > (0)
        replies(node3) should be > (0)
        replies.values.sum should ===(iterationCount)

      }

      enterBarrier("after-2")
    }

    // Excluded on GH Actions: https://github.com/akka/akka-core/issues/30486
    "prefer node with more free heap capacity" taggedAs (LongRunningTest, GHExcludeTest) in {
      System.gc()
      enterBarrier("gc")

      runOn(node2) {
        within(20.seconds) {
          system.actorOf(Props[Memory](), "memory") ! AllocateMemory
          expectMsg("done")
        }
      }
      enterBarrier("heap-allocated")

      runOn(node1) {
        val router2 = startRouter("router2")

        // collect some metrics before we start
        metricsAwait()

        val iterationCount = 3000
        (1 to iterationCount).foreach { _ =>
          router2 ! "hit"
        }

        val replies = receiveReplies(iterationCount)

        replies(node3) should be > (replies(node2))
        replies.values.sum should ===(iterationCount)

      }

      enterBarrier("after-3")
    }

    // Excluded on GH Actions: https://github.com/akka/akka-core/issues/30486
    "create routees from configuration" taggedAs (LongRunningTest, GHExcludeTest) in {
      runOn(node1) {
        val router3 = system.actorOf(FromConfig.props(Props[Memory]()), "router3")
        // it may take some time until router receives cluster member events
        awaitAssert { currentRoutees(router3).size should ===(9) }
        val routees = currentRoutees(router3)
        routees.collect { case ActorRefRoutee(ref) => fullAddress(ref) }.toSet should ===(Set(address(node1)))
      }
      enterBarrier("after-4")
    }

    // Excluded on GH Actions: https://github.com/akka/akka-core/issues/30486
    "create routees from cluster.enabled configuration" taggedAs (LongRunningTest, GHExcludeTest) in {
      runOn(node1) {
        val router4 = system.actorOf(FromConfig.props(Props[Memory]()), "router4")
        // it may take some time until router receives cluster member events
        awaitAssert { currentRoutees(router4).size should ===(6) }
        val routees = currentRoutees(router4)
        routees.collect { case ActorRefRoutee(ref) => fullAddress(ref) }.toSet should ===(
          Set(address(node1), address(node2), address(node3)))
      }
      enterBarrier("after-5")
    }
  }
}
