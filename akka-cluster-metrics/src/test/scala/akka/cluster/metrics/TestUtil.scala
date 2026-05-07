/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import java.io.Closeable
import java.util.logging.LogManager

import scala.language.postfixOps

import org.scalatestplus.mockito.MockitoSugar
import org.slf4j.bridge.SLF4JBridgeHandler

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.ExtendedActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.dispatch.Dispatchers
import akka.dispatch.RequiresMessageQueue
import akka.dispatch.UnboundedMessageQueueSemantics
import akka.testkit.AkkaSpec

/**
 * Redirect different logging sources to SLF4J.
 */
trait RedirectLogging {

  def redirectLogging(): Unit = {
    // Redirect JUL to SLF4J.
    LogManager.getLogManager().reset()
    SLF4JBridgeHandler.install()
  }

  redirectLogging()

}

/**
 * Provide sigar library from `project/target` location.
 */
case class SimpleSigarProvider(location: String = "native") extends SigarProvider {
  def extractFolder = s"${System.getProperty("user.dir")}/target/${location}"
}

/**
 * Provide sigar library as static mock.
 */
case class MockitoSigarProvider(
    pid: Long = 123,
    loadAverage: Array[Double] = Array(0.7, 0.3, 0.1),
    cpuCombined: Double = 0.5,
    cpuStolen: Double = 0.2,
    steps: Int = 5)
    extends SigarProvider
    with MockitoSugar {

  import org.hyperic.sigar._
  import org.mockito.Mockito._

  /** Not used. */
  override def extractFolder = ???

  /** Generate monotonic array from 0 to value. */
  def increase(value: Double): Array[Double] = {
    val delta = value / steps
    (0 to steps).map { _ * delta } toArray
  }

  /** Sigar mock instance. */
  override def verifiedSigarInstance = {

    // Note "thenReturn(0)" invocation is consumed in collector construction.

    val cpuPerc = mock[CpuPerc]
    when(cpuPerc.getCombined).thenReturn(0.0, increase(cpuCombined): _*)
    when(cpuPerc.getStolen).thenReturn(0.0, increase(cpuStolen): _*)

    val sigar = mock[SigarProxy]
    when(sigar.getPid).thenReturn(pid)
    when(sigar.getLoadAverage).thenReturn(loadAverage) // Constant.
    when(sigar.getCpuPerc).thenReturn(cpuPerc) // Increasing.

    sigar
  }
}

/**
 * Used when testing metrics without full cluster
 *
 * TODO change factory after https://github.com/akka/akka-core/issues/16369
 */
trait MetricsCollectorFactory { this: AkkaSpec =>
  import MetricsConfig._
  import org.hyperic.sigar.Sigar

  private def extendedActorSystem = system.asInstanceOf[ExtendedActorSystem]

  def selfAddress = extendedActorSystem.provider.rootPath.address

  def createMetricsCollector: MetricsCollector =
    try {
      new SigarMetricsCollector(selfAddress, defaultDecayFactor, new Sigar())
      //new SigarMetricsCollector(selfAddress, defaultDecayFactor, SimpleSigarProvider().createSigarInstance)
    } catch {
      case e: Throwable =>
        log.warning("Sigar failed to load. Using JMX. Reason: " + e.toString)
        new JmxMetricsCollector(selfAddress, defaultDecayFactor)
    }

  /** Create JMX collector. */
  def collectorJMX: MetricsCollector =
    new JmxMetricsCollector(selfAddress, defaultDecayFactor)

  /** Create Sigar collector. Rely on java agent injection. */
  def collectorSigarDefault: MetricsCollector =
    new SigarMetricsCollector(selfAddress, defaultDecayFactor, new Sigar())

  /** Create Sigar collector. Rely on sigar-loader provisioner. */
  def collectorSigarProvision: MetricsCollector =
    new SigarMetricsCollector(selfAddress, defaultDecayFactor, SimpleSigarProvider().createSigarInstance)

  /** Create Sigar collector. Rely on static sigar library mock. */
  def collectorSigarMockito: MetricsCollector =
    new SigarMetricsCollector(selfAddress, defaultDecayFactor, MockitoSigarProvider().createSigarInstance)

  def isSigar(collector: MetricsCollector): Boolean = collector.isInstanceOf[SigarMetricsCollector]
}

/**
 *
 */
class MockitoSigarMetricsCollector(system: ActorSystem)
    extends SigarMetricsCollector(
      Address("akka", system.name),
      MetricsConfig.defaultDecayFactor,
      MockitoSigarProvider().createSigarInstance) {}

/**
 * Metrics test configurations.
 */
object MetricsConfig {

  val defaultDecayFactor = 2.0 / (1 + 10)

  /** Test w/o cluster, with collection enabled. */
  val defaultEnabled = """
    akka.cluster.metrics {
      collector {
        enabled = on
        sample-interval = 1s
        gossip-interval = 1s
      }
    }
    akka.actor.provider = remote
    akka.remote.artery.canonical.port = 0
  """

  /** Test w/o cluster, with collection disabled. */
  val defaultDisabled = """
    akka.cluster.metrics {
      collector {
        enabled = off
      }
    }
    akka.actor.provider = remote
    akka.remote.artery.canonical.port = 0
  """

  /** Test in cluster, with manual collection activation, collector mock, fast. */
  val clusterSigarMock = """
    akka.cluster.metrics {
      periodic-tasks-initial-delay = 100ms
      collector {
        enabled = off
        sample-interval = 200ms
        gossip-interval = 200ms
        provider = "akka.cluster.metrics.MockitoSigarMetricsCollector"
        fallback = false
      }
    }
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.port = 0
  """
}

/**
 * Current cluster metrics, updated periodically via event bus.
 */
class ClusterMetricsView(system: ExtendedActorSystem) extends Closeable {

  val extension = ClusterMetricsExtension(system)

  /** Current cluster metrics, updated periodically via event bus. */
  @volatile
  private var currentMetricsSet: Set[NodeMetrics] = Set.empty

  /** Collected cluster metrics history. */
  @volatile
  private var collectedMetricsList: List[Set[NodeMetrics]] = List.empty

  /** Create actor that subscribes to the cluster eventBus to update current read view state. */
  private val eventBusListener: ActorRef = {
    system.systemActorOf(
      Props(new Actor with ActorLogging with RequiresMessageQueue[UnboundedMessageQueueSemantics] {
        override def preStart(): Unit = extension.subscribe(self)
        override def postStop(): Unit = extension.unsubscribe(self)
        def receive = {
          case ClusterMetricsChanged(nodes) =>
            currentMetricsSet = nodes
            collectedMetricsList = nodes :: collectedMetricsList
          case _ =>
          // Ignore.
        }
      }).withDispatcher(Dispatchers.DefaultDispatcherId).withDeploy(Deploy.local),
      name = "metrics-event-bus-listener")
  }

  /** Current cluster metrics. */
  def clusterMetrics: Set[NodeMetrics] = currentMetricsSet

  /** Collected cluster metrics history. */
  def metricsHistory: List[Set[NodeMetrics]] = collectedMetricsList

  /** Unsubscribe from cluster events. */
  def close(): Unit = eventBusListener ! PoisonPill

}
