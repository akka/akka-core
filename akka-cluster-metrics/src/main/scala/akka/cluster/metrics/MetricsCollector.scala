/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import java.io.Closeable
import java.lang.System.{ currentTimeMillis => newTimestamp }
import java.lang.management.ManagementFactory
import java.lang.management.MemoryMXBean
import java.lang.management.MemoryUsage
import java.lang.management.OperatingSystemMXBean

import org.hyperic.sigar.SigarProxy

import akka.ConfigurationException
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.cluster.Cluster
import akka.event.Logging

/**
 * Metrics sampler.
 *
 * Implementations of cluster system metrics collectors extend this trait.
 */
trait MetricsCollector extends Closeable {

  /**
   * Samples and collects new data points.
   * This method is invoked periodically and should return
   * current metrics for this node.
   */
  def sample(): NodeMetrics
}

/**
 * INTERNAL API
 *
 * Factory to create configured [[MetricsCollector]].
 *
 * Metrics collector instantiation priority order:
 * 1) Provided custom collector
 * 2) Internal [[SigarMetricsCollector]]
 * 3) Internal [[JmxMetricsCollector]]
 */
private[metrics] object MetricsCollector {

  /** Try to create collector instance in the order of priority. */
  def apply(system: ActorSystem): MetricsCollector = {
    val log = Logging(system, classOf[MetricsCollector])
    val settings = ClusterMetricsSettings(system.settings.config)
    import settings._

    val collectorCustom = CollectorProvider
    val collectorSigar = classOf[SigarMetricsCollector].getName
    val collectorJMX = classOf[JmxMetricsCollector].getName

    val useCustom = !CollectorFallback
    val useInternal = CollectorFallback && CollectorProvider == ""

    def create(provider: String) = TryNative {
      log.debug(s"Trying ${provider}.")
      system
        .asInstanceOf[ExtendedActorSystem]
        .dynamicAccess
        .createInstanceFor[MetricsCollector](provider, List(classOf[ActorSystem] -> system))
        .get
    }

    val collector =
      if (useCustom)
        create(collectorCustom)
      else if (useInternal)
        create(collectorSigar).orElse(create(collectorJMX))
      else // Use complete fall back chain.
        create(collectorCustom).orElse(create(collectorSigar)).orElse(create(collectorJMX))

    collector.recover {
      case e => throw new ConfigurationException(s"Could not create metrics collector: ${e.getMessage}", e)
    }.get
  }
}

/**
 * Loads JVM and system metrics through JMX monitoring beans.
 *
 * @param address The [[akka.actor.Address]] of the node being sampled
 * @param decayFactor how quickly the exponential weighting of past data is decayed
 */
class JmxMetricsCollector(address: Address, decayFactor: Double) extends MetricsCollector {
  import StandardMetrics._

  private def this(address: Address, settings: ClusterMetricsSettings) =
    this(address, EWMA.alpha(settings.CollectorMovingAverageHalfLife, settings.CollectorSampleInterval))

  /**
   * This constructor is used when creating an instance from configured FQCN
   */
  def this(system: ActorSystem) = this(Cluster(system).selfAddress, ClusterMetricsExtension(system).settings)

  private val decayFactorOption = Some(decayFactor)

  private val memoryMBean: MemoryMXBean = ManagementFactory.getMemoryMXBean

  private val osMBean: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean

  /**
   * Samples and collects new data points.
   * Creates a new instance each time.
   */
  def sample(): NodeMetrics = NodeMetrics(address, newTimestamp, metrics())

  /**
   * Generate metrics set.
   * Creates a new instance each time.
   */
  def metrics(): Set[Metric] = {
    val heap = heapMemoryUsage
    Set(systemLoadAverage, heapUsed(heap), heapCommitted(heap), heapMax(heap), processors).flatten
  }

  /**
   * (JMX) Returns the OS-specific average load on the CPUs in the system, for the past 1 minute.
   * On some systems the JMX OS system load average may not be available, in which case a -1 is
   * returned from JMX, and None is returned from this method.
   * Creates a new instance each time.
   */
  def systemLoadAverage: Option[Metric] =
    Metric.create(name = SystemLoadAverage, value = osMBean.getSystemLoadAverage, decayFactor = None)

  /**
   * (JMX) Returns the number of available processors
   * Creates a new instance each time.
   */
  def processors: Option[Metric] =
    Metric.create(name = Processors, value = osMBean.getAvailableProcessors, decayFactor = None)

  /**
   * Current heap to be passed in to heapUsed, heapCommitted and heapMax
   */
  def heapMemoryUsage: MemoryUsage = memoryMBean.getHeapMemoryUsage

  /**
   * (JMX) Returns the current sum of heap memory used from all heap memory pools (in bytes).
   * Creates a new instance each time.
   */
  def heapUsed(heap: MemoryUsage): Option[Metric] =
    Metric.create(name = HeapMemoryUsed, value = heap.getUsed, decayFactor = decayFactorOption)

  /**
   * (JMX) Returns the current sum of heap memory guaranteed to be available to the JVM
   * from all heap memory pools (in bytes).
   * Creates a new instance each time.
   */
  def heapCommitted(heap: MemoryUsage): Option[Metric] =
    Metric.create(name = HeapMemoryCommitted, value = heap.getCommitted, decayFactor = decayFactorOption)

  /**
   * (JMX) Returns the maximum amount of memory (in bytes) that can be used
   * for JVM memory management. If not defined the metrics value is None, i.e.
   * never negative.
   * Creates a new instance each time.
   */
  def heapMax(heap: MemoryUsage): Option[Metric] =
    Metric.create(name = HeapMemoryMax, value = heap.getMax, decayFactor = None)

  override def close(): Unit = ()

}

/**
 * Loads metrics through Hyperic SIGAR and JMX monitoring beans. This
 * loads wider and more accurate range of metrics compared to JmxMetricsCollector
 * by using SIGAR's native OS library.
 *
 * The constructor will by design throw exception if org.hyperic.sigar.Sigar can't be loaded, due
 * to missing classes or native libraries.
 *
 * @param address The [[akka.actor.Address]] of the node being sampled
 * @param decayFactor how quickly the exponential weighting of past data is decayed
 * @param sigar the org.hyperic.Sigar instance
 */
class SigarMetricsCollector(address: Address, decayFactor: Double, sigar: SigarProxy)
    extends JmxMetricsCollector(address, decayFactor) {

  import StandardMetrics._
  import org.hyperic.sigar.CpuPerc

  def this(address: Address, settings: ClusterMetricsSettings, sigar: SigarProxy) =
    this(address, EWMA.alpha(settings.CollectorMovingAverageHalfLife, settings.CollectorSampleInterval), sigar)

  def this(address: Address, settings: ClusterMetricsSettings) =
    this(address, settings, DefaultSigarProvider(settings).createSigarInstance)

  /**
   * This constructor is used when creating an instance from configured FQCN
   */
  def this(system: ActorSystem) = this(Cluster(system).selfAddress, ClusterMetricsExtension(system).settings)

  private val decayFactorOption = Some(decayFactor)

  /**
   * Verify at the end of construction that Sigar is operational.
   */
  metrics()

  // Construction complete.

  override def metrics(): Set[Metric] = {
    // Must obtain cpuPerc in one shot. See https://github.com/akka/akka-core/issues/16121
    val cpuPerc = sigar.getCpuPerc
    super.metrics().union(Set(cpuCombined(cpuPerc), cpuStolen(cpuPerc)).flatten)
  }

  /**
   * (SIGAR) Returns the OS-specific average load on the CPUs in the system, for the past 1 minute.
   *
   * Creates a new instance each time.
   */
  override def systemLoadAverage: Option[Metric] =
    Metric.create(name = SystemLoadAverage, value = sigar.getLoadAverage()(0).asInstanceOf[Number], decayFactor = None)

  /**
   * (SIGAR) Returns the combined CPU sum of User + Sys + Nice + Wait, in percentage. This metric can describe
   * the amount of time the CPU spent executing code during n-interval and how much more it could
   * theoretically. Note that 99% CPU utilization can be optimal or indicative of failure.
   *
   * In the data stream, this will sometimes return with a valid metric value, and sometimes as a NaN or Infinite.
   * Documented bug <a href="https://bugzilla.redhat.com/show_bug.cgi?id=749121">749121</a> and several others.
   *
   * Creates a new instance each time.
   */
  def cpuCombined(cpuPerc: CpuPerc): Option[Metric] =
    Metric.create(name = CpuCombined, value = cpuPerc.getCombined.asInstanceOf[Number], decayFactor = decayFactorOption)

  /**
   * (SIGAR) Returns the stolen CPU time. Relevant to virtual hosting environments.
   * For details please see: <a href="https://en.wikipedia.org/wiki/CPU_time#Subdivision">Wikipedia - CPU time subdivision</a> and
   * <a href="https://www.datadoghq.com/2013/08/understanding-aws-stolen-cpu-and-how-it-affects-your-apps/">Understanding AWS stolen CPU and how it affects your apps</a>
   *
   * Creates a new instance each time.
   */
  def cpuStolen(cpuPerc: CpuPerc): Option[Metric] =
    Metric.create(name = CpuStolen, value = cpuPerc.getStolen.asInstanceOf[Number], decayFactor = decayFactorOption)

  /**
   * (SIGAR) Returns the idle CPU time.
   * Amount of CPU time left after combined and stolen are removed.
   *
   * Creates a new instance each time.
   */
  def cpuIdle(cpuPerc: CpuPerc): Option[Metric] =
    Metric.create(name = CpuIdle, value = cpuPerc.getIdle.asInstanceOf[Number], decayFactor = decayFactorOption)

  /**
   * Releases any native resources associated with this instance.
   */
  override def close(): Unit = SigarProvider.close(sigar)

}
