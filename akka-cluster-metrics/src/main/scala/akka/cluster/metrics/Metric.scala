/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.jdk.CollectionConverters._

import akka.actor.Address

/**
 * Metrics key/value.
 *
 * Equality of Metric is based on its name.
 *
 * @param name the metric name
 * @param value the metric value, which must be a valid numerical value,
 *   a valid value is neither negative nor NaN/Infinite.
 * @param average the data stream of the metric value, for trending over time. Metrics that are already
 *   averages (e.g. system load average) or finite (e.g. as number of processors), are not trended.
 */
@SerialVersionUID(1L)
final case class Metric private[metrics] (name: String, value: Number, average: Option[EWMA])
    extends MetricNumericConverter {

  require(defined(value), s"Invalid Metric [$name] value [$value]")

  /**
   * Updates the data point, and if defined, updates the data stream (average).
   * Returns the updated metric.
   */
  def :+(latest: Metric): Metric =
    if (this.sameAs(latest)) average match {
      case Some(avg)                        => copy(value = latest.value, average = Some(avg :+ latest.value.doubleValue))
      case None if latest.average.isDefined => copy(value = latest.value, average = latest.average)
      case _                                => copy(value = latest.value)
    } else this

  /**
   * The numerical value of the average, if defined, otherwise the latest value
   */
  def smoothValue: Double = average match {
    case Some(avg) => avg.value
    case None      => value.doubleValue
  }

  /**
   * @return true if this value is smoothed
   */
  def isSmooth: Boolean = average.isDefined

  /**
   * Returns true if <code>that</code> is tracking the same metric as this.
   */
  def sameAs(that: Metric): Boolean = name == that.name

  override def hashCode = name.##
  override def equals(obj: Any) = obj match {
    case other: Metric => sameAs(other)
    case _             => false
  }

}

/**
 * Factory for creating valid Metric instances.
 */
object Metric extends MetricNumericConverter {

  /**
   * Creates a new Metric instance if the value is valid, otherwise None
   * is returned. Invalid numeric values are negative and NaN/Infinite.
   */
  def create(name: String, value: Number, decayFactor: Option[Double]): Option[Metric] =
    if (defined(value)) Some(new Metric(name, value, createEWMA(value.doubleValue, decayFactor)))
    else None

  /**
   * Creates a new Metric instance if the Try is successful and the value is valid,
   * otherwise None is returned. Invalid numeric values are negative and NaN/Infinite.
   */
  def create(name: String, value: Try[Number], decayFactor: Option[Double]): Option[Metric] = value match {
    case Success(v) => create(name, v, decayFactor)
    case Failure(_) => None
  }

  def createEWMA(value: Double, decayFactor: Option[Double]): Option[EWMA] = decayFactor match {
    case Some(alpha) => Some(EWMA(value, alpha))
    case None        => None
  }

}

/**
 * Definitions of the built-in standard metrics.
 *
 * The following extractors and data structures makes it easy to consume the
 * [[NodeMetrics]] in for example load balancers.
 */
object StandardMetrics {

  // Constants for the heap related Metric names
  final val HeapMemoryUsed = "heap-memory-used"
  final val HeapMemoryCommitted = "heap-memory-committed"
  final val HeapMemoryMax = "heap-memory-max"

  // Constants for the cpu related Metric names
  final val SystemLoadAverage = "system-load-average"
  final val Processors = "processors"
  // In latest Linux kernels: CpuCombined + CpuStolen + CpuIdle = 1.0  or 100%.
  /** Sum of User + Sys + Nice + Wait. See `org.hyperic.sigar.CpuPerc` */
  final val CpuCombined = "cpu-combined"

  /** The amount of CPU 'stolen' from this virtual machine by the hypervisor for other tasks (such as running another virtual machine). */
  final val CpuStolen = "cpu-stolen"

  /** Amount of CPU time left after combined and stolen are removed. */
  final val CpuIdle = "cpu-idle"

  object HeapMemory {

    /**
     * Given a NodeMetrics it returns the HeapMemory data if the nodeMetrics contains
     * necessary heap metrics.
     * @return if possible a tuple matching the HeapMemory constructor parameters
     */
    def unapply(nodeMetrics: NodeMetrics): Option[(Address, Long, Long, Long, Option[Long])] = {
      for {
        used <- nodeMetrics.metric(HeapMemoryUsed)
        committed <- nodeMetrics.metric(HeapMemoryCommitted)
      } yield (
        nodeMetrics.address,
        nodeMetrics.timestamp,
        used.smoothValue.longValue,
        committed.smoothValue.longValue,
        nodeMetrics.metric(HeapMemoryMax).map(_.smoothValue.longValue))
    }

  }

  /**
   * Java API to extract HeapMemory data from nodeMetrics, if the nodeMetrics
   * contains necessary heap metrics, otherwise it returns null.
   */
  def extractHeapMemory(nodeMetrics: NodeMetrics): HeapMemory = nodeMetrics match {
    case HeapMemory(address, timestamp, used, committed, max) =>
      // note that above extractor returns tuple
      HeapMemory(address, timestamp, used, committed, max)
    case _ => null
  }

  /**
   * The amount of used and committed memory will always be &lt;= max if max is defined.
   * A memory allocation may fail if it attempts to increase the used memory such that used &gt; committed
   * even if used &lt;= max is true (e.g. when the system virtual memory is low).
   *
   * @param address [[akka.actor.Address]] of the node the metrics are gathered at
   * @param timestamp the time of sampling, in milliseconds since midnight, January 1, 1970 UTC
   * @param used the current sum of heap memory used from all heap memory pools (in bytes)
   * @param committed the current sum of heap memory guaranteed to be available to the JVM
   *   from all heap memory pools (in bytes). Committed will always be greater than or equal to used.
   * @param max the maximum amount of memory (in bytes) that can be used for JVM memory management.
   *   Can be undefined on some OS.
   */
  @SerialVersionUID(1L)
  final case class HeapMemory(address: Address, timestamp: Long, used: Long, committed: Long, max: Option[Long]) {
    require(committed > 0L, "committed heap expected to be > 0 bytes")
    require(max.isEmpty || max.get > 0L, "max heap expected to be > 0 bytes")
  }

  object Cpu {

    /**
     * Given a NodeMetrics it returns the Cpu data if the nodeMetrics contains
     * necessary cpu metrics.
     * @return if possible a tuple matching the Cpu constructor parameters
     */
    def unapply(
        nodeMetrics: NodeMetrics): Option[(Address, Long, Option[Double], Option[Double], Option[Double], Int)] = {
      for {
        processors <- nodeMetrics.metric(Processors)
      } yield (
        nodeMetrics.address,
        nodeMetrics.timestamp,
        nodeMetrics.metric(SystemLoadAverage).map(_.smoothValue),
        nodeMetrics.metric(CpuCombined).map(_.smoothValue),
        nodeMetrics.metric(CpuStolen).map(_.smoothValue),
        processors.value.intValue)
    }

  }

  /**
   * Java API to extract Cpu data from nodeMetrics, if the nodeMetrics
   * contains necessary cpu metrics, otherwise it returns null.
   */
  def extractCpu(nodeMetrics: NodeMetrics): Cpu = nodeMetrics match {
    case Cpu(address, timestamp, systemLoadAverage, cpuCombined, cpuStolen, processors) =>
      // note that above extractor returns tuple
      Cpu(address, timestamp, systemLoadAverage, cpuCombined, cpuStolen, processors)
    case _ => null
  }

  /**
   * @param address [[akka.actor.Address]] of the node the metrics are gathered at
   * @param timestamp the time of sampling, in milliseconds since midnight, January 1, 1970 UTC
   * @param systemLoadAverage OS-specific average load on the CPUs in the system, for the past 1 minute,
   *    The system is possibly nearing a bottleneck if the system load average is nearing number of cpus/cores.
   * @param cpuCombined combined CPU sum of User + Sys + Nice + Wait, in percentage ([0.0 - 1.0]. This
   *   metric can describe the amount of time the CPU spent executing code during n-interval and how
   *   much more it could theoretically.
   * @param cpuStolen stolen CPU time, in percentage ([0.0 - 1.0].
   * @param processors the number of available processors
   */
  @SerialVersionUID(1L)
  final case class Cpu(
      address: Address,
      timestamp: Long,
      systemLoadAverage: Option[Double],
      cpuCombined: Option[Double],
      cpuStolen: Option[Double],
      processors: Int) {

    cpuCombined match {
      case Some(x) => require(0.0 <= x && x <= 1.0, s"cpuCombined must be between [0.0 - 1.0], was [$x]")
      case None    =>
    }

    cpuStolen match {
      case Some(x) => require(0.0 <= x && x <= 1.0, s"cpuStolen must be between [0.0 - 1.0], was [$x]")
      case None    =>
    }

  }

}

/**
 * INTERNAL API
 *
 * Encapsulates evaluation of validity of metric values, conversion of an actual metric value to
 * a [[akka.cluster.metrics.Metric]] for consumption by subscribed cluster entities.
 */
private[metrics] trait MetricNumericConverter {

  /**
   * An defined value is neither negative nor NaN/Infinite:
   * <ul><li>JMX system load average and max heap can be 'undefined' for certain OS, in which case a -1 is returned</li>
   * <li>SIGAR combined CPU can occasionally return a NaN or Infinite (known bug)</li></ul>
   */
  def defined(value: Number): Boolean = convertNumber(value) match {
    case Left(a)  => a >= 0
    case Right(b) => !(b < 0.0 || b.isNaN || b.isInfinite)
  }

  /**
   * May involve rounding or truncation.
   */
  def convertNumber(from: Any): Either[Long, Double] = from match {
    case n: Int                  => Left(n)
    case n: Long                 => Left(n)
    case n: Double               => Right(n)
    case n: Float                => Right(n)
    case n: BigInt               => Left(n.longValue)
    case n: java.math.BigInteger => Left(n.longValue)
    case n: BigDecimal           => Right(n.doubleValue)
    case n: java.math.BigDecimal => Right(n.doubleValue)
    case x                       => throw new IllegalArgumentException(s"Not a number [$x]")
  }

}

/**
 * The snapshot of current sampled health metrics for any monitored process.
 * Collected and gossipped at regular intervals for dynamic cluster management strategies.
 *
 * Equality of NodeMetrics is based on its address.
 *
 * @param address [[akka.actor.Address]] of the node the metrics are gathered at
 * @param timestamp the time of sampling, in milliseconds since midnight, January 1, 1970 UTC
 * @param metrics the set of sampled [[akka.cluster.metrics.Metric]]
 */
@SerialVersionUID(1L)
final case class NodeMetrics(address: Address, timestamp: Long, metrics: Set[Metric] = Set.empty[Metric]) {

  /**
   * Returns the most recent data.
   */
  def merge(that: NodeMetrics): NodeMetrics = {
    require(address == that.address, s"merge only allowed for same address, [$address] != [$that.address]")
    if (timestamp >= that.timestamp) this // that is older
    else {
      // equality is based on the name of the Metric
      copy(metrics = that.metrics.union(metrics.diff(that.metrics)), timestamp = that.timestamp)
    }
  }

  /**
   * Returns the most recent data with [[EWMA]] averaging.
   */
  def update(that: NodeMetrics): NodeMetrics = {
    require(address == that.address, s"update only allowed for same address, [$address] != [$that.address]")
    // Apply sample ordering.
    val (latestNode, currentNode) = if (this.timestamp >= that.timestamp) (this, that) else (that, this)
    // Average metrics present in both latest and current.
    val updated = for {
      latest <- latestNode.metrics
      current <- currentNode.metrics
      if latest.sameAs(current)
    } yield {
      current :+ latest
    }
    // Append metrics missing from either latest or current.
    // Equality is based on the [[Metric.name]]
    val merged =
      updated.union(latestNode.metrics.diff(updated)).union(currentNode.metrics.diff(updated).diff(latestNode.metrics))
    copy(metrics = merged, timestamp = latestNode.timestamp)
  }

  def metric(key: String): Option[Metric] = metrics.collectFirst { case m if m.name == key => m }

  /**
   * Java API
   */
  def getMetrics: java.lang.Iterable[Metric] = metrics.asJava

  /**
   * Returns true if <code>that</code> address is the same as this
   */
  def sameAs(that: NodeMetrics): Boolean = address == that.address

  override def hashCode = address.##
  override def equals(obj: Any) = obj match {
    case other: NodeMetrics => sameAs(other)
    case _                  => false
  }

}

/**
 * INTERNAL API
 */
private[metrics] object MetricsGossip {
  val empty = MetricsGossip(Set.empty[NodeMetrics])
}

/**
 * INTERNAL API
 *
 * @param nodes metrics per node
 */
@SerialVersionUID(1L)
private[metrics] final case class MetricsGossip(nodes: Set[NodeMetrics]) {

  /**
   * Removes nodes if their correlating node ring members are not [[akka.cluster.MemberStatus]] `Up`.
   */
  def remove(node: Address): MetricsGossip = copy(nodes = nodes.filterNot(_.address == node))

  /**
   * Only the nodes that are in the `includeNodes` Set.
   */
  def filter(includeNodes: Set[Address]): MetricsGossip =
    copy(nodes = nodes.filter { includeNodes contains _.address })

  /**
   * Adds new remote [[NodeMetrics]] and merges existing from a remote gossip.
   */
  def merge(otherGossip: MetricsGossip): MetricsGossip =
    otherGossip.nodes.foldLeft(this) { (gossip, nodeMetrics) =>
      gossip :+ nodeMetrics
    }

  /**
   * Adds new local [[NodeMetrics]], or merges an existing.
   */
  def :+(newNodeMetrics: NodeMetrics): MetricsGossip = nodeMetricsFor(newNodeMetrics.address) match {
    case Some(existingNodeMetrics) =>
      copy(nodes = nodes - existingNodeMetrics + (existingNodeMetrics.update(newNodeMetrics)))
    case None => copy(nodes = nodes + newNodeMetrics)
  }

  /**
   * Returns [[NodeMetrics]] for a node if exists.
   */
  def nodeMetricsFor(address: Address): Option[NodeMetrics] = nodes.find { n =>
    n.address == address
  }

}
