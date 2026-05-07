/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import java.util.Arrays
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable

import com.typesafe.config.Config

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.DynamicAccess
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.cluster.Cluster
import akka.cluster.routing.ClusterRouterSettingsBase
import akka.dispatch.Dispatchers
import akka.japi.Util.immutableSeq
import akka.routing._

/**
 * Load balancing of messages to cluster nodes based on cluster metric data.
 *
 * It uses random selection of routees based on probabilities derived from
 * the remaining capacity of corresponding node.
 *
 * @param system the actor system hosting this router
 *
 * @param metricsSelector decides what probability to use for selecting a routee, based
 *   on remaining capacity as indicated by the node metrics
 */
final case class AdaptiveLoadBalancingRoutingLogic(
    system: ActorSystem,
    metricsSelector: MetricsSelector = MixMetricsSelector)
    extends RoutingLogic
    with NoSerializationVerificationNeeded {

  private val cluster = Cluster(system)

  // The current weighted routees, if any. Weights are produced by the metricsSelector
  // via the metricsListener Actor. It's only updated by the actor, but accessed from
  // the threads of the sender()s.
  private val weightedRouteesRef =
    new AtomicReference[(immutable.IndexedSeq[Routee], Set[NodeMetrics], Option[WeightedRoutees])](
      (Vector.empty, Set.empty, None))

  @tailrec final def metricsChanged(event: ClusterMetricsChanged): Unit = {
    val oldValue = weightedRouteesRef.get
    val (routees, _, _) = oldValue
    val weightedRoutees = Some(
      new WeightedRoutees(routees, cluster.selfAddress, metricsSelector.weights(event.nodeMetrics)))
    // retry when CAS failure
    if (!weightedRouteesRef.compareAndSet(oldValue, (routees, event.nodeMetrics, weightedRoutees)))
      metricsChanged(event)
  }

  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee =
    if (routees.isEmpty) NoRoutee
    else {

      def updateWeightedRoutees(): Option[WeightedRoutees] = {
        val oldValue = weightedRouteesRef.get
        val (oldRoutees, oldMetrics, oldWeightedRoutees) = oldValue

        if (routees ne oldRoutees) {
          val weightedRoutees = Some(
            new WeightedRoutees(routees, cluster.selfAddress, metricsSelector.weights(oldMetrics)))
          // ignore, don't update, in case of CAS failure
          weightedRouteesRef.compareAndSet(oldValue, (routees, oldMetrics, weightedRoutees))
          weightedRoutees
        } else oldWeightedRoutees
      }

      updateWeightedRoutees() match {
        case Some(weighted) =>
          if (weighted.isEmpty) NoRoutee
          else weighted(ThreadLocalRandom.current.nextInt(weighted.total) + 1)
        case None =>
          routees(ThreadLocalRandom.current.nextInt(routees.size))
      }

    }
}

/**
 * A router pool that performs load balancing of messages to cluster nodes based on
 * cluster metric data.
 *
 * It uses random selection of routees based on probabilities derived from
 * the remaining capacity of corresponding node.
 *
 * The configuration parameter trumps the constructor arguments. This means that
 * if you provide `nrOfInstances` during instantiation they will be ignored if
 * the router is defined in the configuration file for the actor being used.
 *
 * <h1>Supervision Setup</h1>
 *
 * Any routees that are created by a router will be created as the router's children.
 * The router is therefore also the children's supervisor.
 *
 * The supervision strategy of the router actor can be configured with
 * [[#withSupervisorStrategy]]. If no strategy is provided, routers default to
 * a strategy of “always escalate”. This means that errors are passed up to the
 * router's supervisor for handling.
 *
 * The router's supervisor will treat the error as an error with the router itself.
 * Therefore a directive to stop or restart will cause the router itself to stop or
 * restart. The router, in turn, will cause its children to stop and restart.
 *
 * @param metricsSelector decides what probability to use for selecting a routee, based
 *   on remaining capacity as indicated by the node metrics
 *
 * @param nrOfInstances initial number of routees in the pool
 *
 * @param supervisorStrategy strategy for supervising the routees, see 'Supervision Setup'
 *
 * @param routerDispatcher dispatcher to use for the router head actor, which handles
 *   supervision, death watch and router management messages
 */
@SerialVersionUID(1L)
final case class AdaptiveLoadBalancingPool(
    metricsSelector: MetricsSelector = MixMetricsSelector,
    val nrOfInstances: Int = 0,
    override val supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy,
    override val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
    override val usePoolDispatcher: Boolean = false)
    extends Pool {

  def this(config: Config, dynamicAccess: DynamicAccess) =
    this(
      nrOfInstances = ClusterRouterSettingsBase.getMaxTotalNrOfInstances(config),
      metricsSelector = MetricsSelector.fromConfig(config, dynamicAccess),
      usePoolDispatcher = config.hasPath("pool-dispatcher"))

  /**
   * Java API
   * @param metricsSelector decides what probability to use for selecting a routee, based
   *   on remaining capacity as indicated by the node metrics
   * @param nr initial number of routees in the pool
   */
  def this(metricsSelector: MetricsSelector, nr: Int) = this(nrOfInstances = nr)

  override def resizer: Option[Resizer] = None

  override def nrOfInstances(sys: ActorSystem) = this.nrOfInstances

  override def createRouter(system: ActorSystem): Router =
    new Router(AdaptiveLoadBalancingRoutingLogic(system, metricsSelector))

  override def routingLogicController(routingLogic: RoutingLogic): Option[Props] =
    Some(
      Props(
        classOf[AdaptiveLoadBalancingMetricsListener],
        routingLogic.asInstanceOf[AdaptiveLoadBalancingRoutingLogic]))

  /**
   * Setting the supervisor strategy to be used for the “head” Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): AdaptiveLoadBalancingPool =
    copy(supervisorStrategy = strategy)

  /**
   * Setting the dispatcher to be used for the router head actor,  which handles
   * supervision, death watch and router management messages.
   */
  def withDispatcher(dispatcherId: String): AdaptiveLoadBalancingPool = copy(routerDispatcher = dispatcherId)

  /**
   * Uses the supervisor strategy of the given RouterConfig
   * if this RouterConfig doesn't have one
   */
  override def withFallback(other: RouterConfig): RouterConfig =
    if (this.supervisorStrategy ne Pool.defaultSupervisorStrategy) this
    else
      other match {
        case _: FromConfig | _: NoRouter => this // NoRouter is the default, hence “neutral”
        case otherRouter: AdaptiveLoadBalancingPool =>
          if (otherRouter.supervisorStrategy eq Pool.defaultSupervisorStrategy) this
          else this.withSupervisorStrategy(otherRouter.supervisorStrategy)
        case _ => throw new IllegalArgumentException("Expected AdaptiveLoadBalancingPool, got [%s]".format(other))
      }

}

/**
 * A router group that performs load balancing of messages to cluster nodes based on
 * cluster metric data.
 *
 * It uses random selection of routees based on probabilities derived from
 * the remaining capacity of corresponding node.
 *
 * The configuration parameter trumps the constructor arguments. This means that
 * if you provide `paths` during instantiation they will be ignored if
 * the router is defined in the configuration file for the actor being used.
 *
 * @param metricsSelector decides what probability to use for selecting a routee, based
 *   on remaining capacity as indicated by the node metrics
 *
 * @param paths string representation of the actor paths of the routees, messages are
 *   sent with [[akka.actor.ActorSelection]] to these paths
 *
 * @param routerDispatcher dispatcher to use for the router head actor, which handles
 *   router management messages
 */
@SerialVersionUID(1L)
final case class AdaptiveLoadBalancingGroup(
    metricsSelector: MetricsSelector = MixMetricsSelector,
    val paths: immutable.Iterable[String] = Nil,
    override val routerDispatcher: String = Dispatchers.DefaultDispatcherId)
    extends Group {

  def this(config: Config, dynamicAccess: DynamicAccess) =
    this(
      metricsSelector = MetricsSelector.fromConfig(config, dynamicAccess),
      paths = immutableSeq(config.getStringList("routees.paths")))

  /**
   * Java API
   * @param metricsSelector decides what probability to use for selecting a routee, based
   *   on remaining capacity as indicated by the node metrics
   * @param routeesPaths string representation of the actor paths of the routees, messages are
   *   sent with [[akka.actor.ActorSelection]] to these paths
   */
  def this(metricsSelector: MetricsSelector, routeesPaths: java.lang.Iterable[String]) =
    this(paths = immutableSeq(routeesPaths))

  override def paths(system: ActorSystem): immutable.Iterable[String] = this.paths

  override def createRouter(system: ActorSystem): Router =
    new Router(AdaptiveLoadBalancingRoutingLogic(system, metricsSelector))

  override def routingLogicController(routingLogic: RoutingLogic): Option[Props] =
    Some(
      Props(
        classOf[AdaptiveLoadBalancingMetricsListener],
        routingLogic.asInstanceOf[AdaptiveLoadBalancingRoutingLogic]))

  /**
   * Setting the dispatcher to be used for the router head actor, which handles
   * router management messages
   */
  def withDispatcher(dispatcherId: String): AdaptiveLoadBalancingGroup = copy(routerDispatcher = dispatcherId)

}

/**
 * MetricsSelector that uses the heap metrics.
 * Low heap capacity => small weight.
 */
@SerialVersionUID(1L)
case object HeapMetricsSelector extends CapacityMetricsSelector {
  import akka.cluster.metrics.StandardMetrics.HeapMemory

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this

  override def capacity(nodeMetrics: Set[NodeMetrics]): Map[Address, Double] = {
    nodeMetrics.collect {
      case HeapMemory(address, _, used, committed, max) =>
        val capacity = max match {
          case None    => (committed - used).toDouble / committed
          case Some(m) => (m - used).toDouble / m
        }
        (address, capacity)
    }.toMap
  }
}

/**
 * MetricsSelector that uses the combined CPU time metrics and stolen CPU time metrics.
 * In modern Linux kernels: CpuCombined + CpuStolen + CpuIdle = 1.0  or 100%.
 * Combined CPU is sum of User + Sys + Nice + Wait times, as percentage.
 * Stolen CPU is the amount of CPU taken away from this virtual machine by the hypervisor, as percentage.
 *
 * Low CPU capacity => small node weight.
 */
@SerialVersionUID(1L)
case object CpuMetricsSelector extends CapacityMetricsSelector {
  import akka.cluster.metrics.StandardMetrics.Cpu

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this

  // Notes from reading around:
  // In modern Linux kernels: CpuCombined + CpuStolen + CpuIdle = 1.0  or 100%. More convoluted for other o/s.
  // We could use CpuIdle as the only capacity measure: http://axibase.com/news/ec2-monitoring-the-case-of-stolen-cpu/
  // But not all "idle time"s are created equal: https://docs.newrelic.com/docs/servers/new-relic-servers-linux/maintenance/servers-linux-faq
  // Example: assume that combined+stolen=70%, idle=30%. Then 50/20/30 system will be more responsive then 20/50/30 system (combined/stolen/idle ratio).
  // Current approach: "The more stolen resources there are, the less active the virtual machine needs to be to generate a high load rating."

  // TODO read factor from reference.conf
  /** How much extra weight to give to the stolen time. */
  val factor = 0.3
  require(0.0 <= factor, s"factor must be non negative: ${factor}")

  override def capacity(nodeMetrics: Set[NodeMetrics]): Map[Address, Double] = {
    nodeMetrics.collect {
      case Cpu(address, _, _, Some(cpuCombined), Some(cpuStolen), _) =>
        // Arbitrary load rating function which skews in favor of stolen time.
        val load = cpuCombined + cpuStolen * (1.0 + factor)
        val capacity = if (load >= 1.0) 0.0 else 1.0 - load
        (address, capacity)
    }.toMap
  }
}

/**
 * MetricsSelector that uses the system load average metrics.
 * System load average is OS-specific average load on the CPUs in the system,
 * for the past 1 minute. The system is possibly nearing a bottleneck if the
 * system load average is nearing number of cpus/cores.
 * Low load average capacity => small weight.
 */
@SerialVersionUID(1L)
case object SystemLoadAverageMetricsSelector extends CapacityMetricsSelector {
  import akka.cluster.metrics.StandardMetrics.Cpu

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this

  override def capacity(nodeMetrics: Set[NodeMetrics]): Map[Address, Double] = {
    nodeMetrics.collect {
      case Cpu(address, _, Some(systemLoadAverage), _, _, processors) =>
        val capacity = 1.0 - math.min(1.0, systemLoadAverage / processors)
        (address, capacity)
    }.toMap
  }
}

/**
 * Singleton instance of the default MixMetricsSelector, which uses [akka.cluster.routing.HeapMetricsSelector],
 * [akka.cluster.routing.CpuMetricsSelector], and [akka.cluster.routing.SystemLoadAverageMetricsSelector]
 */
@SerialVersionUID(1L)
object MixMetricsSelector
    extends MixMetricsSelectorBase(Vector(HeapMetricsSelector, CpuMetricsSelector, SystemLoadAverageMetricsSelector)) {

  /**
   * Java API: get the default singleton instance
   */
  def getInstance = this
}

/**
 * MetricsSelector that combines other selectors and aggregates their capacity
 * values. By default it uses [akka.cluster.routing.HeapMetricsSelector],
 * [akka.cluster.routing.CpuMetricsSelector], and [akka.cluster.routing.SystemLoadAverageMetricsSelector]
 */
@SerialVersionUID(1L)
final case class MixMetricsSelector(selectors: immutable.IndexedSeq[CapacityMetricsSelector])
    extends MixMetricsSelectorBase(selectors)

/**
 * Base class for MetricsSelector that combines other selectors and aggregates their capacity.
 */
@SerialVersionUID(1L)
abstract class MixMetricsSelectorBase(selectors: immutable.IndexedSeq[CapacityMetricsSelector])
    extends CapacityMetricsSelector {

  /**
   * Java API: construct a mix-selector from a sequence of selectors
   */
  def this(selectors: java.lang.Iterable[CapacityMetricsSelector]) = this(immutableSeq(selectors).toVector)

  override def capacity(nodeMetrics: Set[NodeMetrics]): Map[Address, Double] = {
    val combined: immutable.IndexedSeq[(Address, Double)] = selectors.flatMap(_.capacity(nodeMetrics).toSeq)
    // aggregated average of the capacities by address
    val init: Map[Address, (Double, Int)] = Map.empty.withDefaultValue((0.0, 0))
    combined
      .foldLeft(init) {
        case (acc, (address, capacity)) =>
          val (sum, count) = acc(address)
          acc + (address -> ((sum + capacity, count + 1)))
      }
      .map {
        case (address, (sum, count)) => address -> (sum / count)
      }
  }

}

object MetricsSelector {
  def fromConfig(config: Config, dynamicAccess: DynamicAccess) =
    config.getString("metrics-selector") match {
      case "mix"  => MixMetricsSelector
      case "heap" => HeapMetricsSelector
      case "cpu"  => CpuMetricsSelector
      case "load" => SystemLoadAverageMetricsSelector
      case fqn =>
        val args = List(classOf[Config] -> config)
        dynamicAccess
          .createInstanceFor[MetricsSelector](fqn, args)
          .recover({
            case exception =>
              throw new IllegalArgumentException(
                (s"Cannot instantiate metrics-selector [$fqn], " +
                "make sure it extends [akka.cluster.routing.MetricsSelector] and " +
                "has constructor with [com.typesafe.config.Config] parameter"),
                exception)
          })
          .get
    }
}

/**
 * A MetricsSelector is responsible for producing weights from the node metrics.
 */
@nowarn("msg=@SerialVersionUID")
@SerialVersionUID(1L)
trait MetricsSelector extends Serializable {

  /**
   * The weights per address, based on the nodeMetrics.
   */
  def weights(nodeMetrics: Set[NodeMetrics]): Map[Address, Int]
}

/**
 * A MetricsSelector producing weights from remaining capacity.
 * The weights are typically proportional to the remaining capacity.
 */
@SerialVersionUID(1L)
abstract class CapacityMetricsSelector extends MetricsSelector {

  /**
   * Remaining capacity for each node. The value is between
   * 0.0 and 1.0, where 0.0 means no remaining capacity (full
   * utilization) and 1.0 means full remaining capacity (zero
   * utilization).
   */
  def capacity(nodeMetrics: Set[NodeMetrics]): Map[Address, Double]

  /**
   * Converts the capacity values to weights. The node with lowest
   * capacity gets weight 1 (lowest usable capacity is 1%) and other
   * nodes gets weights proportional to their capacity compared to
   * the node with lowest capacity.
   */
  def weights(capacity: Map[Address, Double]): Map[Address, Int] = {
    if (capacity.isEmpty) Map.empty[Address, Int]
    else {
      val (_, min) = capacity.minBy { case (_, c) => c }
      // lowest usable capacity is 1% (>= 0.5% will be rounded to weight 1), also avoids div by zero
      val divisor = math.max(0.01, min)
      capacity.map { case (address, c) => (address -> math.round((c) / divisor).toInt) }
    }
  }

  /**
   * The weights per address, based on the capacity produced by
   * the nodeMetrics.
   */
  override def weights(nodeMetrics: Set[NodeMetrics]): Map[Address, Int] =
    weights(capacity(nodeMetrics))

}

/**
 * INTERNAL API
 *
 * Pick routee based on its weight. Higher weight, higher probability.
 */
private[metrics] class WeightedRoutees(
    routees: immutable.IndexedSeq[Routee],
    selfAddress: Address,
    weights: Map[Address, Int]) {

  // fill an array of same size as the refs with accumulated weights,
  // binarySearch is used to pick the right bucket from a requested value
  // from 1 to the total sum of the used weights.
  private val buckets: Array[Int] = {
    def fullAddress(routee: Routee): Address = {
      val a = routee match {
        case ActorRefRoutee(ref)       => ref.path.address
        case ActorSelectionRoutee(sel) => sel.anchor.path.address
        case _                         => throw new RuntimeException()
      }
      a match {
        case Address(_, _, None, None) => selfAddress
        case a                         => a
      }
    }
    val buckets = new Array[Int](routees.size)
    val meanWeight = if (weights.isEmpty) 1 else weights.values.sum / weights.size
    val w = weights.withDefaultValue(meanWeight) // we don’t necessarily have metrics for all addresses
    var i = 0
    var sum = 0
    routees.foreach { r =>
      sum += w(fullAddress(r))
      buckets(i) = sum
      i += 1
    }
    buckets
  }

  def isEmpty: Boolean = buckets.length == 0 || buckets(buckets.length - 1) == 0

  def total: Int = {
    require(!isEmpty, "WeightedRoutees must not be used when empty")
    buckets(buckets.length - 1)
  }

  /**
   * Pick the routee matching a value, from 1 to total.
   */
  def apply(value: Int): Routee = {
    require(1 <= value && value <= total, "value must be between [1 - %s]".format(total))
    routees(idx(Arrays.binarySearch(buckets, value)))
  }

  /**
   * Converts the result of Arrays.binarySearch into a index in the buckets array
   * see documentation of Arrays.binarySearch for what it returns
   */
  private def idx(i: Int): Int = {
    if (i >= 0) i // exact match
    else {
      val j = math.abs(i + 1)
      if (j >= buckets.length)
        throw new IndexOutOfBoundsException("Requested index [%s] is > max index [%s]".format(i, buckets.length))
      else j
    }
  }
}

/**
 * INTERNAL API
 * Subscribe to [[ClusterMetricsEvent]]s and update routing logic depending on the events.
 */
private[metrics] class AdaptiveLoadBalancingMetricsListener(routingLogic: AdaptiveLoadBalancingRoutingLogic)
    extends Actor {

  def extension = ClusterMetricsExtension(context.system)

  override def preStart(): Unit = extension.subscribe(self)

  override def postStop(): Unit = extension.unsubscribe(self)

  def receive = {
    case event: ClusterMetricsChanged => routingLogic.metricsChanged(event)
    case _                            => // ignore
  }

}
