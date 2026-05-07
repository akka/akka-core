/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import java.util.concurrent.ThreadLocalRandom

import scala.collection.immutable
import scala.jdk.CollectionConverters._

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Address
import akka.actor.DeadLetterSuppression
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ClusterEvent
import akka.cluster.Member
import akka.cluster.MemberStatus

/**
 *  Runtime collection management commands.
 */
sealed abstract class CollectionControlMessage extends Serializable

/**
 * Command for [[ClusterMetricsSupervisor]] to start metrics collection.
 */
@SerialVersionUID(1L)
case object CollectionStartMessage extends CollectionControlMessage {

  /** Java API */
  def getInstance = CollectionStartMessage
}

/**
 * Command for [[ClusterMetricsSupervisor]] to stop metrics collection.
 */
@SerialVersionUID(1L)
case object CollectionStopMessage extends CollectionControlMessage {

  /** Java API */
  def getInstance = CollectionStopMessage
}

/**
 * INTERNAL API.
 *
 * Actor providing customizable metrics collection supervision.
 */
private[metrics] class ClusterMetricsSupervisor extends Actor with ActorLogging {
  val metrics = ClusterMetricsExtension(context.system)
  import context._
  import metrics.settings._

  override val supervisorStrategy = metrics.strategy

  var collectorInstance = 0

  def collectorName = s"collector-${collectorInstance}"

  override def preStart() = {
    if (CollectorEnabled) {
      self ! CollectionStartMessage
    } else {
      log.warning(
        s"Metrics collection is disabled in configuration. Use subtypes of ${classOf[CollectionControlMessage].getName} to manage collection at runtime.")
    }
  }

  override def receive = {
    case CollectionStartMessage =>
      children.foreach(stop)
      collectorInstance += 1
      actorOf(Props(new ClusterMetricsCollector), collectorName)
      log.debug(s"Collection started.")
    case CollectionStopMessage =>
      children.foreach(stop)
      log.debug(s"Collection stopped.")
  }

}

/**
 * Local cluster metrics extension events.
 *
 * Published to local event bus subscribers by [[ClusterMetricsCollector]].
 */
trait ClusterMetricsEvent

/**
 * Current snapshot of cluster node metrics.
 */
final case class ClusterMetricsChanged(nodeMetrics: Set[NodeMetrics]) extends ClusterMetricsEvent {

  /** Java API */
  def getNodeMetrics: java.lang.Iterable[NodeMetrics] = nodeMetrics.asJava
}

/**
 * INTERNAL API.
 *
 * Remote cluster metrics extension messages.
 *
 * Published to cluster members with metrics extension.
 */
private[metrics] trait ClusterMetricsMessage extends Serializable

/**
 * INTERNAL API.
 *
 * Envelope adding a sender address to the cluster metrics gossip.
 */
@SerialVersionUID(1L)
private[metrics] final case class MetricsGossipEnvelope(from: Address, gossip: MetricsGossip, reply: Boolean)
    extends ClusterMetricsMessage
    with DeadLetterSuppression

/**
 * INTERNAL API.
 */
private[metrics] object ClusterMetricsCollector {
  case object MetricsTick
  case object GossipTick
}

/**
 * INTERNAL API.
 *
 * Actor responsible for periodic data sampling in the node and publication to the cluster.
 */
private[metrics] class ClusterMetricsCollector extends Actor with ActorLogging {
  import ClusterEvent._
  import ClusterMetricsCollector._
  import Member.addressOrdering
  import context.dispatcher
  val cluster = Cluster(context.system)
  import cluster.{ scheduler, selfAddress }
  import cluster.ClusterLogger._
  val metrics = ClusterMetricsExtension(context.system)
  import metrics.settings._

  /**
   * The node ring gossipped that contains only members that are Up.
   */
  var nodes: immutable.SortedSet[Address] = immutable.SortedSet.empty

  /**
   * The latest metric values with their statistical data.
   */
  var latestGossip: MetricsGossip = MetricsGossip.empty

  /**
   * The metrics collector that samples data on the node.
   */
  val collector: MetricsCollector = MetricsCollector(context.system)

  /**
   * Start periodic gossip to random nodes in cluster
   */
  val gossipTask =
    scheduler.scheduleWithFixedDelay(
      PeriodicTasksInitialDelay max CollectorGossipInterval,
      CollectorGossipInterval,
      self,
      GossipTick)

  /**
   * Start periodic metrics collection
   */
  val sampleTask = scheduler.scheduleWithFixedDelay(
    PeriodicTasksInitialDelay max CollectorSampleInterval,
    CollectorSampleInterval,
    self,
    MetricsTick)

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])
    logInfo("Metrics collection has started successfully")
  }

  def receive = {
    case GossipTick                 => gossip()
    case MetricsTick                => sample()
    case msg: MetricsGossipEnvelope => receiveGossip(msg)
    case state: CurrentClusterState => receiveState(state)
    case MemberUp(m)                => addMember(m)
    case MemberWeaklyUp(m)          => addMember(m)
    case MemberRemoved(m, _)        => removeMember(m)
    case MemberExited(m)            => removeMember(m)
    case UnreachableMember(m)       => removeMember(m)
    case ReachableMember(m) =>
      if (m.status == MemberStatus.Up || m.status == MemberStatus.WeaklyUp)
        addMember(m)
    case _: MemberEvent => // not interested in other types of MemberEvent

  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    gossipTask.cancel()
    sampleTask.cancel()
    collector.close()
  }

  /**
   * Adds a member to the node ring.
   */
  def addMember(member: Member): Unit = nodes += member.address

  /**
   * Removes a member from the member node ring.
   */
  def removeMember(member: Member): Unit = {
    nodes -= member.address
    latestGossip = latestGossip.remove(member.address)
    publish()
  }

  /**
   * Updates the initial node ring for those nodes that are [[akka.cluster.MemberStatus]] `Up`.
   */
  def receiveState(state: CurrentClusterState): Unit =
    nodes = (state.members.diff(state.unreachable)).collect {
      case m if m.status == MemberStatus.Up || m.status == MemberStatus.WeaklyUp => m.address
    }

  /**
   * Samples the latest metrics for the node, updates metrics statistics in
   * [[MetricsGossip]], and publishes the change to the event bus.
   *
   * @see [[MetricsCollector]]
   */
  def sample(): Unit = {
    latestGossip :+= collector.sample()
    publish()
  }

  /**
   * Receives changes from peer nodes, merges remote with local gossip nodes, then publishes
   * changes to the event stream for load balancing router consumption, and gossip back.
   */
  def receiveGossip(envelope: MetricsGossipEnvelope): Unit = {
    // remote node might not have same view of member nodes, this side should only care
    // about nodes that are known here, otherwise removed nodes can come back
    val otherGossip = envelope.gossip.filter(nodes)
    latestGossip = latestGossip.merge(otherGossip)
    // changes will be published in the period collect task
    if (!envelope.reply)
      replyGossipTo(envelope.from)
  }

  /**
   * Gossip to peer nodes.
   */
  def gossip(): Unit = selectRandomNode((nodes - selfAddress).toVector).foreach(gossipTo)

  def gossipTo(address: Address): Unit =
    sendGossip(address, MetricsGossipEnvelope(selfAddress, latestGossip, reply = false))

  def replyGossipTo(address: Address): Unit =
    sendGossip(address, MetricsGossipEnvelope(selfAddress, latestGossip, reply = true))

  def sendGossip(address: Address, envelope: MetricsGossipEnvelope): Unit =
    context.actorSelection(self.path.toStringWithAddress(address)) ! envelope

  def selectRandomNode(addresses: immutable.IndexedSeq[Address]): Option[Address] =
    if (addresses.isEmpty) None else Some(addresses(ThreadLocalRandom.current.nextInt(addresses.size)))

  /**
   * Publishes to the event stream.
   */
  def publish(): Unit = context.system.eventStream.publish(ClusterMetricsChanged(latestGossip.nodes))

}
