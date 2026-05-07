/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.util.concurrent.TimeUnit

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable

import akka.actor.Actor
import akka.actor.ActorPath
import akka.actor.ActorSelection
import akka.actor.Address
import akka.actor.DeadLetterSuppression
import akka.actor.Props
import akka.actor.RootActorPath
import akka.annotation.InternalApi
import akka.cluster.ClusterEvent._
import akka.event.ActorWithLogClass
import akka.event.Logging
import akka.remote.FailureDetectorRegistry
import akka.remote.HeartbeatMessage

/**
 * INTERNAL API.
 *
 * Receives Heartbeat messages and replies.
 */
@InternalApi
private[cluster] final class ClusterHeartbeatReceiver(getCluster: () => Cluster) extends Actor {
  import ClusterHeartbeatSender._

  // Important - don't use Cluster(context.system) in constructor because that would
  // cause deadlock. See startup sequence in ClusterDaemon.
  lazy val cluster = getCluster()

  lazy val verboseHeartbeat = cluster.settings.Debug.VerboseHeartbeatLogging

  private lazy val clusterLogger =
    new cluster.ClusterLogger(
      Logging.withMarker(context.system, ActorWithLogClass(this, ClusterLogClass.ClusterHeartbeat)))

  def receive: Receive = {
    case hb: Heartbeat =>
      // TODO log the sequence nr once serializer is enabled
      if (verboseHeartbeat) clusterLogger.logDebug("Heartbeat #{} from [{}]", hb.sequenceNr, hb.from)
      sender() ! HeartbeatRsp(cluster.selfUniqueAddress, hb.sequenceNr, hb.creationTimeNanos)
  }

}

/** INTERNAL API: Utilities to obtain ClusterHeartbeatReceiver paths */
@InternalApi
private[cluster] object ClusterHeartbeatReceiver {

  def props(clusterFactory: () => Cluster): Props = Props(new ClusterHeartbeatReceiver(clusterFactory))
  def name: String = "heartbeatReceiver"
  def path(address: Address): ActorPath =
    RootActorPath(address) / "system" / "cluster" / name
}

/**
 * INTERNAL API
 */
private[cluster] object ClusterHeartbeatSender {

  /**
   * Sent at regular intervals for failure detection.
   */
  final case class Heartbeat(from: Address, sequenceNr: Long, creationTimeNanos: Long)
      extends ClusterMessage
      with HeartbeatMessage
      with DeadLetterSuppression

  /**
   * Sent as reply to [[Heartbeat]] messages.
   */
  final case class HeartbeatRsp(from: UniqueAddress, sequenceNr: Long, creationTimeNanos: Long)
      extends ClusterMessage
      with HeartbeatMessage
      with DeadLetterSuppression

  // sent to self only
  case object HeartbeatTick
  final case class ExpectedFirstHeartbeat(from: UniqueAddress)

}

/*
 * INTERNAL API
 *
 * This actor is responsible for sending the heartbeat messages to
 * a few other nodes, which will reply and then this actor updates the
 * failure detector.
 */
@nowarn("msg=Use Akka Distributed Cluster")
private[cluster] class ClusterHeartbeatSender extends Actor {
  import ClusterHeartbeatSender._

  val cluster = Cluster(context.system)
  val verboseHeartbeat = cluster.settings.Debug.VerboseHeartbeatLogging
  import cluster.scheduler
  import cluster.selfAddress
  import cluster.selfUniqueAddress
  import cluster.settings._
  import context.dispatcher

  private val clusterLogger =
    new cluster.ClusterLogger(
      Logging.withMarker(context.system, ActorWithLogClass(this, ClusterLogClass.ClusterHeartbeat)))
  import clusterLogger._

  val filterInternalClusterMembers: Member => Boolean =
    _.dataCenter == cluster.selfDataCenter

  var sequenceNr = 0

  def selfHeartbeat(): Heartbeat = {
    sequenceNr += 1
    Heartbeat(selfAddress, sequenceNr, System.nanoTime())
  }

  val failureDetector = cluster.failureDetector

  var state: ClusterHeartbeatSenderState = ClusterHeartbeatSenderState(
    ring = HeartbeatNodeRing(selfUniqueAddress, Set(selfUniqueAddress), Set.empty, MonitoredByNrOfMembers),
    oldReceiversNowUnreachable = Set.empty[UniqueAddress],
    failureDetector)

  // start periodic heartbeat to other nodes in cluster
  val heartbeatTask =
    scheduler.scheduleWithFixedDelay(
      PeriodicTasksInitialDelay max HeartbeatInterval,
      HeartbeatInterval,
      self,
      HeartbeatTick)

  // used for logging warning if actual tick interval is unexpected (e.g. due to starvation)
  private var tickTimestamp = System.nanoTime() + (PeriodicTasksInitialDelay max HeartbeatInterval).toNanos

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])
  }

  override def postStop(): Unit = {
    state.activeReceivers.foreach(a => failureDetector.remove(a.address))
    heartbeatTask.cancel()
    cluster.unsubscribe(self)
  }

  /**
   * Looks up and returns the remote cluster heartbeat connection for the specific address.
   */
  def heartbeatReceiver(address: Address): ActorSelection =
    context.actorSelection(ClusterHeartbeatReceiver.path(address))

  def receive = initializing

  def initializing: Actor.Receive = {
    case s: CurrentClusterState =>
      init(s)
      context.become(active)
    case HeartbeatTick =>
      tickTimestamp = System.nanoTime() // start checks when active
  }

  def active: Actor.Receive = {
    case HeartbeatTick                => heartbeat()
    case response: HeartbeatRsp       => heartbeatRsp(response)
    case MemberRemoved(m, _)          => removeMember(m)
    case evt: MemberEvent             => addMember(evt.member)
    case UnreachableMember(m)         => unreachableMember(m)
    case ReachableMember(m)           => reachableMember(m)
    case ExpectedFirstHeartbeat(from) => triggerFirstHeartbeat(from)
  }

  def init(snapshot: CurrentClusterState): Unit = {
    val nodes = snapshot.members.collect { case m if filterInternalClusterMembers(m)           => m.uniqueAddress }
    val unreachable = snapshot.unreachable.collect { case m if filterInternalClusterMembers(m) => m.uniqueAddress }
    state = state.init(nodes, unreachable)
  }

  def addMember(m: Member): Unit =
    if (m.uniqueAddress != selfUniqueAddress && // is not self
        !state.contains(m.uniqueAddress) && // not already added
        filterInternalClusterMembers(m) // should be watching members from this DC (internal / external)
        ) {
      state = state.addMember(m.uniqueAddress)
    }

  def removeMember(m: Member): Unit =
    if (filterInternalClusterMembers(m)) { // we only ever deal with internal cluster members here
      if (m.uniqueAddress == cluster.selfUniqueAddress) {
        // This cluster node will be shutdown, but stop this actor immediately
        // to avoid further updates
        context.stop(self)
      } else {
        state = state.removeMember(m.uniqueAddress)
      }
    }

  def unreachableMember(m: Member): Unit =
    state = state.unreachableMember(m.uniqueAddress)

  def reachableMember(m: Member): Unit =
    state = state.reachableMember(m.uniqueAddress)

  def heartbeat(): Unit = {
    val nextHB = selfHeartbeat()
    state.activeReceivers.foreach { to =>
      if (failureDetector.isMonitoring(to.address)) {
        if (verboseHeartbeat) logDebug("Heartbeat #{} to [{}]", nextHB.sequenceNr, to.address)
      } else {
        if (verboseHeartbeat) logDebug("First Heartbeat #{} to [{}]", nextHB.sequenceNr, to.address)
        // schedule the expected first heartbeat for later, which will give the
        // other side a chance to reply, and also trigger some resends if needed
        scheduler.scheduleOnce(HeartbeatExpectedResponseAfter, self, ExpectedFirstHeartbeat(to))
      }
      heartbeatReceiver(to.address) ! nextHB
    }

    checkTickInterval()
  }

  private def checkTickInterval(): Unit = {
    val now = System.nanoTime()
    if ((now - tickTimestamp) >= (HeartbeatInterval.toNanos * 2))
      logWarning(
        ClusterLogMarker.heartbeatStarvation,
        "Scheduled sending of heartbeat was delayed. " +
        "Previous heartbeat was sent [{}] ms ago, expected interval is [{}] ms. This may cause failure detection " +
        "to mark members as unreachable. The reason can be thread starvation, CPU overload, or GC.",
        TimeUnit.NANOSECONDS.toMillis(now - tickTimestamp),
        HeartbeatInterval.toMillis)
    tickTimestamp = now

  }

  def heartbeatRsp(response: HeartbeatRsp): Unit = {
    if (verboseHeartbeat) {
      val latencyMs = (System.nanoTime() - response.creationTimeNanos) / 1000000
      logDebug(
        "Heartbeat #{} response from [{}] (round trip [{}] ms)",
        response.sequenceNr,
        response.from.address,
        latencyMs)
    }
    state = state.heartbeatRsp(response.from)
  }

  def triggerFirstHeartbeat(from: UniqueAddress): Unit =
    if (state.activeReceivers(from) && !failureDetector.isMonitoring(from.address)) {
      if (verboseHeartbeat) logDebug("Trigger extra expected heartbeat from [{}]", from.address)
      failureDetector.heartbeat(from.address)
    }

}

/**
 * INTERNAL API
 * State of [[ClusterHeartbeatSender]]. Encapsulated to facilitate unit testing.
 * It is immutable, but it updates the failureDetector.
 */
@InternalApi
private[cluster] final case class ClusterHeartbeatSenderState(
    ring: HeartbeatNodeRing,
    oldReceiversNowUnreachable: Set[UniqueAddress],
    failureDetector: FailureDetectorRegistry[Address]) {

  val activeReceivers: Set[UniqueAddress] = ring.myReceivers.union(oldReceiversNowUnreachable)

  def selfAddress = ring.selfAddress

  def init(nodes: Set[UniqueAddress], unreachable: Set[UniqueAddress]): ClusterHeartbeatSenderState =
    copy(ring = ring.copy(nodes = nodes + selfAddress, unreachable = unreachable))

  def contains(node: UniqueAddress): Boolean = ring.nodes(node)

  def addMember(node: UniqueAddress): ClusterHeartbeatSenderState =
    membershipChange(ring :+ node)

  def removeMember(node: UniqueAddress): ClusterHeartbeatSenderState = {
    val newState = membershipChange(ring :- node)

    failureDetector.remove(node.address)
    if (newState.oldReceiversNowUnreachable(node))
      newState.copy(oldReceiversNowUnreachable = newState.oldReceiversNowUnreachable - node)
    else
      newState
  }

  def unreachableMember(node: UniqueAddress): ClusterHeartbeatSenderState =
    membershipChange(ring.copy(unreachable = ring.unreachable + node))

  def reachableMember(node: UniqueAddress): ClusterHeartbeatSenderState =
    membershipChange(ring.copy(unreachable = ring.unreachable - node))

  private def membershipChange(newRing: HeartbeatNodeRing): ClusterHeartbeatSenderState = {
    val oldReceivers = ring.myReceivers
    val removedReceivers = oldReceivers.diff(newRing.myReceivers)
    var adjustedOldReceiversNowUnreachable = oldReceiversNowUnreachable
    removedReceivers.foreach { a =>
      if (failureDetector.isAvailable(a.address))
        failureDetector.remove(a.address)
      else
        adjustedOldReceiversNowUnreachable += a
    }
    copy(newRing, adjustedOldReceiversNowUnreachable)
  }

  def heartbeatRsp(from: UniqueAddress): ClusterHeartbeatSenderState =
    if (activeReceivers(from)) {
      failureDetector.heartbeat(from.address)
      if (oldReceiversNowUnreachable(from)) {
        // back from unreachable, ok to stop heartbeating to it
        if (!ring.myReceivers(from))
          failureDetector.remove(from.address)
        copy(oldReceiversNowUnreachable = oldReceiversNowUnreachable - from)
      } else this
    } else this

}

/**
 * INTERNAL API
 *
 * Data structure for picking heartbeat receivers. The node ring is
 * shuffled by deterministic hashing to avoid picking physically co-located
 * neighbors.
 *
 * It is immutable, i.e. the methods return new instances.
 */
private[cluster] final case class HeartbeatNodeRing(
    selfAddress: UniqueAddress,
    nodes: Set[UniqueAddress],
    unreachable: Set[UniqueAddress],
    monitoredByNrOfMembers: Int) {

  require(nodes contains selfAddress, s"nodes [${nodes.mkString(", ")}] must contain selfAddress [${selfAddress}]")

  private val nodeRing: immutable.SortedSet[UniqueAddress] = {
    implicit val ringOrdering: Ordering[UniqueAddress] = Ordering.fromLessThan[UniqueAddress] { (a, b) =>
      val ha = a.##
      val hb = b.##
      ha < hb || (ha == hb && Member.addressOrdering.compare(a.address, b.address) < 0)
    }

    immutable.SortedSet().union(nodes)
  }

  /**
   * Receivers for `selfAddress`. Cached for subsequent access.
   */
  lazy val myReceivers: Set[UniqueAddress] = receivers(selfAddress)

  private val useAllAsReceivers = monitoredByNrOfMembers >= (nodeRing.size - 1)

  /**
   * The receivers to use from a specified sender.
   */
  def receivers(sender: UniqueAddress): Set[UniqueAddress] =
    if (useAllAsReceivers)
      nodeRing - sender
    else {

      // Pick nodes from the iterator until n nodes that are not unreachable have been selected.
      // Intermediate unreachable nodes up to `monitoredByNrOfMembers` are also included in the result.
      // The reason for not limiting it to strictly monitoredByNrOfMembers is that the leader must
      // be able to continue its duties (e.g. removal of downed nodes) when many nodes are shutdown
      // at the same time and nobody in the remaining cluster is monitoring some of the shutdown nodes.
      // This was reported in issue #16624.
      @tailrec def take(n: Int, iter: Iterator[UniqueAddress], acc: Set[UniqueAddress]): (Int, Set[UniqueAddress]) =
        if (iter.isEmpty || n == 0) (n, acc)
        else {
          val next = iter.next()
          val isUnreachable = unreachable(next)
          if (isUnreachable && acc.size >= monitoredByNrOfMembers)
            take(n, iter, acc) // skip the unreachable, since we have already picked `monitoredByNrOfMembers`
          else if (isUnreachable)
            take(n, iter, acc + next) // include the unreachable, but don't count it
          else
            take(n - 1, iter, acc + next) // include the reachable
        }

      val (remaining, slice1) = take(monitoredByNrOfMembers, nodeRing.rangeFrom(sender).tail.iterator, Set.empty)
      val slice =
        if (remaining == 0)
          slice1
        else {
          // wrap around
          val (_, slice2) = take(remaining, nodeRing.rangeTo(sender).iterator.filterNot(_ == sender), slice1)
          slice2
        }

      slice
    }

  /**
   * Add a node to the ring.
   */
  def :+(node: UniqueAddress): HeartbeatNodeRing = if (nodes contains node) this else copy(nodes = nodes + node)

  /**
   * Remove a node from the ring.
   */
  def :-(node: UniqueAddress): HeartbeatNodeRing =
    if (nodes.contains(node) || unreachable.contains(node))
      copy(nodes = nodes - node, unreachable = unreachable - node)
    else this

}
