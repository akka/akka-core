/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.collection.SortedSet
import scala.collection.immutable

import akka.actor.{ Actor, ActorSelection, Address, NoSerializationVerificationNeeded }
import akka.annotation.InternalApi
import akka.cluster.ClusterEvent._
import akka.cluster.ClusterSettings.DataCenter
import akka.event.ActorWithLogClass
import akka.event.Logging
import akka.remote.FailureDetectorRegistry
import akka.util.ConstantFun

/**
 * INTERNAL API
 *
 * This actor is will be started on all nodes participating in a cluster,
 * however unlike the within-dc heartbeat sender ([[ClusterHeartbeatSender]]),
 * it will only actively work on `n` "oldest" nodes of a given data center.
 *
 * It will monitor it's oldest counterparts in other data centers.
 * For example, a DC configured to have (up to) 4 monitoring actors,
 * will have 4 such active at any point in time, and those will monitor
 * the (at most) 4 oldest nodes of each data center.
 *
 * This monitoring mode is both simple and predictable, and also uses the assumption that
 * "nodes which stay around for a long time, become old", and those rarely change. In a way,
 * they are the "core" of a cluster, while other nodes may be very dynamically changing worked
 * nodes which aggressively come and go as the traffic in the service changes.
 */
@InternalApi
@deprecated("Use Akka Distributed Cluster instead", "2.10.0")
private[cluster] class CrossDcHeartbeatSender extends Actor {
  import CrossDcHeartbeatSender._

  val cluster = Cluster(context.system)

  val verboseHeartbeat = cluster.settings.Debug.VerboseHeartbeatLogging
  import cluster.{ scheduler, selfAddress, selfDataCenter, selfUniqueAddress }
  import cluster.settings.PeriodicTasksInitialDelay
  import context.dispatcher

  private val clusterLogger =
    new cluster.ClusterLogger(
      Logging.withMarker(context.system, ActorWithLogClass(this, ClusterLogClass.ClusterHeartbeat)))
  import clusterLogger._

  // For inspecting if in active state; allows avoiding "becoming active" when already active
  var activelyMonitoring = false

  val isExternalClusterMember: Member => Boolean =
    member => member.dataCenter != cluster.selfDataCenter

  val crossDcSettings: cluster.settings.CrossDcFailureDetectorSettings =
    cluster.settings.MultiDataCenter.CrossDcFailureDetectorSettings

  val crossDcFailureDetector = cluster.crossDcFailureDetector

  var sequenceNr: Long = 0

  def nextHeartBeat() = {
    sequenceNr += 1
    ClusterHeartbeatSender.Heartbeat(selfAddress, sequenceNr, System.nanoTime())
  }

  var dataCentersState: CrossDcHeartbeatingState = CrossDcHeartbeatingState.init(
    selfDataCenter,
    crossDcFailureDetector,
    crossDcSettings.NrOfMonitoringActors,
    immutable.SortedSet.empty)

  // start periodic heartbeat to other nodes in cluster
  val heartbeatTask = scheduler.scheduleWithFixedDelay(
    PeriodicTasksInitialDelay max crossDcSettings.HeartbeatInterval,
    crossDcSettings.HeartbeatInterval,
    self,
    ClusterHeartbeatSender.HeartbeatTick)

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
    if (verboseHeartbeat)
      clusterLogger.logDebug("Initialized cross-dc heartbeat sender as DORMANT in DC: [{}]", selfDataCenter)
  }

  override def postStop(): Unit = {
    dataCentersState.activeReceivers.foreach(a => crossDcFailureDetector.remove(a.address))
    heartbeatTask.cancel()
    cluster.unsubscribe(self)
  }

  /**
   * Looks up and returns the remote cluster heartbeat connection for the specific address.
   */
  def heartbeatReceiver(address: Address): ActorSelection =
    context.actorSelection(ClusterHeartbeatReceiver.path(address))

  def receive: Actor.Receive =
    dormant.orElse(introspecting)

  /**
   * In this state no cross-datacenter heartbeats are sent by this actor.
   * This may be because one of those reasons:
   *   - no nodes in other DCs were detected yet
   *   - nodes in other DCs are present, but this node is not tht n-th oldest in this DC (see
   *     `number-of-cross-datacenter-monitoring-actors`), so it does not have to monitor that other data centers
   *
   * In this state it will however listen to cluster events to eventually take over monitoring other DCs
   * in case it becomes "old enough".
   */
  def dormant: Actor.Receive = {
    case s: CurrentClusterState               => init(s)
    case MemberRemoved(m, _)                  => removeMember(m)
    case evt: MemberEvent                     => addMember(evt.member)
    case ClusterHeartbeatSender.HeartbeatTick => // ignore...
  }

  def active: Actor.Receive = {
    case ClusterHeartbeatSender.HeartbeatTick                => heartbeat()
    case ClusterHeartbeatSender.HeartbeatRsp(from, _, _)     => heartbeatRsp(from)
    case MemberRemoved(m, _)                                 => removeMember(m)
    case evt: MemberEvent                                    => addMember(evt.member)
    case ClusterHeartbeatSender.ExpectedFirstHeartbeat(from) => triggerFirstHeartbeat(from)
  }

  def introspecting: Actor.Receive = {
    case ReportStatus() =>
      sender() ! {
        if (activelyMonitoring) CrossDcHeartbeatSender.MonitoringActive(dataCentersState)
        else CrossDcHeartbeatSender.MonitoringDormant()
      }
  }

  def init(snapshot: CurrentClusterState): Unit = {
    // val unreachable = snapshot.unreachable.collect({ case m if isExternalClusterMember(m) => m.uniqueAddress })
    // nr of monitored nodes is the same as the number of monitoring nodes (`n` oldest in one DC watch `n` oldest in other)
    val nodes = snapshot.members
    val nrOfMonitoredNodes = crossDcSettings.NrOfMonitoringActors
    dataCentersState = CrossDcHeartbeatingState.init(selfDataCenter, crossDcFailureDetector, nrOfMonitoredNodes, nodes)
    becomeActiveIfResponsibleForHeartbeat()
  }

  def addMember(m: Member): Unit =
    if (CrossDcHeartbeatingState.atLeastInUpState(m)) {
      // since we only monitor nodes in Up or later states, due to the n-th oldest requirement
      dataCentersState = dataCentersState.addMember(m)
      if (verboseHeartbeat && m.dataCenter != selfDataCenter)
        clusterLogger.logDebug("Register member {} for cross DC heartbeat (will only heartbeat if oldest)", m)

      becomeActiveIfResponsibleForHeartbeat()
    }

  def removeMember(m: Member): Unit =
    if (m.uniqueAddress == cluster.selfUniqueAddress) {
      // This cluster node will be shutdown, but stop this actor immediately to avoid further updates
      context.stop(self)
    } else {
      dataCentersState = dataCentersState.removeMember(m)
      becomeActiveIfResponsibleForHeartbeat()
    }

  def heartbeat(): Unit = {
    val nextHB = nextHeartBeat()
    dataCentersState.activeReceivers.foreach { to =>
      if (crossDcFailureDetector.isMonitoring(to.address)) {
        if (verboseHeartbeat) logDebug("(Cross) Heartbeat to [{}]", to.address)
      } else {
        if (verboseHeartbeat) logDebug("First (Cross) Heartbeat to [{}]", to.address)
        // schedule the expected first heartbeat for later, which will give the
        // other side a chance to reply, and also trigger some resends if needed
        scheduler.scheduleOnce(
          crossDcSettings.HeartbeatExpectedResponseAfter,
          self,
          ClusterHeartbeatSender.ExpectedFirstHeartbeat(to))
      }
      heartbeatReceiver(to.address) ! nextHB
    }
  }

  def heartbeatRsp(from: UniqueAddress): Unit = {
    if (verboseHeartbeat) logDebug("(Cross) Heartbeat response from [{}]", from.address)
    dataCentersState = dataCentersState.heartbeatRsp(from)
  }

  def triggerFirstHeartbeat(from: UniqueAddress): Unit =
    if (dataCentersState.activeReceivers.contains(from) && !crossDcFailureDetector.isMonitoring(from.address)) {
      if (verboseHeartbeat) logDebug("Trigger extra expected (cross) heartbeat from [{}]", from.address)
      crossDcFailureDetector.heartbeat(from.address)
    }

  private def selfIsResponsibleForCrossDcHeartbeat(): Boolean = {
    val activeDcs: Int = dataCentersState.dataCenters.size
    if (activeDcs > 1) dataCentersState.shouldActivelyMonitorNodes(selfDataCenter, selfUniqueAddress)
    else false
  }

  /** Idempotent, become active if this node is n-th oldest and should monitor other nodes */
  private def becomeActiveIfResponsibleForHeartbeat(): Unit = {
    if (!activelyMonitoring && selfIsResponsibleForCrossDcHeartbeat()) {
      logInfo(
        "Cross DC heartbeat becoming ACTIVE on this node (for DC: {}), monitoring other DCs oldest nodes",
        selfDataCenter)
      activelyMonitoring = true

      context.become(active.orElse(introspecting))
    } else if (!activelyMonitoring)
      if (verboseHeartbeat) logInfo("Remaining DORMANT; others in {} handle heartbeating other DCs", selfDataCenter)
  }

}

/** INTERNAL API */
@InternalApi
private[akka] object CrossDcHeartbeatSender {

  // -- messages intended only for local messaging during testing --
  sealed trait InspectionCommand extends NoSerializationVerificationNeeded
  final case class ReportStatus()

  sealed trait StatusReport extends NoSerializationVerificationNeeded
  sealed trait MonitoringStateReport extends StatusReport
  final case class MonitoringActive(state: CrossDcHeartbeatingState) extends MonitoringStateReport
  final case class MonitoringDormant() extends MonitoringStateReport
  // -- end of messages intended only for local messaging during testing --
}

/** INTERNAL API */
@InternalApi
@nowarn("msg=Use Akka Distributed Cluster")
private[cluster] final case class CrossDcHeartbeatingState(
    selfDataCenter: DataCenter,
    failureDetector: FailureDetectorRegistry[Address],
    nrOfMonitoredNodesPerDc: Int,
    state: Map[ClusterSettings.DataCenter, immutable.SortedSet[Member]]) {
  import CrossDcHeartbeatingState._

  /**
   * Decides if `self` node should become active and monitor other nodes with heartbeats.
   * Only the `nrOfMonitoredNodesPerDc`-oldest nodes in each DC fulfil this role.
   */
  def shouldActivelyMonitorNodes(selfDc: ClusterSettings.DataCenter, selfAddress: UniqueAddress): Boolean = {
    // Since we need ordering of oldests guaranteed, we must only look at Up (or Leaving, Exiting...) nodes

    val selfDcNeighbours: SortedSet[Member] = state.getOrElse(selfDc, emptyMembersSortedSet)
    val selfDcOldOnes = selfDcNeighbours.filter(atLeastInUpState).take(nrOfMonitoredNodesPerDc)

    // if this node is part of the "n oldest nodes" it should indeed monitor other nodes:
    val shouldMonitorActively = selfDcOldOnes.exists(_.uniqueAddress == selfAddress)
    shouldMonitorActively
  }

  def addMember(m: Member): CrossDcHeartbeatingState = {
    val dc = m.dataCenter

    // we need to remove the member first, to avoid having "duplicates"
    // this is because the removal and uniqueness we need is only by uniqueAddress
    // which is not used by the `ageOrdering`
    val oldMembersWithoutM: immutable.SortedSet[Member] =
      state.getOrElse(dc, emptyMembersSortedSet).filterNot(_.uniqueAddress == m.uniqueAddress)

    val updatedMembers = oldMembersWithoutM + m
    val updatedState = this.copy(state = state.updated(dc, updatedMembers))

    // guarding against the case of two members having the same upNumber, in which case the activeReceivers
    // which are based on the ageOrdering could actually have changed by adding a node. In practice this
    // should happen rarely, since upNumbers are assigned sequentially, and we only ever compare nodes
    // in the same DC. If it happens though, we need to remove the previously monitored node from the failure
    // detector, to prevent both a resource leak and that node actually appearing as unreachable in the gossip (!)
    val stoppedMonitoringReceivers = updatedState.activeReceiversIn(dc).diff(this.activeReceiversIn(dc))
    stoppedMonitoringReceivers.foreach(m => failureDetector.remove(m.address)) // at most one element difference

    updatedState
  }

  def removeMember(m: Member): CrossDcHeartbeatingState = {
    val dc = m.dataCenter
    state.get(dc) match {
      case Some(dcMembers) =>
        val updatedMembers = dcMembers.filterNot(_.uniqueAddress == m.uniqueAddress)

        failureDetector.remove(m.address)
        copy(state = state.updated(dc, updatedMembers))
      case None =>
        this // no change needed, was certainly not present (not even its DC was)
    }
  }

  /** Lists addresses that this node should send heartbeats to */
  val activeReceivers: Set[UniqueAddress] = {
    val otherDcs = state.filter(_._1 != selfDataCenter)
    val allOtherNodes = otherDcs.values

    allOtherNodes.flatMap(_.take(nrOfMonitoredNodesPerDc).iterator.map(_.uniqueAddress).to(immutable.IndexedSeq)).toSet
  }

  /** Lists addresses in given DataCenter that this node should send heartbeats to */
  private def activeReceiversIn(dc: DataCenter): Set[UniqueAddress] =
    if (dc == selfDataCenter) Set.empty // CrossDcHeartbeatSender is not supposed to send within its own Dc
    else {
      val otherNodes = state.getOrElse(dc, emptyMembersSortedSet)
      otherNodes.take(nrOfMonitoredNodesPerDc).iterator.map(_.uniqueAddress).to(immutable.Set)
    }

  def allMembers: Iterable[Member] =
    state.values.flatMap(ConstantFun.scalaIdentityFunction)

  def heartbeatRsp(from: UniqueAddress): CrossDcHeartbeatingState = {
    if (activeReceivers.contains(from)) {
      failureDetector.heartbeat(from.address)
    }
    this
  }

  def dataCenters: Set[DataCenter] =
    state.keys.toSet

}

/** INTERNAL API */
@InternalApi
@nowarn("msg=Use Akka Distributed Cluster")
private[cluster] object CrossDcHeartbeatingState {

  /** Sorted by age */
  private def emptyMembersSortedSet: immutable.SortedSet[Member] = immutable.SortedSet.empty[Member](Member.ageOrdering)

  // Since we need ordering of oldests guaranteed, we must only look at Up (or Leaving, Exiting...) nodes
  def atLeastInUpState(m: Member): Boolean =
    m.status != MemberStatus.WeaklyUp && m.status != MemberStatus.Joining

  def init(
      selfDataCenter: DataCenter,
      crossDcFailureDetector: FailureDetectorRegistry[Address],
      nrOfMonitoredNodesPerDc: Int,
      members: immutable.SortedSet[Member]): CrossDcHeartbeatingState = {
    new CrossDcHeartbeatingState(selfDataCenter, crossDcFailureDetector, nrOfMonitoredNodesPerDc, state = {
      // TODO unduplicate this with the logic in MembershipState.ageSortedTopOldestMembersPerDc
      val groupedByDc = members.filter(atLeastInUpState).groupBy(_.dataCenter)

      if (members.ordering == Member.ageOrdering) {
        // we already have the right ordering
        groupedByDc
      } else {
        // we need to enforce the ageOrdering for the SortedSet in each DC
        groupedByDc.map {
          case (dc, ms) =>
            dc -> immutable.SortedSet.empty[Member](Member.ageOrdering).union(ms)
        }
      }
    })
  }

}
