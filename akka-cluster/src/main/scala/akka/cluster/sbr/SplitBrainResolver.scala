/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sbr

import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.actor.Stash
import akka.actor.Timers
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.cluster.Cluster
import akka.cluster.ClusterEvent
import akka.cluster.ClusterEvent._
import akka.cluster.ClusterLogMarker
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.Member
import akka.cluster.Reachability
import akka.cluster.UniqueAddress
import akka.cluster.sbr.DowningStrategy.Decision
import akka.event.DiagnosticMarkerBusLoggingAdapter
import akka.event.Logging
import akka.pattern.pipe
import akka.remote.artery.ThisActorSystemQuarantinedEvent

/**
 * INTERNAL API
 */
@InternalApi private[sbr] object SplitBrainResolver {

  def props(stableAfter: FiniteDuration, strategy: DowningStrategy): Props =
    Props(new SplitBrainResolver(stableAfter, strategy))

  case object Tick

  /**
   * Response (result) of the acquire lease request.
   */
  final case class AcquireLeaseResult(holdingLease: Boolean)

  /**
   * Response (result) of the release lease request.
   */
  final case class ReleaseLeaseResult(released: Boolean)

  /**
   * For delayed acquire of the lease.
   */
  case object AcquireLease

  sealed trait ReleaseLeaseCondition
  object ReleaseLeaseCondition {
    case object NoLease extends ReleaseLeaseCondition
    final case class WhenMembersRemoved(nodes: Set[UniqueAddress]) extends ReleaseLeaseCondition
    final case class WhenTimeElapsed(deadline: Deadline) extends ReleaseLeaseCondition
  }

  final case class ReachabilityChangedStats(
      firstChangeTimestamp: Long,
      latestChangeTimestamp: Long,
      changeCount: Long) {

    def isEmpty: Boolean =
      changeCount == 0

    override def toString: String = {
      if (isEmpty)
        "reachability unchanged"
      else {
        val now = System.nanoTime()
        s"reachability changed $changeCount times since ${(now - firstChangeTimestamp).nanos.toMillis} ms ago, " +
        s"latest change was ${(now - latestChangeTimestamp).nanos.toMillis} ms ago"
      }
    }
  }

}

/**
 * INTERNAL API
 *
 * Unreachable members will be downed by this actor according to the given strategy.
 * It is active on the leader node in the cluster.
 *
 * The implementation is split into two classes SplitBrainResolver and SplitBrainResolverBase to be
 * able to unit test the logic without running cluster.
 */
@nowarn("msg=Use Akka Distributed Cluster")
@InternalApi private[sbr] final class SplitBrainResolver(stableAfter: FiniteDuration, strategy: DowningStrategy)
    extends SplitBrainResolverBase(stableAfter, strategy) {

  private val cluster = Cluster(context.system)

  log.info(
    s"SBR started. Config: strategy [{}], stable-after [{}], down-all-when-unstable [{}], selfUniqueAddress [{}], selfDc [$selfDc].",
    Logging.simpleName(strategy.getClass),
    stableAfter.toCoarsest,
    if (downAllWhenUnstable == Duration.Zero) "off" else downAllWhenUnstable.toCoarsest,
    s"${selfUniqueAddress.address}#${selfUniqueAddress.longUid}")

  override def selfUniqueAddress: UniqueAddress = cluster.selfUniqueAddress
  override def selfDc: DataCenter = cluster.selfDataCenter

  // re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterDomainEvent])
    // note that this is artery only
    context.system.eventStream.subscribe(self, classOf[ThisActorSystemQuarantinedEvent])
    super.preStart()
  }
  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    context.system.eventStream.unsubscribe(self, classOf[ThisActorSystemQuarantinedEvent])
    super.postStop()
  }

  override def down(node: UniqueAddress, decision: Decision): Unit = {
    log.info(ClusterLogMarker.sbrDowningNode(node, decision), "SBR is downing [{}]", node)
    cluster.down(node.address)
  }

}

/**
 * INTERNAL API
 *
 * The implementation is split into two classes SplitBrainResolver and SplitBrainResolverBase to be
 * able to unit test the logic without running cluster.
 */
@nowarn("msg=Use Akka Distributed Cluster")
@InternalApi private[sbr] abstract class SplitBrainResolverBase(stableAfter: FiniteDuration, _strategy: DowningStrategy)
    extends Actor
    with Stash
    with Timers {

  import DowningStrategy._
  import SplitBrainResolver._
  import SplitBrainResolver.ReleaseLeaseCondition.NoLease

  val log: DiagnosticMarkerBusLoggingAdapter = Logging.withMarker(this)

  @InternalStableApi
  def strategy: DowningStrategy = _strategy

  @InternalStableApi
  def selfUniqueAddress: UniqueAddress

  def selfDc: DataCenter

  def down(node: UniqueAddress, decision: Decision): Unit

  // would be better as constructor parameter, but don't want to break Cinnamon instrumentation
  private val settings = new SplitBrainResolverSettings(context.system.settings.config)

  def downAllWhenUnstable: FiniteDuration =
    settings.DownAllWhenUnstable

  private def releaseLeaseAfter: FiniteDuration = strategy match {
    case lm: LeaseMajority => lm.releaseAfter
    case other =>
      throw new IllegalStateException(s"Unexpected use of releaseLeaseAfter for strategy [${other.getClass.getName}]")
  }

  def tickInterval: FiniteDuration = 1.second

  timers.startTimerWithFixedDelay(Tick, Tick, tickInterval)

  var leader = false
  var selfMemberAdded = false

  private def internalDispatcher: ExecutionContext =
    context.system.asInstanceOf[ExtendedActorSystem].dispatchers.internalDispatcher

  // overridden in tests
  protected def newStableDeadline(): Deadline = Deadline.now + stableAfter
  var stableDeadline: Deadline = _
  def resetStableDeadline(): Unit = {
    stableDeadline = newStableDeadline()
  }

  resetStableDeadline()

  private var reachabilityChangedStats: ReachabilityChangedStats =
    ReachabilityChangedStats(System.nanoTime(), System.nanoTime(), 0)

  private def resetReachabilityChangedStats(): Unit = {
    val now = System.nanoTime()
    reachabilityChangedStats = ReachabilityChangedStats(now, now, 0)
  }

  private def resetReachabilityChangedStatsIfAllUnreachableDowned(): Unit = {
    if (!reachabilityChangedStats.isEmpty && strategy.isAllUnreachableDownOrExiting) {
      log.debug("SBR resetting reachability stats, after all unreachable healed, downed or removed")
      resetReachabilityChangedStats()
    }
  }

  private var releaseLeaseCondition: ReleaseLeaseCondition = NoLease

  /** Helper to wrap updates to strategy info with, so that stable-after timer is reset and information is logged about state change */
  def mutateMemberInfo(resetStable: Boolean)(f: () => Unit): Unit = {
    val unreachableBefore = strategy.unreachable.size
    f()
    val unreachableAfter = strategy.unreachable.size

    def earliestTimeOfDecision: String =
      Instant.now().plus(stableAfter.toMillis, ChronoUnit.MILLIS).toString

    if (resetStable) {
      if (isResponsible) {
        if (unreachableBefore == 0 && unreachableAfter > 0) {
          log.info(
            "SBR found unreachable members, waiting for stable-after = {} ms before taking downing decision. " +
            "Now {} unreachable members found. Downing decision will not be made before {}.",
            stableAfter.toMillis,
            unreachableAfter,
            earliestTimeOfDecision)
        } else if (unreachableBefore > 0 && unreachableAfter == 0) {
          log.info(
            "SBR found all unreachable members healed during stable-after period, no downing decision necessary for now.")
        } else if (unreachableAfter > 0) {
          log.info(
            "SBR found unreachable members changed during stable-after period. Resetting timer. " +
            "Now {} unreachable members found. Downing decision will not be made before {}.",
            unreachableAfter,
            earliestTimeOfDecision)
        }
        // else no unreachable members found but set of members changed
      }

      log.debug("SBR reset stable deadline when members/unreachable changed")
      resetStableDeadline()
    }
  }

  /** Helper to wrap updates to `leader` and `selfMemberAdded` to log changes in responsibility status */
  def mutateResponsibilityInfo(f: () => Unit): Unit = {
    val responsibleBefore = isResponsible
    f()
    val responsibleAfter = isResponsible

    if (!responsibleBefore && responsibleAfter)
      log.info(
        "This node is now the leader responsible for taking SBR decisions among the reachable nodes " +
        "(more leaders may exist).")
    else if (responsibleBefore && !responsibleAfter)
      log.info("This node is not the leader any more and not responsible for taking SBR decisions.")

    if (leader && !selfMemberAdded)
      log.debug("This node is leader but !selfMemberAdded.")
  }

  private var unreachableDataCenters = Set.empty[DataCenter]

  override def postStop(): Unit = {
    if (releaseLeaseCondition != NoLease) {
      log.info(
        "SBR is stopped and owns the lease. The lease will not be released until after the " +
        "lease heartbeat-timeout.")
    }
    super.postStop()
  }

  def receive: Receive = {
    case SeenChanged(_, seenBy)                     => seenChanged(seenBy)
    case MemberJoined(m)                            => addJoining(m)
    case MemberWeaklyUp(m)                          => addWeaklyUp(m)
    case MemberUp(m)                                => addUp(m)
    case MemberLeft(m)                              => leaving(m)
    case MemberExited(m)                            => exited(m)
    case UnreachableMember(m)                       => unreachableMember(m)
    case MemberDowned(m)                            => unreachableMember(m)
    case ReachableMember(m)                         => reachableMember(m)
    case ReachabilityChanged(r)                     => reachabilityChanged(r)
    case MemberRemoved(m, _)                        => remove(m)
    case UnreachableDataCenter(dc)                  => unreachableDataCenter(dc)
    case ReachableDataCenter(dc)                    => reachableDataCenter(dc)
    case LeaderChanged(leaderOption)                => leaderChanged(leaderOption)
    case ReleaseLeaseResult(released)               => releaseLeaseResult(released)
    case Tick                                       => tick()
    case ThisActorSystemQuarantinedEvent(_, remote) => thisActorSystemWasQuarantined(remote)
    case _: ClusterDomainEvent                      => // not interested in other events
  }

  private def leaderChanged(leaderOption: Option[Address]): Unit = {
    mutateResponsibilityInfo { () =>
      leader = leaderOption.contains(selfUniqueAddress.address)
    }
  }

  private def tick(): Unit = {
    // note the DownAll due to instability is running on all nodes to make that decision as quickly and
    // aggressively as possible if time is out
    if (reachabilityChangedStats.changeCount > 0) {
      val now = System.nanoTime()
      val durationSinceLatestChange = (now - reachabilityChangedStats.latestChangeTimestamp).nanos
      val durationSinceFirstChange = (now - reachabilityChangedStats.firstChangeTimestamp).nanos

      val downAllWhenUnstableEnabled = downAllWhenUnstable > Duration.Zero
      if (downAllWhenUnstableEnabled && durationSinceFirstChange > (stableAfter + downAllWhenUnstable)) {
        log.warning(
          ClusterLogMarker.sbrInstability,
          "SBR detected instability and will down all nodes: {}",
          reachabilityChangedStats)
        actOnDecision(DownAll)
      } else if (!downAllWhenUnstableEnabled && durationSinceLatestChange > (stableAfter * 2)) {
        // downAllWhenUnstable is disabled but reset for meaningful logging
        log.debug("SBR no reachability changes within {} ms, resetting stats", (stableAfter * 2).toMillis)
        resetReachabilityChangedStats()
      }
    }

    if (isResponsible && strategy.unreachable.nonEmpty && stableDeadline.isOverdue()) {
      strategy.decide() match {
        case decision: AcquireLeaseDecision =>
          strategy.lease match {
            case Some(lease) =>
              if (lease.checkLease()) {
                log.info(
                  ClusterLogMarker.sbrLeaseAcquired(decision),
                  "SBR has acquired lease for decision [{}]",
                  decision)
                actOnDecision(decision)
              } else {
                if (decision.acquireDelay == Duration.Zero)
                  acquireLease() // reply message is AcquireLeaseResult
                else {
                  log.debug("SBR delayed attempt to acquire lease for [{} ms]", decision.acquireDelay.toMillis)
                  timers.startSingleTimer(AcquireLease, AcquireLease, decision.acquireDelay)
                }
                context.become(waitingForLease(decision))
              }
            case None =>
              throw new IllegalStateException("Unexpected lease decision although lease is not configured")
          }

        case decision =>
          actOnDecision(decision)
      }
    }

    releaseLeaseCondition match {
      case ReleaseLeaseCondition.WhenTimeElapsed(deadline) =>
        if (deadline.isOverdue())
          releaseLease() // reply message is ReleaseLeaseResult, which will update the releaseLeaseCondition
      case _ =>
      // no lease or first waiting for downed nodes to be removed
    }
  }

  private def thisActorSystemWasQuarantined(remoteUnique: akka.remote.UniqueAddress): Unit = {
    val remote = UniqueAddress(remoteUnique.address, remoteUnique.uid)
    if (Cluster(context.system).state.members.exists(m => m.uniqueAddress == remote)) {
      actOnDecision(DowningStrategy.DownSelfQuarantinedByRemote)
    } else {
      log.debug("Remote [{}] quarantined this system but is not part of cluster, ignoring", remote)
    }
  }

  private def acquireLease(): Unit = {
    log.debug("SBR trying to acquire lease")
    implicit val ec: ExecutionContext = internalDispatcher
    strategy.lease.foreach(
      _.acquire()
        .recover {
          case t =>
            log.error(t, "SBR acquire of lease failed")
            false
        }
        .map(AcquireLeaseResult.apply)
        .pipeTo(self))
  }

  def waitingForLease(decision: AcquireLeaseDecision): Receive = {
    case AcquireLease =>
      acquireLease() // reply message is LeaseResult

    case AcquireLeaseResult(holdingLease) =>
      if (holdingLease) {
        log.info(ClusterLogMarker.sbrLeaseAcquired(decision), "SBR acquired lease for decision [{}]", decision)
        val downedNodes = actOnDecision(decision)
        releaseLeaseCondition = releaseLeaseCondition match {
          case ReleaseLeaseCondition.WhenMembersRemoved(nodes) =>
            ReleaseLeaseCondition.WhenMembersRemoved(nodes.union(downedNodes))
          case _ =>
            if (downedNodes.isEmpty)
              ReleaseLeaseCondition.WhenTimeElapsed(Deadline.now + releaseLeaseAfter)
            else
              ReleaseLeaseCondition.WhenMembersRemoved(downedNodes)
        }
      } else {
        val reverseDecision = strategy.reverseDecision(decision)
        log.info(
          ClusterLogMarker.sbrLeaseDenied(reverseDecision),
          "SBR couldn't acquire lease, reverse decision [{}] to [{}]",
          decision,
          reverseDecision)
        actOnDecision(reverseDecision)
        releaseLeaseCondition = NoLease
      }

      unstashAll()
      context.become(receive)

    case ReleaseLeaseResult(_) => // superseded by new acquire release request
    case Tick                  => // ignore ticks while waiting
    case _ =>
      stash()
  }

  private def releaseLeaseResult(released: Boolean): Unit = {
    releaseLeaseCondition match {
      case ReleaseLeaseCondition.WhenTimeElapsed(deadline) =>
        if (released && deadline.isOverdue()) {
          log.info(ClusterLogMarker.sbrLeaseReleased, "SBR released lease.")
          releaseLeaseCondition = NoLease // released successfully
        }
      case _ =>
      // no lease or first waiting for downed nodes to be removed
    }
  }

  /**
   * @return the nodes that were downed
   */
  def actOnDecision(decision: Decision): Set[UniqueAddress] = {
    val nodesToDown =
      try {
        val nodes = strategy.nodesToDown(decision)
        // Safeguard for indirectly connected decisions:
        // If the decision would down more than a threshold fraction of members,
        // convert to DownAll to prevent split brain from stale gossip in asymmetric partitions.
        if (decision.isIndirectlyConnected) {
          val threshold = settings.DownAllWhenIndirectlyConnectedThreshold
          val memberCount = strategy.members.size
          if (memberCount > 0 && threshold < 1.0 && nodes.size.toDouble / memberCount >= threshold) {
            log.warning(
              ClusterLogMarker.sbrDowning(decision),
              "SBR indirectly connected decision would down [{}] of [{}] members which is more than " +
              "the configured down-all-when-indirectly-connected threshold of [{}], downing all instead",
              nodes.size,
              memberCount,
              settings.DownAllWhenIndirectlyConnectedThreshold)
            strategy.nodesToDown(DownAll)
          } else {
            nodes
          }
        } else {
          nodes
        }
      } catch {
        case e: IllegalStateException =>
          log.warning(e.getMessage)
          strategy.nodesToDown(DownAll)
      }

    observeDecision(decision, nodesToDown, unreachableDataCenters)

    if (nodesToDown.nonEmpty) {
      val downMyself = nodesToDown.contains(selfUniqueAddress)
      // downing is idempotent, and we also avoid calling down on nodes with status Down
      // down selfAddress last, since it may shutdown itself if down alone
      nodesToDown.foreach(uniqueAddress => if (uniqueAddress != selfUniqueAddress) down(uniqueAddress, decision))
      if (downMyself)
        down(selfUniqueAddress, decision)

      resetReachabilityChangedStats()
      resetStableDeadline()
    }
    nodesToDown
  }

  @InternalStableApi
  def observeDecision(
      decision: Decision,
      nodesToDown: Set[UniqueAddress],
      unreachableDataCenters: Set[DataCenter]): Unit = {
    val downMyself = nodesToDown.contains(selfUniqueAddress)

    val indirectlyConnectedLogMessage =
      if (decision.isIndirectlyConnected)
        s", indirectly connected [${strategy.indirectlyConnected.mkString(", ")}]"
      else ""
    val unreachableDataCentersLogMessage =
      if (unreachableDataCenters.nonEmpty)
        s", unreachable DCs [${unreachableDataCenters.mkString(", ")}]"
      else ""

    log.warning(
      ClusterLogMarker.sbrDowning(decision),
      s"SBR took decision $decision and is downing [${nodesToDown.map(_.address).mkString(", ")}]${if (downMyself) " including myself"
      else ""}, " +
      s"[${strategy.unreachable.size}] unreachable of [${strategy.members.size}] members" +
      indirectlyConnectedLogMessage +
      s", all members in DC [${strategy.allMembersInDC.mkString(", ")}], full reachability status: [${strategy.reachability}]" +
      unreachableDataCentersLogMessage)
  }

  def isResponsible: Boolean = leader && selfMemberAdded

  def unreachableMember(m: Member): Unit = {
    if (m.uniqueAddress != selfUniqueAddress && m.dataCenter == selfDc) {
      log.debug("SBR unreachableMember [{}]", m)
      mutateMemberInfo(resetStable = true) { () =>
        strategy.addUnreachable(m)
        updateReachabilityChangedStats()
        resetReachabilityChangedStatsIfAllUnreachableDowned()
        if (!reachabilityChangedStats.isEmpty)
          log.debug("SBR noticed {}", reachabilityChangedStats)
      }
    }
  }

  def reachableMember(m: Member): Unit = {
    if (m.uniqueAddress != selfUniqueAddress && m.dataCenter == selfDc) {
      log.debug("SBR reachableMember [{}]", m)
      mutateMemberInfo(resetStable = true) { () =>
        strategy.addReachable(m)
        updateReachabilityChangedStats()
        resetReachabilityChangedStatsIfAllUnreachableDowned()
        if (!reachabilityChangedStats.isEmpty)
          log.debug("SBR noticed {}", reachabilityChangedStats)
      }
    }
  }

  private[sbr] def reachabilityChanged(r: Reachability): Unit = {
    strategy.setReachability(r)
  }

  private def updateReachabilityChangedStats(): Unit = {
    val now = System.nanoTime()
    if (reachabilityChangedStats.changeCount == 0)
      reachabilityChangedStats = ReachabilityChangedStats(now, now, 1)
    else
      reachabilityChangedStats = reachabilityChangedStats.copy(
        latestChangeTimestamp = now,
        changeCount = reachabilityChangedStats.changeCount + 1)
  }

  def unreachableDataCenter(dc: DataCenter): Unit = {
    unreachableDataCenters += dc
    log.warning(
      "Data center [{}] observed as unreachable. " +
      "Note that nodes in other data center will not be downed by SBR in this data center [{}]",
      dc,
      selfDc)
  }

  def reachableDataCenter(dc: DataCenter): Unit = {
    unreachableDataCenters -= dc
    log.info("Data center [{}] observed as reachable again", dc)
  }

  def seenChanged(seenBy: Set[Address]): Unit = {
    strategy.setSeenBy(seenBy)
  }

  def addUp(m: Member): Unit = {
    if (selfDc == m.dataCenter) {
      log.debug("SBR add Up [{}]", m)
      mutateMemberInfo(resetStable = true) { () =>
        strategy.add(m)
        if (m.uniqueAddress == selfUniqueAddress) mutateResponsibilityInfo { () =>
          selfMemberAdded = true
        }
      }
      strategy match {
        case s: StaticQuorum =>
          if (s.isTooManyMembers)
            log.warning(
              "The cluster size is [{}] and static-quorum.quorum-size is [{}]. You should not add " +
              "more than [{}] (static-quorum.size * 2 - 1) members to the cluster. If the exceeded cluster size " +
              "remains when a SBR decision is needed it will down all nodes.",
              s.membersWithRole.size,
              s.quorumSize,
              s.quorumSize * 2 - 1)
        case _ => // ok
      }
    }
  }

  def leaving(m: Member): Unit = {
    if (selfDc == m.dataCenter) {
      log.debug("SBR leaving [{}]", m)
      mutateMemberInfo(resetStable = false) { () =>
        strategy.add(m)
      }
    }
  }

  def exited(m: Member): Unit = {
    if (selfDc == m.dataCenter) {
      log.debug("SBR exited [{}]", m)
      mutateMemberInfo(resetStable = true) { () =>
        strategy.add(m)
      }
    }
  }

  def addJoining(m: Member): Unit = {
    if (selfDc == m.dataCenter) {
      log.debug("SBR add Joining/WeaklyUp [{}]", m)
      strategy.add(m)
    }
  }

  def addWeaklyUp(m: Member): Unit = {
    if (m.uniqueAddress == selfUniqueAddress) mutateResponsibilityInfo { () =>
      selfMemberAdded = true
    }
    // treat WeaklyUp in same way as joining
    addJoining(m)
  }

  def remove(m: Member): Unit = {
    if (selfDc == m.dataCenter) {
      if (m.uniqueAddress == selfUniqueAddress)
        context.stop(self)
      else
        mutateMemberInfo(resetStable = false) { () =>
          log.debug("SBR remove [{}]", m)
          strategy.remove(m)

          resetReachabilityChangedStatsIfAllUnreachableDowned()

          releaseLeaseCondition = releaseLeaseCondition match {
            case ReleaseLeaseCondition.WhenMembersRemoved(downedNodes) =>
              val remainingDownedNodes = downedNodes - m.uniqueAddress
              if (remainingDownedNodes.isEmpty)
                ReleaseLeaseCondition.WhenTimeElapsed(Deadline.now + releaseLeaseAfter)
              else
                ReleaseLeaseCondition.WhenMembersRemoved(remainingDownedNodes)
            case other =>
              // no lease or not holding lease
              other
          }
        }
    }
  }

  private def releaseLease(): Unit = {
    implicit val ec: ExecutionContext = internalDispatcher
    strategy.lease.foreach { l =>
      if (releaseLeaseCondition != NoLease) {
        log.debug("SBR releasing lease")
        l.release().recover { case _ => false }.map(ReleaseLeaseResult.apply).pipeTo(self)
      }
    }
  }
}
