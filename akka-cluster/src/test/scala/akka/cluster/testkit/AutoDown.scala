/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.testkit

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Cancellable
import akka.actor.Props
import akka.actor.Scheduler
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.DowningProvider
import akka.cluster.Member
import akka.cluster.MembershipState
import akka.cluster.UniqueAddress
import akka.util.Helpers.ConfigOps
import akka.util.Helpers.Requiring
import akka.util.Helpers.toRootLowerCase

/**
 * Downing provider used for testing.
 *
 * Auto-downing is a naïve approach to remove unreachable nodes from the cluster membership.
 * In a production environment it will eventually break down the cluster.
 * When a network partition occurs, both sides of the partition will see the other side as unreachable
 * and remove it from the cluster. This results in the formation of two separate, disconnected, clusters
 * (known as *Split Brain*).
 *
 * This behavior is not limited to network partitions. It can also occur if a node in the cluster is
 * overloaded, or experiences a long GC pause.
 *
 * When using Cluster Singleton or Cluster Sharding it can break the contract provided by those features.
 * Both provide a guarantee that an actor will be unique in a cluster.
 * With the auto-down feature enabled, it is possible for multiple independent clusters to form (*Split Brain*).
 * When this happens the guaranteed uniqueness will no longer be true resulting in undesirable behavior
 * in the system.
 *
 * This is even more severe when Akka Persistence is used in conjunction with Cluster Sharding.
 * In this case, the lack of unique actors can cause multiple actors to write to the same journal.
 * Akka Persistence operates on a single writer principle. Having multiple writers will corrupt
 * the journal and make it unusable.
 *
 * Finally, even if you don't use features such as Persistence, Sharding, or Singletons, auto-downing can lead the
 * system to form multiple small clusters. These small clusters will be independent from each other. They will be
 * unable to communicate and as a result you may experience performance degradation. Once this condition occurs,
 * it will require manual intervention in order to reform the cluster.
 *
 * Because of these issues, auto-downing should never be used in a production environment.
 */
final class AutoDowning(system: ActorSystem) extends DowningProvider {

  private def clusterSettings = Cluster(system).settings

  private val AutoDownUnreachableAfter: Duration = {
    val key = "akka.cluster.testkit.auto-down-unreachable-after"
    // it's not in reference.conf, since only used in tests
    if (clusterSettings.config.hasPath(key)) {
      toRootLowerCase(clusterSettings.config.getString(key)) match {
        case "off" => Duration.Undefined
        case _     => clusterSettings.config.getMillisDuration(key).requiring(_ >= Duration.Zero, key + " >= 0s, or off")
      }
    } else
      Duration.Undefined
  }

  override def downRemovalMargin: FiniteDuration = clusterSettings.DownRemovalMargin

  override def downingActorProps: Option[Props] =
    AutoDownUnreachableAfter match {
      case d: FiniteDuration => Some(AutoDown.props(d))
      case _                 => None // auto-down-unreachable-after = off
    }
}

/**
 * INTERNAL API
 */
private[cluster] object AutoDown {

  def props(autoDownUnreachableAfter: FiniteDuration): Props =
    Props(classOf[AutoDown], autoDownUnreachableAfter)

  final case class UnreachableTimeout(node: UniqueAddress)
}

/**
 * INTERNAL API
 *
 * An unreachable member will be downed by this actor if it remains unreachable
 * for the specified duration and this actor is running on the leader node in the
 * cluster.
 *
 * The implementation is split into two classes AutoDown and AutoDownBase to be
 * able to unit test the logic without running cluster.
 */
private[cluster] class AutoDown(autoDownUnreachableAfter: FiniteDuration)
    extends AutoDownBase(autoDownUnreachableAfter)
    with ActorLogging {

  val cluster = Cluster(context.system)
  import cluster.ClusterLogger._

  override def selfAddress = cluster.selfAddress

  override def scheduler: Scheduler = cluster.scheduler

  // re-subscribe when restart
  override def preStart(): Unit = {
    log.debug("Auto-down is enabled in test.")
    cluster.subscribe(self, classOf[ClusterDomainEvent])
    super.preStart()
  }
  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    super.postStop()
  }

  override def down(node: Address): Unit = {
    require(leader)
    logInfo("Leader is auto-downing unreachable node [{}].", node)
    cluster.down(node)
  }

}

/**
 * INTERNAL API
 *
 * The implementation is split into two classes AutoDown and AutoDownBase to be
 * able to unit test the logic without running cluster.
 */
private[cluster] abstract class AutoDownBase(autoDownUnreachableAfter: FiniteDuration) extends Actor {

  import AutoDown._

  def selfAddress: Address

  def down(node: Address): Unit

  def scheduler: Scheduler

  import context.dispatcher

  val skipMemberStatus = MembershipState.convergenceSkipUnreachableWithMemberStatus

  var scheduledUnreachable: Map[UniqueAddress, Cancellable] = Map.empty
  var pendingUnreachable: Set[UniqueAddress] = Set.empty
  var leader = false

  override def postStop(): Unit = {
    scheduledUnreachable.values.foreach { _.cancel() }
  }

  def receive = {
    case state: CurrentClusterState =>
      leader = state.leader.exists(_ == selfAddress)
      state.unreachable.foreach(unreachableMember)

    case UnreachableMember(m) => unreachableMember(m)

    case ReachableMember(m)  => remove(m.uniqueAddress)
    case MemberRemoved(m, _) => remove(m.uniqueAddress)

    case LeaderChanged(leaderOption) =>
      leader = leaderOption.exists(_ == selfAddress)
      if (leader) {
        pendingUnreachable.foreach(node => down(node.address))
        pendingUnreachable = Set.empty
      }

    case UnreachableTimeout(node) =>
      if (scheduledUnreachable contains node) {
        scheduledUnreachable -= node
        downOrAddPending(node)
      }

    case _: ClusterDomainEvent => // not interested in other events

  }

  def unreachableMember(m: Member): Unit =
    if (!skipMemberStatus(m.status) && !scheduledUnreachable.contains(m.uniqueAddress))
      scheduleUnreachable(m.uniqueAddress)

  def scheduleUnreachable(node: UniqueAddress): Unit = {
    if (autoDownUnreachableAfter == Duration.Zero) {
      downOrAddPending(node)
    } else {
      val task = scheduler.scheduleOnce(autoDownUnreachableAfter, self, UnreachableTimeout(node))
      scheduledUnreachable += (node -> task)
    }
  }

  def downOrAddPending(node: UniqueAddress): Unit = {
    if (leader) {
      down(node.address)
    } else {
      // it's supposed to be downed by another node, current leader, but if that crash
      // a new leader must pick up these
      pendingUnreachable += node
    }
  }

  def remove(node: UniqueAddress): Unit = {
    scheduledUnreachable.get(node).foreach { _.cancel() }
    scheduledUnreachable -= node
    pendingUnreachable -= node
  }

}
