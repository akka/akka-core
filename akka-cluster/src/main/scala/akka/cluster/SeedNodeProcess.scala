/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable
import scala.concurrent.duration.{ Deadline, _ }

import akka.actor.{ Actor, ActorRef, Address, CoordinatedShutdown, ReceiveTimeout }
import akka.annotation.{ InternalApi, InternalStableApi }

/**
 * INTERNAL API.
 */
@InternalApi
private[cluster] abstract class SeedNodeProcess(joinConfigCompatChecker: JoinConfigCompatChecker) extends Actor {
  import ClusterUserAction.JoinTo
  import InternalClusterAction._

  val cluster = Cluster(context.system)
  import cluster.ClusterLogger._
  import cluster.settings._

  val selfAddress = cluster.selfAddress

  private val JoinToIncompatibleConfigUnenforced =
    "Join will be performed because compatibility check is configured to not be enforced."

  private val ValidatedIncompatibleConfig = "Cluster validated this node config, but sent back incompatible settings"

  private val NodeShutdownWarning =
    "It's recommended to perform a full cluster shutdown in order to deploy this new version. " +
    "If a cluster shutdown isn't an option, you may want to disable this protection by setting " +
    "'akka.cluster.configuration-compatibility-check.enforce-on-join = off'. " +
    "Note that disabling it will allow the formation of a cluster with nodes having incompatible configuration settings. " +
    "This node will be shutdown!"

  private def stopOrBecome(behavior: Option[Actor.Receive]): Unit =
    behavior match {
      case Some(done) => context.become(done) // JoinSeedNodeProcess
      case None       => context.stop(self) // FirstSeedNodeProcess
    }

  /**
   * INTERNAL API.
   *
   * @param seedNodes the remaining (if first seed process) otherwise the other seed nodes to send `InitJoin` commands to
   */
  final def receiveJoinSeedNode(seedNodes: Set[Address]): Unit = {
    val requiredNonSensitiveKeys =
      JoinConfigCompatChecker.removeSensitiveKeys(joinConfigCompatChecker.requiredKeys, cluster.settings)
    // configToValidate only contains the keys that are required according to JoinConfigCompatChecker on this node
    val configToValidate =
      JoinConfigCompatChecker.filterWithKeys(requiredNonSensitiveKeys, context.system.settings.config)

    seedNodes.foreach { a =>
      context.actorSelection(context.parent.path.toStringWithAddress(a)) ! InitJoin(configToValidate)
    }
  }

  /**
   * INTERNAL API.
   * Received first InitJoinAck reply with an `IncompatibleConfig`.
   * After sending JoinTo(address), `FirstSeedNodeProcess` calls `context.stop(self)`,
   * `JoinSeedNodeProcess` calls `context.become(done)`.
   *
   * @param joinTo the address to join to
   * @param origin the `InitJoinAck` sender
   * @param behavior on `JoinTo` being sent, if defined, `context.become`  is called, otherwise  `context.stop`.
   */
  final def receiveInitJoinAckIncompatibleConfig(
      joinTo: Address,
      origin: ActorRef,
      behavior: Option[Actor.Receive]): Unit = {

    // first InitJoinAck reply, but incompatible
    if (ByPassConfigCompatCheck) {
      logInitJoinAckReceived(origin)
      logWarning("Joining cluster with incompatible configurations. {}", JoinToIncompatibleConfigUnenforced)

      // only join if set to ignore config validation
      context.parent ! JoinTo(joinTo)
      stopOrBecome(behavior)

    } else {
      logError(
        ClusterLogMarker.joinFailed,
        s"Couldn't join seed nodes because of incompatible cluster configuration. $NodeShutdownWarning")
      context.stop(self)
      CoordinatedShutdown(context.system).run(CoordinatedShutdown.IncompatibleConfigurationDetectedReason)
    }
  }

  final def receiveInitJoinAckCompatibleConfig(
      joinTo: Address,
      origin: ActorRef,
      configCheck: CompatibleConfig,
      behavior: Option[Actor.Receive]): Unit = {

    logInitJoinAckReceived(origin)

    // validates config coming from cluster against this node config
    joinConfigCompatChecker.check(configCheck.clusterConfig, context.system.settings.config) match {
      case Valid =>
        // first InitJoinAck reply
        context.parent ! JoinTo(joinTo)
        stopOrBecome(behavior)

      case checked: Invalid if ByPassConfigCompatCheck =>
        logWarningInvalidConfigIfBypassConfigCheck(checked)
        context.parent ! JoinTo(joinTo)
        stopOrBecome(behavior)

      case checked: Invalid =>
        logErrorInvalidConfig(checked)
        context.stop(self)
        CoordinatedShutdown(context.system).run(CoordinatedShutdown.IncompatibleConfigurationDetectedReason)
    }
  }

  final def receiveInitJoinAckUncheckedConfig(
      joinTo: Address,
      origin: ActorRef,
      behavior: Option[Actor.Receive]): Unit = {

    logInitJoinAckReceived(origin)
    logWarning("Joining a cluster without configuration compatibility check feature.")
    context.parent ! JoinTo(joinTo)

    stopOrBecome(behavior)
  }

  private def logInitJoinAckReceived(from: ActorRef): Unit =
    logInfo("Received InitJoinAck message from [{}] to [{}]", from, selfAddress)

  private def logWarningInvalidConfigIfBypassConfigCheck(invalid: Invalid): Unit =
    logWarning(
      "{}: {}. {}.",
      ValidatedIncompatibleConfig,
      invalid.errorMessages.mkString(", "),
      JoinToIncompatibleConfigUnenforced)

  private def logErrorInvalidConfig(validation: Invalid): Unit =
    logError("{}: {}. {}", ValidatedIncompatibleConfig, validation.errorMessages.mkString(", "), NodeShutdownWarning)

}

/**
 * INTERNAL API.
 *
 * Used only for the first seed node.
 * Sends InitJoin to all seed nodes (except itself).
 * logInfo("Received InitJoinAck message from [{}] to [{}]", from, selfAddress)
 * If other seed nodes are not part of the cluster yet they will reply with
 * InitJoinNack or not respond at all and then the first seed node
 * will join itself to initialize the new cluster. When the first
 * seed node is restarted, and some other seed node is part of the cluster
 * it will reply with InitJoinAck and then the first seed node will join
 * that other seed node to join existing cluster.
 */
@InternalApi
private[cluster] final class FirstSeedNodeProcess(
    seedNodes: immutable.IndexedSeq[Address],
    joinConfigCompatChecker: JoinConfigCompatChecker)
    extends SeedNodeProcess(joinConfigCompatChecker) {

  import ClusterUserAction.JoinTo
  import InternalClusterAction._
  import cluster.ClusterLogger._

  if (seedNodes.size <= 1 || seedNodes.head != selfAddress)
    throw new IllegalArgumentException("Join seed node should not be done")

  val timeout = Deadline.now + cluster.settings.SeedNodeTimeout

  var remainingSeedNodes = seedNodes.toSet - selfAddress

  // retry until one ack, or all nack, or timeout
  import context.dispatcher
  val retryTask = cluster.scheduler.scheduleWithFixedDelay(1.second, 1.second, self, JoinSeedNode)
  self ! JoinSeedNode

  override def postStop(): Unit = retryTask.cancel()

  def receive: Receive = {
    case JoinSeedNode =>
      if (timeout.hasTimeLeft()) {
        // send InitJoin to remaining seed nodes (except myself)
        receiveJoinSeedNode(remainingSeedNodes)
      } else {
        // no InitJoinAck received, initialize new cluster by joining myself
        if (isDebugEnabled)
          logDebug("Couldn't join other seed nodes, will join myself. seed-nodes=[{}]", seedNodes.mkString(", "))
        context.parent ! JoinTo(selfAddress)
        context.stop(self)
      }

    case InitJoinAck(address, compatible: CompatibleConfig) =>
      receiveInitJoinAckCompatibleConfig(joinTo = address, origin = sender(), configCheck = compatible, behavior = None)

    case InitJoinAck(address, UncheckedConfig) =>
      receiveInitJoinAckUncheckedConfig(joinTo = address, origin = sender(), behavior = None)

    case InitJoinAck(address, IncompatibleConfig) =>
      receiveInitJoinAckIncompatibleConfig(joinTo = address, origin = sender(), behavior = None)

    case InitJoinNack(address) =>
      logInfo("Received InitJoinNack message from [{}] to [{}]", sender(), selfAddress)
      remainingSeedNodes -= address
      if (remainingSeedNodes.isEmpty) {
        // initialize new cluster by joining myself when nacks from all other seed nodes
        context.parent ! JoinTo(selfAddress)
        context.stop(self)
      }
  }

}

/**
 * INTERNAL API.
 *
 * Sends InitJoin to all seed nodes (except itself) and expect
 * InitJoinAck reply back. The seed node that replied first
 * will be used, joined to. InitJoinAck replies received after the
 * first one are ignored.
 *
 * Retries if no InitJoinAck replies are received within the
 * SeedNodeTimeout.
 * When at least one reply has been received it stops itself after
 * an idle SeedNodeTimeout.
 *
 * The seed nodes can be started in any order, but they will not be "active",
 * until they have been able to join another seed node (seed1).
 * They will retry the join procedure.
 * So one possible startup scenario is:
 * 1. seed2 started, but doesn't get any ack from seed1 or seed3
 * 2. seed3 started, doesn't get any ack from seed1 or seed3 (seed2 doesn't reply)
 * 3. seed1 is started and joins itself
 * 4. seed2 retries the join procedure and gets an ack from seed1, and then joins to seed1
 * 5. seed3 retries the join procedure and gets acks from seed2 first, and then joins to seed2
 */
@InternalApi
private[cluster] final class JoinSeedNodeProcess(
    seedNodes: immutable.IndexedSeq[Address],
    joinConfigCompatChecker: JoinConfigCompatChecker)
    extends SeedNodeProcess(joinConfigCompatChecker) {

  import InternalClusterAction._
  import cluster.ClusterLogger._
  import cluster.settings._

  if (seedNodes.isEmpty || seedNodes.head == selfAddress)
    throw new IllegalArgumentException("Join seed node should not be done")

  context.setReceiveTimeout(SeedNodeTimeout)

  var attempt = 0

  // all seed nodes, except this one
  val otherSeedNodes = seedNodes.toSet - selfAddress

  override def preStart(): Unit = self ! JoinSeedNode

  def receive: Receive = {
    case JoinSeedNode =>
      attempt += 1
      // send InitJoin to all seed nodes (except myself)
      receiveJoinSeedNode(otherSeedNodes)

    case InitJoinAck(address, compatible: CompatibleConfig) =>
      receiveInitJoinAckCompatibleConfig(address, sender(), compatible, behavior = Some(done))

    case InitJoinAck(address, UncheckedConfig) =>
      receiveInitJoinAckUncheckedConfig(joinTo = address, origin = sender(), behavior = Some(done))

    case InitJoinAck(address, IncompatibleConfig) =>
      // first InitJoinAck reply, but incompatible
      receiveInitJoinAckIncompatibleConfig(joinTo = address, origin = sender(), behavior = Some(done))

    case InitJoinNack(_) => // that seed was uninitialized

    case ReceiveTimeout =>
      if (attempt >= 2)
        logWarning(
          ClusterLogMarker.joinFailed,
          "Couldn't join seed nodes after [{}] attempts, will try again. seed-nodes=[{}]",
          attempt,
          seedNodes.filterNot(_ == selfAddress).mkString(", "))
      // no InitJoinAck received, try again
      self ! JoinSeedNode
      onReceiveTimeout(seedNodes, attempt)
  }

  @InternalStableApi
  private[akka] def onReceiveTimeout(seedNodes: immutable.IndexedSeq[Address], attempt: Int): Unit = {}

  def done: Actor.Receive = {
    case InitJoinAck(_, _) => // already received one, skip rest
    case ReceiveTimeout    => context.stop(self)
  }
}
