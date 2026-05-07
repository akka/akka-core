/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import java.time.Instant

import akka.Done
import akka.actor.typed.ActorRef
import akka.annotation.DoNotInherit
import akka.cluster.sharding.typed.internal.ClusterShardingTypedSerializable
import akka.pattern.StatusReply

/**
 * Commands for interacting with the sharded daemon process
 *
 * Not for user extension
 */
@DoNotInherit
trait ShardedDaemonProcessCommand {}

/**
 * Tell the sharded daemon process to rescale to the given number of processes.
 *
 * @param newNumberOfProcesses The number of processes to scale up to
 * @param replyTo Reply to this actor once scaling is successfully done, or with details if it failed
 *                Note that a successful response may take a long time, depending on how fast
 *                the daemon process actors stop after getting their stop message.
 */
final class ChangeNumberOfProcesses(val newNumberOfProcesses: Int, val replyTo: ActorRef[StatusReply[Done]])
    extends ShardedDaemonProcessCommand
    with ClusterShardingTypedSerializable {

  override def equals(other: Any): Boolean = other match {
    case that: ChangeNumberOfProcesses =>
      newNumberOfProcesses == that.newNumberOfProcesses &&
      replyTo == that.replyTo
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Array[Any](newNumberOfProcesses, replyTo)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object ChangeNumberOfProcesses {

  /**
   * Scala API: Tell the sharded daemon process to rescale to the given number of processes.
   *
   * @param newNumberOfProcesses The number of processes to scale up to
   * @param replyTo              Reply to this actor once scaling is successfully done, or with details if it failed
   *                             Note that a successful response may take a long time, depending on how fast
   *                             the daemon process actors stop after getting their stop message.
   */
  def apply(newNumberOfProcesses: Int, replyTo: ActorRef[StatusReply[Done]]): ChangeNumberOfProcesses =
    new ChangeNumberOfProcesses(newNumberOfProcesses, replyTo)

}

/**
 * Query the sharded daemon process for the current scale
 */
final class GetNumberOfProcesses(val replyTo: ActorRef[NumberOfProcesses])
    extends ShardedDaemonProcessCommand
    with ClusterShardingTypedSerializable {

  override def equals(other: Any): Boolean = other match {
    case that: GetNumberOfProcesses =>
      replyTo == that.replyTo
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(replyTo)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object GetNumberOfProcesses {
  def apply(replyTo: ActorRef[NumberOfProcesses]): GetNumberOfProcesses =
    new GetNumberOfProcesses(replyTo)
}

/**
 * Reply for [[GetNumberOfProcesses]]
 *
 * Not for user extension
 */
@DoNotInherit
trait NumberOfProcesses {

  def numberOfProcesses: Int

  /**
   * The timestamp when the change to the current number of processes was initiated. If the number is the initial
   * number of processes this value is "some time" after cluster startup.
   */
  def started: Instant

  def rescaleInProgress: Boolean

  /**
   * Revision number increased for every re-scale that has been triggered with [[ChangeNumberOfProcesses]]
   */
  def revision: Long
}
