/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.snapshot

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import akka.actor.{ ActorPath, ActorRef }
import akka.actor.ActorSystem
import akka.annotation.{ DoNotInherit, InternalApi }
import akka.pattern.ask
import akka.stream.{ Attributes, Materializer }
import akka.stream.SystemMaterializer
import akka.stream.impl.{ PhasedFusingActorMaterializer, StreamSupervisor }
import akka.stream.impl.fusing.ActorGraphInterpreter
import akka.util.Timeout

/**
 * Debug utility to dump the running streams of a materializers in a structure describing the graph layout
 * and "waits-on" relationships.
 *
 * Some of the data extracted may be off unless the stream has settled, for example in when deadlocked, but the
 * structure should be valid regardless. Extracting the information often will have an impact on the performance
 * of the running streams.
 */
object MaterializerState {

  /**
   * Dump stream snapshots of all streams of the default system materializer.
   */
  def streamSnapshots(system: ActorSystem): Future[immutable.Seq[StreamSnapshot]] = {
    SystemMaterializer(system).materializer match {
      case impl: PhasedFusingActorMaterializer =>
        requestFromSupervisor(impl.supervisor)(impl.system.dispatchers.internalDispatcher)
      case other => throw new IllegalArgumentException(s"Unsupported Materializer type ${other.getClass}")
    }
  }

  /**
   * Dump stream snapshots of all streams of the given materializer.
   */
  def streamSnapshots(mat: Materializer): Future[immutable.Seq[StreamSnapshot]] = {
    mat match {
      case impl: PhasedFusingActorMaterializer =>
        requestFromSupervisor(impl.supervisor)(impl.system.dispatchers.internalDispatcher)
      case other => throw new IllegalArgumentException(s"Unsupported Materializer type ${other.getClass}")
    }
  }

  /** INTERNAL API */
  @InternalApi
  private[akka] def requestFromSupervisor(supervisor: ActorRef)(
      implicit ec: ExecutionContext): Future[immutable.Seq[StreamSnapshot]] = {
    // Arbitrary timeout: operation should always be quick, when it times out it will be because the materializer stopped
    implicit val timeout: Timeout = 10.seconds
    supervisor
      .askWithStatus(StreamSupervisor.GetChildrenSnapshots(timeout.duration))
      .mapTo[StreamSupervisor.ChildrenSnapshots]
      .map(_.seq)
  }

  /** INTERNAL API */
  @InternalApi
  private[akka] def requestFromChild(child: ActorRef): Future[StreamSnapshot] = {
    // FIXME arbitrary timeout
    implicit val timeout: Timeout = 10.seconds
    (child ? ActorGraphInterpreter.Snapshot).mapTo[StreamSnapshot]
  }

}

/**
 * A snapshot of one running stream
 *
 * Not for user extension
 */
@DoNotInherit
sealed trait StreamSnapshot {

  /**
   * Running interpreters
   */
  def activeInterpreters: Seq[RunningInterpreter]

  /**
   * Interpreters that has been created but not yet initialized - the stream is not yet running
   */
  def newShells: Seq[UninitializedInterpreter]
}

/**
 * A snapshot of one interpreter - contains a set of logics running in the same underlying actor. Note that
 * multiple interpreters may be running in the same actor (because of submaterialization)
 *
 * Not for user extension
 */
@DoNotInherit
sealed trait InterpreterSnapshot {
  def logics: immutable.Seq[LogicSnapshot]
}

/**
 * A stream interpreter that was not yet initialized when the snapshot was taken
 *
 * Not for user extension
 */
@DoNotInherit
sealed trait UninitializedInterpreter extends InterpreterSnapshot

/**
 * A stream interpreter that is running/has been started
 */
@DoNotInherit
sealed trait RunningInterpreter extends InterpreterSnapshot {

  /**
   * Each of the materialized graph stage logics running inside the interpreter
   */
  def logics: immutable.Seq[LogicSnapshot]

  /**
   * Each connection between logics in the interpreter
   */
  def connections: immutable.Seq[ConnectionSnapshot]

  /**
   * Total number of non-stopped logics in the interpreter
   */
  def runningLogicsCount: Int

  /**
   * All logics that has completed and is no longer executing
   */
  def stoppedLogics: immutable.Seq[LogicSnapshot]
}

/**
 * Not for user extension
 */
@DoNotInherit
sealed trait LogicSnapshot {
  def label: String
  def attributes: Attributes
}

object ConnectionSnapshot {

  /**
   * Not for user extension
   */
  @DoNotInherit
  sealed trait ConnectionState
  case object ShouldPull extends ConnectionState
  case object ShouldPush extends ConnectionState
  case object Closed extends ConnectionState
}

/**
 * Not for user extension
 */
@DoNotInherit
sealed trait ConnectionSnapshot {
  def in: LogicSnapshot
  def out: LogicSnapshot
  def state: ConnectionSnapshot.ConnectionState
}

/**
 * INTERNAL API
 */
@InternalApi
final private[akka] case class StreamSnapshotImpl(
    self: ActorPath,
    activeInterpreters: Seq[RunningInterpreter],
    newShells: Seq[UninitializedInterpreter])
    extends StreamSnapshot {
  override def toString: String = s"StreamSnapshot($self, $activeInterpreters, $newShells)"
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class UninitializedInterpreterImpl(logics: immutable.Seq[LogicSnapshot])
    extends UninitializedInterpreter

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class RunningInterpreterImpl(
    logics: immutable.Seq[LogicSnapshot],
    connections: immutable.Seq[ConnectionSnapshot],
    queueStatus: String,
    runningLogicsCount: Int,
    stoppedLogics: immutable.Seq[LogicSnapshot])
    extends RunningInterpreter
    with HideImpl

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class LogicSnapshotImpl(index: Int, label: String, attributes: Attributes)
    extends LogicSnapshot
    with HideImpl {

  override def toString: String = s"Logic($label)"
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class ConnectionSnapshotImpl(
    id: Int,
    in: LogicSnapshot,
    out: LogicSnapshot,
    state: ConnectionSnapshot.ConnectionState)
    extends ConnectionSnapshot
    with HideImpl

/**
 * INTERNAL API
 */
@InternalApi
trait HideImpl {
  override def toString: String = super.toString.replaceFirst("Impl", "")
}
