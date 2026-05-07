/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.internal

import scala.concurrent.duration._

import akka.actor.typed.Behavior
import akka.actor.typed.Signal
import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.persistence._
import akka.persistence.state.scaladsl.GetObjectResult
import akka.persistence.typed.state.RecoveryCompleted
import akka.persistence.typed.state.RecoveryFailed
import akka.persistence.typed.state.internal.DurableStateBehaviorImpl.GetState
import akka.persistence.typed.state.internal.Running.WithRevisionAccessible
import akka.persistence.typed.telemetry.DurableStateBehaviorInstrumentation
import akka.util.PrettyDuration._

/**
 * INTERNAL API
 *
 * Second (of three) behavior of a `DurableStateBehavior`.
 *
 * In this behavior the recovery process is initiated.
 * We try to obtain the state from the configured `DurableStateStore`,
 * and if it exists, we use it instead of the initial `emptyState`.
 *
 * See next behavior [[Running]].
 * See previous behavior [[RequestingRecoveryPermit]].
 */
@InternalApi
private[akka] object Recovering {

  def apply[C, S](setup: BehaviorSetup[C, S], receivedPoisonPill: Boolean): Behavior[InternalProtocol] = {
    Behaviors.setup { _ =>
      // protect against store stalling forever because of store overloaded and such
      setup.startRecoveryTimer()
      val recoveryState = RecoveryState[S](0L, null.asInstanceOf[S], receivedPoisonPill, System.nanoTime())
      new Recovering(setup.setMdcPhase(PersistenceMdc.RecoveringState), recoveryState)
    }
  }

  @InternalApi
  private[akka] final case class RecoveryState[State](
      revision: Long,
      state: State,
      receivedPoisonPill: Boolean,
      recoveryStartTime: Long)

}

@InternalApi
private[akka] class Recovering[C, S](
    override val setup: BehaviorSetup[C, S],
    var recoveryState: Recovering.RecoveryState[S])
    extends AbstractBehavior[InternalProtocol](setup.context) // must be class for WithSeqNrAccessible
    with DurableStateStoreInteractions[C, S]
    with StashManagement[C, S]
    with WithRevisionAccessible {

  import InternalProtocol._
  import Recovering.RecoveryState

  onRecoveryStart(setup.context)
  internalGet(setup.context)

  override def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = {
    msg match {
      case success: GetSuccess[S @unchecked] => onGetSuccess(success.result)
      case GetFailure(exc)                   => onGetFailure(exc)
      case RecoveryTimeout                   => onRecoveryTimeout()
      case cmd: IncomingCommand[C @unchecked] =>
        if (recoveryState.receivedPoisonPill) {
          if (setup.settings.logOnStashing)
            setup.internalLogger.debug("Discarding message [{}], because actor is to be stopped.", cmd)
          Behaviors.unhandled
        } else
          onCommand(cmd)
      case get: GetState[S @unchecked] => stashInternal(get)
      case RecoveryPermitGranted       => Behaviors.unhandled // should not happen, we already have the permit
      case UpsertSuccess               => Behaviors.unhandled
      case _: UpsertFailure            => Behaviors.unhandled
      case DeleteSuccess               => Behaviors.unhandled
      case _: DeleteFailure            => Behaviors.unhandled
      case ContinueUnstash             => Behaviors.unhandled
    }
  }

  override def onSignal: PartialFunction[Signal, Behavior[InternalProtocol]] = {
    case PoisonPill =>
      recoveryState = recoveryState.copy(receivedPoisonPill = true)
      this
    case signal =>
      if (setup.onSignal(recoveryState.state, signal, catchAndLog = true)) this
      else Behaviors.unhandled
  }

  /**
   * Called whenever recovery fails.
   *
   * This method throws `DurableStateStoreException` which will be caught by the internal
   * supervision strategy to stop or restart the actor with backoff.
   *
   * @param cause failure cause.
   */
  private def onRecoveryFailure(cause: Throwable): Behavior[InternalProtocol] = {
    setup.instrumentation.recoveryFailed(setup.context.self, cause)
    onRecoveryFailed(setup.context, cause)
    setup.onSignal(setup.emptyState, RecoveryFailed(cause), catchAndLog = true)
    setup.cancelRecoveryTimer()

    tryReturnRecoveryPermit("on recovery failure: " + cause.getMessage)

    if (setup.internalLogger.isDebugEnabled)
      setup.internalLogger.debug("Recovery failure for persistenceId [{}]", setup.persistenceId)

    val msg = s"Exception during recovery. " +
      s"PersistenceId [${setup.persistenceId.id}]. ${cause.getMessage}"
    throw new DurableStateStoreException(msg, cause)
  }

  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  def onRecoveryStart(context: ActorContext[_]): Unit = ()
  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  def onRecoveryComplete(context: ActorContext[_]): Unit = ()
  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  def onRecoveryFailed(context: ActorContext[_], reason: Throwable): Unit = ()

  private def onRecoveryTimeout(): Behavior[InternalProtocol] = {
    val ex = new RecoveryTimedOut(s"Recovery timed out, didn't get state within ${setup.settings.recoveryTimeout}")
    onRecoveryFailure(ex)
  }

  def onCommand(cmd: IncomingCommand[C]): Behavior[InternalProtocol] = {
    // during recovery, stash all incoming commands
    stashInternal(cmd)
  }

  def onGetSuccess(result: GetObjectResult[S]): Behavior[InternalProtocol] = {
    val state = result.value match {
      case Some(s) => setup.snapshotAdapter.fromJournal(s)
      case None    => setup.emptyState
    }

    setup.internalLogger.debug("Recovered from revision [{}]", result.revision)

    setup.cancelRecoveryTimer()

    onRecoveryCompleted(RecoveryState(result.revision, state, recoveryState.receivedPoisonPill, System.nanoTime()))

  }

  private def onRecoveryCompleted(state: RecoveryState[S]): Behavior[InternalProtocol] =
    try {
      recoveryState = state
      setup.instrumentation.recoveryDone(setup.context.self)
      onRecoveryComplete(setup.context)
      tryReturnRecoveryPermit("recovery completed successfully")
      if (setup.internalLogger.isDebugEnabled) {
        setup.internalLogger.debug(
          "Recovery for persistenceId [{}] took {}",
          setup.persistenceId,
          (System.nanoTime() - state.recoveryStartTime).nanos.pretty)
      }

      setup.onSignal(state.state, RecoveryCompleted, catchAndLog = false)

      if (state.receivedPoisonPill && isInternalStashEmpty && !isUnstashAllInProgress)
        Behaviors.stopped
      else {
        val runningState = Running.RunningState[S](
          revision = state.revision,
          state = state.state,
          receivedPoisonPill = state.receivedPoisonPill,
          instrumentationContext = DurableStateBehaviorInstrumentation.EmptyContext)
        val running = new Running(setup.setMdcPhase(PersistenceMdc.RunningCmds))
        tryUnstashOne(new running.HandlingCommands(runningState))
      }
    } finally {
      setup.cancelRecoveryTimer()
    }

  def onGetFailure(cause: Throwable): Behavior[InternalProtocol] = {
    onRecoveryFailure(cause)
  }

  override def currentRevision: Long =
    recoveryState.revision

}
