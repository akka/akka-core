/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.internal

import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.PreRestart
import akka.actor.typed.Signal
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.persistence._
import akka.persistence.state.scaladsl.GetObjectResult
import akka.util.unused

/** INTERNAL API */
@InternalApi
private[akka] trait DurableStateStoreInteractions[C, S] {

  def setup: BehaviorSetup[C, S]

  // FIXME use CircuitBreaker here, that can also replace the RecoveryTimeout

  protected def internalGet(ctx: ActorContext[InternalProtocol]): Unit = {
    val persistenceId = setup.persistenceId.id
    ctx.pipeToSelf[GetObjectResult[Any]](setup.durableStateStore.getObject(persistenceId)) {
      case Success(state) => InternalProtocol.GetSuccess(state)
      case Failure(cause) => InternalProtocol.GetFailure(cause)
    }
  }

  protected def internalUpsert(
      ctx: ActorContext[InternalProtocol],
      cmd: Any,
      state: Running.RunningState[S],
      value: Any): Running.RunningState[S] = {

    val newRunningState = state.nextRevision()
    val persistenceId = setup.persistenceId.id

    onWriteInitiated(ctx, cmd)

    ctx.pipeToSelf[Done](
      setup.durableStateStore.upsertObject(persistenceId, newRunningState.revision, value, setup.tag)) {
      case Success(_)     => InternalProtocol.UpsertSuccess
      case Failure(cause) => InternalProtocol.UpsertFailure(cause)
    }

    newRunningState
  }

  protected def internalDelete(
      ctx: ActorContext[InternalProtocol],
      @unused cmd: Any,
      state: Running.RunningState[S]): Running.RunningState[S] = {

    val newRunningState = state.nextRevision().copy(state = setup.emptyState)
    val persistenceId = setup.persistenceId.id

    // TODO Might need to call hook method for Telemetry

    ctx.pipeToSelf[Done](setup.durableStateStore.deleteObject(persistenceId, newRunningState.revision)) {
      case Success(_)     => InternalProtocol.DeleteSuccess
      case Failure(cause) => InternalProtocol.DeleteFailure(cause)
    }

    newRunningState
  }

  // TODO These hook methods are for Telemetry. What more parameters are needed? persistenceId?
  @InternalStableApi
  private[akka] def onWriteInitiated(@unused ctx: ActorContext[_], @unused cmd: Any): Unit = ()

  protected def requestRecoveryPermit(): Unit = {
    setup.persistence.recoveryPermitter.tell(RecoveryPermitter.RequestRecoveryPermit, setup.selfClassic)
  }

  /** Intended to be used in .onSignal(returnPermitOnStop) by behaviors */
  protected def returnPermitOnStop
      : PartialFunction[(ActorContext[InternalProtocol], Signal), Behavior[InternalProtocol]] = {
    case (_, PostStop) =>
      tryReturnRecoveryPermit("PostStop")
      Behaviors.stopped
    case (_, PreRestart) =>
      tryReturnRecoveryPermit("PreRestart")
      Behaviors.stopped
  }

  /** Mutates setup, by setting the `holdingRecoveryPermit` to false */
  protected def tryReturnRecoveryPermit(reason: String): Unit = {
    if (setup.holdingRecoveryPermit) {
      setup.internalLogger.debug("Returning recovery permit, reason: {}", reason)
      setup.persistence.recoveryPermitter.tell(RecoveryPermitter.ReturnRecoveryPermit, setup.selfClassic)
      setup.holdingRecoveryPermit = false
    } // else, no need to return the permit
  }

}
