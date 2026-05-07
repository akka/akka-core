/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.internal

import scala.util.control.NonFatal

import org.slf4j.Logger
import org.slf4j.MDC

import akka.actor.{ ActorRef => ClassicActorRef }
import akka.actor.Cancellable
import akka.actor.typed.Signal
import akka.actor.typed.scaladsl.ActorContext
import akka.annotation.InternalApi
import akka.persistence._
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.DurableStateUpdateStore
import akka.persistence.state.scaladsl.DurableStateUpdateWithChangeEventStore
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.SnapshotAdapter
import akka.persistence.typed.state.internal.InternalProtocol.RecoveryTimeout
import akka.persistence.typed.state.scaladsl.ChangeEventHandler
import akka.persistence.typed.state.scaladsl.DurableStateBehavior
import akka.persistence.typed.telemetry.DurableStateBehaviorInstrumentation
import akka.util.OptionVal

/**
 * INTERNAL API: Carry state for the `DurableStateBehavior` implementation behaviors.
 */
@InternalApi
private[akka] final class BehaviorSetup[C, S](
    val context: ActorContext[InternalProtocol],
    val persistenceId: PersistenceId,
    val emptyState: S,
    val commandHandler: DurableStateBehavior.CommandHandler[C, S],
    private val signalHandler: PartialFunction[(S, Signal), Unit],
    val tag: String,
    val snapshotAdapter: SnapshotAdapter[S],
    var holdingRecoveryPermit: Boolean,
    val settings: DurableStateSettings,
    val stashState: StashState,
    private val internalLoggerFactory: () => Logger,
    val changeEventHandler: Option[ChangeEventHandler[Any, S, Any]],
    val instrumentation: DurableStateBehaviorInstrumentation) {

  import akka.actor.typed.scaladsl.adapter._

  val persistence: Persistence = Persistence(context.system)

  // Any instead S because adapter may change the type
  val durableStateStore: DurableStateUpdateStore[Any] =
    DurableStateStoreRegistry(context.system)
      .durableStateStoreFor[DurableStateUpdateStore[Any]](settings.durableStateStorePluginId)

  // fail early if state store doesn't implement change events
  if (changeEventHandler.isDefined && !durableStateStore.isInstanceOf[DurableStateUpdateWithChangeEventStore[_]])
    new IllegalArgumentException(
      "Change event handler was defined but the DurableStateStore " +
      s"[${durableStateStore.getClass.getName}] doesn't implement [DurableStateUpdateWithChangeEventStore]")

  def selfClassic: ClassicActorRef = context.self.toClassic

  private var mdcPhase = PersistenceMdc.Initializing

  def internalLogger: Logger = {
    PersistenceMdc.setMdc(persistenceId, mdcPhase)
    internalLoggerFactory()
  }

  def setMdcPhase(phaseName: String): BehaviorSetup[C, S] = {
    mdcPhase = phaseName
    this
  }

  private var recoveryTimer: OptionVal[Cancellable] = OptionVal.None

  def startRecoveryTimer(): Unit = {
    cancelRecoveryTimer()
    val timer = context.scheduleOnce(settings.recoveryTimeout, context.self, RecoveryTimeout)
    recoveryTimer = OptionVal.Some(timer)
  }

  def cancelRecoveryTimer(): Unit = {
    recoveryTimer match {
      case OptionVal.Some(t) => t.cancel()
      case _                 =>
    }
    recoveryTimer = OptionVal.None
  }

  /**
   * Applies the `signalHandler` if defined and returns true, otherwise returns false.
   * If an exception is thrown and `catchAndLog=true` it is logged and returns true, otherwise it is thrown.
   *
   * `catchAndLog=true` should be used for "unknown" signals in the phases before Running
   * to avoid restart loops if restart supervision is used.
   */
  def onSignal[T](state: S, signal: Signal, catchAndLog: Boolean): Boolean = {
    try {
      var handled = true
      signalHandler.applyOrElse((state, signal), (_: (S, Signal)) => handled = false)
      handled
    } catch {
      case NonFatal(ex) =>
        if (catchAndLog) {
          internalLogger.error(s"Error while processing signal [$signal]: $ex", ex)
          true
        } else {
          if (internalLogger.isDebugEnabled)
            internalLogger.debug(s"Error while processing signal [$signal]: $ex", ex)
          throw ex
        }
    }
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object PersistenceMdc {
  // format: OFF
  val Initializing      = "initializing"
  val AwaitingPermit    = "get-permit"
  val RecoveringState = "recovering"
  val RunningCmds       = "running-cmd"
  val PersistingState  = "persist-state"
  // format: ON

  val PersistencePhaseKey = "persistencePhase"
  val PersistenceIdKey = "persistenceId"

  // MDC is cleared (if used) from aroundReceive in ActorAdapter after processing each message,
  // but important to call `context.log` to mark MDC as used
  def setMdc(persistenceId: PersistenceId, phase: String): Unit = {
    MDC.put(PersistenceIdKey, persistenceId.id)
    MDC.put(PersistencePhaseKey, phase)
  }

}
