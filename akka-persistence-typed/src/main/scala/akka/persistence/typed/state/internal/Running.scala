/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state.internal

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable

import akka.actor.UnhandledMessage
import akka.actor.typed.Behavior
import akka.actor.typed.Signal
import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.persistence.typed.state.internal.DurableStateBehaviorImpl.GetState
import akka.persistence.typed.state.scaladsl.Effect
import akka.persistence.typed.telemetry.DurableStateBehaviorInstrumentation
import akka.util.OptionVal

/**
 * INTERNAL API
 *
 * Conceptually third (of three) -- also known as 'final' or 'ultimate' -- form of `DurableStateBehavior`.
 *
 * In this phase recovery has completed successfully and we continue handling incoming commands,
 * as well as persisting new state as dictated by the user handlers.
 *
 * This behavior operates in two phases (also behaviors):
 * - HandlingCommands - where the command handler is invoked for incoming commands
 * - PersistingState - where incoming commands are stashed until persistence completes
 *
 * This is implemented as such to avoid creating many Running instances,
 * which perform the Persistence extension lookup on creation and similar things (config lookup)
 *
 * See previous [[Recovering]].
 */
@InternalApi
private[akka] object Running {

  private val MaxRecursiveUnstash = 100

  trait WithRevisionAccessible {
    def currentRevision: Long
  }

  final case class RunningState[State](
      revision: Long,
      state: State,
      receivedPoisonPill: Boolean,
      instrumentationContext: DurableStateBehaviorInstrumentation.Context) {

    def nextRevision(): RunningState[State] =
      copy(revision = revision + 1)

    def applyState[C, E](
        @nowarn("msg=never used") setup: BehaviorSetup[C, State],
        updated: State): RunningState[State] = {
      copy(state = updated)
    }

    def updateInstrumentationContext(
        instrumentationContext: DurableStateBehaviorInstrumentation.Context): RunningState[State] = {
      if (instrumentationContext eq this.instrumentationContext) this // avoid instance creation for EmptyContext
      else copy(instrumentationContext = instrumentationContext)
    }

    def clearInstrumentationContext: RunningState[State] =
      updateInstrumentationContext(DurableStateBehaviorInstrumentation.EmptyContext)
  }
}

// ===============================================

/** INTERNAL API */
@InternalApi private[akka] final class Running[C, S](override val setup: BehaviorSetup[C, S])
    extends DurableStateStoreInteractions[C, S]
    with StashManagement[C, S] {
  import InternalProtocol._
  import Running.RunningState
  import Running.WithRevisionAccessible
  import Running.MaxRecursiveUnstash

  // Needed for WithSeqNrAccessible, when unstashing
  private var _currentRevision = 0L

  private var recursiveUnstashOne = 0

  final class HandlingCommands(state: RunningState[S])
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithRevisionAccessible {

    _currentRevision = state.revision

    def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = msg match {
      case IncomingCommand(c: C @unchecked) => onCommand(state, c)
      case get: GetState[S @unchecked]      => onGetState(get)
      case _                                => Behaviors.unhandled
    }

    override def onSignal: PartialFunction[Signal, Behavior[InternalProtocol]] = {
      case PoisonPill =>
        if (isInternalStashEmpty && !isUnstashAllInProgress) Behaviors.stopped
        else new HandlingCommands(state.copy(receivedPoisonPill = true))
      case signal =>
        if (setup.onSignal(state.state, signal, catchAndLog = false)) this
        else Behaviors.unhandled
    }

    def onCommand(state: RunningState[S], cmd: C): Behavior[InternalProtocol] = {
      val effect = setup.commandHandler(state.state, cmd)
      val (next, doUnstash) = applyEffects(cmd, state, effect.asInstanceOf[EffectImpl[S]])
      if (doUnstash) tryUnstashOne(next)
      else {
        recursiveUnstashOne = 0
        next
      }
    }

    // Used by DurableStateBehaviorTestKit to retrieve the state.
    def onGetState(get: GetState[S]): Behavior[InternalProtocol] = {
      get.replyTo ! state.state
      this
    }

    private def handlePersist(
        newState: S,
        cmd: Any,
        sideEffects: immutable.Seq[SideEffect[S]]): (Behavior[InternalProtocol], Boolean) = {
      _currentRevision = state.revision + 1

      val stateAfterApply = state.applyState(setup, newState)
      val stateToPersist = adaptState(newState)

      val changeEvent = setup.changeEventHandler match {
        case None          => OptionVal.None
        case Some(handler) => OptionVal.Some(handler.updateHandler(state.state, stateAfterApply.state, cmd))
      }

      val newState2 =
        internalUpsert(setup.context, cmd, stateAfterApply, stateToPersist, changeEvent)

      (persistingState(newState2, state, sideEffects), false)
    }

    private def handleDelete(
        cmd: Any,
        sideEffects: immutable.Seq[SideEffect[S]]): (Behavior[InternalProtocol], Boolean) = {
      _currentRevision = state.revision + 1

      val changeEvent = setup.changeEventHandler match {
        case None          => OptionVal.None
        case Some(handler) => OptionVal.Some(handler.deleteHandler(state.state, cmd))
      }

      val nextState = internalDelete(setup.context, cmd, state, changeEvent)

      (persistingState(nextState, state, sideEffects), false)
    }

    @tailrec def applyEffects(
        msg: Any,
        state: RunningState[S],
        effect: Effect[S],
        sideEffects: immutable.Seq[SideEffect[S]] = Nil): (Behavior[InternalProtocol], Boolean) = {
      if (setup.internalLogger.isDebugEnabled && !effect.isInstanceOf[CompositeEffect[_]])
        setup.internalLogger.debug(
          s"Handled command [{}], resulting effect: [{}], side effects: [{}]",
          msg.getClass.getName,
          effect,
          sideEffects.size)

      effect match {
        case CompositeEffect(eff, currentSideEffects: Seq[SideEffect[S @unchecked]]) =>
          // unwrap and accumulate effects
          applyEffects(msg, state, eff, currentSideEffects ++ sideEffects)

        case Persist(newState) =>
          handlePersist(newState, msg, sideEffects)

        case _: PersistNothing.type =>
          (applySideEffects(sideEffects, state), true)

        case _: Delete[_] =>
          handleDelete(msg, sideEffects)

        case _: Unhandled.type =>
          import akka.actor.typed.scaladsl.adapter._
          setup.context.system.toClassic.eventStream
            .publish(UnhandledMessage(msg, setup.context.system.toClassic.deadLetters, setup.context.self.toClassic))
          (applySideEffects(sideEffects, state), true)

        case _: Stash.type =>
          stashUser(IncomingCommand(msg))
          (applySideEffects(sideEffects, state), true)

        case unexpected => throw new IllegalStateException(s"Unexpected effect: $unexpected")
      }
    }

    def adaptState(newState: S): Any = {
      setup.snapshotAdapter.toJournal(newState)
    }

    setup.setMdcPhase(PersistenceMdc.RunningCmds)

    override def currentRevision: Long =
      _currentRevision

    // note that this shadows tryUnstashOne in StashManagement from HandlingCommands
    private def tryUnstashOne(behavior: Behavior[InternalProtocol]): Behavior[InternalProtocol] = {
      if (isStashEmpty) {
        recursiveUnstashOne = 0
        behavior
      } else {
        recursiveUnstashOne += 1
        if (recursiveUnstashOne >= MaxRecursiveUnstash && behavior.isInstanceOf[HandlingCommands]) {
          // avoid StackOverflow from too many recursive tryUnstashOne (stashed read only commands)
          recursiveUnstashOne = 0
          setup.context.self ! ContinueUnstash
          new WaitingForContinueUnstash(state)
        } else
          Running.this.tryUnstashOne(behavior)
      }
    }
  }

  // ===============================================

  def persistingState(
      state: RunningState[S],
      visibleState: RunningState[S], // previous state until write success
      sideEffects: immutable.Seq[SideEffect[S]]): Behavior[InternalProtocol] = {
    setup.setMdcPhase(PersistenceMdc.PersistingState)
    recursiveUnstashOne = 0
    new PersistingState(state, visibleState, sideEffects)
  }

  /** INTERNAL API */
  @InternalApi private[akka] class PersistingState(
      var state: RunningState[S],
      var visibleState: RunningState[S], // previous state until write success
      var sideEffects: immutable.Seq[SideEffect[S]],
      persistStartTime: Long = System.nanoTime())
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithRevisionAccessible {

    override def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = {
      msg match {
        case UpsertSuccess                     => onUpsertSuccess()
        case UpsertFailure(exc)                => onUpsertFailed(exc)
        case in: IncomingCommand[C @unchecked] => onCommand(in)
        case get: GetState[S @unchecked]       => stashInternal(get)
        case DeleteSuccess                     => onDeleteSuccess()
        case DeleteFailure(exc)                => onDeleteFailed(exc)
        case RecoveryTimeout                   => Behaviors.unhandled
        case RecoveryPermitGranted             => Behaviors.unhandled
        case _: GetSuccess[_]                  => Behaviors.unhandled
        case _: GetFailure                     => Behaviors.unhandled
        case ContinueUnstash                   => Behaviors.unhandled
      }
    }

    def onCommand(cmd: IncomingCommand[C]): Behavior[InternalProtocol] = {
      if (state.receivedPoisonPill) {
        if (setup.settings.logOnStashing)
          setup.internalLogger.debug("Discarding message [{}], because actor is to be stopped.", cmd)
        Behaviors.unhandled
      } else {
        stashInternal(cmd)
      }
    }

    final def onUpsertSuccess(): Behavior[InternalProtocol] = {
      if (setup.internalLogger.isDebugEnabled) {
        setup.internalLogger
          .debug("Received UpsertSuccess response after: {} nanos", System.nanoTime() - persistStartTime)
      }

      val instrumentationContext2 =
        setup.instrumentation.persistStateWritten(setup.context.self, state.state, state.instrumentationContext)
      val state2 = state.updateInstrumentationContext(instrumentationContext2)
      onWriteSuccess(setup.context)

      visibleState = state2
      val behavior = applySideEffects(sideEffects, state2.clearInstrumentationContext)
      setup.instrumentation.persistStateDone(setup.context.self, instrumentationContext2)
      tryUnstashOne(behavior)
    }

    final def onUpsertFailed(cause: Throwable): Behavior[InternalProtocol] = {
      setup.instrumentation.persistFailed(
        setup.context.self,
        cause,
        state.state,
        state.revision,
        state.instrumentationContext)
      onWriteFailed(setup.context, cause)
      throw new DurableStateStoreException(setup.persistenceId, currentRevision, cause)
    }

    final def onDeleteSuccess(): Behavior[InternalProtocol] = {
      if (setup.internalLogger.isDebugEnabled) {
        setup.internalLogger
          .debug("Received DeleteSuccess response after: {} nanos", System.nanoTime() - persistStartTime)
      }

      val instrumentationContext2 =
        setup.instrumentation.persistStateWritten(setup.context.self, state.state, state.instrumentationContext)
      val state2 = state.updateInstrumentationContext(instrumentationContext2)

      visibleState = state2
      val behavior = applySideEffects(sideEffects, state2.clearInstrumentationContext)
      setup.instrumentation.persistStateDone(setup.context.self, instrumentationContext2)
      tryUnstashOne(behavior)
    }

    final def onDeleteFailed(cause: Throwable): Behavior[InternalProtocol] = {
      setup.instrumentation.persistFailed(
        setup.context.self,
        cause,
        state.state,
        state.revision,
        state.instrumentationContext)
      throw new DurableStateStoreException(setup.persistenceId, currentRevision, cause)
    }

    override def onSignal: PartialFunction[Signal, Behavior[InternalProtocol]] = {
      case PoisonPill =>
        // wait for store responses before stopping
        state = state.copy(receivedPoisonPill = true)
        this
      case signal =>
        if (setup.onSignal(visibleState.state, signal, catchAndLog = false)) this
        else Behaviors.unhandled
    }

    override def currentRevision: Long =
      _currentRevision
  }

  // ===============================================

  /** INTERNAL API */
  @InternalApi private[akka] class WaitingForContinueUnstash(state: RunningState[S])
      extends AbstractBehavior[InternalProtocol](setup.context)
      with WithRevisionAccessible {

    def onCommand(cmd: IncomingCommand[C]): Behavior[InternalProtocol] = {
      if (state.receivedPoisonPill) {
        if (setup.settings.logOnStashing)
          setup.internalLogger.debug("Discarding message [{}], because actor is to be stopped.", cmd)
        Behaviors.unhandled
      } else {
        stashInternal(cmd)
      }
    }

    def onMessage(msg: InternalProtocol): Behavior[InternalProtocol] = msg match {
      case ContinueUnstash =>
        tryUnstashOne(new HandlingCommands(state))
      case cmd: IncomingCommand[C] @unchecked =>
        onCommand(cmd)
      case get: GetState[S @unchecked] =>
        stashInternal(get)
      case _ =>
        Behaviors.unhandled
    }

    override def onSignal: PartialFunction[Signal, Behavior[InternalProtocol]] = {
      case PoisonPill =>
        // wait for ContinueUnstash before stopping
        new WaitingForContinueUnstash(state.copy(receivedPoisonPill = true))
      case signal =>
        if (setup.onSignal(state.state, signal, catchAndLog = false))
          Behaviors.same
        else
          Behaviors.unhandled
    }

    override def currentRevision: Long =
      _currentRevision
  }

  // ===============================================

  def applySideEffects(effects: immutable.Seq[SideEffect[S]], state: RunningState[S]): Behavior[InternalProtocol] = {
    var behavior: Behavior[InternalProtocol] = new HandlingCommands(state)
    val it = effects.iterator

    // if at least one effect results in a `stop`, we need to stop
    // manual loop implementation to avoid allocations and multiple scans
    while (it.hasNext) {
      val effect = it.next()
      behavior = applySideEffect(effect, state, behavior)
    }

    if (state.receivedPoisonPill && isInternalStashEmpty && !isUnstashAllInProgress)
      Behaviors.stopped
    else
      behavior
  }

  def applySideEffect(
      effect: SideEffect[S],
      state: RunningState[S],
      behavior: Behavior[InternalProtocol]): Behavior[InternalProtocol] = {
    effect match {
      case _: Stop.type @unchecked =>
        Behaviors.stopped

      case _: UnstashAll.type @unchecked =>
        unstashAll()
        behavior

      case callback: Callback[Any] @unchecked =>
        callback.sideEffect(state.state)
        behavior

      case _ =>
        // case _: Callback[S] should be covered by above case, but needed needed to silence Scala 3 exhaustive match
        throw new IllegalStateException(
          s"Unexpected effect [${effect.getClass.getName}]. This is a bug, please report https://github.com/akka/akka-core/issues")
    }
  }

  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  private[akka] def onWriteFailed(ctx: ActorContext[_], reason: Throwable): Unit = ()
  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  private[akka] def onWriteSuccess(ctx: ActorContext[_]): Unit = ()

}
