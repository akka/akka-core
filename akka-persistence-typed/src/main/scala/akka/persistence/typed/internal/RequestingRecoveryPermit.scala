/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import akka.actor.typed.Behavior
import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.annotation.{ InternalApi, InternalStableApi }

/**
 * INTERNAL API
 *
 * First (of four) behavior of an PersistentBehaviour.
 *
 * Requests a permit to start replaying this actor; this is tone to avoid
 * hammering the journal with too many concurrently replaying actors.
 *
 * See next behavior [[ReplayingSnapshot]].
 */
@InternalApi
private[akka] object RequestingRecoveryPermit {

  def apply[C, E, S](setup: BehaviorSetup[C, E, S]): Behavior[InternalProtocol] =
    new RequestingRecoveryPermit(setup.setMdcPhase(PersistenceMdc.AwaitingPermit)).createBehavior()

}

@InternalApi
private[akka] class RequestingRecoveryPermit[C, E, S](override val setup: BehaviorSetup[C, E, S])
    extends StashManagement[C, E, S]
    with JournalInteractions[C, E, S]
    with SnapshotInteractions[C, E, S] {

  onRequestingRecoveryPermit(setup.context)

  def createBehavior(): Behavior[InternalProtocol] = {
    val instCtx =
      setup.instrumentation.beforeRequestRecoveryPermit(setup.context.self)

    // request a permit, as only once we obtain one we can start replaying
    requestRecoveryPermit()

    setup.instrumentation.afterRequestRecoveryPermit(setup.context.self, instCtx)

    def stay(receivedPoisonPill: Boolean): Behavior[InternalProtocol] = {
      Behaviors
        .receiveMessage[InternalProtocol] {
          case InternalProtocol.RecoveryPermitGranted =>
            becomeReplaying(receivedPoisonPill)

          case other =>
            if (receivedPoisonPill) {
              if (setup.settings.logOnStashing)
                setup.internalLogger.debug("Discarding message [{}], because actor is to be stopped.", other)
              Behaviors.unhandled
            } else {
              stashInternal(other)
            }

        }
        .receiveSignal {
          case (_, PoisonPill) =>
            stay(receivedPoisonPill = true)
          case (_, signal) =>
            if (setup.onSignal(setup.emptyState, signal, catchAndLog = true)) Behaviors.same
            else Behaviors.unhandled
        }
    }
    stay(receivedPoisonPill = false)
  }

  // FIXME remove instrumentation hook method in 2.10.0
  @InternalStableApi
  def onRequestingRecoveryPermit(context: ActorContext[_]): Unit = ()

  private def becomeReplaying(receivedPoisonPill: Boolean): Behavior[InternalProtocol] = {
    setup.instrumentation.recoveryStarted(setup.context.self)
    setup.internalLogger.debug(s"Initializing snapshot recovery: {}", setup.recovery)

    setup.holdingRecoveryPermit = true
    ReplayingSnapshot(setup, receivedPoisonPill)
  }

}
