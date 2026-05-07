/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Terminated
import akka.annotation.{ InternalApi, InternalStableApi }
import akka.persistence.telemetry.RecoveryPermitterInstrumentationProvider
import akka.util.MessageBuffer

/**
 * INTERNAL API
 */
@InternalApi private[akka] object RecoveryPermitter {
  def props(maxPermits: Int): Props =
    Props(new RecoveryPermitter(maxPermits))

  sealed trait Protocol
  sealed trait Request extends Protocol
  sealed trait Reply extends Protocol
  case object RequestRecoveryPermit extends Request
  case object RecoveryPermitGranted extends Reply
  case object ReturnRecoveryPermit extends Request

}

/**
 * INTERNAL API: When starting many persistent actors at the same time the journal
 * its data store is protected from being overloaded by limiting number
 * of recoveries that can be in progress at the same time.
 */
@InternalApi private[akka] class RecoveryPermitter(maxPermits: Int) extends Actor with ActorLogging {
  import RecoveryPermitter._

  private var usedPermits = 0

  @InternalStableApi
  private val pendingBuffer = MessageBuffer.empty
  private var maxPendingStats = 0

  private val instrumentation = RecoveryPermitterInstrumentationProvider(context.system).instrumentation

  def receive: Actor.Receive = {
    case RequestRecoveryPermit =>
      context.watch(sender())
      if (usedPermits >= maxPermits) {
        if (pendingBuffer.isEmpty)
          log.debug("Exceeded max-concurrent-recoveries [{}]. First pending {}", maxPermits, sender())
        pendingBuffer.append(RequestRecoveryPermit, sender())
        maxPendingStats = math.max(maxPendingStats, pendingBuffer.size)
      } else {
        recoveryPermitGranted(sender())
      }

    case ReturnRecoveryPermit =>
      onReturnRecoveryPermit(sender())

    case Terminated(ref) =>
      // pre-mature termination should be rare
      val before = pendingBuffer.size
      pendingBuffer.filterNot { case (_, r) => r == ref }
      if (before == pendingBuffer.size)
        onReturnRecoveryPermit(ref) // it wasn't pending, so return permit
  }

  private def onReturnRecoveryPermit(ref: ActorRef): Unit = {
    usedPermits -= 1
    context.unwatch(ref)
    if (usedPermits < 0) throw new IllegalStateException(s"permits must not be negative (returned by: ${ref})")
    if (!pendingBuffer.isEmpty) {
      val ref = pendingBuffer.head()._2
      pendingBuffer.dropHead()
      recoveryPermitGranted(ref)
    }
    if (pendingBuffer.isEmpty && maxPendingStats > 0) {
      log.debug(
        "Drained pending recovery permit requests, max in progress was [{}], still [{}] in progress",
        usedPermits + maxPendingStats,
        usedPermits)
      maxPendingStats = 0
    }
    instrumentation.recoveryPermitterStatus(self, maxPermits, usedPermits, pendingBuffer.size)
  }

  private def recoveryPermitGranted(ref: ActorRef): Unit = {
    usedPermits += 1
    instrumentation.recoveryPermitterStatus(self, maxPermits, usedPermits, pendingBuffer.size)
    ref ! RecoveryPermitGranted
  }

}
