/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.util.control.NonFatal

import akka.actor.Actor
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] class SubReceive(initial: Actor.Receive) extends Actor.Receive {
  private var currentReceive = initial

  override def isDefinedAt(msg: Any): Boolean = currentReceive.isDefinedAt(msg)
  override def apply(msg: Any): Unit = currentReceive.apply(msg)

  def become(newBehavior: Actor.Receive): Unit = {
    currentReceive = newBehavior
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait Inputs {
  def NeedsInput: TransferState
  def NeedsInputOrComplete: TransferState

  def dequeueInputElement(): Any

  def subreceive: SubReceive

  def cancel(): Unit
  def isClosed: Boolean
  def isOpen: Boolean = !isClosed

  def inputsDepleted: Boolean
  def inputsAvailable: Boolean
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait DefaultInputTransferStates extends Inputs {
  override val NeedsInput: TransferState = new TransferState {
    def isReady = inputsAvailable
    def isCompleted = inputsDepleted
  }
  override val NeedsInputOrComplete: TransferState = new TransferState {
    def isReady = inputsAvailable || inputsDepleted
    def isCompleted = false
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait Outputs {
  def NeedsDemand: TransferState
  def NeedsDemandOrCancel: TransferState

  def demandAvailable: Boolean
  def enqueueOutputElement(elem: Any): Unit

  def subreceive: SubReceive

  // FIXME: This is completely unnecessary, refactor MapFutureProcessorImpl
  def demandCount: Long = -1L

  def complete(): Unit
  def cancel(): Unit
  def error(e: Throwable): Unit
  def isClosed: Boolean
  def isOpen: Boolean = !isClosed
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait DefaultOutputTransferStates extends Outputs {
  override val NeedsDemand: TransferState = new TransferState {
    def isReady = demandAvailable
    def isCompleted = isClosed
  }
  override def NeedsDemandOrCancel: TransferState = new TransferState {
    def isReady = demandAvailable || isClosed
    def isCompleted = false
  }
}

// States of the operation that is executed by this processor
/**
 * INTERNAL API
 */
@InternalApi private[akka] trait TransferState {
  def isReady: Boolean
  def isCompleted: Boolean
  def isExecutable = isReady && !isCompleted

  def ||(other: TransferState): TransferState = new TransferState {
    def isReady: Boolean = TransferState.this.isReady || other.isReady
    def isCompleted: Boolean = TransferState.this.isCompleted && other.isCompleted
  }

  def &&(other: TransferState): TransferState = new TransferState {
    def isReady: Boolean = TransferState.this.isReady && other.isReady
    def isCompleted: Boolean = TransferState.this.isCompleted || other.isCompleted
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object Completed extends TransferState {
  def isReady = false
  def isCompleted = true
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object NotInitialized extends TransferState {
  def isReady = false
  def isCompleted = false
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] case class WaitingForUpstreamSubscription(remaining: Int, andThen: TransferPhase)
    extends TransferState {
  def isReady = false
  def isCompleted = false
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object Always extends TransferState {
  def isReady = true
  def isCompleted = false
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class TransferPhase(precondition: TransferState)(val action: () => Unit)

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait Pump {
  private var transferState: TransferState = NotInitialized
  private var currentAction: () => Unit =
    () => throw new IllegalStateException("Pump has been not initialized with a phase")

  final def initialPhase(waitForUpstream: Int, andThen: TransferPhase): Unit = {
    require(waitForUpstream >= 1, s"waitForUpstream must be >= 1 (was $waitForUpstream)")
    if (transferState != NotInitialized)
      throw new IllegalStateException(s"initialPhase expected NotInitialized, but was [$transferState]")
    transferState = WaitingForUpstreamSubscription(waitForUpstream, andThen)
  }

  final def waitForUpstreams(waitForUpstream: Int): Unit = {
    require(waitForUpstream >= 1, s"waitForUpstream must be >= 1 (was $waitForUpstream)")
    transferState = WaitingForUpstreamSubscription(waitForUpstream, TransferPhase(transferState)(currentAction))
  }

  def gotUpstreamSubscription(): Unit = {
    transferState match {
      case WaitingForUpstreamSubscription(1, andThen) =>
        transferState = andThen.precondition
        currentAction = andThen.action
      case WaitingForUpstreamSubscription(remaining, andThen) =>
        transferState = WaitingForUpstreamSubscription(remaining - 1, andThen)
      case _ => // ok, initial phase not used, or passed already
    }
    pump()
  }

  final def nextPhase(phase: TransferPhase): Unit = transferState match {
    case WaitingForUpstreamSubscription(remaining, _) =>
      transferState = WaitingForUpstreamSubscription(remaining, phase)
    case _ =>
      transferState = phase.precondition
      currentAction = phase.action
  }

  final def isPumpFinished: Boolean = transferState.isCompleted

  protected final val completedPhase = TransferPhase(Completed) { () =>
    throw new IllegalStateException("The action of completed phase must be never executed")
  }

  // Exchange input buffer elements and output buffer "requests" until one of them becomes empty.
  // Generate upstream requestMore for every Nth consumed input element
  final def pump(): Unit = {
    try while (transferState.isExecutable) {
      currentAction()
    } catch { case NonFatal(e) => pumpFailed(e) }

    if (isPumpFinished) pumpFinished()
  }

  protected def pumpFailed(e: Throwable): Unit
  protected def pumpFinished(): Unit

}
