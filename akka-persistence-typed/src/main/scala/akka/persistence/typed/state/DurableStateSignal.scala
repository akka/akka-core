/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.state

import akka.actor.typed.Signal
import akka.annotation.DoNotInherit

/**
 * Supertype for all `DurableStateBehavior` specific signals
 *
 * Not for user extension
 */
@DoNotInherit
sealed trait DurableStateSignal extends Signal

@DoNotInherit sealed abstract class RecoveryCompleted extends DurableStateSignal
case object RecoveryCompleted extends RecoveryCompleted {
  def instance: RecoveryCompleted = this
}

final case class RecoveryFailed(failure: Throwable) extends DurableStateSignal {

  /**
   * Java API
   */
  def getFailure(): Throwable = failure
}
