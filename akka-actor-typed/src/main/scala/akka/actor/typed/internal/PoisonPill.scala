/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import akka.actor.typed.Behavior
import akka.actor.typed.BehaviorInterceptor
import akka.actor.typed.BehaviorSignalInterceptor
import akka.actor.typed.Signal
import akka.actor.typed.TypedActorContext
import akka.annotation.InternalApi

/**
 * INTERNAL API
 *
 * Note that this is a `Signal` poison pill, not a universal poison pill like the classic actor one.
 * This requires special handling on the receiving side where it is used (for example with the interceptor below).
 */
@InternalApi private[akka] sealed abstract class PoisonPill extends Signal

/**
 * INTERNAL API
 */
@InternalApi private[akka] case object PoisonPill extends PoisonPill {
  def instance: PoisonPill = this
}

/**
 * INTERNAL API
 *
 * Returns `Behaviors.stopped` for [[PoisonPill]] signals unless it has been handled by the target `Behavior`.
 * Used by Cluster Sharding to automatically stop entities without defining a stop message in the
 * application protocol. Persistent actors handle `PoisonPill` and run side effects after persist
 * and process stashed messages before stopping.
 */
@InternalApi private[akka] final class PoisonPillInterceptor[M] extends BehaviorSignalInterceptor[M] {

  override def aroundSignal(
      ctx: TypedActorContext[M],
      signal: Signal,
      target: BehaviorInterceptor.SignalTarget[M]): Behavior[M] = {
    signal match {
      case p: PoisonPill =>
        val next = target(ctx, p)
        if (Behavior.isUnhandled(next)) BehaviorImpl.stopped
        else next
      case _ => target(ctx, signal)
    }
  }

  override def isSame(other: BehaviorInterceptor[Any, Any]): Boolean =
    // only one interceptor per behavior stack is needed
    other.isInstanceOf[PoisonPillInterceptor[_]]
}
