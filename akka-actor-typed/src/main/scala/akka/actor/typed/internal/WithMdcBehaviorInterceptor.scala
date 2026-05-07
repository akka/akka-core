/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import scala.reflect.ClassTag

import org.slf4j.MDC

import akka.actor.typed.{ Behavior, BehaviorInterceptor, Signal, TypedActorContext }
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object WithMdcBehaviorInterceptor {
  val noMdcPerMessage = (_: Any) => Map.empty[String, String]

  def apply[T: ClassTag](
      staticMdc: Map[String, String],
      mdcForMessage: T => Map[String, String],
      behavior: Behavior[T]): Behavior[T] = {
    BehaviorImpl.intercept(() => new WithMdcBehaviorInterceptor[T](staticMdc, mdcForMessage))(behavior)
  }

}

/**
 * Support for Mapped Diagnostic Context for logging
 *
 * INTERNAL API
 */
@InternalApi private[akka] final class WithMdcBehaviorInterceptor[T: ClassTag] private (
    staticMdc: Map[String, String],
    mdcForMessage: T => Map[String, String])
    extends BehaviorInterceptor[T, T] {

  import BehaviorInterceptor._

  override def aroundStart(ctx: TypedActorContext[T], target: PreStartTarget[T]): Behavior[T] = {
    // when declaring we expect the outermost to win
    // for example with
    // val behavior = ...
    // val withMdc1 = withMdc(Map("first" -> true))
    // ...
    // val withMdc2 = withMdc(Map("second" -> true))
    // we'd expect the second one to be used
    // so we need to look through the stack and eliminate any MCD already existing
    def loop(next: Behavior[T]): Behavior[T] = {
      next match {
        case i: InterceptorImpl[_, T @unchecked]
            if i.interceptor.isSame(this.asInstanceOf[BehaviorInterceptor[Any, Any]]) =>
          // eliminate that interceptor
          loop(i.nestedBehavior)

        case i: InterceptorImpl[T @unchecked, T @unchecked] =>
          val nested = i.nestedBehavior
          val inner = loop(nested)
          if (inner eq nested) i
          else i.replaceNested(inner)

        case b => b
      }
    }
    try {
      setMdcValues(Map.empty)
      loop(target.start(ctx))
    } finally {
      MDC.clear()
    }
  }

  // in the normal case, a new withMDC replaces the previous one
  override def isSame(other: BehaviorInterceptor[Any, Any]): Boolean = other match {
    case _: WithMdcBehaviorInterceptor[_] => true
    case _                                => false
  }

  override def aroundReceive(ctx: TypedActorContext[T], msg: T, target: ReceiveTarget[T]): Behavior[T] = {
    try {
      setMdcValues(mdcForMessage(msg))
      target(ctx, msg)
    } finally {
      MDC.clear()
    }
  }

  override def aroundSignal(ctx: TypedActorContext[T], signal: Signal, target: SignalTarget[T]): Behavior[T] = {
    try {
      setMdcValues(Map.empty)
      target(ctx, signal)
    } finally {
      MDC.clear()
    }
  }

  private def setMdcValues(dynamicMdc: Map[String, String]): Unit = {
    if (staticMdc.nonEmpty) staticMdc.foreach {
      case (key, value) => MDC.put(key, value)
    }
    if (dynamicMdc.nonEmpty) dynamicMdc.foreach {
      case (key, value) => MDC.put(key, value)
    }
  }

  override def toString: String = s"WithMdc($staticMdc)"
}
