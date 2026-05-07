/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import scala.reflect.ClassTag

import org.slf4j.LoggerFactory
import org.slf4j.event.Level

import akka.actor.typed
import akka.actor.typed._
import akka.actor.typed.LogOptions
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.util.LineNumbers

/**
 * Provides the impl of any behavior that could nest another behavior
 *
 * INTERNAL API
 */
@InternalApi
private[akka] object InterceptorImpl {

  def apply[O, I](interceptor: () => BehaviorInterceptor[O, I], nestedBehavior: Behavior[I]): Behavior[O] = {
    BehaviorImpl.DeferredBehavior[O] { ctx =>
      val interceptorBehavior = new InterceptorImpl[O, I](interceptor(), nestedBehavior)
      interceptorBehavior.preStart(ctx)
    }
  }
}

/**
 * Provides the impl of any behavior that could nest another behavior
 *
 * INTERNAL API
 */
@InternalApi
private[akka] final class InterceptorImpl[O, I](
    val interceptor: BehaviorInterceptor[O, I],
    val nestedBehavior: Behavior[I])
    extends ExtensibleBehavior[O] {

  import BehaviorInterceptor._

  private val preStartTarget: PreStartTarget[I] = new PreStartTarget[I] {
    override def start(ctx: TypedActorContext[_]): Behavior[I] = {
      Behavior.start[I](nestedBehavior, ctx.asInstanceOf[TypedActorContext[I]])
    }
    override def toString: String = s"PreStartTarget($nestedBehavior)"
  }

  private val receiveTarget: ReceiveTarget[I] = new ReceiveTarget[I] {
    override def apply(ctx: TypedActorContext[_], msg: I): Behavior[I] =
      Behavior.interpretMessage(nestedBehavior, ctx.asInstanceOf[TypedActorContext[I]], msg)

    override def signalRestart(ctx: TypedActorContext[_]): Unit =
      Behavior.interpretSignal(nestedBehavior, ctx.asInstanceOf[TypedActorContext[I]], PreRestart)

    override def toString: String = s"ReceiveTarget($nestedBehavior)"
  }

  private val signalTarget = new SignalTarget[I] {
    override def apply(ctx: TypedActorContext[_], signal: Signal): Behavior[I] =
      Behavior.interpretSignal(nestedBehavior, ctx.asInstanceOf[TypedActorContext[I]], signal)
    override def toString: String = s"SignalTarget($nestedBehavior)"
  }

  // invoked pre-start to start/de-duplicate the initial behavior stack
  def preStart(ctx: typed.TypedActorContext[O]): Behavior[O] = {
    val started = interceptor.aroundStart(ctx, preStartTarget)
    deduplicate(started, ctx)
  }

  def replaceNested(newNested: Behavior[I]): Behavior[O] =
    new InterceptorImpl(interceptor, newNested)

  override def receive(ctx: typed.TypedActorContext[O], msg: O): Behavior[O] = {
    // TODO performance optimization could maybe to avoid isAssignableFrom if interceptMessageClass is Class[Object]?
    val interceptMessageClass = interceptor.interceptMessageClass
    val result =
      if ((interceptMessageClass ne null) && interceptor.interceptMessageClass.isAssignableFrom(msg.getClass))
        interceptor.aroundReceive(ctx, msg, receiveTarget)
      else
        receiveTarget.apply(ctx, msg.asInstanceOf[I])
    deduplicate(result, ctx)
  }

  override def receiveSignal(ctx: typed.TypedActorContext[O], signal: Signal): Behavior[O] = {
    val interceptedResult = interceptor.aroundSignal(ctx, signal, signalTarget)
    deduplicate(interceptedResult, ctx)
  }

  private def deduplicate(interceptedResult: Behavior[I], ctx: TypedActorContext[O]): Behavior[O] = {
    val started = Behavior.start(interceptedResult, ctx.asInstanceOf[TypedActorContext[I]])
    if (started == BehaviorImpl.UnhandledBehavior || started == BehaviorImpl.SameBehavior || !Behavior.isAlive(started)) {
      started.unsafeCast[O]
    } else {
      // returned behavior could be nested in setups, so we need to start before we deduplicate
      val duplicateInterceptExists = Behavior.existsInStack(started) {
        case i: InterceptorImpl[_, _]
            if interceptor.isSame(i.interceptor.asInstanceOf[BehaviorInterceptor[Any, Any]]) =>
          true
        case _ => false
      }

      if (duplicateInterceptExists) started.unsafeCast[O]
      else new InterceptorImpl[O, I](interceptor, started)
    }
  }

  override def toString(): String = s"Interceptor($interceptor, $nestedBehavior)"
}

/**
 * Fire off any incoming message to another actor before receiving it ourselves.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] final case class MonitorInterceptor[T: ClassTag](actorRef: ActorRef[T])
    extends BehaviorInterceptor[T, T] {
  import BehaviorInterceptor._

  override def aroundReceive(ctx: TypedActorContext[T], msg: T, target: ReceiveTarget[T]): Behavior[T] = {
    actorRef ! msg
    target(ctx, msg)
  }

  // only once to the same actor in the same behavior stack
  override def isSame(other: BehaviorInterceptor[Any, Any]): Boolean = other match {
    case MonitorInterceptor(`actorRef`) => true
    case _                              => false
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object LogMessagesInterceptor {
  def apply[T](opts: LogOptions): BehaviorInterceptor[T, T] = {
    new LogMessagesInterceptor(opts).asInstanceOf[BehaviorInterceptor[T, T]]
  }

  private val LogMessageTemplate = "actor [{}] received message: {}"
  private val LogSignalTemplate = "actor [{}] received signal: {}"
}

/**
 * Log all messages for this decorated ReceiveTarget[T] to logger before receiving it ourselves.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] final class LogMessagesInterceptor(val opts: LogOptions) extends BehaviorInterceptor[Any, Any] {

  import BehaviorInterceptor._
  import LogMessagesInterceptor._

  private val logger = opts.getLogger.orElse(LoggerFactory.getLogger(getClass))

  override def aroundReceive(ctx: TypedActorContext[Any], msg: Any, target: ReceiveTarget[Any]): Behavior[Any] = {
    log(LogMessageTemplate, msg, ctx)
    target(ctx, msg)
  }

  override def aroundSignal(ctx: TypedActorContext[Any], signal: Signal, target: SignalTarget[Any]): Behavior[Any] = {
    log(LogSignalTemplate, signal, ctx)
    target(ctx, signal)
  }

  private def log(template: String, messageOrSignal: Any, context: TypedActorContext[Any]): Unit = {
    if (opts.enabled) {
      val selfPath = context.asScala.self.path
      opts.level match {
        case Level.ERROR => logger.error(template, selfPath, messageOrSignal)
        case Level.WARN  => logger.warn(template, selfPath, messageOrSignal)
        case Level.INFO  => logger.info(template, selfPath, messageOrSignal)
        case Level.DEBUG => logger.debug(template, selfPath, messageOrSignal)
        case Level.TRACE => logger.trace(template, selfPath, messageOrSignal)
      }
    }
  }

  // only once in the same behavior stack
  override def isSame(other: BehaviorInterceptor[Any, Any]): Boolean = other match {
    case a: LogMessagesInterceptor => a.opts == opts
    case _                         => false
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object TransformMessagesInterceptor {

  private final val _notMatchIndicator: Any = new AnyRef
  private final val _any2NotMatchIndicator = (_: Any) => _notMatchIndicator
  private final def any2NotMatchIndicator[T] = _any2NotMatchIndicator.asInstanceOf[Any => T]
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class TransformMessagesInterceptor[O: ClassTag, I](matcher: PartialFunction[O, I])
    extends BehaviorInterceptor[O, I] {
  import BehaviorInterceptor._
  import TransformMessagesInterceptor._

  override def isSame(other: BehaviorInterceptor[Any, Any]): Boolean = other match {
    // If they use the same pf instance we can allow it, to have one way to workaround defining
    // "recursive" narrowed behaviors.
    case TransformMessagesInterceptor(`matcher`)    => true
    case TransformMessagesInterceptor(otherMatcher) =>
      // there is no safe way to allow this
      throw new IllegalStateException(
        "transformMessages can only be used one time in the same behavior stack. " +
        s"One defined in ${LineNumbers(matcher)}, and another in ${LineNumbers(otherMatcher)}")
    case _ => false
  }

  def aroundReceive(ctx: TypedActorContext[O], msg: O, target: ReceiveTarget[I]): Behavior[I] = {
    matcher.applyOrElse(msg, any2NotMatchIndicator) match {
      case result if _notMatchIndicator == result => Behaviors.unhandled
      case transformed                            => target(ctx, transformed)
    }
  }

  override def toString: String = s"TransformMessages(${LineNumbers(matcher)})"
}
