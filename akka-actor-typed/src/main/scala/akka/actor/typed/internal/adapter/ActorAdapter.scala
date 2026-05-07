/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed
package internal
package adapter

import java.lang.reflect.InvocationTargetException

import scala.annotation.switch
import scala.annotation.tailrec
import scala.util.control.Exception.Catcher
import scala.util.control.NonFatal

import akka.{ actor => classic }
import akka.actor.ActorInitializationException
import akka.actor.ActorRefWithCell
import akka.actor.typed.internal.BehaviorImpl.DeferredBehavior
import akka.actor.typed.internal.BehaviorImpl.StoppedBehavior
import akka.actor.typed.internal.TimerSchedulerImpl.TimerMsg
import akka.actor.typed.internal.adapter.ActorAdapter.TypedActorFailedException
import akka.annotation.InternalApi
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi private[typed] object ActorAdapter {

  /**
   * Thrown to indicate that a Behavior has failed so that the parent gets
   * the cause and can fill in the cause in the `ChildFailed` signal
   * Wrapped to avoid it being logged as the typed supervision will already
   * have logged it.
   *
   * Should only be thrown if the parent is known to be an `ActorAdapter`.
   */
  final case class TypedActorFailedException(cause: Throwable) extends RuntimeException

  private val DummyReceive: classic.Actor.Receive = {
    case _ => throw new RuntimeException("receive should never be called on the typed ActorAdapter")
  }

  private val classicSupervisorDecider: Throwable => classic.SupervisorStrategy.Directive = { exc =>
    // ActorInitializationException => Stop in defaultDecider
    classic.SupervisorStrategy.defaultDecider.applyOrElse(exc, (_: Throwable) => classic.SupervisorStrategy.Restart)
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[typed] final class ActorAdapter[T](_initialBehavior: Behavior[T], rethrowTypedFailure: Boolean)
    extends classic.Actor {

  private var behavior: Behavior[T] = _initialBehavior
  def currentBehavior: Behavior[T] = behavior

  // context adapter construction must be lazy because so that it is not created before the system is ready
  // when the adapter is used for the user guardian (which avoids touching context until it is safe)
  private var _ctx: ActorContextAdapter[T] = _
  def ctx: ActorContextAdapter[T] = {
    if (_ctx eq null) _ctx = new ActorContextAdapter[T](this)
    _ctx
  }

  /**
   * Failures from failed children, that were stopped through classic supervision, this is what allows us to pass
   * child exception in Terminated for direct children.
   */
  private var failures: Map[classic.ActorRef, Throwable] = Map.empty

  def receive: Receive = ActorAdapter.DummyReceive

  override protected[akka] def aroundReceive(receive: Receive, msg: Any): Unit = {
    ctx.setCurrentActorThread()
    try {
      // as we know we never become in "normal" typed actors, it is just the current behavior that
      // changes, we can avoid some overhead with the partial function/behavior stack of untyped entirely
      // we also know that the receive is total, so we can avoid the orElse part as well.
      msg match {
        case classic.Terminated(ref) =>
          val msg =
            if (failures contains ref) {
              val ex = failures(ref)
              failures -= ref
              ChildFailed(ActorRefAdapter(ref), ex)
            } else Terminated(ActorRefAdapter(ref))
          handleSignal(msg)
        case classic.ReceiveTimeout =>
          // discard when null as timeout was cancelled after RecieveTimeout was already enqueued into the mailbox
          if (ctx.receiveTimeoutMsg != null)
            handleMessage(ctx.receiveTimeoutMsg)
        case wrapped: AdaptMessage[Any, T] @unchecked =>
          withSafelyAdapted(() => wrapped.adapt()) {
            case AdaptWithRegisteredMessageAdapter(msg) =>
              adaptAndHandle(msg)
            case msg: T @unchecked =>
              handleMessage(msg)
          }
        case AdaptWithRegisteredMessageAdapter(msg) =>
          adaptAndHandle(msg)
        case signal: Signal =>
          handleSignal(signal)
        case msg =>
          val t = msg.asInstanceOf[T]
          handleMessage(t)
      }
    } finally {
      ctx.clearCurrentActorThread()
      ctx.clearMdc()
    }
  }

  private def handleMessage(msg: T): Unit = {
    try {
      val c = ctx
      if (c.hasTimer) {
        msg match {
          case timerMsg: TimerMsg =>
            //we can only get this kind of message if the timer is of this concrete class
            c.timer.asInstanceOf[TimerSchedulerImpl[T]].interceptTimerMsg(ctx.log, timerMsg) match {
              case OptionVal.Some(m) =>
                next(Behavior.interpretMessage(behavior, c, m), m)
              case _ => // means TimerMsg not applicable, discard
            }
          case _ =>
            next(Behavior.interpretMessage(behavior, c, msg), msg)
        }
      } else {
        next(Behavior.interpretMessage(behavior, c, msg), msg)
      }
    } catch handleUnstashException
  }

  private def handleSignal(sig: Signal): Unit = {
    try {
      next(Behavior.interpretSignal(behavior, ctx, sig), sig)
    } catch handleUnstashException
  }

  private def handleUnstashException: Catcher[Unit] = {
    case e: UnstashException[T] @unchecked =>
      behavior = e.behavior
      throw e.cause
    case TypedActorFailedException(e: UnstashException[T] @unchecked) =>
      behavior = e.behavior
      throw TypedActorFailedException(e.cause)
    case ActorInitializationException(actor, message, e: UnstashException[T] @unchecked) =>
      behavior = e.behavior
      throw ActorInitializationException(actor, message, e.cause)
  }

  private def next(b: Behavior[T], msg: Any): Unit = {
    (b._tag: @switch) match {
      case BehaviorTags.UnhandledBehavior =>
        unhandled(msg)
      case BehaviorTags.FailedBehavior =>
        val f = b.asInstanceOf[BehaviorImpl.FailedBehavior]
        // For the parent classic supervisor to pick up the exception
        if (rethrowTypedFailure) throw TypedActorFailedException(f.cause)
        else context.stop(self)
      case BehaviorTags.StoppedBehavior =>
        val stopped = b.asInstanceOf[StoppedBehavior[T]]
        behavior = new ComposedStoppingBehavior[T](behavior, stopped)
        context.stop(self)
      case _ =>
        behavior = Behavior.canonicalize(b, behavior, ctx)
    }
  }

  private def adaptAndHandle(msg: Any): Unit = {
    @tailrec def handle(adapters: List[(Class[_], Any => T)]): Unit = {
      adapters match {
        case Nil =>
          // no adapter function registered for message class
          unhandled(msg)
        case (clazz, f) :: tail =>
          if (clazz.isAssignableFrom(msg.getClass)) {
            withSafelyAdapted(() => f(msg))(handleMessage)
          } else
            handle(tail) // recursive
      }
    }
    handle(ctx.messageAdapters)
  }

  private def withSafelyAdapted[U, V](adapt: () => U)(body: U => V): Unit = {
    var failed = false
    val adapted: U = try {
      adapt()
    } catch {
      case NonFatal(ex) =>
        // pass it on through the signal handler chain giving supervision a chance to deal with it
        handleSignal(MessageAdaptionFailure(ex))
        // Signal handler should actually throw so this is mostly to keep compiler happy (although a user could override
        // the MessageAdaptionFailure handling to do something weird)
        failed = true
        null.asInstanceOf[U]
    }
    if (!failed) {
      if (adapted != null) body(adapted)
      else
        ctx.log.warn(
          "Adapter function returned null which is not valid as an actor message, ignoring. This can happen for example when using pipeToSelf and returning null from the adapt function. Null value is ignored and not passed on to actor.")
    }
  }

  override def unhandled(msg: Any): Unit = msg match {

    case Terminated(ref) =>
      // this should never get here, because if it did, the death pact could
      // not be supervised - interpretSignal is where this actually happens
      throw DeathPactException(ref)
    case _: Signal =>
    // that's ok
    case other =>
      super.unhandled(other)
  }

  final override def supervisorStrategy = classic.OneForOneStrategy(loggingEnabled = false) {
    case ex =>
      ctx.setCurrentActorThread()
      try ex match {
        case TypedActorFailedException(cause) =>
          // These have already been optionally logged by typed supervision
          recordChildFailure(cause)
          classic.SupervisorStrategy.Stop
        case _ =>
          val isTypedActor = sender() match {
            case afwc: ActorRefWithCell =>
              afwc.underlying.props.producer.actorClass == classOf[ActorAdapter[_]]
            case _ =>
              false
          }
          recordChildFailure(ex)
          val logMessage = ex match {
            case e: ActorInitializationException if e.getCause ne null =>
              e.getCause match {
                case cause: InvocationTargetException if ex.getCause ne null => cause.getCause.getMessage
                case cause                                                   => cause.getMessage
              }
            case e => e.getMessage
          }
          // log at Error as that is what the supervision strategy would have done.
          ctx.log.error(logMessage, ex)
          if (isTypedActor)
            classic.SupervisorStrategy.Stop
          else
            ActorAdapter.classicSupervisorDecider(ex)
      } finally {
        ctx.clearCurrentActorThread()
      }
  }

  private def recordChildFailure(ex: Throwable): Unit = {
    val ref = sender()
    if (context.asInstanceOf[classic.ActorCell].isWatching(ref)) {
      failures = failures.updated(ref, ex)
    }
  }

  override protected[akka] def aroundPreStart(): Unit = {
    ctx.setCurrentActorThread()
    try super.aroundPreStart()
    finally ctx.clearCurrentActorThread()
  }

  override protected[akka] def aroundPreRestart(reason: Throwable, message: Option[Any]): Unit = {
    ctx.setCurrentActorThread()
    try super.aroundPreRestart(reason, message)
    finally ctx.clearCurrentActorThread()
  }

  override protected[akka] def aroundPostRestart(reason: Throwable): Unit = {
    ctx.setCurrentActorThread()
    try super.aroundPostRestart(reason)
    finally ctx.clearCurrentActorThread()
  }

  override protected[akka] def aroundPostStop(): Unit = {
    ctx.setCurrentActorThread()
    try super.aroundPostStop()
    finally ctx.clearCurrentActorThread()
  }

  override def preStart(): Unit = {
    try {
      if (Behavior.isAlive(behavior)) {
        behavior = Behavior.validateAsInitial(Behavior.start(behavior, ctx))
      }
      // either was stopped initially or became stopped on start
      if (!Behavior.isAlive(behavior)) context.stop(self)
    } finally ctx.clearMdc()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    try {
      ctx.cancelAllTimers()
      Behavior.interpretSignal(behavior, ctx, PreRestart)
      behavior = BehaviorImpl.stopped
    } finally ctx.clearMdc()
  }

  override def postRestart(reason: Throwable): Unit = {
    try {
      ctx.cancelAllTimers()
      behavior = Behavior.validateAsInitial(Behavior.start(behavior, ctx))
      if (!Behavior.isAlive(behavior)) context.stop(self)
    } finally ctx.clearMdc()
  }

  override def postStop(): Unit = {
    try {
      ctx.cancelAllTimers()
      behavior match {
        case _: DeferredBehavior[_] =>
        // Do not undefer a DeferredBehavior as that may cause creation side-effects, which we do not want on termination.
        case b => Behavior.interpretSignal(b, ctx, PostStop)
      }
      behavior = BehaviorImpl.stopped
    } finally ctx.clearMdc()
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[typed] final class ComposedStoppingBehavior[T](
    lastBehavior: Behavior[T],
    stopBehavior: StoppedBehavior[T])
    extends ExtensibleBehavior[T] {
  override def receive(ctx: TypedActorContext[T], msg: T): Behavior[T] =
    throw new IllegalStateException("Stopping, should never receieve a message")
  override def receiveSignal(ctx: TypedActorContext[T], msg: Signal): Behavior[T] = {
    if (msg != PostStop)
      throw new IllegalArgumentException(
        s"The ComposedStoppingBehavior should only ever receive a PostStop signal, but received $msg")
    // first pass the signal to the previous behavior, so that it and potential interceptors
    // will get the PostStop signal, unless it is deferred, we don't start a behavior while stopping
    lastBehavior match {
      case _: DeferredBehavior[_] => // no starting of behaviors on actor stop
      case nonDeferred            => Behavior.interpretSignal(nonDeferred, ctx, PostStop)
    }
    // and then to the potential stop hook, which can have a call back or not
    stopBehavior.onPostStop(ctx)
    BehaviorImpl.empty
  }
}
