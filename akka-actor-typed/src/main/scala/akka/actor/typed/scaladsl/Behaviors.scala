/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed
package scaladsl

import scala.reflect.ClassTag

import akka.actor.typed.internal._
import akka.annotation.{ DoNotInherit, InternalApi }

/**
 * Factories for [[akka.actor.typed.Behavior]].
 */
object Behaviors {

  /**
   * `setup` is a factory for a behavior. Creation of the behavior instance is deferred until
   * the actor is started, as opposed to [[Behaviors.receive]] that creates the behavior instance
   * immediately before the actor is running. The `factory` function pass the `ActorContext`
   * as parameter and that can for example be used for spawning child actors.
   *
   * `setup` is typically used as the outer most behavior when spawning an actor, but it
   * can also be returned as the next behavior when processing a message or signal. In that
   * case it will be started immediately after it is returned, i.e. next message will be
   * processed by the started behavior.
   */
  def setup[T](factory: ActorContext[T] => Behavior[T]): Behavior[T] =
    BehaviorImpl.DeferredBehavior(factory)

  /**
   * Support for stashing messages to unstash at a later time.
   */
  def withStash[T](capacity: Int)(factory: StashBuffer[T] => Behavior[T]): Behavior[T] =
    setup(ctx => {
      val stash = StashBuffer[T](ctx, capacity)
      factory(stash)
    })

  /**
   * Return this behavior from message processing in order to advise the
   * system to reuse the previous behavior. This is provided in order to
   * avoid the allocation overhead of recreating the current behavior where
   * that is not necessary.
   */
  def same[T]: Behavior[T] = BehaviorImpl.same

  /**
   * Return this behavior from message processing in order to advise the
   * system to reuse the previous behavior, including the hint that the
   * message has not been handled. This hint may be used by composite
   * behaviors that delegate (partial) handling to other behaviors.
   */
  def unhandled[T]: Behavior[T] = BehaviorImpl.unhandled

  /**
   * Return this behavior from message processing to signal that this actor
   * shall terminate voluntarily. If this actor has created child actors then
   * these will be stopped as part of the shutdown procedure.
   *
   * The `PostStop` signal that results from stopping this actor will be passed to the
   * current behavior. All other messages and signals will effectively be
   * ignored.
   */
  def stopped[T]: Behavior[T] = BehaviorImpl.stopped

  /**
   * Return this behavior from message processing to signal that this actor
   * shall terminate voluntarily. If this actor has created child actors then
   * these will be stopped as part of the shutdown procedure.
   *
   * The `PostStop` signal that results from stopping this actor will first be passed to the
   * current behavior and then the provided `postStop` callback will be invoked.
   * All other messages and signals will effectively be ignored.
   *
   * An example of when the callback can be useful compared to the `PostStop` signal
   * if you want to send a reply to the message that initiated a graceful stop.
   */
  def stopped[T](postStop: () => Unit): Behavior[T] = BehaviorImpl.stopped(postStop)

  /**
   * A behavior that treats every incoming message as unhandled.
   */
  def empty[T]: Behavior[T] = BehaviorImpl.empty

  /**
   * A behavior that ignores every incoming message and returns “same”.
   */
  def ignore[T]: Behavior[T] = BehaviorImpl.ignore

  /**
   * Construct an actor behavior that can react to both incoming messages and
   * lifecycle signals. After spawning this actor from another actor (or as the
   * guardian of an [[akka.actor.typed.ActorSystem]]) it will be executed within an
   * [[ActorContext]] that allows access to the system, spawning and watching
   * other actors, etc.
   *
   * Compared to using [[AbstractBehavior]] this factory is a more functional style
   * of defining the `Behavior`. Processing the next message results in a new behavior
   * that can potentially be different from this one. State is maintained by returning
   * a new behavior that holds the new immutable state.
   */
  def receive[T](onMessage: (ActorContext[T], T) => Behavior[T]): Receive[T] =
    new ReceiveImpl(onMessage)

  /**
   * Simplified version of [[Receive]] with only a single argument - the message
   * to be handled. Useful for when the context is already accessible by other means,
   * like being wrapped in an [[setup]] or similar.
   *
   * Construct an actor behavior that can react to both incoming messages and
   * lifecycle signals. After spawning this actor from another actor (or as the
   * guardian of an [[akka.actor.typed.ActorSystem]]) it will be executed within an
   * [[ActorContext]] that allows access to the system, spawning and watching
   * other actors, etc.
   *
   * Compared to using [[AbstractBehavior]] this factory is a more functional style
   * of defining the `Behavior`. Processing the next message results in a new behavior
   * that can potentially be different from this one. State is maintained by returning
   * a new behavior that holds the new immutable state.
   */
  def receiveMessage[T](onMessage: T => Behavior[T]): Receive[T] =
    new ReceiveMessageImpl(onMessage)

  /**
   * Construct an actor `Behavior` from a partial message handler which treats undefined messages as unhandled.
   */
  def receivePartial[T](onMessage: PartialFunction[(ActorContext[T], T), Behavior[T]]): Receive[T] =
    Behaviors.receive[T] { (ctx, t) =>
      onMessage.applyOrElse((ctx, t), (_: (ActorContext[T], T)) => Behaviors.unhandled[T])
    }

  /**
   * Construct an actor `Behavior` from a partial message handler which treats undefined messages as unhandled.
   */
  def receiveMessagePartial[T](onMessage: PartialFunction[T, Behavior[T]]): Receive[T] =
    Behaviors.receive[T] { (_, t) =>
      onMessage.applyOrElse(t, (_: T) => Behaviors.unhandled[T])
    }

  /**
   * Construct an actor `Behavior` that can react to lifecycle signals only.
   */
  def receiveSignal[T](handler: PartialFunction[(ActorContext[T], Signal), Behavior[T]]): Behavior[T] =
    receive[T]((_, _) => same).receiveSignal(handler)

  /**
   * Intercept messages and signals for a `behavior` by first passing them to a [[akka.actor.typed.BehaviorInterceptor]]
   *
   * When a behavior returns a new behavior as a result of processing a signal or message and that behavior already contains
   * the same interceptor (defined by the `isSame` method on the `BehaviorInterceptor`) only the innermost interceptor
   * is kept. This is to protect against stack overflow when recursively defining behaviors.
   *
   * The interceptor is created with a factory function in case it has state and should not be shared.
   * If the interceptor has no state the same instance can be returned from the factory to avoid unnecessary object
   * creation.
   */
  def intercept[O, I](behaviorInterceptor: () => BehaviorInterceptor[O, I])(behavior: Behavior[I]): Behavior[O] =
    BehaviorImpl.intercept(behaviorInterceptor)(behavior)

  /**
   * Behavior decorator that copies all received message to the designated
   * monitor [[akka.actor.typed.ActorRef]] before invoking the wrapped behavior. The
   * wrapped behavior can evolve (i.e. return different behavior) without needing to be
   * wrapped in a `monitor` call again.
   *
   * The `ClassTag` for `T` ensures that the messages of this class or a subclass thereof will be
   * sent to the `monitor`. Other message types (e.g. a private protocol) will bypass the interceptor
   * and be continue to the inner behavior.
   *
   * @param monitor The messages will also be sent to this `ActorRef`
   * @param behavior The inner behavior that is decorated
   */
  def monitor[T: ClassTag](monitor: ActorRef[T], behavior: Behavior[T]): Behavior[T] =
    BehaviorImpl.intercept(() => new MonitorInterceptor[T](monitor))(behavior)

  /**
   * Behavior decorator that logs all messages to the [[akka.actor.typed.Behavior]] using the provided
   * [[akka.actor.typed.LogOptions]] default configuration before invoking the wrapped behavior.
   * To include an MDC context then first wrap `logMessages` with `withMDC`.
   */
  def logMessages[T](behavior: Behavior[T]): Behavior[T] =
    BehaviorImpl.intercept(() => LogMessagesInterceptor[T](LogOptions()))(behavior)

  /**
   * Behavior decorator that logs all messages to the [[akka.actor.typed.Behavior]] using the provided
   * [[akka.actor.typed.LogOptions]] configuration before invoking the wrapped behavior.
   * To include an MDC context then first wrap `logMessages` with `withMDC`.
   */
  def logMessages[T](logOptions: LogOptions, behavior: Behavior[T]): Behavior[T] =
    BehaviorImpl.intercept(() => LogMessagesInterceptor[T](logOptions))(behavior)

  /**
   * Wrap the given behavior with the given [[SupervisorStrategy]] for
   * the given exception.
   * Exceptions that are not subtypes of `Thr` will not be
   * caught and thus lead to the termination of the actor.
   *
   * It is possible to specify different supervisor strategies, such as restart,
   * resume, backoff.
   *
   * Note that only [[scala.util.control.NonFatal]] throwables will trigger the supervision strategy.
   *
   * Example:
   * {{{
   * val dbConnector: Behavior[DbCommand] = ...
   *
   * val dbRestarts =
   *    Behaviors.supervise(dbConnector)
   *      .onFailure(SupervisorStrategy.restart) // handle all NonFatal exceptions
   *
   * val dbSpecificResumes =
   *    Behaviors.supervise(dbConnector)
   *      .onFailure[IndexOutOfBoundsException](SupervisorStrategy.resume) // resume for IndexOutOfBoundsException exceptions
   * }}}
   */
  def supervise[T](wrapped: Behavior[T]): Supervise[T] =
    new Supervise[T](wrapped)

  private final val ThrowableClassTag = ClassTag(classOf[Throwable])
  final class Supervise[T] private[akka] (val wrapped: Behavior[T]) extends AnyVal {

    /** Specify the [[SupervisorStrategy]] to be invoked when the wrapped behavior throws. */
    def onFailure[Thr <: Throwable](strategy: SupervisorStrategy)(
        implicit tag: ClassTag[Thr] = ThrowableClassTag): Behavior[T] = {
      val effectiveTag = if (tag == ClassTag.Nothing) ThrowableClassTag else tag
      Supervisor(Behavior.validateAsInitial(wrapped), strategy)(effectiveTag)
    }
  }

  /**
   * Support for scheduled `self` messages in an actor.
   * It takes care of the lifecycle of the timers such as cancelling them when the actor
   * is restarted or stopped.
   *
   * @see [[TimerScheduler]]
   */
  def withTimers[T](factory: TimerScheduler[T] => Behavior[T]): Behavior[T] =
    TimerSchedulerImpl.withTimers(factory)

  /**
   * Per message MDC (Mapped Diagnostic Context) logging.
   *
   * The `ClassTag` for `T` ensures that only messages of this class or a subclass thereof will be
   * intercepted. Other message types (e.g. a private protocol) will bypass the interceptor and be
   * continue to the inner behavior untouched.
   *
   * @param mdcForMessage Is invoked before each message is handled, allowing to setup MDC, MDC is cleared after
   *                 each message processing by the inner behavior is done.
   * @param behavior The actual behavior handling the messages, the MDC is used for the log entries logged through
   *                 `ActorContext.log`
   *
   */
  def withMdc[T: ClassTag](mdcForMessage: T => Map[String, String])(behavior: Behavior[T]): Behavior[T] =
    withMdc[T](Map.empty[String, String], mdcForMessage)(behavior)

  /**
   * Static MDC (Mapped Diagnostic Context)
   *
   * The `ClassTag` for `T` ensures that only messages of this class or a subclass thereof will be
   * intercepted. Other message types (e.g. a private protocol) will bypass the interceptor and be
   * continue to the inner behavior untouched.
   *
   * @param staticMdc This MDC is setup in the logging context for every message
   * @param behavior The actual behavior handling the messages, the MDC is used for the log entries logged through
   *                 `ActorContext.log`
   *
   */
  def withMdc[T: ClassTag](staticMdc: Map[String, String])(behavior: Behavior[T]): Behavior[T] =
    withMdc[T](staticMdc, (_: T) => Map.empty[String, String])(behavior)

  /**
   * Combination of static and per message MDC (Mapped Diagnostic Context).
   *
   * Each message will get the static MDC plus the MDC returned for the message. If the same key
   * are in both the static and the per message MDC the per message one overwrites the static one
   * in the resulting log entries.
   *
   * The `staticMdc` or `mdcForMessage` may be empty.
   *
   * The `ClassTag` for `T` ensures that only messages of this class or a subclass thereof will be
   * intercepted. Other message types (e.g. a private protocol) will bypass the interceptor and be
   * continue to the inner behavior untouched.
   *
   * @param staticMdc A static MDC applied for each message
   * @param mdcForMessage Is invoked before each message is handled, allowing to setup MDC, MDC is cleared after
   *                 each message processing by the inner behavior is done.
   * @param behavior The actual behavior handling the messages, the MDC is used for the log entries logged through
   *                 `ActorContext.log`
   *
   */
  def withMdc[T: ClassTag](staticMdc: Map[String, String], mdcForMessage: T => Map[String, String])(
      behavior: Behavior[T]): Behavior[T] =
    WithMdcBehaviorInterceptor[T](staticMdc, mdcForMessage, behavior)

  /**
   * `Behavior` that exposes additional fluent DSL methods to further change the message or
   * signal reception behavior. It's returned by for example [[Behaviors.receiveMessage]].
   */
  @DoNotInherit
  trait Receive[T] extends Behavior[T] {
    def receiveSignal(onSignal: PartialFunction[(ActorContext[T], Signal), Behavior[T]]): Behavior[T]
  }

  @InternalApi
  private[akka] final class ReceiveImpl[T](onMessage: (ActorContext[T], T) => Behavior[T])
      extends BehaviorImpl.ReceiveBehavior[T](onMessage)
      with Receive[T] {

    override def receiveSignal(onSignal: PartialFunction[(ActorContext[T], Signal), Behavior[T]]): Behavior[T] =
      new BehaviorImpl.ReceiveBehavior(onMessage, onSignal)
  }
  @InternalApi
  private[akka] final class ReceiveMessageImpl[T](onMessage: T => Behavior[T])
      extends BehaviorImpl.ReceiveMessageBehavior[T](onMessage)
      with Receive[T] {

    override def receiveSignal(onSignal: PartialFunction[(ActorContext[T], Signal), Behavior[T]]): Behavior[T] =
      new BehaviorImpl.ReceiveMessageBehavior[T](onMessage, onSignal)
  }

}
