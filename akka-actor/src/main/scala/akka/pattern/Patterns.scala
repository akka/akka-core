/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import java.util.Optional
import java.util.concurrent.{ Callable, CompletionStage, TimeUnit }
import java.util.function.Predicate

import scala.concurrent.ExecutionContext
import scala.jdk.DurationConverters._
import scala.jdk.FutureConverters._

import akka.actor.{ ActorSelection, ClassicActorSystemProvider, Scheduler }

/**
 * Java API: for Akka patterns such as `ask`, `pipe` and others which work with [[java.util.concurrent.CompletionStage]].
 */
object Patterns {
  import scala.concurrent.Future
  import scala.concurrent.duration._

  import akka.actor.ActorRef
  import akka.japi
  import akka.pattern.{
    after => scalaAfter,
    ask => scalaAsk,
    askWithStatus => scalaAskWithStatus,
    gracefulStop => scalaGracefulStop,
    pipe => scalaPipe,
    retry => scalaRetry
  }
  import akka.util.Timeout

  /**
   * <i>Java API for `akka.pattern.ask`:</i>
   * Sends a message asynchronously and returns a [[scala.concurrent.Future]]
   * holding the eventual reply message; this means that the target actor
   * needs to send the result to the `sender` reference provided.
   *
   * The Future will be completed with an [[akka.pattern.AskTimeoutException]] after the
   * given timeout has expired; this is independent from any timeout applied
   * while awaiting a result for this future (i.e. in
   * `Await.result(..., timeout)`). A typical reason for `AskTimeoutException` is that the
   * recipient actor didn't send a reply.
   *
   * <b>Warning:</b>
   * When using future callbacks, inside actors you need to carefully avoid closing over
   * the containing actor’s object, i.e. do not call methods or access mutable state
   * on the enclosing actor from within the callback. This would break the actor
   * encapsulation and may introduce synchronization bugs and race conditions because
   * the callback will be scheduled concurrently to the enclosing actor. Unfortunately
   * there is not yet a way to detect these illegal accesses at compile time.
   *
   * <b>Recommended usage:</b>
   *
   * {{{
   *   final Future<Object> f = Patterns.ask(worker, request, timeout);
   *   f.onSuccess(new Procedure<Object>() {
   *     public void apply(Object o) {
   *       nextActor.tell(new EnrichedResult(request, o));
   *     }
   *   });
   * }}}
   */
  def ask(actor: ActorRef, message: Any, timeout: Timeout): Future[AnyRef] =
    scalaAsk(actor, message)(timeout).asInstanceOf[Future[AnyRef]]

  /**
   * <i>Java API for `akka.pattern.ask`:</i>
   * Sends a message asynchronously and returns a [[java.util.concurrent.CompletionStage]]
   * holding the eventual reply message; this means that the target actor
   * needs to send the result to the `sender` reference provided.
   *
   * The CompletionStage will be completed with an [[akka.pattern.AskTimeoutException]] after the
   * given timeout has expired; this is independent from any timeout applied
   * while awaiting a result for this future (i.e. in
   * `Await.result(..., timeout)`). A typical reason for `AskTimeoutException` is that the
   * recipient actor didn't send a reply.
   *
   * <b>Warning:</b>
   * When using future callbacks, inside actors you need to carefully avoid closing over
   * the containing actor’s object, i.e. do not call methods or access mutable state
   * on the enclosing actor from within the callback. This would break the actor
   * encapsulation and may introduce synchronization bugs and race conditions because
   * the callback will be scheduled concurrently to the enclosing actor. Unfortunately
   * there is not yet a way to detect these illegal accesses at compile time.
   *
   * <b>Recommended usage:</b>
   *
   * {{{
   *   final CompletionStage<Object> f = Patterns.ask(worker, request, duration);
   *   f.thenRun(result -> nextActor.tell(new EnrichedResult(request, result)));
   * }}}
   */
  def ask(actor: ActorRef, message: Any, timeout: java.time.Duration): CompletionStage[AnyRef] =
    scalaAsk(actor, message)(timeout.toScala).asJava.asInstanceOf[CompletionStage[AnyRef]]

  /**
   * Use for messages whose response is known to be a [[akka.pattern.StatusReply]]. When a [[akka.pattern.StatusReply#success]] response
   * arrives the future is completed with the wrapped value, if a [[akka.pattern.StatusReply#error]] arrives the future is instead
   * failed.
   */
  def askWithStatus(actor: ActorRef, message: Any, timeout: java.time.Duration): CompletionStage[AnyRef] =
    scalaAskWithStatus(actor, message)(timeout.toScala).asJava.asInstanceOf[CompletionStage[AnyRef]]

  /**
   * A variation of ask which allows to implement "replyTo" pattern by including
   * sender reference in message.
   *
   * {{{
   * final Future<Object> f = Patterns.askWithReplyTo(
   *   worker,
   *   replyTo -> new Request(replyTo),
   *   timeout);
   * }}}
   */
  def askWithReplyTo(actor: ActorRef, messageFactory: japi.Function[ActorRef, Any], timeout: Timeout): Future[AnyRef] =
    extended.ask(actor, messageFactory.apply _)(timeout).asInstanceOf[Future[AnyRef]]

  /**
   * A variation of ask which allows to implement "replyTo" pattern by including
   * sender reference in message.
   *
   * {{{
   * final CompletionStage<Object> f = Patterns.askWithReplyTo(
   *   worker,
   *   askSender -> new Request(askSender),
   *   timeout);
   * }}}
   *
   * @param actor          the actor to be asked
   * @param messageFactory function taking an actor ref and returning the message to be sent
   * @param timeout        the timeout for the response before failing the returned completion stage
   */
  def askWithReplyTo(
      actor: ActorRef,
      messageFactory: japi.function.Function[ActorRef, Any],
      timeout: java.time.Duration): CompletionStage[AnyRef] =
    extended.ask(actor, messageFactory.apply _)(Timeout.create(timeout)).asJava.asInstanceOf[CompletionStage[AnyRef]]

  /**
   * <i>Java API for `akka.pattern.ask`:</i>
   * Sends a message asynchronously and returns a [[scala.concurrent.Future]]
   * holding the eventual reply message; this means that the target actor
   * needs to send the result to the `sender` reference provided.
   *
   * The Future will be completed with an [[akka.pattern.AskTimeoutException]] after the
   * given timeout has expired; this is independent from any timeout applied
   * while awaiting a result for this future (i.e. in
   * `Await.result(..., timeout)`). A typical reason for `AskTimeoutException` is that the
   * recipient actor didn't send a reply.
   *
   * <b>Warning:</b>
   * When using future callbacks, inside actors you need to carefully avoid closing over
   * the containing actor’s object, i.e. do not call methods or access mutable state
   * on the enclosing actor from within the callback. This would break the actor
   * encapsulation and may introduce synchronization bugs and race conditions because
   * the callback will be scheduled concurrently to the enclosing actor. Unfortunately
   * there is not yet a way to detect these illegal accesses at compile time.
   *
   * <b>Recommended usage:</b>
   *
   * {{{
   *   final Future<Object> f = Patterns.ask(worker, request, timeout);
   *   f.onSuccess(new Procedure<Object>() {
   *     public void apply(Object o) {
   *       nextActor.tell(new EnrichedResult(request, o));
   *     }
   *   });
   * }}}
   */
  def ask(actor: ActorRef, message: Any, timeoutMillis: Long): Future[AnyRef] =
    scalaAsk(actor, message)(new Timeout(timeoutMillis, TimeUnit.MILLISECONDS)).asInstanceOf[Future[AnyRef]]

  /**
   * A variation of ask which allows to implement "replyTo" pattern by including
   * sender reference in message.
   *
   * {{{
   * final Future<Object> f = Patterns.askWithReplyTo(
   *   worker,
   *   replyTo -> new Request(replyTo),
   *   timeout);
   * }}}
   */
  def askWithReplyTo(
      actor: ActorRef,
      messageFactory: japi.Function[ActorRef, Any],
      timeoutMillis: Long): Future[AnyRef] =
    extended.ask(actor, messageFactory.apply _)(Timeout(timeoutMillis.millis)).asInstanceOf[Future[AnyRef]]

  /**
   * <i>Java API for `akka.pattern.ask`:</i>
   * Sends a message asynchronously and returns a [[scala.concurrent.Future]]
   * holding the eventual reply message; this means that the target [[akka.actor.ActorSelection]]
   * needs to send the result to the `sender` reference provided.
   *
   * The Future will be completed with an [[akka.pattern.AskTimeoutException]] after the
   * given timeout has expired; this is independent from any timeout applied
   * while awaiting a result for this future (i.e. in
   * `Await.result(..., timeout)`). A typical reason for `AskTimeoutException` is that the
   * recipient actor didn't send a reply.
   *
   * <b>Warning:</b>
   * When using future callbacks, inside actors you need to carefully avoid closing over
   * the containing actor’s object, i.e. do not call methods or access mutable state
   * on the enclosing actor from within the callback. This would break the actor
   * encapsulation and may introduce synchronization bugs and race conditions because
   * the callback will be scheduled concurrently to the enclosing actor. Unfortunately
   * there is not yet a way to detect these illegal accesses at compile time.
   *
   * <b>Recommended usage:</b>
   *
   * {{{
   *   final Future<Object> f = Patterns.ask(selection, request, timeout);
   *   f.onSuccess(new Procedure<Object>() {
   *     public void apply(Object o) {
   *       nextActor.tell(new EnrichedResult(request, o));
   *     }
   *   });
   * }}}
   */
  def ask(selection: ActorSelection, message: Any, timeout: Timeout): Future[AnyRef] =
    scalaAsk(selection, message)(timeout).asInstanceOf[Future[AnyRef]]

  /**
   * <i>Java API for `akka.pattern.ask`:</i>
   * Sends a message asynchronously and returns a [[java.util.concurrent.CompletionStage]]
   * holding the eventual reply message; this means that the target [[akka.actor.ActorSelection]]
   * needs to send the result to the `sender` reference provided.
   *
   * The CompletionStage will be completed with an [[akka.pattern.AskTimeoutException]] after the
   * given timeout has expired; this is independent from any timeout applied
   * while awaiting a result for this future (i.e. in
   * `Await.result(..., timeout)`). A typical reason for `AskTimeoutException` is that the
   * recipient actor didn't send a reply.
   *
   * <b>Warning:</b>
   * When using future callbacks, inside actors you need to carefully avoid closing over
   * the containing actor’s object, i.e. do not call methods or access mutable state
   * on the enclosing actor from within the callback. This would break the actor
   * encapsulation and may introduce synchronization bugs and race conditions because
   * the callback will be scheduled concurrently to the enclosing actor. Unfortunately
   * there is not yet a way to detect these illegal accesses at compile time.
   *
   * <b>Recommended usage:</b>
   *
   * {{{
   *   final CompletionStage<Object> f = Patterns.ask(selection, request, duration);
   *   f.thenRun(result -> nextActor.tell(new EnrichedResult(request, result)));
   * }}}
   */
  def ask(selection: ActorSelection, message: Any, timeout: java.time.Duration): CompletionStage[AnyRef] =
    scalaAsk(selection, message)(timeout.toScala).asJava.asInstanceOf[CompletionStage[AnyRef]]

  /**
   * <i>Java API for `akka.pattern.ask`:</i>
   * Sends a message asynchronously and returns a [[scala.concurrent.Future]]
   * holding the eventual reply message; this means that the target [[akka.actor.ActorSelection]]
   * needs to send the result to the `sender` reference provided.
   *
   * The Future will be completed with an [[akka.pattern.AskTimeoutException]] after the
   * given timeout has expired; this is independent from any timeout applied
   * while awaiting a result for this future (i.e. in
   * `Await.result(..., timeout)`). A typical reason for `AskTimeoutException` is that the
   * recipient actor didn't send a reply.
   *
   * <b>Warning:</b>
   * When using future callbacks, inside actors you need to carefully avoid closing over
   * the containing actor’s object, i.e. do not call methods or access mutable state
   * on the enclosing actor from within the callback. This would break the actor
   * encapsulation and may introduce synchronization bugs and race conditions because
   * the callback will be scheduled concurrently to the enclosing actor. Unfortunately
   * there is not yet a way to detect these illegal accesses at compile time.
   *
   * <b>Recommended usage:</b>
   *
   * {{{
   *   final Future<Object> f = Patterns.ask(selection, request, timeout);
   *   f.onSuccess(new Procedure<Object>() {
   *     public void apply(Object o) {
   *       nextActor.tell(new EnrichedResult(request, o));
   *     }
   *   });
   * }}}
   */
  def ask(selection: ActorSelection, message: Any, timeoutMillis: Long): Future[AnyRef] =
    scalaAsk(selection, message)(new Timeout(timeoutMillis, TimeUnit.MILLISECONDS)).asInstanceOf[Future[AnyRef]]

  /**
   * A variation of ask which allows to implement "replyTo" pattern by including
   * sender reference in message.
   *
   * {{{
   * final Future<Object> f = Patterns.askWithReplyTo(
   *   selection,
   *   replyTo -> new Request(replyTo),
   *   timeout);
   * }}}
   */
  def askWithReplyTo(
      selection: ActorSelection,
      messageFactory: japi.Function[ActorRef, Any],
      timeoutMillis: Long): Future[AnyRef] =
    extended.ask(selection, messageFactory.apply _)(Timeout(timeoutMillis.millis)).asInstanceOf[Future[AnyRef]]

  /**
   * A variation of ask which allows to implement "replyTo" pattern by including
   * sender reference in message.
   *
   * {{{
   * final CompletionStage<Object> f = Patterns.askWithReplyTo(
   *   selection,
   *   replyTo -> new Request(replyTo),
   *   timeout);
   * }}}
   */
  def askWithReplyTo(
      selection: ActorSelection,
      messageFactory: japi.Function[ActorRef, Any],
      timeout: java.time.Duration): CompletionStage[AnyRef] =
    extended.ask(selection, messageFactory.apply _)(timeout.toScala).asJava.asInstanceOf[CompletionStage[AnyRef]]

  /**
   * Register an onComplete callback on this [[scala.concurrent.Future]] to send
   * the result to the given [[akka.actor.ActorRef]] or [[akka.actor.ActorSelection]].
   * Returns the original Future to allow method chaining.
   * If the future was completed with failure it is sent as a [[akka.actor.Status.Failure]]
   * to the recipient.
   *
   * <b>Recommended usage example:</b>
   *
   * {{{
   *   final Future<Object> f = Patterns.ask(worker, request, timeout);
   *   // apply some transformation (i.e. enrich with request info)
   *   final Future<Object> transformed = f.map(new akka.japi.Function<Object, Object>() { ... });
   *   // send it on to the next operator
   *   Patterns.pipe(transformed, context).to(nextActor);
   * }}}
   */
  def pipe[T](future: Future[T], context: ExecutionContext): PipeableFuture[T] = scalaPipe(future)(context)

  /**
   * When this [[java.util.concurrent.CompletionStage]] finishes, send its result to the given
   * [[akka.actor.ActorRef]] or [[akka.actor.ActorSelection]].
   * Returns the original CompletionStage to allow method chaining.
   * If the future was completed with failure it is sent as a [[akka.actor.Status.Failure]]
   * to the recipient.
   *
   * <b>Recommended usage example:</b>
   *
   * {{{
   *   final CompletionStage<Object> f = Patterns.ask(worker, request, timeout);
   *   // apply some transformation (i.e. enrich with request info)
   *   final CompletionStage<Object> transformed = f.thenApply(result -> { ... });
   *   // send it on to the next operator
   *   Patterns.pipe(transformed, context).to(nextActor);
   * }}}
   */
  def pipe[T](future: CompletionStage[T], context: ExecutionContext): PipeableCompletionStage[T] =
    pipeCompletionStage(future)(context)

  /**
   * Returns a [[scala.concurrent.Future]] that will be completed with success (value `true`) when
   * existing messages of the target actor has been processed and the actor has been
   * terminated.
   *
   * Useful when you need to wait for termination or compose ordered termination of several actors.
   *
   * If the target actor isn't terminated within the timeout the [[scala.concurrent.Future]]
   * is completed with failure [[akka.pattern.AskTimeoutException]].
   */
  def gracefulStop(target: ActorRef, timeout: FiniteDuration): Future[java.lang.Boolean] =
    scalaGracefulStop(target, timeout).asInstanceOf[Future[java.lang.Boolean]]

  /**
   * Returns a [[java.util.concurrent.CompletionStage]] that will be completed with success (value `true`) when
   * existing messages of the target actor has been processed and the actor has been
   * terminated.
   *
   * Useful when you need to wait for termination or compose ordered termination of several actors.
   *
   * If the target actor isn't terminated within the timeout the [[java.util.concurrent.CompletionStage]]
   * is completed with failure [[akka.pattern.AskTimeoutException]].
   */
  def gracefulStop(target: ActorRef, timeout: java.time.Duration): CompletionStage[java.lang.Boolean] =
    scalaGracefulStop(target, timeout.toScala).asJava.asInstanceOf[CompletionStage[java.lang.Boolean]]

  /**
   * Returns a [[scala.concurrent.Future]] that will be completed with success (value `true`) when
   * existing messages of the target actor has been processed and the actor has been
   * terminated.
   *
   * Useful when you need to wait for termination or compose ordered termination of several actors.
   *
   * If you want to invoke specialized stopping logic on your target actor instead of PoisonPill, you can pass your
   * stop command as `stopMessage` parameter
   *
   * If the target actor isn't terminated within the timeout the [[scala.concurrent.Future]]
   * is completed with failure [[akka.pattern.AskTimeoutException]].
   */
  def gracefulStop(target: ActorRef, timeout: FiniteDuration, stopMessage: Any): Future[java.lang.Boolean] =
    scalaGracefulStop(target, timeout, stopMessage).asInstanceOf[Future[java.lang.Boolean]]

  /**
   * Returns a [[java.util.concurrent.CompletionStage]] that will be completed with success (value `true`) when
   * existing messages of the target actor has been processed and the actor has been
   * terminated.
   *
   * Useful when you need to wait for termination or compose ordered termination of several actors.
   *
   * If you want to invoke specialized stopping logic on your target actor instead of PoisonPill, you can pass your
   * stop command as `stopMessage` parameter
   *
   * If the target actor isn't terminated within the timeout the [[java.util.concurrent.CompletionStage]]
   * is completed with failure [[akka.pattern.AskTimeoutException]].
   */
  def gracefulStop(
      target: ActorRef,
      timeout: java.time.Duration,
      stopMessage: Any): CompletionStage[java.lang.Boolean] =
    scalaGracefulStop(target, timeout.toScala, stopMessage).asJava.asInstanceOf[CompletionStage[java.lang.Boolean]]

  /**
   * Returns a [[scala.concurrent.Future]] that will be completed with the success or failure of the provided Callable
   * after the specified duration.
   */
  def after[T](
      duration: FiniteDuration,
      scheduler: Scheduler,
      context: ExecutionContext,
      value: Callable[Future[T]]): Future[T] =
    scalaAfter(duration, scheduler)(value.call())(context)

  /**
   * Returns a [[java.util.concurrent.CompletionStage]] that will be completed with the success or failure of the provided Callable
   * after the specified duration.
   */
  def after[T](
      duration: java.time.Duration,
      system: ClassicActorSystemProvider,
      value: Callable[CompletionStage[T]]): CompletionStage[T] =
    after(duration, system.classicSystem.scheduler, system.classicSystem.dispatcher, value)

  /**
   * Returns a [[java.util.concurrent.CompletionStage]] that will be completed with the success or failure of the provided Callable
   * after the specified duration.
   */
  def after[T](
      duration: java.time.Duration,
      scheduler: Scheduler,
      context: ExecutionContext,
      value: Callable[CompletionStage[T]]): CompletionStage[T] =
    afterCompletionStage(duration.toScala, scheduler)(value.call())(context)

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]]
   * The first attempt will be made immediately, each subsequent attempt will be made immediately
   * if the previous attempt failed.
   *
   * If attempts are exhausted the returned completion CompletionStage is simply the result of invoking attempt.
   * Note that the attempt function will be invoked on the given execution context for subsequent tries
   * and therefore must be thread safe (not touch unsafe mutable state).
   */
  def retry[T](attempt: Callable[CompletionStage[T]], attempts: Int, ec: ExecutionContext): CompletionStage[T] = {
    require(attempt != null, "Parameter attempt should not be null.")
    scalaRetry(() => attempt.call().asScala, attempts)(ec).asJava
  }

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]]
   * The first attempt will be made immediately, each subsequent attempt will be made immediately
   * if the previous attempt failed and the provided predicate tests true for the failure's exception.
   * If the predicate tests false, the failed attempt will be returned.  This allows for short-circuiting
   * in situations where the retries cannot be expected to succeed (e.g. in a situation where the legality
   * of arguments can only be determined asynchronously).
   *
   * If attempts are exhausted, the returned CompletionStage is that of the last attempt.
   * Note that the attempt function will be executed on the given execution context for subsequent tries
   * and therefore must be thread safe (not touch unsafe mutable state).
   */
  def retry[T](
      attempt: Callable[CompletionStage[T]],
      shouldRetry: Predicate[Throwable],
      attempts: Int,
      ec: ExecutionContext): CompletionStage[T] =
    scalaRetry(() => attempt.call().asScala, (ex) => shouldRetry.test(ex), attempts)(ec).asJava

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]]
   * The first attempt will be made immediately, each subsequent attempt will be made with a backoff time,
   * if the previous attempt failed.
   *
   * If attempts are exhausted the returned future is simply the result of invoking attempt.
   * Note that the attempt function will be invoked on the given execution context for subsequent tries and
   * therefore must be thread safe (not touch unsafe mutable state).
   *
   * @param minBackoff   minimum (initial) duration until the child actor will
   *                     started again, if it is terminated
   * @param maxBackoff   the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *                     random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *                     In order to skip this additional delay pass in `0`.
   */
  def retry[T](
      attempt: Callable[CompletionStage[T]],
      attempts: Int,
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      system: ClassicActorSystemProvider): CompletionStage[T] =
    retry(
      attempt,
      attempts,
      minBackoff,
      maxBackoff,
      randomFactor,
      system.classicSystem.scheduler,
      system.classicSystem.dispatcher)

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]]
   * The first attempt will be made immediately, each subsequent attempt will be made with a backoff time
   * if the preceding attempt failed and the provided predicate tests true for the failure's exception.
   * If the predicate tests false, the failed attempt will be returned.  This allows for short-circuiting
   * in situations where the retries cannot be expected to succeed (e.g. in a situation where the legality of
   * arguments can only be determined asynchronously).
   *
   * If attempts are exhausted, the returned CompletionStage is that of the last attempt.
   * Note that the attempt function will be executed on the actor system's dispatcher for subsequent tries
   * and therefore must be thread safe (not touch unsafe mutable state).
   */
  def retry[T](
      attempt: Callable[CompletionStage[T]],
      attempts: Int,
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      shouldRetry: Predicate[Throwable],
      system: ClassicActorSystemProvider): CompletionStage[T] =
    retry(
      attempt,
      attempts,
      minBackoff,
      maxBackoff,
      randomFactor,
      shouldRetry,
      system.classicSystem.scheduler,
      system.classicSystem.dispatcher)

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]]
   * The first attempt will be made immediately, each subsequent attempt will be made with a backoff time,
   * if the previous attempt failed.
   *
   * If attempts are exhausted the returned future is simply the result of invoking attempt.
   * Note that the attempt function will be invoked on the given execution context for subsequent tries and
   * therefore must be thread safe (not touch unsafe mutable state).
   *
   * @param minBackoff   minimum (initial) duration until the  attempt will be retried
   * @param maxBackoff   the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *                     random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *                     In order to skip this additional delay pass in `0`.
   */
  def retry[T](
      attempt: Callable[CompletionStage[T]],
      attempts: Int,
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      scheduler: Scheduler,
      ec: ExecutionContext): CompletionStage[T] = {
    require(attempt != null, "Parameter attempt should not be null.")
    require(minBackoff != null, "Parameter minBackoff should not be null.")
    require(maxBackoff != null, "Parameter minBackoff should not be null.")
    scalaRetry(() => attempt.call().asScala, attempts, minBackoff.toScala, maxBackoff.toScala, randomFactor)(
      ec,
      scheduler).asJava
  }

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]]
   * The first attempt will be made immediately, each subsequent attempt will be made with a backoff time
   * if the preceding attempt failed and the provided predicate tests true for the failure's exception.
   * If the predicate tests false, the failed attempt will be returned.  This allows for short-circuiting
   * in situations where the retries cannot be expected to succeed (e.g. in a situation where the legality of
   * arguments can only be determined asynchronously).
   *
   * If attempts are exhausted, the returned CompletionStage is that of the last attempt.
   * Note that the attempt function will be executed on the given execution context for subsequent tries
   * and therefore must be thread safe (not touch unsafe mutable state).
   */
  def retry[T](
      attempt: Callable[CompletionStage[T]],
      attempts: Int,
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      shouldRetry: Predicate[Throwable],
      scheduler: Scheduler,
      ec: ExecutionContext): CompletionStage[T] =
    scalaRetry(
      () => attempt.call().asScala,
      (ex) => shouldRetry.test(ex),
      attempts,
      minBackoff.toScala,
      maxBackoff.toScala,
      randomFactor)(ec, scheduler).asJava

  /**
   * Returns an internally retrying [[scala.concurrent.Future]]
   * The first attempt will be made immediately, and each subsequent attempt will be made after 'delay'.
   * A scheduler (eg context.system.scheduler) must be provided to delay each retry
   *
   * If attempts are exhausted the returned future is simply the result of invoking attempt.
   * Note that the attempt function will be invoked on the given execution context for subsequent tries and
   * therefore must be thread safe (not touch unsafe mutable state).
   */
  def retry[T](
      attempt: Callable[Future[T]],
      attempts: Int,
      delay: FiniteDuration,
      scheduler: Scheduler,
      context: ExecutionContext): Future[T] = {
    require(attempt != null, "Parameter attempt should not be null.")
    scalaRetry(() => attempt.call, attempts, delay)(context, scheduler)
  }

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]]
   * The first attempt will be made immediately, and each subsequent attempt will be made after 'delay'.
   * A scheduler (eg context.system.scheduler) must be provided to delay each retry
   *
   * If attempts are exhausted the returned CompletionStage is simply the result of invoking attempt.
   * Note that the attempt function will be invoked on the given execution context for subsequent tries
   * and therefore must be thread safe (not touch unsafe mutable state).
   */
  def retry[T](
      attempt: Callable[CompletionStage[T]],
      attempts: Int,
      delay: java.time.Duration,
      system: ClassicActorSystemProvider): CompletionStage[T] =
    retry(attempt, attempts, delay, system.classicSystem.scheduler, system.classicSystem.dispatcher)

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]]
   * The first attempt will be made immediately, and each subsequent attempt will be made after 'delay'.
   * A scheduler (eg context.system.scheduler) must be provided to delay each retry
   *
   * If attempts are exhausted the returned CompletionStage is simply the result of invoking attempt.
   * Note that the attempt function will be invoked on the given execution context for subsequent tries
   * and therefore must be thread safe (not touch unsafe mutable state).
   */
  def retry[T](
      attempt: Callable[CompletionStage[T]],
      attempts: Int,
      delay: java.time.Duration,
      scheduler: Scheduler,
      ec: ExecutionContext): CompletionStage[T] = {
    require(attempt != null, "Parameter attempt should not be null.")
    scalaRetry(() => attempt.call().asScala, attempts, delay.toScala)(ec, scheduler).asJava
  }

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]].
   * The first attempt will be made immediately, each subsequent attempt will be made after
   * the 'delay' return by `delayFunction`(the input next attempt count start from 1).
   * Return an empty [[Optional]] instance for no delay.
   * A scheduler (eg context.system.scheduler) must be provided to delay each retry.
   * You could provide a function to generate the next delay duration after first attempt,
   * this function should never return `null`, otherwise an [[IllegalArgumentException]] will be through.
   *
   * If attempts are exhausted the returned future is simply the result of invoking attempt.
   * Note that the attempt function will be invoked on the given execution context for subsequent tries and
   * therefore must be thread safe (not touch unsafe mutable state).
   */
  def retry[T](
      attempt: Callable[CompletionStage[T]],
      attempts: Int,
      delayFunction: java.util.function.IntFunction[Optional[java.time.Duration]],
      scheduler: Scheduler,
      context: ExecutionContext): CompletionStage[T] = {
    import scala.jdk.OptionConverters._
    require(attempt != null, "Parameter attempt should not be null.")
    scalaRetry(
      () => attempt.call().asScala,
      attempts,
      attempted => delayFunction.apply(attempted).toScala.map(_.toScala))(context, scheduler).asJava
  }

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]]
   * The first attempt will be made immediately, any subsequent attempt will be made after the delay
   * returned by the delay function (which can return an empty [[Optional]] for an immediate retry; it must never
   * return `null`).
   * A scheduler (e.g. context.system().scheduler()) must be provided to delay retries.
   *
   * If attempts are exhausted, the returned CompletionStage is that of the last attempt.
   * Note that the attempt function will be invoked on the given execution context for subsequent tries and therefore
   * must be thread safe (not touch unsafe mutable state).
   *
   * If an attempt fails, the exception from the failure will be tested with the provided predicate; if that predicate
   * tests true, a retry will be attempted, if false, the most recent failure is returned.  This allows for
   * short-circuiting in situations where the retries cannot be expected to succeed (e.g. in a situation where the
   * legality of arguments can only be determined asynchronously).
   */
  def retry[T](
      attempt: Callable[CompletionStage[T]],
      attempts: Int,
      delayFunction: java.util.function.IntFunction[Optional[java.time.Duration]],
      shouldRetry: Predicate[Throwable],
      scheduler: Scheduler,
      context: ExecutionContext): CompletionStage[T] = {
    import scala.jdk.OptionConverters._

    scalaRetry(
      () => attempt.call().asScala,
      (ex) => shouldRetry.test(ex),
      attempts,
      (attempted) => delayFunction.apply(attempted).toScala.map(_.toScala))(context, scheduler).asJava
  }

  /**
   * Returns an internally retrying [[java.util.concurrent.CompletionStage]].
   * The first attempt will be made immediately, each subsequent attempt will be made based on the provided [[akka.pattern.RetrySettings]].
   *
   * If attempts are exhausted, the returned CompletionStage is that of the last attempt.
   * Note that the attempt function will be executed on the actor system's dispatcher for subsequent tries
   * and therefore must be thread safe (not touch unsafe mutable state).
   */
  def retry[T](
      attempt: Callable[CompletionStage[T]],
      retrySettings: RetrySettings,
      system: ClassicActorSystemProvider): CompletionStage[T] = {
    scalaRetry(retrySettings) { () =>
      attempt.call().asScala
    }(system).asJava
  }

  /**
   * Calculates an exponential back off delay.
   */
  def calculateExponentialBackoffDelay(
      attempt: Int,
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double): java.time.Duration =
    RetrySupport.calculateExponentialBackoffDelay(attempt, minBackoff.toScala, maxBackoff.toScala, randomFactor).toJava
}
