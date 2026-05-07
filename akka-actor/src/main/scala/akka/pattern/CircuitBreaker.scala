/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import java.util.Optional
import java.util.concurrent.{ Callable, CompletionException, CompletionStage, CopyOnWriteArrayList, ThreadLocalRandom }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger, AtomicLong }
import java.util.function.BiFunction
import java.util.function.Consumer

import scala.annotation.nowarn
import scala.jdk.FutureConverters._
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.util.{ Failure, Success, Try }
import scala.util.control.NoStackTrace
import scala.util.control.NonFatal

import akka.AkkaException
import akka.actor.ClassicActorSystemProvider
import akka.actor.Scheduler
import akka.pattern.internal.{ CircuitBreakerNoopTelemetry, CircuitBreakerTelemetry }

/**
 * Companion object providing factory methods for Circuit Breaker which runs callbacks in caller's thread
 */
object CircuitBreaker {

  /**
   * Create a new CircuitBreaker.
   *
   * Callbacks run in caller's thread when using withSyncCircuitBreaker, and in same ExecutionContext as the passed
   * in Future when using withCircuitBreaker. To use another ExecutionContext for the callbacks you can specify the
   * executor in the constructor.
   *
   * @param scheduler Reference to Akka scheduler
   * @param maxFailures Maximum number of failures before opening the circuit
   * @param callTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to consider a call a failure
   * @param resetTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to attempt to close the circuit
   */
  def apply(
      scheduler: Scheduler,
      maxFailures: Int,
      callTimeout: FiniteDuration,
      resetTimeout: FiniteDuration): CircuitBreaker =
    new CircuitBreaker(scheduler, maxFailures, callTimeout, resetTimeout)(ExecutionContext.parasitic)

  /**
   * Create or find a CircuitBreaker in registry.
   *
   * @param id Circuit Breaker identifier
   * @param system [[ActorSystem]] that is storing this [[CircuitBreaker]]
   */
  def apply(id: String)(implicit system: ClassicActorSystemProvider): CircuitBreaker =
    CircuitBreakersRegistry(system).get(id)

  /**
   * Java API: Create a new CircuitBreaker.
   *
   * Callbacks run in caller's thread when using withSyncCircuitBreaker, and in same ExecutionContext as the passed
   * in Future when using withCircuitBreaker. To use another ExecutionContext for the callbacks you can specify the
   * executor in the constructor.
   *
   * @param scheduler Reference to Akka scheduler
   * @param maxFailures Maximum number of failures before opening the circuit
   * @param callTimeout [[java.time.Duration]] of time after which to consider a call a failure
   * @param resetTimeout [[java.time.Duration]] of time after which to attempt to close the circuit
   */
  def create(
      scheduler: Scheduler,
      maxFailures: Int,
      callTimeout: java.time.Duration,
      resetTimeout: java.time.Duration): CircuitBreaker =
    apply(scheduler, maxFailures, callTimeout.toScala, resetTimeout.toScala)

  /**
   * Java API: Create or find a CircuitBreaker in registry.
   *
   * @param id Circuit Breaker identifier
   * @param system [[ActorSystem]] that is storing this [[CircuitBreaker]]
   */
  def lookup(id: String, system: ClassicActorSystemProvider): CircuitBreaker =
    apply(id)(system)

  protected def convertJavaFailureFnToScala[T](
      javaFn: BiFunction[Optional[T], Optional[Throwable], java.lang.Boolean]): Try[T] => Boolean = {
    val failureFnInScala: Try[T] => Boolean = {
      case Success(t)   => javaFn(Optional.of(t), Optional.empty())
      case Failure(err) => javaFn(Optional.empty(), Optional.of(err))
    }
    failureFnInScala
  }
}

/**
 * Provides circuit breaker functionality for stability when working with "dangerous" operations, e.g. calls to
 * remote systems.
 *
 * Transitions through three states:
 * - In *Closed* state, calls pass through until the `maxFailures` count is reached.  This causes the circuit breaker
 * to open.  Both exceptions and calls exceeding `callTimeout` are considered failures.
 * - In *Open* state, calls fail-fast with an exception.  After `resetTimeout`, circuit breaker transitions to
 * half-open state.
 * - In *Half-Open* state, the first call will be allowed through, if it succeeds the circuit breaker will reset to
 * closed state.  If it fails, the circuit breaker will re-open to open state.  All calls beyond the first that
 * execute while the first is running will fail-fast with an exception.
 *
 * @param scheduler Reference to Akka scheduler
 * @param maxFailures Maximum number of failures before opening the circuit
 * @param callTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to consider a call a failure
 * @param resetTimeout [[scala.concurrent.duration.FiniteDuration]] of time after which to attempt to close the circuit
 * @param randomFactor after calculation of the exponential back-off an additional random delay
 *                     based on this factor is added, e.g. `0.2` adds up to `20%` delay.
 *                     randomFactor should be in range `0.0` (inclusive) and `1.0` (inclusive).
 *                     In order to skip this additional delay pass in `0`.
 * @param executor [[scala.concurrent.ExecutionContext]] used for execution of state transition listeners
 */
class CircuitBreaker(
    scheduler: Scheduler,
    maxFailures: Int,
    callTimeout: FiniteDuration,
    val resetTimeout: FiniteDuration,
    maxResetTimeout: FiniteDuration,
    exponentialBackoffFactor: Double,
    randomFactor: Double,
    val allowExceptions: Set[String],
    val telemetry: CircuitBreakerTelemetry)(implicit executor: ExecutionContext)
    extends AbstractCircuitBreaker {

  require(exponentialBackoffFactor >= 1.0, "exponentialBackoffFactor must be >= 1.0")
  require(0.0 <= randomFactor && randomFactor <= 1.0, "randomFactor must be between 0.0 and 1.0")

  def this(
      scheduler: Scheduler,
      maxFailures: Int,
      callTimeout: FiniteDuration,
      resetTimeout: FiniteDuration,
      maxResetTimeout: FiniteDuration,
      exponentialBackoffFactor: Double,
      randomFactor: Double)(implicit executor: ExecutionContext) = {
    this(
      scheduler,
      maxFailures,
      callTimeout,
      resetTimeout,
      maxResetTimeout,
      exponentialBackoffFactor,
      randomFactor,
      Set.empty,
      CircuitBreakerNoopTelemetry)(executor)
  }

  def this(
      executor: ExecutionContext,
      scheduler: Scheduler,
      maxFailures: Int,
      callTimeout: java.time.Duration,
      resetTimeout: java.time.Duration) = {
    this(
      scheduler,
      maxFailures,
      callTimeout.toScala,
      resetTimeout.toScala,
      maxResetTimeout = 36500.days,
      exponentialBackoffFactor = 1.0,
      randomFactor = 0.0)(executor)
  }

  // add the old constructor to make it binary compatible
  def this(scheduler: Scheduler, maxFailures: Int, callTimeout: FiniteDuration, resetTimeout: FiniteDuration)(
      implicit
      executor: ExecutionContext) = {
    this(
      scheduler,
      maxFailures,
      callTimeout,
      resetTimeout,
      maxResetTimeout = 36500.days,
      exponentialBackoffFactor = 1.0,
      randomFactor = 0.0)(executor)
  }

  // add the old constructor to make it binary compatible
  def this(
      scheduler: Scheduler,
      maxFailures: Int,
      callTimeout: FiniteDuration,
      resetTimeout: FiniteDuration,
      maxResetTimeout: FiniteDuration,
      exponentialBackoffFactor: Double)(
      implicit
      executor: ExecutionContext) = {
    this(scheduler, maxFailures, callTimeout, resetTimeout, maxResetTimeout, exponentialBackoffFactor, 0.0)(executor)
  }

  /**
   * The `resetTimeout` will be increased exponentially for each failed attempt to close the circuit.
   * The default exponential backoff factor is 2.
   *
   * @param maxResetTimeout the upper bound of resetTimeout
   */
  def withExponentialBackoff(maxResetTimeout: FiniteDuration): CircuitBreaker = {
    new CircuitBreaker(
      scheduler,
      maxFailures,
      callTimeout,
      resetTimeout,
      maxResetTimeout,
      2.0,
      randomFactor,
      allowExceptions,
      telemetry)(executor)
  }

  /**
   * The `resetTimeout` will be increased exponentially for each failed attempt to close the circuit.
   * The default exponential backoff factor is 2.
   *
   * @param maxResetTimeout the upper bound of resetTimeout
   */
  def withExponentialBackoff(maxResetTimeout: java.time.Duration): CircuitBreaker = {
    withExponentialBackoff(maxResetTimeout.toScala)
  }

  /**
   * Adds jitter to the delay.
   * @param randomFactor after calculation of the back-off an additional random delay based on this
   *                     factor is added, e.g. 0.2 adds up to 20% delay. In order to skip this
   *                     additional delay pass in 0.
   */
  def withRandomFactor(randomFactor: Double): CircuitBreaker = {
    new CircuitBreaker(
      scheduler,
      maxFailures,
      callTimeout,
      resetTimeout,
      maxResetTimeout,
      exponentialBackoffFactor,
      randomFactor,
      allowExceptions,
      telemetry)(executor)
  }

  /**
   * Holds reference to current state of CircuitBreaker - *access only via helper methods*
   */
  @volatile
  @nowarn("msg=never updated")
  private[this] var _currentStateDoNotCallMeDirectly: State = Closed

  /**
   * Holds reference to current resetTimeout of CircuitBreaker - *access only via helper methods*
   */
  @volatile
  @nowarn("msg=never updated")
  private[this] var _currentResetTimeoutDoNotCallMeDirectly: FiniteDuration = resetTimeout

  @nowarn private def _preventPrivateUnusedErasure = {
    _currentStateDoNotCallMeDirectly
    _currentResetTimeoutDoNotCallMeDirectly
  }

  /**
   * Helper method for access to underlying state via Unsafe
   *
   * @param oldState Previous state on transition
   * @param newState Next state on transition
   * @return Whether the previous state matched correctly
   */
  @inline
  private[this] def swapState(oldState: State, newState: State): Boolean = {
    AbstractCircuitBreaker.currentStateHandle.compareAndSet(this, oldState, newState)
  }

  /**
   * Helper method for accessing underlying state via Unsafe
   *
   * @return Reference to current state
   */
  @inline
  private[this] def currentState: State =
    AbstractCircuitBreaker.currentStateHandle.getVolatile(this).asInstanceOf[State]

  /**
   * Helper method for updating the underlying resetTimeout via Unsafe
   */
  @inline
  private[this] def swapResetTimeout(oldResetTimeout: FiniteDuration, newResetTimeout: FiniteDuration): Boolean =
    AbstractCircuitBreaker.resetTimeoutHandle.compareAndSet(this, oldResetTimeout, newResetTimeout)

  /**
   * Helper method for accessing to the underlying resetTimeout via Unsafe
   */
  @inline
  private[this] def currentResetTimeout: FiniteDuration =
    AbstractCircuitBreaker.resetTimeoutHandle.getVolatile(this).asInstanceOf[FiniteDuration]

  /**
   * Wraps invocations of asynchronous calls that need to be protected.
   *
   * @param body Call needing protected
   * @param defineFailureFn function that define what should be consider failure and thus increase failure count
   * @return [[scala.concurrent.Future]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   */
  def withCircuitBreaker[T](body: => Future[T], defineFailureFn: Try[T] => Boolean): Future[T] =
    currentState.invoke(body, defineFailureFn)

  /**
   * Wraps invocations of asynchronous calls that need to be protected.
   *
   * @param body Call needing protected
   * @return [[scala.concurrent.Future]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   *
   */
  def withCircuitBreaker[T](body: => Future[T]): Future[T] =
    currentState.invoke(body, failureFn)

  /**
   * Java API for [[#withCircuitBreaker]].
   *
   * @param body Call needing protected
   * @return [[scala.concurrent.Future]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   */
  def callWithCircuitBreaker[T](body: Callable[Future[T]]): Future[T] =
    withCircuitBreaker(body.call)

  /**
   * Java API for [[#withCircuitBreaker]].
   *
   * @param body Call needing protected
   * @param defineFailureFn function that define what should be consider failure and thus increase failure count
   * @return [[scala.concurrent.Future]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   */
  def callWithCircuitBreaker[T](
      body: Callable[Future[T]],
      defineFailureFn: BiFunction[Optional[T], Optional[Throwable], java.lang.Boolean]): Future[T] = {
    val failureFnInScala = CircuitBreaker.convertJavaFailureFnToScala(defineFailureFn)

    withCircuitBreaker(body.call, failureFnInScala)
  }

  /**
   * Java API (8) for [[#withCircuitBreaker]].
   *
   * @param body Call needing protected
   * @return [[java.util.concurrent.CompletionStage]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   */
  def callWithCircuitBreakerCS[T](body: Callable[CompletionStage[T]]): CompletionStage[T] =
    callWithCircuitBreaker(new Callable[Future[T]] {
      override def call(): Future[T] = body.call().asScala
    }).asJava

  /**
   * Java API (8) for [[#withCircuitBreaker]].
   *
   * @param body Call needing protected
   * @param defineFailureFn function that define what should be consider failure and thus increase failure count
   * @return [[java.util.concurrent.CompletionStage]] containing the call result or a
   *   `scala.concurrent.TimeoutException` if the call timed out
   */
  def callWithCircuitBreakerCS[T](
      body: Callable[CompletionStage[T]],
      defineFailureFn: BiFunction[Optional[T], Optional[Throwable], java.lang.Boolean]): CompletionStage[T] =
    callWithCircuitBreaker(new Callable[Future[T]] {
      override def call(): Future[T] = body.call().asScala
    }, defineFailureFn).asJava

  /**
   * Wraps invocations of synchronous calls that need to be protected.
   *
   * Calls are run in the caller's thread. Because of the synchronous nature of
   * this call, the `scala.concurrent.TimeoutException` will only be thrown
   * after the body has completed.
   *
   * Throws java.util.concurrent.TimeoutException if the call timed out.
   *
   * @param body Call needing protected
   * @return The result of the call
   */
  def withSyncCircuitBreaker[T](body: => T): T =
    withSyncCircuitBreaker(body, this.failureFn)

  /**
   * Wraps invocations of synchronous calls that need to be protected.
   *
   * Calls are run in caller's thread. Because of the synchronous nature of
   * this call the  `scala.concurrent.TimeoutException` will only be thrown
   * after the body has completed.
   *
   * Throws java.util.concurrent.TimeoutException if the call timed out.
   *
   * @param body Call needing protected
   * @param defineFailureFn function that define what should be consider failure and thus increase failure count
   * @return The result of the call
   */
  def withSyncCircuitBreaker[T](body: => T, defineFailureFn: Try[T] => Boolean): T =
    Await.result(
      withCircuitBreaker(
        try Future.successful(body)
        catch { case NonFatal(t) => Future.failed(t) },
        defineFailureFn),
      callTimeout)

  /**
   * Java API for [[#withSyncCircuitBreaker]]. Throws [[java.util.concurrent.TimeoutException]] if the call timed out.
   *
   * @param body Call needing protected
   * @return The result of the call
   */
  def callWithSyncCircuitBreaker[T](body: Callable[T]): T =
    withSyncCircuitBreaker(body.call)

  /**
   * Java API for [[#withSyncCircuitBreaker]]. Throws [[java.util.concurrent.TimeoutException]] if the call timed out.
   *
   * @param body Call needing protected
   * @param defineFailureFn function that define what should be consider failure and thus increase failure count
   * @return The result of the call
   */
  def callWithSyncCircuitBreaker[T](
      body: Callable[T],
      defineFailureFn: BiFunction[Optional[T], Optional[Throwable], java.lang.Boolean]): T = {
    val failureFnInScala = CircuitBreaker.convertJavaFailureFnToScala(defineFailureFn)
    withSyncCircuitBreaker(body.call, failureFnInScala)
  }

  /**
   * Mark a successful call through CircuitBreaker. Sometimes the callee of CircuitBreaker sends back a message to the
   * caller Actor. In such a case, it is convenient to mark a successful call instead of using Future
   * via [[withCircuitBreaker]]
   */
  def succeed(): Unit = {
    currentState.callSucceeds()
  }

  /**
   * Mark a failed call through CircuitBreaker. Sometimes the callee of CircuitBreaker sends back a message to the
   * caller Actor. In such a case, it is convenient to mark a failed call instead of using Future
   * via [[withCircuitBreaker]]
   */
  def fail(): Unit = {
    currentState.callFails()
  }

  /**
   * Return true if the internal state is Closed. WARNING: It is a "power API" call which you should use with care.
   * Ordinal use cases of CircuitBreaker expects a remote call to return Future, as in withCircuitBreaker.
   * So, if you check the state by yourself, and make a remote call outside CircuitBreaker, you should
   * manage the state yourself.
   */
  def isClosed: Boolean = {
    currentState == Closed
  }

  /**
   * Return true if the internal state is Open. WARNING: It is a "power API" call which you should use with care.
   * Ordinal use cases of CircuitBreaker expects a remote call to return Future, as in withCircuitBreaker.
   * So, if you check the state by yourself, and make a remote call outside CircuitBreaker, you should
   * manage the state yourself.
   */
  def isOpen: Boolean = {
    currentState == Open
  }

  /**
   * Return true if the internal state is HalfOpen. WARNING: It is a "power API" call which you should use with care.
   * Ordinal use cases of CircuitBreaker expects a remote call to return Future, as in withCircuitBreaker.
   * So, if you check the state by yourself, and make a remote call outside CircuitBreaker, you should
   * manage the state yourself.
   */
  def isHalfOpen: Boolean = {
    currentState == HalfOpen
  }

  /**
   * Adds a callback to execute when circuit breaker opens
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def onOpen(callback: => Unit): CircuitBreaker = addOnOpenListener(new Runnable { def run = callback })

  /**
   * Java API for onOpen
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def addOnOpenListener(callback: Runnable): CircuitBreaker = {
    Open.addListener(callback)
    this
  }

  /**
   * Adds a callback to execute when circuit breaker transitions to half-open
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def onHalfOpen(callback: => Unit): CircuitBreaker = addOnHalfOpenListener(new Runnable { def run = callback })

  /**
   * JavaAPI for onHalfOpen
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def addOnHalfOpenListener(callback: Runnable): CircuitBreaker = {
    HalfOpen.addListener(callback)
    this
  }

  /**
   * Adds a callback to execute when circuit breaker state closes
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def onClose(callback: => Unit): CircuitBreaker = addOnCloseListener(new Runnable { def run = callback })

  /**
   * JavaAPI for onClose
   *
   * @param callback Handler to be invoked on state change
   * @return CircuitBreaker for fluent usage
   */
  def addOnCloseListener(callback: Runnable): CircuitBreaker = {
    Closed.addListener(callback)
    this
  }

  /**
   * Adds a callback to execute when call finished with success.
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on successful call, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def onCallSuccess(callback: Long => Unit): CircuitBreaker =
    addOnCallSuccessListener(new Consumer[Long] {
      def accept(result: Long): Unit = callback(result)
    })

  /**
   * JavaAPI for onCallSuccess
   *
   * @param callback Handler to be invoked on successful call, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def addOnCallSuccessListener(callback: Consumer[Long]): CircuitBreaker = {
    successListeners.add(callback)
    this
  }

  /**
   * Adds a callback to execute if the call finished with failure.
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on failed call, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def onCallFailure(callback: Long => Unit): CircuitBreaker =
    addOnCallFailureListener(new Consumer[Long] {
      def accept(result: Long): Unit = callback(result)
    })

  /**
   * JavaAPI for onCallFailure.
   *
   * @param callback Handler to be invoked on failed call, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def addOnCallFailureListener(callback: Consumer[Long]): CircuitBreaker = {
    callFailureListeners.add(callback)
    this
  }

  /**
   * Adds a callback to execute if a call finished with timeout.
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on call finished with timeout, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def onCallTimeout(callback: Long => Unit): CircuitBreaker =
    addOnCallTimeoutListener(new Consumer[Long] {
      def accept(result: Long): Unit = callback(result)
    })

  /**
   * JavaAPI for onCallTimeout.
   *
   * @param callback Handler to be invoked on call finished with timeout, where passed value is elapsed time in nanoseconds.
   * @return CircuitBreaker for fluent usage
   */
  def addOnCallTimeoutListener(callback: Consumer[Long]): CircuitBreaker = {
    callTimeoutListeners.add(callback)
    this
  }

  /**
   * Adds a callback to execute if call was failed due to open breaker.
   *
   * The callback is run in the [[scala.concurrent.ExecutionContext]] supplied in the constructor.
   *
   * @param callback Handler to be invoked on call failed due to open breaker.
   * @return CircuitBreaker for fluent usage
   */
  def onCallBreakerOpen(callback: => Unit): CircuitBreaker =
    addOnCallBreakerOpenListener(new Runnable { def run = callback })

  /**
   * JavaAPI for onCallBreakerOpen.
   *
   * @param callback Handler to be invoked on call failed due to open breaker.
   * @return CircuitBreaker for fluent usage
   */
  def addOnCallBreakerOpenListener(callback: Runnable): CircuitBreaker = {
    callBreakerOpenListeners.add(callback)
    this
  }

  /**
   * Retrieves current failure count.
   *
   * @return count
   */
  private[akka] def currentFailureCount: Int = Closed.get

  /**
   * Implements consistent transition between states. Throws IllegalStateException if an invalid transition is attempted.
   *
   * @param fromState State being transitioning from
   * @param toState State being transitioning from
   */
  private def transition(fromState: State, toState: State): Unit = {
    if (swapState(fromState, toState))
      toState.enter()
    // else some other thread already swapped state
  }

  /**
   * Trips breaker to an open state.  This is valid from Closed or Half-Open states.
   *
   * @param fromState State we're coming from (Closed or Half-Open)
   */
  private def tripBreaker(fromState: State): Unit = transition(fromState, Open)

  /**
   * Resets breaker to a closed state.  This is valid from an Half-Open state only.
   */
  private def resetBreaker(): Unit = transition(HalfOpen, Closed)

  /**
   * Invokes all onSuccess callback handlers.
   *
   * @param start time in nanoseconds of call invocation
   */
  private def notifyCallSuccessListeners(start: Long): Unit = if (!successListeners.isEmpty) {
    val elapsed = System.nanoTime() - start
    val iterator = successListeners.iterator()
    while (iterator.hasNext) {
      val listener = iterator.next()
      executor.execute(new Runnable {
        def run() = listener.accept(elapsed)
      })
    }
  }

  /**
   * Invokes all onCallFailure callback handlers.
   *
   * @param start time in nanoseconds of call invocation
   */
  private def notifyCallFailureListeners(start: Long): Unit = if (!callFailureListeners.isEmpty) {
    val elapsed = System.nanoTime() - start
    val iterator = callFailureListeners.iterator()
    while (iterator.hasNext) {
      val listener = iterator.next()
      executor.execute(new Runnable {
        def run() = listener.accept(elapsed)
      })
    }
  }

  /**
   * Invokes all onCallTimeout callback handlers.
   *
   * @param start time in nanoseconds of call invocation
   */
  private def notifyCallTimeoutListeners(start: Long): Unit = if (!callTimeoutListeners.isEmpty) {
    val elapsed = System.nanoTime() - start
    val iterator = callTimeoutListeners.iterator()
    while (iterator.hasNext) {
      val listener = iterator.next()
      executor.execute(new Runnable {
        def run() = listener.accept(elapsed)
      })
    }
  }

  /**
   * Invokes all onCallBreakerOpen callback handlers.
   */
  private def notifyCallBreakerOpenListeners(): Unit = if (!callBreakerOpenListeners.isEmpty) {
    val iterator = callBreakerOpenListeners.iterator()
    while (iterator.hasNext) {
      val listener = iterator.next()
      executor.execute(listener)
    }
  }

  /**
   * Attempts to reset breaker by transitioning to a half-open state.  This is valid from an Open state only.
   */
  private def attemptReset(): Unit = transition(Open, HalfOpen)

  private val timeoutEx = new TimeoutException("Circuit Breaker Timed out.") with NoStackTrace

  private val callFailureListeners = new CopyOnWriteArrayList[Consumer[Long]]

  private val callTimeoutListeners = new CopyOnWriteArrayList[Consumer[Long]]

  private val callBreakerOpenListeners = new CopyOnWriteArrayList[Runnable]

  private val successListeners = new CopyOnWriteArrayList[Consumer[Long]]

  if (telemetry != CircuitBreakerNoopTelemetry) {
    onOpen(telemetry.onOpen())
    onHalfOpen(telemetry.onHalfOpen())
    onClose(telemetry.onClose())
    onCallBreakerOpen(telemetry.onCallBreakerOpenFailure())
    onCallTimeout(telemetry.onCallTimeoutFailure)
    onCallFailure(telemetry.onCallFailure)
    onCallSuccess(telemetry.onCallSuccess)
  }

  private def isIgnoredException(ex: Any): Boolean =
    allowExceptions.nonEmpty && (ex match {
      case ce: CompletionException => ce.getCause != null && allowExceptions.contains(ce.getCause.getClass.getName)
      case _                       => allowExceptions.contains(ex.getClass.getName)
    })

  private val failureFn: Try[_] => Boolean = {
    case _: Success[_]                       => false
    case Failure(t) if isIgnoredException(t) => false
    case _                                   => true
  }

  /**
   * Internal state abstraction
   */
  private sealed trait State {
    private val listeners = new CopyOnWriteArrayList[Runnable]

    /**
     * Add a listener function which is invoked on state entry
     *
     * @param listener listener implementation
     */
    def addListener(listener: Runnable): Unit = listeners.add(listener)

    /**
     * Test for whether listeners exist
     *
     * @return whether listeners exist
     */
    private def hasListeners: Boolean = !listeners.isEmpty

    /**
     * Notifies the listeners of the transition event via a Future executed in implicit parameter ExecutionContext
     *
     * @return Promise which executes listener in supplied [[scala.concurrent.ExecutionContext]]
     */
    protected def notifyTransitionListeners(): Unit = {
      if (hasListeners) {
        val iterator = listeners.iterator
        while (iterator.hasNext) {
          val listener = iterator.next
          executor.execute(listener)
        }
      }
    }

    /**
     * Shared implementation of call across all states.  Thrown exception or execution of the call beyond the allowed
     * call timeout is counted as a failed call, otherwise a successful call
     *
     * @param body Implementation of the call
     * @param defineFailureFn function that define what should be consider failure and thus increase failure count
     * @return Future containing the result of the call
     */
    def callThrough[T](body: => Future[T], defineFailureFn: Try[T] => Boolean): Future[T] = {

      def materialize[U](value: => Future[U]): Future[U] =
        try value
        catch { case NonFatal(t) => Future.failed(t) }

      if (callTimeout == Duration.Zero) {
        val start = System.nanoTime()
        val f = materialize(body)

        f.onComplete {
          case _: Success[_] =>
            notifyCallSuccessListeners(start)
            callSucceeds()
          case Failure(_) =>
            notifyCallFailureListeners(start)
            callFails()
        }

        f
      } else {
        val start = System.nanoTime()
        val p = Promise[T]()

        p.future.onComplete { fResult =>
          if (defineFailureFn(fResult)) {
            callFails()
          } else {
            notifyCallSuccessListeners(start)
            callSucceeds()
          }
        }(ExecutionContext.parasitic)

        val timeout = scheduler.scheduleOnce(callTimeout) {
          if (p.tryFailure(timeoutEx)) {
            notifyCallTimeoutListeners(start)
          }
        }(ExecutionContext.parasitic)

        materialize(body).onComplete {
          case Success(result) =>
            p.trySuccess(result)
            timeout.cancel()
          case Failure(ex) =>
            if (p.tryFailure(ex)) {
              if (!isIgnoredException(ex)) notifyCallFailureListeners(start)
            }
            timeout.cancel()
        }(ExecutionContext.parasitic)
        p.future
      }
    }

    /**
     * Shared implementation of call across all states.  Thrown exception or execution of the call beyond the allowed
     * call timeout is counted as a failed call, otherwise a successful call
     *
     * @param body Implementation of the call
     * @return Future containing the result of the call
     */
    def callThrough[T](body: => Future[T]): Future[T] = callThrough(body, failureFn)

    /**
     * Abstract entry point for all states
     *
     * @param body Implementation of the call that needs protected
     * @param defineFailureFn function that define what should be consider failure and thus increase failure count
     * @return Future containing result of protected call
     */
    def invoke[T](body: => Future[T], defineFailureFn: Try[T] => Boolean): Future[T]

    /**
     * Abstract entry point for all states
     *
     * @param body Implementation of the call that needs protected
     * @return Future containing result of protected call
     */
    def invoke[T](body: => Future[T]): Future[T] = invoke(body, failureFn)

    /**
     * Invoked when call succeeds
     *
     */
    def callSucceeds(): Unit

    /**
     * Invoked when call fails
     *
     */
    def callFails(): Unit

    /**
     * Invoked on the transitioned-to state during transition.  Notifies listeners after invoking subclass template
     * method _enter
     *
     */
    final def enter(): Unit = {
      _enter()
      notifyTransitionListeners()
    }

    /**
     * Template method for concrete traits
     *
     */
    def _enter(): Unit
  }

  /**
   * Concrete implementation of Closed state
   */
  private object Closed extends AtomicInteger with State {

    /**
     * Implementation of invoke, which simply attempts the call
     *
     * @param body Implementation of the call that needs protected
     * @return Future containing result of protected call
     */
    override def invoke[T](body: => Future[T], defineFailureFn: Try[T] => Boolean): Future[T] =
      callThrough(body, defineFailureFn)

    /**
     * On successful call, the failure count is reset to 0
     *
     * @return
     */
    override def callSucceeds(): Unit = set(0)

    /**
     * On failed call, the failure count is incremented.  The count is checked against the configured maxFailures, and
     * the breaker is tripped if we have reached maxFailures.
     *
     * @return
     */
    override def callFails(): Unit = if (incrementAndGet() == maxFailures) tripBreaker(Closed)

    /**
     * On entry of this state, failure count and resetTimeout is reset.
     *
     * @return
     */
    override def _enter(): Unit = {
      set(0)
      swapResetTimeout(currentResetTimeout, resetTimeout)
    }

    /**
     * Override for more descriptive toString
     *
     * @return
     */
    override def toString: String = "Closed with failure count = " + get()
  }

  /**
   * Concrete implementation of half-open state
   */
  private object HalfOpen extends AtomicBoolean(true) with State {

    /**
     * Allows a single call through, during which all other callers fail-fast.  If the call fails, the breaker reopens.
     * If the call succeeds the breaker closes.
     *
     * @param body Implementation of the call that needs protected
     * @return Future containing result of protected call
     */
    override def invoke[T](body: => Future[T], defineFailureFn: Try[T] => Boolean): Future[T] =
      if (compareAndSet(true, false))
        callThrough(body, defineFailureFn)
      else {
        notifyCallBreakerOpenListeners()
        Future.failed[T](new CircuitBreakerOpenException(0.seconds))
      }

    /**
     * Reset breaker on successful call.
     *
     * @return
     */
    override def callSucceeds(): Unit = resetBreaker()

    /**
     * Reopen breaker on failed call.
     *
     * @return
     */
    override def callFails(): Unit = tripBreaker(HalfOpen)

    /**
     * On entry, guard should be reset for that first call to get in
     *
     * @return
     */
    override def _enter(): Unit = set(true)

    /**
     * Override for more descriptive toString
     *
     * @return
     */
    override def toString: String = "Half-Open currently testing call for success = " + get()
  }

  /**
   * Concrete implementation of Open state
   */
  private object Open extends AtomicLong with State {

    /**
     * Fail-fast on any invocation
     *
     * @param body Implementation of the call that needs protected
     * @return Future containing result of protected call
     */
    override def invoke[T](body: => Future[T], defineFailureFn: Try[T] => Boolean): Future[T] = {
      notifyCallBreakerOpenListeners()
      Future.failed(new CircuitBreakerOpenException(remainingDuration()))
    }

    /**
     * Calculate remaining duration until reset to inform the caller in case a backoff algorithm is useful
     *
     * @return duration to when the breaker will attempt a reset by transitioning to half-open
     */
    private def remainingDuration(): FiniteDuration = {
      val fromOpened = System.nanoTime() - get
      val diff = currentResetTimeout.toNanos - fromOpened
      if (diff <= 0L) Duration.Zero
      else diff.nanos
    }

    /**
     * No-op for open, calls are never executed so cannot succeed or fail
     *
     * @return
     */
    override def callSucceeds(): Unit = ()

    /**
     * No-op for open, calls are never executed so cannot succeed or fail
     *
     * @return
     */
    override def callFails(): Unit = ()

    /**
     * On entering this state, schedule an attempted reset via [[akka.actor.Scheduler]] and store the entry time to
     * calculate remaining time before attempted reset.
     *
     * @return
     */
    override def _enter(): Unit = {
      set(System.nanoTime())
      scheduler.scheduleOnce(currentResetTimeout) {
        attemptReset()
      }
      val rnd = 1.0 + ThreadLocalRandom.current().nextDouble() * randomFactor
      val nextResetTimeout = currentResetTimeout * exponentialBackoffFactor * rnd match {
        case f: FiniteDuration => f
        case _                 => currentResetTimeout
      }

      if (nextResetTimeout < maxResetTimeout)
        swapResetTimeout(currentResetTimeout, nextResetTimeout)
    }

    /**
     * Override for more descriptive toString
     *
     * @return
     */
    override def toString: String = "Open"
  }

}

/**
 * Exception thrown when Circuit Breaker is open.
 *
 * @param remainingDuration Stores remaining time before attempting a reset.  Zero duration means the breaker is
 *                          currently in half-open state.
 * @param message Defaults to "Circuit Breaker is open; calls are failing fast"
 */
class CircuitBreakerOpenException(
    val remainingDuration: FiniteDuration,
    message: String = "Circuit Breaker is open; calls are failing fast")
    extends AkkaException(message)
    with NoStackTrace
