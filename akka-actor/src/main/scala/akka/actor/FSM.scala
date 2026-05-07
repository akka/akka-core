/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.annotation.nowarn
import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import language.implicitConversions

import akka.annotation.InternalApi
import akka.routing.{ Deafen, Listen, Listeners }

object FSM {

  /**
   * A partial function value which does not match anything and can be used to
   * “reset” `whenUnhandled` and `onTermination` handlers.
   *
   * {{{
   * onTermination(FSM.NullFunction)
   * }}}
   */
  object NullFunction extends PartialFunction[Any, Nothing] {
    def isDefinedAt(o: Any) = false
    def apply(o: Any) = sys.error("undefined")
  }

  /**
   * Message type which is sent directly to the subscribed actor in
   * [[akka.actor.FSM.SubscribeTransitionCallBack]] before sending any
   * [[akka.actor.FSM.Transition]] messages.
   */
  final case class CurrentState[S](fsmRef: ActorRef, state: S)

  /**
   * Message type which is used to communicate transitions between states to
   * all subscribed listeners (use [[akka.actor.FSM.SubscribeTransitionCallBack]]).
   */
  final case class Transition[S](fsmRef: ActorRef, from: S, to: S)

  /**
   * Send this to an [[akka.actor.FSM]] to request first the [[FSM.CurrentState]]
   * and then a series of [[FSM.Transition]] updates. Cancel the subscription
   * using [[FSM.UnsubscribeTransitionCallBack]].
   */
  final case class SubscribeTransitionCallBack(actorRef: ActorRef)

  /**
   * Unsubscribe from [[akka.actor.FSM.Transition]] notifications which was
   * effected by sending the corresponding [[akka.actor.FSM.SubscribeTransitionCallBack]].
   */
  final case class UnsubscribeTransitionCallBack(actorRef: ActorRef)

  /**
   * Reason why this [[akka.actor.FSM]] is shutting down.
   */
  sealed trait Reason

  /**
   * Default reason if calling `stop()`.
   */
  case object Normal extends Reason

  /**
   * Reason given when someone was calling `system.stop(fsm)` from outside;
   * also applies to `Stop` supervision directive.
   */
  case object Shutdown extends Reason

  /**
   * Signifies that the [[akka.actor.FSM]] is shutting itself down because of
   * an error, e.g. if the state to transition into does not exist. You can use
   * this to communicate a more precise cause to the `onTermination` block.
   */
  final case class Failure(cause: Any) extends Reason

  /**
   * This case object is received in case of a state timeout.
   */
  case object StateTimeout

  /**
   * INTERNAL API
   */
  private final case class TimeoutMarker(generation: Long)

  /** INTERNAL API */
  @InternalApi
  private[akka] sealed trait TimerMode {
    def repeat: Boolean
  }

  /** INTERNAL API */
  @InternalApi
  private[akka] case object FixedRateMode extends TimerMode {
    override def repeat: Boolean = true
  }

  /** INTERNAL API */
  @InternalApi
  private[akka] case object FixedDelayMode extends TimerMode {
    override def repeat: Boolean = true
  }

  /** INTERNAL API */
  @InternalApi
  private[akka] case object SingleMode extends TimerMode {
    override def repeat: Boolean = false
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] final case class Timer(name: String, msg: Any, mode: TimerMode, generation: Int, owner: AnyRef)(
      context: ActorContext)
      extends NoSerializationVerificationNeeded {
    private var ref: Option[Cancellable] = _
    private val scheduler = context.system.scheduler
    private implicit val executionContext: ExecutionContextExecutor = context.dispatcher

    def schedule(actor: ActorRef, timeout: FiniteDuration): Unit = {
      val timerMsg = msg match {
        case m: AutoReceivedMessage => m
        case _                      => this
      }
      ref = Some(mode match {
        case SingleMode     => scheduler.scheduleOnce(timeout, actor, timerMsg)
        case FixedDelayMode => scheduler.scheduleWithFixedDelay(timeout, timeout, actor, timerMsg)
        case FixedRateMode  => scheduler.scheduleAtFixedRate(timeout, timeout, actor, timerMsg)
      })
    }
    def cancel(): Unit =
      if (ref.isDefined) {
        ref.get.cancel()
        ref = None
      }
  }

  /**
   * This extractor is just convenience for matching a (S, S) pair, including a
   * reminder what the new state is.
   */
  object `->` {
    def unapply[S](in: (S, S)) = Some(in)
  }
  val `→` = `->`

  /**
   * Log Entry of the [[akka.actor.LoggingFSM]], can be obtained by calling `getLog`.
   */
  final case class LogEntry[S, D](stateName: S, stateData: D, event: Any)

  /** Used by `forMax` to signal "cancel stateTimeout" */
  private final val SomeMaxFiniteDuration = Some(Long.MaxValue.nanos)

  /**
   * INTERNAL API
   * Using a subclass for binary compatibility reasons
   */
  private[akka] class SilentState[S, D](
      stateName: S,
      stateData: D,
      timeout: Option[FiniteDuration],
      stopReason: Option[Reason],
      replies: List[Any])
      extends State[S, D](stateName, stateData, timeout, stopReason, replies) {

    /**
     * INTERNAL API
     */
    private[akka] override def notifies: Boolean = false

    override def copy(
        stateName: S = stateName,
        stateData: D = stateData,
        timeout: Option[FiniteDuration] = timeout,
        stopReason: Option[Reason] = stopReason,
        replies: List[Any] = replies): State[S, D] = {
      new SilentState(stateName, stateData, timeout, stopReason, replies)
    }

  }

  /**
   * This captures all of the managed state of the [[akka.actor.FSM]]: the state
   * name, the state data, possibly custom timeout, stop reason and replies
   * accumulated while processing the last message.
   */
  object State {
    def apply[S, D](
        stateName: S,
        stateData: D,
        timeout: Option[FiniteDuration] = None,
        stopReason: Option[Reason] = None,
        replies: List[Any] = Nil) = {
      new State(stateName, stateData, timeout, stopReason, replies)
    }

    def unapply[S, D](state: State[S, D]): Option[(S, D, Option[FiniteDuration], Option[Reason], List[Any])] = {
      Some((state.stateName, state.stateData, state.timeout, state.stopReason, state.replies))
    }
  }
  class State[S, D](
      val stateName: S,
      val stateData: D,
      val timeout: Option[FiniteDuration] = None,
      val stopReason: Option[Reason] = None,
      val replies: List[Any] = Nil)
      extends Product
      with Serializable {

    def canEqual(that: Any): Boolean = {
      that match {
        case _: State[_, _] => true
        case _              => false
      }
    }

    override def equals(that: Any) = that match {
      case other: State[_, _] =>
        other.canEqual(this) &&
        this.stateName == other.stateName &&
        this.stateData == other.stateData &&
        this.timeout == other.timeout &&
        this.stopReason == other.stopReason &&
        this.replies == other.replies
      case _ => false
    }

    override def productPrefix = classOf[State[S, D]].getSimpleName

    def productArity: Int = 5
    def productElement(n: Int): Any = n match {
      case 0 => this.stateName
      case 1 => this.stateData
      case 2 => this.timeout
      case 3 => this.stopReason
      case 4 => this.replies
      case _ => throw new IndexOutOfBoundsException(n.toString)
    }

    /**
     * INTERNAL API
     */
    private[akka] def notifies: Boolean = true

    // defined here to be able to override it in SilentState
    def copy(
        stateName: S = stateName,
        stateData: D = stateData,
        timeout: Option[FiniteDuration] = timeout,
        stopReason: Option[Reason] = stopReason,
        replies: List[Any] = replies): State[S, D] = {
      new State(stateName, stateData, timeout, stopReason, replies)
    }

    /**
     * Modify state transition descriptor to include a state timeout for the
     * next state. This timeout overrides any default timeout set for the next
     * state.
     *
     * Use Duration.Inf to deactivate an existing timeout.
     */
    def forMax(timeout: Duration): State[S, D] = timeout match {
      case f: FiniteDuration => copy(timeout = Some(f))
      case Duration.Inf      => copy(timeout = SomeMaxFiniteDuration) // we map the Infinite duration to a special marker,
      case _                 => copy(timeout = None) // that means "cancel stateTimeout". This marker is needed
    } // so we do not have to break source/binary compat.
    // TODO: Can be removed once we can break State#timeout signature to `Option[Duration]`

    /**
     * JAVA API: Modify state transition descriptor to include a state timeout for the
     * next state. This timeout overrides any default timeout set for the next
     * state.
     *
     * Use Duration.Inf to deactivate an existing timeout.
     */
    def forMax(timeout: java.time.Duration): State[S, D] = {
      import scala.jdk.DurationConverters._
      forMax(timeout.toScala)
    }

    /**
     * Send reply to sender of the current message, if available.
     *
     * @return this state transition descriptor
     */
    def replying(replyValue: Any): State[S, D] = {
      copy(replies = replyValue :: replies)
    }

    /**
     * Modify state transition descriptor with new state data. The data will be
     * set when transitioning to the new state.
     */
    @nowarn("msg=deprecated")
    def using(@deprecatedName(Symbol("nextStateDate")) nextStateData: D): State[S, D] = {
      copy(stateData = nextStateData)
    }

    /**
     * INTERNAL API.
     */
    private[akka] def withStopReason(reason: Reason): State[S, D] = {
      copy(stopReason = Some(reason))
    }

    /**
     * INTERNAL API.
     */
    private[akka] def withNotification(notifies: Boolean): State[S, D] = {
      if (notifies)
        State(stateName, stateData, timeout, stopReason, replies)
      else
        new SilentState(stateName, stateData, timeout, stopReason, replies)
    }
  }

  /**
   * All messages sent to the [[akka.actor.FSM]] will be wrapped inside an
   * `Event`, which allows pattern matching to extract both state and data.
   */
  final case class Event[D](event: Any, stateData: D) extends NoSerializationVerificationNeeded

  /**
   * Case class representing the state of the [[akka.actor.FSM]] within the
   * `onTermination` block.
   */
  final case class StopEvent[S, D](reason: Reason, currentState: S, stateData: D)
      extends NoSerializationVerificationNeeded

}

/**
 * Finite State Machine actor trait. Use as follows:
 *
 * <pre>
 *   object A {
 *     trait State
 *     case class One extends State
 *     case class Two extends State
 *
 *     case class Data(i : Int)
 *   }
 *
 *   class A extends Actor with FSM[A.State, A.Data] {
 *     import A._
 *
 *     startWith(One, Data(42))
 *     when(One) {
 *         case Event(SomeMsg, Data(x)) => ...
 *         case Event(SomeOtherMsg, _) => ... // when data not needed
 *     }
 *     when(Two, stateTimeout = 5 seconds) { ... }
 *     initialize()
 *   }
 * </pre>
 *
 * Within the partial function the following values are returned for effecting
 * state transitions:
 *
 *  - <code>stay</code> for staying in the same state
 *  - <code>stay using Data(...)</code> for staying in the same state, but with
 *    different data
 *  - <code>stay forMax 5.millis</code> for staying with a state timeout; can be
 *    combined with <code>using</code>
 *  - <code>goto(...)</code> for changing into a different state; also supports
 *    <code>using</code> and <code>forMax</code>
 *  - <code>stop</code> for terminating this FSM actor
 *
 * Each of the above also supports the method <code>replying(AnyRef)</code> for
 * sending a reply before changing state.
 *
 * While changing state, custom handlers may be invoked which are registered
 * using <code>onTransition</code>. This is meant to enable concentrating
 * different concerns in different places; you may choose to use
 * <code>when</code> for describing the properties of a state, including of
 * course initiating transitions, but you can describe the transitions using
 * <code>onTransition</code> to avoid having to duplicate that code among
 * multiple paths which lead to a transition:
 *
 * <pre>
 * onTransition {
 *   case Active -&gt; _ =&gt; cancelTimer("activeTimer")
 * }
 * </pre>
 *
 * Multiple such blocks are supported and all of them will be called, not only
 * the first matching one.
 *
 * Another feature is that other actors may subscribe for transition events by
 * sending a <code>SubscribeTransitionCallback</code> message to this actor.
 * Stopping a listener without unregistering will not remove the listener from the
 * subscription list; use <code>UnsubscribeTransitionCallback</code> before stopping
 * the listener.
 *
 * State timeouts set an upper bound to the time which may pass before another
 * message is received in the current state. If no external message is
 * available, then upon expiry of the timeout a StateTimeout message is sent.
 * Note that this message will only be received in the state for which the
 * timeout was set and that any message received will cancel the timeout
 * (possibly to be started again by the next transition).
 *
 * Another feature is the ability to install and cancel single-shot as well as
 * repeated timers which arrange for the sending of a user-specified message:
 *
 * <pre>
 *   setTimer("tock", TockMsg, 1 second, true) // repeating
 *   setTimer("lifetime", TerminateMsg, 1 hour, false) // single-shot
 *   cancelTimer("tock")
 *   isTimerActive("tock")
 * </pre>
 */
trait FSM[S, D] extends Actor with Listeners with ActorLogging {

  import FSM._

  type State = FSM.State[S, D]
  type Event = FSM.Event[D]
  type StopEvent = FSM.StopEvent[S, D]
  type StateFunction = scala.PartialFunction[Event, State]
  type Timeout = Option[FiniteDuration]
  type TransitionHandler = PartialFunction[(S, S), Unit]

  /*
   * “import” so that these are visible without an import
   */
  val Event: FSM.Event.type = FSM.Event
  val StopEvent: FSM.StopEvent.type = FSM.StopEvent

  /**
   * This extractor is just convenience for matching a (S, S) pair, including a
   * reminder what the new state is.
   */
  val `->` = FSM.`->`

  /**
   * This case object is received in case of a state timeout.
   */
  val StateTimeout = FSM.StateTimeout

  /**
   * ****************************************
   *                 DSL
   * ****************************************
   */
  /**
   * Insert a new StateFunction at the end of the processing chain for the
   * given state. If the stateTimeout parameter is set, entering this state
   * without a differing explicit timeout setting will trigger a StateTimeout
   * event; the same is true when using #stay.
   *
   * @param stateName designator for the state
   * @param stateTimeout default state timeout for this state
   * @param stateFunction partial function describing response to input
   */
  final def when(stateName: S, stateTimeout: FiniteDuration = null)(stateFunction: StateFunction): Unit =
    register(stateName, stateFunction, Option(stateTimeout))

  /**
   * Set initial state. Call this method from the constructor before the [[#initialize]] method.
   * If different state is needed after a restart this method, followed by [[#initialize]], can
   * be used in the actor life cycle hooks [[akka.actor.Actor#preStart]] and [[akka.actor.Actor#postRestart]].
   *
   * @param stateName initial state designator
   * @param stateData initial state data
   * @param timeout state timeout for the initial state, overriding the default timeout for that state
   */
  final def startWith(stateName: S, stateData: D, timeout: Timeout = None): Unit =
    currentState = FSM.State(stateName, stateData, timeout)

  /**
   * Produce transition to other state.
   * Return this from a state function in order to effect the transition.
   *
   * This method always triggers transition events, even for `A -> A` transitions.
   * If you want to stay in the same state without triggering an state transition event use [[#stay]] instead.
   *
   * @param nextStateName state designator for the next state
   * @return state transition descriptor
   */
  final def goto(nextStateName: S): State = FSM.State(nextStateName, currentState.stateData)

  /**
   * Produce "empty" transition descriptor.
   * Return this from a state function when no state change is to be effected.
   *
   * No transition event will be triggered by [[#stay]].
   * If you want to trigger an event like `S -&gt; S` for `onTransition` to handle use `goto` instead.
   *
   * @return descriptor for staying in current state
   */
  final def stay(): State =
    goto(currentState.stateName)
      .withNotification(false) // cannot directly use currentState because of the timeout field

  /**
   * Produce change descriptor to stop this FSM actor with reason "Normal".
   */
  final def stop(): State = stop(Normal)

  /**
   * Produce change descriptor to stop this FSM actor including specified reason.
   */
  final def stop(reason: Reason): State = stop(reason, currentState.stateData)

  /**
   * Produce change descriptor to stop this FSM actor including specified reason.
   */
  final def stop(reason: Reason, stateData: D): State = stay().using(stateData).withStopReason(reason)

  final class TransformHelper(func: StateFunction) {
    def using(andThen: PartialFunction[State, State]): StateFunction =
      func.andThen(andThen.orElse { case x => x })
  }

  final def transform(func: StateFunction): TransformHelper = new TransformHelper(func)

  /**
   * Schedules a message to be sent repeatedly to the `self` actor with a
   * fixed `delay` between messages.
   *
   * It will not compensate the delay between messages if scheduling is delayed
   * longer than specified for some reason. The delay between sending of subsequent
   * messages will always be (at least) the given `delay`.
   *
   * In the long run, the frequency of messages will generally be slightly lower than
   * the reciprocal of the specified `delay`.
   *
   * Each timer has a `name` and if a new timer with same `name` is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  def startTimerWithFixedDelay(name: String, msg: Any, delay: FiniteDuration): Unit =
    startTimer(name, msg, delay, FixedDelayMode)

  /**
   * Schedules a message to be sent repeatedly to the `self` actor with a
   * given frequency.
   *
   * It will compensate the delay for a subsequent message if the sending of previous
   * message was delayed more than specified. In such cases, the actual message interval
   * will differ from the interval passed to the method.
   *
   * If the execution is delayed longer than the `interval`, the subsequent message will
   * be sent immediately after the prior one. This also has the consequence that after
   * long garbage collection pauses or other reasons when the JVM was suspended all
   * "missed" messages will be sent when the process wakes up again.
   *
   * In the long run, the frequency of messages will be exactly the reciprocal of the
   * specified `interval`.
   *
   * Warning: `startTimerAtFixedRate` can result in bursts of scheduled messages after long
   * garbage collection pauses, which may in worst case cause undesired load on the system.
   * Therefore `startTimerWithFixedDelay` is often preferred.
   *
   * Each timer has a `name` and if a new timer with same `name` is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  def startTimerAtFixedRate(name: String, msg: Any, interval: FiniteDuration): Unit =
    startTimer(name, msg, interval, FixedRateMode)

  /**
   * Start a timer that will send `msg` once to the `self` actor after
   * the given `delay`.
   *
   * Each timer has a `name` and if a new timer with same `name` is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  def startSingleTimer(name: String, msg: Any, delay: FiniteDuration): Unit =
    startTimer(name, msg, delay, SingleMode)

  /**
   * Schedule named timer to deliver message after given delay, possibly repeating.
   * Any existing timer with the same name will automatically be canceled before
   * adding the new timer.
   * @param name identifier to be used with cancelTimer()
   * @param msg message to be delivered
   * @param timeout delay of first message delivery and between subsequent messages
   * @param repeat send once if false, scheduleAtFixedRate if true
   */
  @deprecated(
    "Use startSingleTimer, startTimerWithFixedDelay or startTimerAtFixedRate instead. This has the same semantics as " +
    "startTimerAtFixedRate, but startTimerWithFixedDelay is often preferred.",
    since = "2.6.0")
  final def setTimer(name: String, msg: Any, timeout: FiniteDuration, repeat: Boolean = false): Unit = {
    // repeat => FixedRateMode for compatibility
    val mode = if (repeat) FixedRateMode else SingleMode
    startTimer(name, msg, timeout, mode)
  }

  private def startTimer(name: String, msg: Any, timeout: FiniteDuration, mode: TimerMode): Unit = {
    if (debugEvent)
      log.debug("setting " + (if (mode.repeat) "repeating " else "") + "timer '" + name + "'/" + timeout + ": " + msg)
    if (timers contains name) {
      timers(name).cancel()
    }
    val timer = Timer(name, msg, mode, timerGen.next(), this)(context)
    timer.schedule(self, timeout)
    timers(name) = timer
  }

  /**
   * Cancel named timer, ensuring that the message is not subsequently delivered (no race).
   * @param name of the timer to cancel
   */
  final def cancelTimer(name: String): Unit = {
    if (debugEvent)
      log.debug("canceling timer '" + name + "'")
    if (timers contains name) {
      timers(name).cancel()
      timers -= name
    }
  }

  /**
   * Inquire whether the named timer is still active. Returns true unless the
   * timer does not exist, has previously been canceled or if it was a
   * single-shot timer whose message was already received.
   */
  final def isTimerActive(name: String): Boolean = timers contains name

  /**
   * Set state timeout explicitly. This method can safely be used from within a
   * state handler.
   */
  final def setStateTimeout(state: S, timeout: Timeout): Unit = stateTimeouts(state) = timeout

  /**
   * INTERNAL API, used for testing.
   */
  private[akka] final def isStateTimerActive = timeoutFuture.isDefined

  /**
   * Set handler which is called upon each state transition, i.e. not when
   * staying in the same state. This may use the pair extractor defined in the
   * FSM companion object like so:
   *
   * <pre>
   * onTransition {
   *   case Old -&gt; New =&gt; doSomething
   * }
   * </pre>
   *
   * It is also possible to supply a 2-ary function object:
   *
   * <pre>
   * onTransition(handler _)
   *
   * private def handler(from: S, to: S) { ... }
   * </pre>
   *
   * The underscore is unfortunately necessary to enable the nicer syntax shown
   * above (it uses the implicit conversion total2pf under the hood).
   *
   * <b>Multiple handlers may be installed, and every one of them will be
   * called, not only the first one matching.</b>
   */
  final def onTransition(transitionHandler: TransitionHandler): Unit = transitionEvent :+= transitionHandler

  /**
   * Convenience wrapper for using a total function instead of a partial
   * function literal. To be used with onTransition.
   */
  implicit final def total2pf(transitionHandler: (S, S) => Unit): TransitionHandler =
    new TransitionHandler {
      def isDefinedAt(in: (S, S)) = true
      def apply(in: (S, S)): Unit = { transitionHandler(in._1, in._2) }
    }

  /**
   * Set handler which is called upon termination of this FSM actor. Calling
   * this method again will overwrite the previous contents.
   */
  final def onTermination(terminationHandler: PartialFunction[StopEvent, Unit]): Unit =
    terminateEvent = terminationHandler

  /**
   * Set handler which is called upon reception of unhandled messages. Calling
   * this method again will overwrite the previous contents.
   *
   * The current state may be queried using ``stateName``.
   */
  final def whenUnhandled(stateFunction: StateFunction): Unit =
    handleEvent = stateFunction.orElse(handleEventDefault)

  /**
   * Verify existence of initial state and setup timers. This should be the
   * last call within the constructor, or [[akka.actor.Actor#preStart]] and
   * [[akka.actor.Actor#postRestart]]
   *
   * An initial `currentState -> currentState` notification will be triggered by calling this method.
   *
   * @see [[#startWith]]
   */
  final def initialize(): Unit =
    if (currentState != null) makeTransition(currentState)
    else throw new IllegalStateException("You must call `startWith` before calling `initialize`")

  /**
   * Return current state name (i.e. object of type S)
   */
  final def stateName: S = {
    if (currentState != null) currentState.stateName
    else throw new IllegalStateException("You must call `startWith` before using `stateName`")
  }

  /**
   * Return current state data (i.e. object of type D)
   */
  final def stateData: D =
    if (currentState != null) currentState.stateData
    else throw new IllegalStateException("You must call `startWith` before using `stateData`")

  /**
   * Return next state data (available in onTransition handlers)
   */
  final def nextStateData = nextState match {
    case null => throw new IllegalStateException("nextStateData is only available during onTransition")
    case x    => x.stateData
  }

  /*
   * ****************************************************************
   *                PRIVATE IMPLEMENTATION DETAILS
   * ****************************************************************
   */

  private[akka] def debugEvent: Boolean = false

  /*
   * FSM State data and current timeout handling
   */
  private var currentState: State = _
  private var timeoutFuture: Option[Cancellable] = None
  private var nextState: State = _
  private var generation: Long = 0L

  /*
   * Timer handling
   */
  private val timers = mutable.Map[String, Timer]()
  private val timerGen = Iterator.from(0)

  /*
   * State definitions
   */
  private val stateFunctions = mutable.Map[S, StateFunction]()
  private val stateTimeouts = mutable.Map[S, Timeout]()

  private def register(name: S, function: StateFunction, timeout: Timeout): Unit = {
    if (stateFunctions contains name) {
      stateFunctions(name) = stateFunctions(name).orElse(function)
      stateTimeouts(name) = timeout.orElse(stateTimeouts(name))
    } else {
      stateFunctions(name) = function
      stateTimeouts(name) = timeout
    }
  }

  /*
   * unhandled event handler
   */
  private val handleEventDefault: StateFunction = {
    case Event(value, _) =>
      log.warning("unhandled event " + value + " in state " + stateName)
      stay()
  }
  private var handleEvent: StateFunction = handleEventDefault

  /*
   * termination handling
   */
  private var terminateEvent: PartialFunction[StopEvent, Unit] = NullFunction

  /*
   * transition handling
   */
  private var transitionEvent: List[TransitionHandler] = Nil
  private def handleTransition(prev: S, next: S): Unit = {
    val tuple = (prev, next)
    for (te <- transitionEvent) { if (te.isDefinedAt(tuple)) te(tuple) }
  }

  /*
   * *******************************************
   *       Main actor receive() method
   * *******************************************
   */
  override def receive: Receive = {
    case TimeoutMarker(gen) =>
      if (generation == gen) {
        processMsg(StateTimeout, "state timeout")
      }
    case t @ Timer(name, msg, mode, gen, owner) =>
      if ((owner eq this) && (timers contains name) && (timers(name).generation == gen)) {
        if (timeoutFuture.isDefined) {
          timeoutFuture.get.cancel()
          timeoutFuture = None
        }
        generation += 1
        if (!mode.repeat) {
          timers -= name
        }
        processMsg(msg, t)
      }
    case SubscribeTransitionCallBack(actorRef) =>
      // TODO Use context.watch(actor) and receive Terminated(actor) to clean up list
      listeners.add(actorRef)
      // send current state back as reference point
      actorRef ! CurrentState(self, currentState.stateName)
    case Listen(actorRef) =>
      // TODO Use context.watch(actor) and receive Terminated(actor) to clean up list
      listeners.add(actorRef)
      // send current state back as reference point
      actorRef ! CurrentState(self, currentState.stateName)
    case UnsubscribeTransitionCallBack(actorRef) =>
      listeners.remove(actorRef)
    case Deafen(actorRef) =>
      listeners.remove(actorRef)
    case value =>
      if (timeoutFuture.isDefined) {
        timeoutFuture.get.cancel()
        timeoutFuture = None
      }
      generation += 1
      processMsg(value, sender())
  }

  private def processMsg(value: Any, source: AnyRef): Unit = {
    val event = Event(value, currentState.stateData)
    processEvent(event, source)
  }

  private[akka] def processEvent(event: Event, @nowarn("msg=never used") source: AnyRef): Unit = {
    val stateFunc = stateFunctions(currentState.stateName)
    val nextState = if (stateFunc.isDefinedAt(event)) {
      stateFunc(event)
    } else {
      // handleEventDefault ensures that this is always defined
      handleEvent(event)
    }
    applyState(nextState)
  }

  private[akka] def applyState(nextState: State): Unit = {
    nextState.stopReason match {
      case None => makeTransition(nextState)
      case _ =>
        nextState.replies.reverse.foreach { r =>
          sender() ! r
        }
        terminate(nextState)
        context.stop(self)
    }
  }

  private[akka] def makeTransition(nextState: State): Unit = {
    if (!stateFunctions.contains(nextState.stateName)) {
      terminate(stay().withStopReason(Failure("Next state %s does not exist".format(nextState.stateName))))
    } else {
      nextState.replies.reverse.foreach { r =>
        sender() ! r
      }
      if (currentState.stateName != nextState.stateName || nextState.notifies) {
        this.nextState = nextState
        handleTransition(currentState.stateName, nextState.stateName)
        gossip(Transition(self, currentState.stateName, nextState.stateName))
        this.nextState = null
      }
      currentState = nextState

      def scheduleTimeout(d: FiniteDuration): Some[Cancellable] = {
        import context.dispatcher
        Some(context.system.scheduler.scheduleOnce(d, self, TimeoutMarker(generation)))
      }

      currentState.timeout match {
        case SomeMaxFiniteDuration                    => // effectively disable stateTimeout
        case Some(d: FiniteDuration) if d.length >= 0 => timeoutFuture = scheduleTimeout(d)
        case _ =>
          val timeout = stateTimeouts(currentState.stateName)
          if (timeout.isDefined) timeoutFuture = scheduleTimeout(timeout.get)
      }
    }
  }

  /**
   * Call `onTermination` hook; if you want to retain this behavior when
   * overriding make sure to call `super.postStop()`.
   *
   * Please note that this method is called by default from `preRestart()`,
   * so override that one if `onTermination` shall not be called during
   * restart.
   */
  override def postStop(): Unit = {
    /*
     * setting this instance’s state to terminated does no harm during restart
     * since the new instance will initialize fresh using startWith()
     */
    terminate(stay().withStopReason(Shutdown))
    super.postStop()
  }

  private def terminate(nextState: State): Unit = {
    if (currentState.stopReason.isEmpty) {
      val reason = nextState.stopReason.get
      logTermination(reason)
      for (timer <- timers.values) timer.cancel()
      timers.clear()
      timeoutFuture.foreach { _.cancel() }
      currentState = nextState

      val stopEvent = StopEvent(reason, currentState.stateName, currentState.stateData)
      if (terminateEvent.isDefinedAt(stopEvent))
        terminateEvent(stopEvent)
    }
  }

  /**
   * By default [[FSM.Failure]] is logged at error level and other reason
   * types are not logged. It is possible to override this behavior.
   */
  protected def logTermination(reason: Reason): Unit = reason match {
    case Failure(ex: Throwable) => log.error(ex, "terminating due to Failure")
    case Failure(msg: AnyRef)   => log.error(msg.toString)
    case _                      =>
  }
}

/**
 * Stackable trait for [[akka.actor.FSM]] which adds a rolling event log and
 * debug logging capabilities (analogous to [[akka.event.LoggingReceive]]).
 *
 * @since 1.2
 */
trait LoggingFSM[S, D] extends FSM[S, D] { this: Actor =>

  import FSM._

  def logDepth: Int = 0

  private[akka] override val debugEvent = context.system.settings.FsmDebugEvent

  private val events = new Array[Event](logDepth)
  private val states = new Array[AnyRef](logDepth)
  private var pos = 0
  private var full = false

  private def advance(): Unit = {
    val n = pos + 1
    if (n == logDepth) {
      full = true
      pos = 0
    } else {
      pos = n
    }
  }

  private[akka] abstract override def processEvent(event: Event, source: AnyRef): Unit = {
    if (debugEvent) {
      val srcstr = source match {
        case s: String               => s
        case Timer(name, _, _, _, _) => "timer " + name
        case a: ActorRef             => a.toString
        case _                       => "unknown"
      }
      log.debug("processing {} from {} in state {}", event, srcstr, stateName)
    }

    if (logDepth > 0) {
      states(pos) = stateName.asInstanceOf[AnyRef]
      events(pos) = event
      advance()
    }

    val oldState = stateName
    super.processEvent(event, source)
    val newState = stateName

    if (debugEvent && oldState != newState)
      log.debug("transition " + oldState + " -> " + newState)
  }

  /**
   * Retrieve current rolling log in oldest-first order. The log is filled with
   * each incoming event before processing by the user supplied state handler.
   * The log entries are lost when this actor is restarted.
   */
  protected def getLog: IndexedSeq[LogEntry[S, D]] = {
    val log =
      events.zip(states).filter(_._1 ne null).map(x => LogEntry(x._2.asInstanceOf[S], x._1.stateData, x._1.event))
    if (full) {
      IndexedSeq() ++ log.drop(pos) ++ log.take(pos)
    } else {
      IndexedSeq() ++ log
    }
  }

}
