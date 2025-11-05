/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.fsm

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import language.implicitConversions

import akka.actor._
import akka.japi.pf.{ FSMTransitionHandlerBuilder, UnitMatch, UnitPFBuilder }
import akka.routing.{ Deafen, Listen, Listeners }
import akka.util.JavaDurationConverters._
import akka.util.unused

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
 *         case Event(SomeOtherMsg, _) => ... // convenience when data not needed
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
 *   startTimerWithFixedDelay("tock", TockMsg, 1 second) // repeating
 *   startSingleTimer("lifetime", TerminateMsg, 1 hour) // single-shot
 *   cancelTimer("tock")
 *   isTimerActive("tock")
 * </pre>
 *
 */
@deprecated("Use EventSourcedBehavior", "2.6.0")
trait PersistentFSMBase[S, D, E] extends Actor with Listeners with ActorLogging {

  import akka.persistence.fsm.PersistentFSM._

  type State = PersistentFSM.State[S, D, E]
  type Event = PersistentFSM.Event[D]
  type StopEvent = PersistentFSM.StopEvent[S, D]
  type StateFunction = scala.PartialFunction[Event, State]
  type Timeout = Option[FiniteDuration]
  type TransitionHandler = PartialFunction[(S, S), Unit]

  /*
   * “import” so that these are visible without an import
   */
  val Event: PersistentFSM.Event.type = PersistentFSM.Event
  val StopEvent: PersistentFSM.StopEvent.type = PersistentFSM.StopEvent

  /**
   * This extractor is just convenience for matching a (S, S) pair, including a
   * reminder what the new state is.
   */
  val `->` = PersistentFSM.`->`

  /**
   * This case object is received in case of a state timeout.
   */
  val StateTimeout = PersistentFSM.StateTimeout

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
    currentState = PersistentFSM.State(stateName, stateData, timeout)()

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
  final def goto(nextStateName: S): State = PersistentFSM.State(nextStateName, currentState.stateData)()

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
  final def stop(reason: Reason, stateData: D): State = stay().copy0(stopReason = Some(reason), stateData = stateData)

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
   * Verify existence of initial state and setup timers. Used in [[akka.persistence.fsm.PersistentFSM]]
   * on recovery.
   *
   * An initial `currentState -> currentState` notification will be triggered by calling this method.
   *
   * @see [[akka.persistence.fsm.PersistentFSM#receiveRecover]]
   */
  @deprecated("Removed from API, called internally", "2.4.5")
  private[akka] final def initialize(): Unit =
    if (currentState != null) makeTransition(currentState)
    else throw new IllegalStateException("You must call `startWith` before calling `initialize`")

  /**
   * Return current state name (i.e. object of type S)
   */
  final def stateName: S =
    if (currentState != null) currentState.stateName
    else throw new IllegalStateException("You must call `startWith` before using `stateName`")

  /**
   * Return current state data (i.e. object of type D)
   */
  final def stateData: D =
    if (currentState != null) currentState.stateData
    else throw new IllegalStateException("You must call `startWith` before using `stateData`")

  /**
   * Return all defined state names
   */
  private[akka] final def stateNames: Iterable[S] = stateFunctions.keys

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
      actorRef ! CurrentState(self, currentState.stateName, currentState.timeout)
    case Listen(actorRef) =>
      // TODO Use context.watch(actor) and receive Terminated(actor) to clean up list
      listeners.add(actorRef)
      // send current state back as reference point
      actorRef ! CurrentState(self, currentState.stateName, currentState.timeout)
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

  private[akka] def processEvent(event: Event, @unused source: AnyRef): Unit = {
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
        gossip(Transition(self, currentState.stateName, nextState.stateName, nextState.timeout))
        this.nextState = null
      }
      currentState = nextState
      val timeout =
        currentState.timeout match {
          case PersistentFSM.SomeMaxFiniteDuration => None
          case x: Some[FiniteDuration]             => x
          case None                                => stateTimeouts(currentState.stateName)
        }

      if (timeout.isDefined) {
        val t = timeout.get
        if (t.isFinite && t.length >= 0) {
          import context.dispatcher
          timeoutFuture = Some(context.system.scheduler.scheduleOnce(t, self, TimeoutMarker(generation)))
        }
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
  @throws(classOf[Exception])
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
      currentState = nextState

      val stopEvent = StopEvent(reason, currentState.stateName, currentState.stateData)
      if (terminateEvent.isDefinedAt(stopEvent))
        terminateEvent(stopEvent)
    }
  }

  /**
   * By default [[PersistentFSM.Failure]] is logged at error level and other reason
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
 */
@deprecated("Use EventSourcedBehavior", "2.6.0")
trait LoggingPersistentFSM[S, D, E] extends PersistentFSMBase[S, D, E] { this: Actor =>

  import akka.persistence.fsm.PersistentFSM._

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

/**
 * Java API: compatible with lambda expressions
 *
 */
@deprecated("Use EventSourcedBehavior", "2.6.0")
object AbstractPersistentFSMBase {

  /**
   * A partial function value which does not match anything and can be used to
   * “reset” `whenUnhandled` and `onTermination` handlers.
   *
   * {{{
   * onTermination(FSM.NullFunction())
   * }}}
   */
  def NullFunction[S, D]: PartialFunction[S, D] = PersistentFSM.NullFunction
}

/**
 * Java API: compatible with lambda expressions
 *
 * Finite State Machine actor abstract base class.
 *
 */
@deprecated("Use EventSourcedBehavior", "2.6.0")
abstract class AbstractPersistentFSMBase[S, D, E] extends PersistentFSMBase[S, D, E] {
  import java.util.{ List => JList }

  import PersistentFSM._

  import akka.japi.pf.FI._
  import akka.persistence.fsm.japi.pf.FSMStateFunctionBuilder
  import akka.persistence.fsm.japi.pf.FSMStopBuilder

  /**
   * Returns this AbstractActor's ActorContext
   * The ActorContext is not thread safe so do not expose it outside of the
   * AbstractActor.
   */
  def getContext(): AbstractActor.ActorContext = context.asInstanceOf[AbstractActor.ActorContext]

  /**
   * Returns the ActorRef for this actor.
   *
   * Same as `self()`.
   */
  def getSelf(): ActorRef = self

  /**
   * The reference sender Actor of the currently processed message. This is
   * always a legal destination to send to, even if there is no logical recipient
   * for the reply, in which case it will be sent to the dead letter mailbox.
   *
   * Same as `sender()`.
   *
   * WARNING: Only valid within the Actor itself, so do not close over it and
   * publish it to other threads!
   */
  def getSender(): ActorRef = sender()

  /**
   * Insert a new StateFunction at the end of the processing chain for the
   * given state.
   *
   * @param stateName designator for the state
   * @param stateFunction partial function describing response to input
   */
  final def when(stateName: S)(stateFunction: StateFunction): Unit =
    when(stateName, null: FiniteDuration)(stateFunction)

  /**
   * Insert a new StateFunction at the end of the processing chain for the
   * given state.
   *
   * @param stateName designator for the state
   * @param stateFunctionBuilder partial function builder describing response to input
   */
  final def when(stateName: S, stateFunctionBuilder: FSMStateFunctionBuilder[S, D, E]): Unit =
    when(stateName, null, stateFunctionBuilder)

  /**
   * Insert a new StateFunction at the end of the processing chain for the
   * given state. If the stateTimeout parameter is set, entering this state
   * without a differing explicit timeout setting will trigger a StateTimeout
   * event; the same is true when using #stay.
   *
   * @param stateName designator for the state
   * @param stateTimeout default state timeout for this state
   * @param stateFunctionBuilder partial function builder describing response to input
   */
  final def when(
      stateName: S,
      stateTimeout: FiniteDuration,
      stateFunctionBuilder: FSMStateFunctionBuilder[S, D, E]): Unit =
    when(stateName, stateTimeout)(stateFunctionBuilder.build())

  /**
   * Set initial state. Call this method from the constructor before the [[#initialize]] method.
   * If different state is needed after a restart this method, followed by [[#initialize]], can
   * be used in the actor life cycle hooks [[akka.actor.Actor#preStart]] and [[akka.actor.Actor#postRestart]].
   *
   * @param stateName initial state designator
   * @param stateData initial state data
   */
  final def startWith(stateName: S, stateData: D): Unit =
    startWith(stateName, stateData, null: FiniteDuration)

  /**
   * Set initial state. Call this method from the constructor before the [[#initialize]] method.
   * If different state is needed after a restart this method, followed by [[#initialize]], can
   * be used in the actor life cycle hooks [[akka.actor.Actor#preStart]] and [[akka.actor.Actor#postRestart]].
   *
   * @param stateName initial state designator
   * @param stateData initial state data
   * @param timeout state timeout for the initial state, overriding the default timeout for that state
   */
  final def startWith(stateName: S, stateData: D, timeout: FiniteDuration): Unit =
    startWith(stateName, stateData, Option(timeout))

  /**
   * Add a handler which is called upon each state transition, i.e. not when
   * staying in the same state.
   *
   * <b>Multiple handlers may be installed, and every one of them will be
   * called, not only the first one matching.</b>
   */
  final def onTransition(transitionHandlerBuilder: FSMTransitionHandlerBuilder[S]): Unit =
    onTransition(transitionHandlerBuilder.build().asInstanceOf[TransitionHandler])

  /**
   * Add a handler which is called upon each state transition, i.e. not when
   * staying in the same state.
   *
   * <b>Multiple handlers may be installed, and every one of them will be
   * called, not only the first one matching.</b>
   */
  final def onTransition(transitionHandler: UnitApply2[S, S]): Unit =
    super.onTransition(transitionHandler(_: S, _: S))

  /**
   * Set handler which is called upon reception of unhandled messages. Calling
   * this method again will overwrite the previous contents.
   *
   * The current state may be queried using ``stateName``.
   */
  final def whenUnhandled(stateFunctionBuilder: FSMStateFunctionBuilder[S, D, E]): Unit =
    whenUnhandled(stateFunctionBuilder.build())

  /**
   * Set handler which is called upon termination of this FSM actor. Calling
   * this method again will overwrite the previous contents.
   */
  final def onTermination(stopBuilder: FSMStopBuilder[S, D]): Unit =
    onTermination(stopBuilder.build().asInstanceOf[PartialFunction[StopEvent, Unit]])

  /**
   * Create an [[akka.japi.pf.FSMStateFunctionBuilder]] with the first case statement set.
   *
   * A case statement that matches on an event and data type and a predicate.
   *
   * @param eventType  the event type to match on
   * @param dataType  the data type to match on
   * @param predicate  a predicate to evaluate on the matched types
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchEvent[ET, DT <: D](
      eventType: Class[ET],
      dataType: Class[DT],
      predicate: TypedPredicate2[ET, DT],
      apply: Apply2[ET, DT, State]): FSMStateFunctionBuilder[S, D, E] =
    new FSMStateFunctionBuilder[S, D, E]().event(eventType, dataType, predicate, apply)

  /**
   * Create an [[akka.japi.pf.FSMStateFunctionBuilder]] with the first case statement set.
   *
   * A case statement that matches on an event and data type.
   *
   * @param eventType  the event type to match on
   * @param dataType  the data type to match on
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchEvent[ET, DT <: D](
      eventType: Class[ET],
      dataType: Class[DT],
      apply: Apply2[ET, DT, State]): FSMStateFunctionBuilder[S, D, E] =
    new FSMStateFunctionBuilder[S, D, E]().event(eventType, dataType, apply)

  /**
   * Create an [[akka.japi.pf.FSMStateFunctionBuilder]] with the first case statement set.
   *
   * A case statement that matches if the event type and predicate matches.
   *
   * @param eventType  the event type to match on
   * @param predicate  a predicate that will be evaluated on the data and the event
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchEvent[ET](
      eventType: Class[ET],
      predicate: TypedPredicate2[ET, D],
      apply: Apply2[ET, D, State]): FSMStateFunctionBuilder[S, D, E] =
    new FSMStateFunctionBuilder[S, D, E]().event(eventType, predicate, apply)

  /**
   * Create an [[akka.japi.pf.FSMStateFunctionBuilder]] with the first case statement set.
   *
   * A case statement that matches if the event type matches.
   *
   * @param eventType  the event type to match on
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchEvent[ET](eventType: Class[ET], apply: Apply2[ET, D, State]): FSMStateFunctionBuilder[S, D, E] =
    new FSMStateFunctionBuilder[S, D, E]().event(eventType, apply)

  /**
   * Create an [[akka.japi.pf.FSMStateFunctionBuilder]] with the first case statement set.
   *
   * A case statement that matches if the predicate matches.
   *
   * @param predicate  a predicate that will be evaluated on the data and the event
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchEvent(
      predicate: TypedPredicate2[AnyRef, D],
      apply: Apply2[AnyRef, D, State]): FSMStateFunctionBuilder[S, D, E] =
    new FSMStateFunctionBuilder[S, D, E]().event(predicate, apply)

  /**
   * Create an [[akka.japi.pf.FSMStateFunctionBuilder]] with the first case statement set.
   *
   * A case statement that matches on the data type and if any of the event types
   * in the list match or any of the event instances in the list compares equal.
   *
   * @param eventMatches  a list of types or instances to match against
   * @param dataType  the data type to match on
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchEvent[DT <: D](
      eventMatches: JList[AnyRef],
      dataType: Class[DT],
      apply: Apply2[AnyRef, DT, State]): FSMStateFunctionBuilder[S, D, E] =
    new FSMStateFunctionBuilder[S, D, E]().event(eventMatches, dataType, apply)

  /**
   * Create an [[akka.japi.pf.FSMStateFunctionBuilder]] with the first case statement set.
   *
   * A case statement that matches if any of the event types in the list match or any
   * of the event instances in the list compares equal.
   *
   * @param eventMatches  a list of types or instances to match against
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchEvent(eventMatches: JList[AnyRef], apply: Apply2[AnyRef, D, State]): FSMStateFunctionBuilder[S, D, E] =
    new FSMStateFunctionBuilder[S, D, E]().event(eventMatches, apply)

  /**
   * Create an [[akka.japi.pf.FSMStateFunctionBuilder]] with the first case statement set.
   *
   * A case statement that matches on the data type and if the event compares equal.
   *
   * @param event  an event to compare equal against
   * @param dataType  the data type to match on
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchEventEquals[Ev, DT <: D](
      event: Ev,
      dataType: Class[DT],
      apply: Apply2[Ev, DT, State]): FSMStateFunctionBuilder[S, D, E] =
    new FSMStateFunctionBuilder[S, D, E]().eventEquals(event, dataType, apply)

  /**
   * Create an [[akka.japi.pf.FSMStateFunctionBuilder]] with the first case statement set.
   *
   * A case statement that matches if the event compares equal.
   *
   * @param event  an event to compare equal against
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchEventEquals[Ev](event: Ev, apply: Apply2[Ev, D, State]): FSMStateFunctionBuilder[S, D, E] =
    new FSMStateFunctionBuilder[S, D, E]().eventEquals(event, apply)

  /**
   * Create an [[akka.japi.pf.FSMStateFunctionBuilder]] with the first case statement set.
   *
   * A case statement that matches on any type of event.
   *
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchAnyEvent(apply: Apply2[AnyRef, D, State]): FSMStateFunctionBuilder[S, D, E] =
    new FSMStateFunctionBuilder[S, D, E]().anyEvent(apply)

  /**
   * Create an [[akka.japi.pf.FSMTransitionHandlerBuilder]] with the first case statement set.
   *
   * A case statement that matches on a from state and a to state.
   *
   * @param fromState  the from state to match on
   * @param toState  the to state to match on
   * @param apply  an action to apply when the states match
   * @return the builder with the case statement added
   */
  final def matchState(fromState: S, toState: S, apply: UnitApplyVoid): FSMTransitionHandlerBuilder[S] =
    new FSMTransitionHandlerBuilder[S]().state(fromState, toState, apply)

  /**
   * Create an [[akka.japi.pf.FSMTransitionHandlerBuilder]] with the first case statement set.
   *
   * A case statement that matches on a from state and a to state.
   *
   * @param fromState  the from state to match on
   * @param toState  the to state to match on
   * @param apply  an action to apply when the states match
   * @return the builder with the case statement added
   */
  final def matchState(fromState: S, toState: S, apply: UnitApply2[S, S]): FSMTransitionHandlerBuilder[S] =
    new FSMTransitionHandlerBuilder[S]().state(fromState, toState, apply)

  /**
   * Create an [[akka.japi.pf.FSMStopBuilder]] with the first case statement set.
   *
   * A case statement that matches on an [[PersistentFSM.Reason]].
   *
   * @param reason  the reason for the termination
   * @param apply  an action to apply to the event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchStop(reason: Reason, apply: UnitApply2[S, D]): FSMStopBuilder[S, D] =
    new FSMStopBuilder[S, D]().stop(reason, apply)

  /**
   * Create an [[akka.japi.pf.FSMStopBuilder]] with the first case statement set.
   *
   * A case statement that matches on a reason type.
   *
   * @param reasonType  the reason type to match on
   * @param apply  an action to apply to the reason, event and state data if there is a match
   * @return the builder with the case statement added
   */
  final def matchStop[RT <: Reason](reasonType: Class[RT], apply: UnitApply3[RT, S, D]): FSMStopBuilder[S, D] =
    new FSMStopBuilder[S, D]().stop(reasonType, apply)

  /**
   * Create an [[akka.japi.pf.FSMStopBuilder]] with the first case statement set.
   *
   * A case statement that matches on a reason type and a predicate.
   *
   * @param reasonType  the reason type to match on
   * @param apply  an action to apply to the reason, event and state data if there is a match
   * @param predicate  a predicate that will be evaluated on the reason if the type matches
   * @return the builder with the case statement added
   */
  final def matchStop[RT <: Reason](
      reasonType: Class[RT],
      predicate: TypedPredicate[RT],
      apply: UnitApply3[RT, S, D]): FSMStopBuilder[S, D] =
    new FSMStopBuilder[S, D]().stop(reasonType, predicate, apply)

  /**
   * Create a [[akka.japi.pf.UnitPFBuilder]] with the first case statement set.
   *
   * @param dataType  a type to match the argument against
   * @param apply  an action to apply to the argument if the type matches
   * @return a builder with the case statement added
   */
  final def matchData[DT <: D](dataType: Class[DT], apply: UnitApply[DT]): UnitPFBuilder[D] =
    UnitMatch.`match`(dataType, apply)

  /**
   * Create a [[akka.japi.pf.UnitPFBuilder]] with the first case statement set.
   *
   * @param dataType  a type to match the argument against
   * @param predicate  a predicate that will be evaluated on the argument if the type matches
   * @param apply  an action to apply to the argument if the type and predicate matches
   * @return a builder with the case statement added
   */
  final def matchData[DT <: D](
      dataType: Class[DT],
      predicate: TypedPredicate[DT],
      apply: UnitApply[DT]): UnitPFBuilder[D] =
    UnitMatch.`match`(dataType, predicate, apply)

  /**
   * Produce transition to other state. Return this from a state function in
   * order to effect the transition.
   *
   * @param nextStateName state designator for the next state
   * @return state transition descriptor
   */
  final def goTo(nextStateName: S): State = goto(nextStateName)

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
  def startTimerWithFixedDelay(name: String, msg: Any, delay: java.time.Duration): Unit =
    startTimerWithFixedDelay(name, msg, delay.asScala)

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
  def startTimerAtFixedRate(name: String, msg: Any, interval: java.time.Duration): Unit =
    startTimerAtFixedRate(name, msg, interval.asScala)

  /**
   * Start a timer that will send `msg` once to the `self` actor after
   * the given `delay`.
   *
   * Each timer has a `name` and if a new timer with same `name` is started
   * the previous is cancelled. It is guaranteed that a message from the
   * previous timer is not received, even if it was already enqueued
   * in the mailbox when the new timer was started.
   */
  def startSingleTimer(name: String, msg: Any, delay: java.time.Duration): Unit =
    startSingleTimer(name, msg, delay.asScala)

  /**
   * Schedule named timer to deliver message after given delay, possibly repeating.
   * Any existing timer with the same name will automatically be canceled before
   * adding the new timer.
   * @param name identifier to be used with cancelTimer()
   * @param msg message to be delivered
   * @param timeout delay of first message delivery and between subsequent messages
   */
  @deprecated("Use startSingleTimer instead.", since = "2.6.0")
  final def setTimer(name: String, msg: Any, timeout: FiniteDuration): Unit =
    setTimer(name, msg, timeout, false)

  /**
   * Default reason if calling `stop()`.
   */
  val Normal: PersistentFSM.Reason = PersistentFSM.Normal

  /**
   * Reason given when someone was calling `system.stop(fsm)` from outside;
   * also applies to `Stop` supervision directive.
   */
  val Shutdown: PersistentFSM.Reason = PersistentFSM.Shutdown
}
