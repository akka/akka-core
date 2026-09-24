/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.stage

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.{ immutable, mutable }
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.FiniteDuration
import akka.{ Done, NotUsed }
import akka.actor._
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.japi.function.{ Effect, Procedure }
import akka.stream._
import akka.stream.Attributes.SourceLocation
import akka.stream.impl.{ ReactiveStreamsCompliance, TraversalBuilder }
import akka.stream.impl.ActorSubscriberMessage
import akka.stream.impl.fusing.{ GraphInterpreter, GraphStageModule, SubSink, SubSource }
import akka.stream.scaladsl.GenericGraphWithChangedAttributes
import akka.stream.stage.ConcurrentAsyncCallbackState.{ NoPendingEvents, State }
import akka.util.OptionVal

import java.util.concurrent.CompletionStage

import scala.jdk.FutureConverters.FutureOps

/**
 * Scala API: A GraphStage represents a reusable graph stream processing operator.
 *
 * Extend this `GraphStageWithMaterializedValue` if you want to provide a materialized value,
 * represented by the type parameter `M`. If your GraphStage does not need to provide a materialized
 * value you can instead extende [[GraphStage]] which materializes a [[NotUsed]] value.
 *
 * A GraphStage consists of a [[Shape]] which describes its input and output ports and a factory function that
 * creates a [[GraphStageLogic]] which implements the processing logic that ties the ports together.
 *
 * See also [[AbstractGraphStageWithMaterializedValue]] for Java DSL for this operator.
 */
abstract class GraphStageWithMaterializedValue[+S <: Shape, +M] extends Graph[S, M] {

  /**
   * Grants access to the materializer before preStart of the graph stage logic is invoked.
   *
   * INTERNAL API
   */
  @InternalApi
  private[akka] def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes,
      @nowarn("msg=never used") materializer: Materializer): (GraphStageLogic, M) =
    createLogicAndMaterializedValue(inheritedAttributes)

  @throws(classOf[Exception])
  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, M)

  protected def initialAttributes: Attributes = Attributes.none

  private var _traversalBuilder: TraversalBuilder = _

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] final override def traversalBuilder: TraversalBuilder = {
    // _traversalBuilder instance is cached to avoid allocations, no need for volatile or synchronization
    if (_traversalBuilder eq null) {
      val attr = initialAttributes
      _traversalBuilder = TraversalBuilder.atomic(GraphStageModule(shape, attr, this), attr)
    }
    _traversalBuilder
  }

  final override def withAttributes(attr: Attributes): Graph[S, M] =
    new GenericGraphWithChangedAttributes(shape, GraphStageWithMaterializedValue.this.traversalBuilder, attr)
}

/**
 * Java API: A GraphStage represents a reusable graph stream processing operator.
 *
 * Extend this `AbstractGraphStageWithMaterializedValue` if you want to provide a materialized value,
 * represented by the type parameter `M`. If your GraphStage does not need to provide a materialized
 * value you can instead extend [[GraphStage]] which materializes a [[NotUsed]] value.
 *
 * A GraphStage consists of a [[Shape]] which describes its input and output ports and a factory function that
 * creates a [[GraphStageLogic]] which implements the processing logic that ties the ports together.
 *
 * See also [[GraphStageWithMaterializedValue]] for Scala DSL for this operator.
 */
abstract class AbstractGraphStageWithMaterializedValue[+S <: Shape, M] extends GraphStageWithMaterializedValue[S, M] {
  @throws(classOf[Exception])
  final def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, M) = {
    val pair = createLogicAndMaterializedValuePair(inheritedAttributes)
    pair.first -> pair.second
  }

  @throws(classOf[Exception])
  def createLogicAndMaterializedValuePair(inheritedAttributes: Attributes): akka.japi.Pair[GraphStageLogic, M]
}

/**
 * A GraphStage represents a reusable graph stream processing operator.
 *
 * A GraphStage consists of a [[Shape]] which describes its input and output ports and a factory function that
 * creates a [[GraphStageLogic]] which implements the processing logic that ties the ports together.
 */
abstract class GraphStage[S <: Shape] extends GraphStageWithMaterializedValue[S, NotUsed] {
  final override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, NotUsed) =
    (createLogic(inheritedAttributes), NotUsed)

  @throws(classOf[Exception])
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic
}

private object TimerMessages {
  final case class Scheduled(timerKey: Any, timerId: Int, repeating: Boolean) extends DeadLetterSuppression
  final case class Timer(id: Int, task: Cancellable)
}

object GraphStageLogic {
  final case class StageActorRefNotInitializedException()
      extends RuntimeException("You must first call getStageActor, to initialize the Actors behavior")

  /**
   * Input handler that terminates the operator upon receiving completion.
   * The operator fails upon receiving a failure.
   */
  object EagerTerminateInput extends InHandler {
    override def onPush(): Unit = ()
    override def toString = "EagerTerminateInput"
  }

  /**
   * Input handler that does not terminate the operator upon receiving completion.
   * The operator fails upon receiving a failure.
   */
  object IgnoreTerminateInput extends InHandler {
    override def onPush(): Unit = ()
    override def onUpstreamFinish(): Unit = ()
    override def toString = "IgnoreTerminateInput"
  }

  /**
   * Input handler that terminates the state upon receiving completion if the
   * given condition holds at that time. The operator fails upon receiving a failure.
   */
  class ConditionalTerminateInput(predicate: () => Boolean) extends InHandler {
    override def onPush(): Unit = ()
    override def onUpstreamFinish(): Unit =
      if (predicate()) GraphInterpreter.currentInterpreter.activeStage.completeStage()
  }

  /**
   * Input handler that does not terminate the operator upon receiving completion
   * nor failure.
   */
  object TotallyIgnorantInput extends InHandler {
    override def onPush(): Unit = ()
    override def onUpstreamFinish(): Unit = ()
    override def onUpstreamFailure(ex: Throwable): Unit = ()
  }

  /**
   * Output handler that terminates the operator upon cancellation.
   */
  object EagerTerminateOutput extends OutHandler {
    override def onPull(): Unit = ()
    override def toString = "EagerTerminateOutput"
  }

  /**
   * Output handler that does not terminate the operator upon cancellation.
   */
  object IgnoreTerminateOutput extends OutHandler {
    override def onPull(): Unit = ()
    override def onDownstreamFinish(cause: Throwable): Unit = ()
    override def toString = "IgnoreTerminateOutput"
  }

  /**
   * Output handler that terminates the state upon receiving completion if the
   * given condition holds at that time. The operator fails upon receiving a failure.
   */
  class ConditionalTerminateOutput(predicate: () => Boolean) extends OutHandler {
    override def onPull(): Unit = ()
    override def onDownstreamFinish(cause: Throwable): Unit =
      if (predicate()) GraphInterpreter.currentInterpreter.activeStage.cancelStage(cause)
  }

  private object DoNothing extends (() => Unit) {
    def apply(): Unit = ()
  }

  /**
   * Minimal actor to work with other actors and watch them in a synchronous ways
   *
   * Not for user instantiation, use [[#getStageActor]].
   */
  final class StageActor @InternalApi() private[akka] (
      materializer: Materializer,
      getAsyncCallback: StageActorRef.Receive => AsyncCallback[(ActorRef, Any)],
      initialReceive: StageActorRef.Receive,
      name: String) {

    private val callback = getAsyncCallback(internalReceive)

    private def cell = materializer.supervisor match {
      case ref: LocalActorRef => ref.underlying
      case unknown =>
        throw new IllegalStateException(s"Stream supervisor must be a local actor, was [${unknown.getClass.getName}]")
    }
    private val functionRef: FunctionRef = {
      val f: (ActorRef, Any) => Unit = {
        case (_, m @ (PoisonPill | Kill)) =>
          materializer.logger.warning(
            "{} message sent to StageActor({}) will be ignored, since it is not a real Actor." +
            "Use a custom message type to communicate with it instead.",
            m,
            functionRef.path)
        case pair => callback.invoke(pair)
      }

      cell.addFunctionRef(f, name)
    }

    /**
     * The ActorRef by which this StageActor can be contacted from the outside.
     * This is a full-fledged ActorRef that supports watching and being watched
     * as well as location transparent (remote) communication.
     */
    def ref: ActorRef = functionRef

    @volatile
    private[this] var behavior = initialReceive

    /** INTERNAL API */
    private[akka] def internalReceive(pack: (ActorRef, Any)): Unit = {
      pack._2 match {
        case Terminated(ref) =>
          if (functionRef.isWatching(ref)) {
            functionRef.unwatch(ref)
            behavior(pack)
          }
        case _ => behavior(pack)
      }
    }

    /**
     * Special `become` allowing to swap the behavior of this StageActorRef.
     * Unbecome is not available.
     */
    def become(receive: StageActorRef.Receive): Unit = {
      behavior = receive
    }

    def stop(): Unit = {
      cell.removeFunctionRef(functionRef)
    }

    def watch(actorRef: ActorRef): Unit = functionRef.watch(actorRef)

    def unwatch(actorRef: ActorRef): Unit = functionRef.unwatch(actorRef)
  }
  object StageActorRef {
    type Receive = ((ActorRef, Any)) => Unit
  }

  /**
   * Internal API
   *
   * Marker value to pass to onAsyncInput if no promise was supplied.
   */
  @InternalApi
  private[stream] val NoPromise: Promise[Done] = Promise.successful(Done)
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object ConcurrentAsyncCallbackState {
  sealed trait State[+E]
  // waiting for materialization completion or during dispatching of initially queued events
  final case class Pending[E](pendingEvents: List[Event[E]]) extends State[E]
  // stream is initialized and so no threads can just send events without any synchronization overhead
  case object Initialized extends State[Nothing]
  // Event with feedback promise
  final case class Event[+E](e: E, handlingPromise: Promise[Done])

  val NoPendingEvents: Pending[Nothing] = Pending[Nothing](Nil)
}

/**
 * Represents the processing logic behind a [[GraphStage]]. Roughly speaking, a subclass of [[GraphStageLogic]] is a
 * collection of the following parts:
 *  * A set of [[InHandler]] and [[OutHandler]] instances and their assignments to the [[Inlet]]s and [[Outlet]]s
 *    of the enclosing [[GraphStage]]
 *  * Possible mutable state, accessible from the [[InHandler]] and [[OutHandler]] callbacks, but not from anywhere
 *    else (as such access would not be thread-safe)
 *  * The lifecycle hooks [[preStart]] and [[postStop]]
 *  * Methods for performing stream processing actions, like pulling or pushing elements
 *
 * The operator logic is completed once all its input and output ports have been closed. This can be changed by
 * setting `setKeepGoing` to true.
 *
 * The `postStop` lifecycle hook on the logic itself is called once all ports are closed. This is the only tear down
 * callback that is guaranteed to happen, if the actor system or the materializer is terminated the handlers may never
 * see any callbacks to `onUpstreamFailure`, `onUpstreamFinish` or `onDownstreamFinish`. Therefore operator resource
 * cleanup should always be done in `postStop`.
 */
abstract class GraphStageLogic private[stream] (val inCount: Int, val outCount: Int) {

  import GraphInterpreter._
  import GraphStageLogic._

  def this(shape: Shape) = this(shape.inlets.size, shape.outlets.size)

  /**
   * INTERNAL API
   */
  private[stream] var stageId: Int = Int.MinValue

  /**
   * INTERNAL API
   */
  private[stream] var attributes: Attributes = Attributes.none

  /**
   * INTERNAL API
   *
   * If possible a link back to the operator that the logic was created with, used for debugging.
   */
  private[stream] var originalStage: OptionVal[GraphStageWithMaterializedValue[_ <: Shape, _]] = OptionVal.None

  /**
   * INTERNAL API
   *
   * Input handlers followed by output handlers, use `inHandler(id)` and `outHandler(id)` to access the respective
   * handlers.
   */
  private[stream] val handlers = new Array[Any](inCount + outCount)

  /**
   * INTERNAL API
   */
  private[stream] def inHandler(id: Int): InHandler = {
    if (id > inCount) throw new IllegalArgumentException(s"$id not in inHandler range $inCount in $this")
    if (inCount < 1)
      throw new IllegalArgumentException(s"Tried to access inHandler $id but there are no in ports in $this")
    handlers(id).asInstanceOf[InHandler]
  }

  private[stream] def outHandler(id: Int): OutHandler = {
    if (id > outCount) throw new IllegalArgumentException(s"$id not in outHandler range $outCount in $this")
    if (outCount < 1)
      throw new IllegalArgumentException(s"Tried to access outHandler $id but there are no out ports $this")
    handlers(inCount + id).asInstanceOf[OutHandler]
  }

  /**
   * INTERNAL API
   */
  // Using common array to reduce overhead for small port counts
  private[stream] val portToConn = new Array[Connection](handlers.length)

  /**
   * INTERNAL API
   */
  private[this] var _interpreter: GraphInterpreter = _

  /**
   * INTERNAL API
   */
  private[stream] def interpreter_=(gi: GraphInterpreter): Unit = _interpreter = gi

  /**
   * INTERNAL API
   */
  private[akka] def interpreter: GraphInterpreter =
    if (_interpreter == null)
      throw new IllegalStateException(
        "not yet initialized: only setHandler is allowed in GraphStageLogic constructor. To access materializer use Source/Flow/Sink.fromMaterializer factory")
    else _interpreter

  /**
   * The [[akka.stream.Materializer]] that has set this GraphStage in motion.
   *
   * Can not be used from a `GraphStage` constructor. Access to materializer is provided by the
   * [[akka.stream.scaladsl.Source.fromMaterializer]], [[akka.stream.scaladsl.Flow.fromMaterializer]] and [[akka.stream.scaladsl.Sink.fromMaterializer]]
   * and their corresponding Java API factories.
   */
  protected def materializer: Materializer = interpreter.materializer

  /**
   * An [[akka.stream.Materializer]] that may run fusable parts of the graphs
   * that it materializes within the same actor as the current GraphStage (if
   * fusing is available). This materializer must not be shared outside of the
   * GraphStage.
   */
  protected def subFusingMaterializer: Materializer = interpreter.subFusingMaterializer

  /**
   * Input handler that terminates the operator upon receiving completion.
   * The operator fails upon receiving a failure.
   */
  final protected def eagerTerminateInput: InHandler = EagerTerminateInput

  /**
   * Input handler that does not terminate the operator upon receiving completion.
   * The operator fails upon receiving a failure.
   */
  final protected def ignoreTerminateInput: InHandler = IgnoreTerminateInput

  /**
   * Input handler that terminates the state upon receiving completion if the
   * given condition holds at that time. The operator fails upon receiving a failure.
   */
  final protected def conditionalTerminateInput(predicate: () => Boolean): InHandler =
    new ConditionalTerminateInput(predicate)

  /**
   * Input handler that does not terminate the operator upon receiving completion
   * nor failure.
   */
  final protected def totallyIgnorantInput: InHandler = TotallyIgnorantInput

  /**
   * Output handler that terminates the operator upon cancellation.
   */
  final protected def eagerTerminateOutput: OutHandler = EagerTerminateOutput

  /**
   * Output handler that does not terminate the operator upon cancellation.
   */
  final protected def ignoreTerminateOutput: OutHandler = IgnoreTerminateOutput

  /**
   * Output handler that terminates the state upon receiving completion if the
   * given condition holds at that time. The operator fails upon receiving a failure.
   */
  final protected def conditionalTerminateOutput(predicate: () => Boolean): OutHandler =
    new ConditionalTerminateOutput(predicate)

  /**
   * Assigns callbacks for the events for an [[Inlet]]
   */
  final protected def setHandler(in: Inlet[_], handler: InHandler): Unit = {
    handlers(in.id) = handler
    if (_interpreter != null) _interpreter.setHandler(conn(in), handler)
  }

  /**
   * Assign callbacks for linear operator for both [[Inlet]] and [[Outlet]]
   */
  final protected def setHandlers(in: Inlet[_], out: Outlet[_], handler: InHandler with OutHandler): Unit = {
    setHandler(in, handler)
    setHandler(out, handler)
  }

  /**
   * Retrieves the current callback for the events on the given [[Inlet]]
   */
  final protected def getHandler(in: Inlet[_]): InHandler = {
    handlers(in.id).asInstanceOf[InHandler]
  }

  /**
   * Assigns callbacks for the events for an [[Outlet]]
   */
  final protected def setHandler(out: Outlet[_], handler: OutHandler): Unit = {
    handlers(out.id + inCount) = handler
    if (_interpreter != null) _interpreter.setHandler(conn(out), handler)
  }

  private def conn(in: Inlet[_]): Connection = portToConn(in.id)
  private def conn(out: Outlet[_]): Connection = portToConn(out.id + inCount)

  /**
   * Retrieves the current callback for the events on the given [[Outlet]]
   */
  final protected def getHandler(out: Outlet[_]): OutHandler = {
    handlers(out.id + inCount).asInstanceOf[OutHandler]
  }

  private def getNonEmittingHandler(out: Outlet[_]): OutHandler =
    getHandler(out) match {
      case e: Emitting[_] => e.previous
      case other          => other
    }

  /**
   * Requests an element on the given port. Calling this method twice before an element arrived will fail.
   * There can only be one outstanding request at any given time. The method [[hasBeenPulled]] can be used
   * query whether pull is allowed to be called or not. This method will also fail if the port is already closed.
   */
  final protected def pull[T](in: Inlet[T]): Unit = {
    val connection = conn(in)
    val it = interpreter
    val portState = connection.portState

    if ((portState & (InReady | InClosed | OutClosed)) == InReady) {
      connection.portState = portState ^ PullStartFlip
      it.chasePull(connection)
    } else {
      // Detailed error information should not add overhead to the hot path
      if (isClosed(in)) throw new IllegalArgumentException(s"Cannot pull closed port ($in)")
      if (hasBeenPulled(in)) throw new IllegalArgumentException(s"Cannot pull port ($in) twice")

      // There were no errors, the pull was simply ignored as the target stage already closed its port. We
      // still need to track proper state though.
      connection.portState = portState ^ PullStartFlip
    }
  }

  /**
   * Requests an element on the given port unless the port is already closed.
   * Calling this method twice before an element arrived will fail.
   * There can only be one outstanding request at any given time. The method [[hasBeenPulled]] can be used
   * query whether pull is allowed to be called or not.
   */
  final protected def tryPull[T](in: Inlet[T]): Unit = if (!isClosed(in)) pull(in)

  /**
   * Requests to stop receiving events from a given input port. Cancelling clears any ungrabbed elements from the port.
   *
   * If cancellation is due to an error, use `cancel(in, cause)` instead to propagate that cause upstream. This overload
   * is a shortcut for `cancel(in, SubscriptionWithCancelException.NoMoreElementsNeeded)`
   */
  final protected def cancel[T](in: Inlet[T]): Unit = cancel(in, SubscriptionWithCancelException.NoMoreElementsNeeded)

  /**
   * Requests to stop receiving events from a given input port. Cancelling clears any ungrabbed elements from the port.
   */
  final protected def cancel[T](in: Inlet[T], cause: Throwable): Unit = cancel(conn(in), cause)

  private def cancel[T](connection: Connection, cause: Throwable): Unit =
    attributes.mandatoryAttribute[Attributes.CancellationStrategy].strategy match {
      case Attributes.CancellationStrategy.AfterDelay(delay, _) =>
        // since the port is not actually cancelled, we install a handler to ignore upcoming
        // ignore pushs now, since the stage wanted it cancelled already
        // do not ignore termination signals
        connection.inHandler = EagerTerminateInput
        val callback = getAsyncCallback[(Connection, Throwable)] {
          case (connection, cause) => doCancel(connection, cause)
        }
        materializer.scheduleOnce(delay, () => callback.invoke((connection, cause)))
      case _ =>
        doCancel(connection, cause)
    }

  private def doCancel[T](connection: Connection, cause: Throwable): Unit = interpreter.cancel(connection, cause)

  /**
   * Once the callback [[InHandler.onPush]] for an input port has been invoked, the element that has been pushed
   * can be retrieved via this method. After [[grab]] has been called the port is considered to be empty, and further
   * calls to [[grab]] will fail until the port is pulled again and a new element is pushed as a response.
   *
   * The method [[isAvailable]] can be used to query if the port has an element that can be grabbed or not.
   */
  final protected def grab[T](in: Inlet[T]): T = {
    val connection = conn(in)
    val elem = connection.slot

    // Fast path for active connections
    if ((connection.portState & (InReady | InFailed | InClosed)) == InReady && (elem.asInstanceOf[AnyRef] ne Empty)) {
      connection.slot = Empty
      elem.asInstanceOf[T]
    } else {
      // Slow path for grabbing element from already failed or completed connections
      if (!isAvailable(in))
        throw new IllegalArgumentException(s"Cannot get element from already empty input port ($in)")

      if ((connection.portState & (InReady | InFailed)) == (InReady | InFailed)) {
        // failed
        val failed = connection.slot.asInstanceOf[Failed]
        val elem = failed.previousElem.asInstanceOf[T]
        connection.slot = Failed(failed.ex, Empty)
        elem
      } else {
        // completed
        val elem = connection.slot.asInstanceOf[T]
        connection.slot = Empty
        elem
      }
    }
  }

  /**
   * Indicates whether there is already a pending pull for the given input port. If this method returns true
   * then [[isAvailable]] must return false for that same port.
   */
  final protected def hasBeenPulled[T](in: Inlet[T]): Boolean = (conn(in).portState & (InReady | InClosed)) == 0

  /**
   * Indicates whether there is an element waiting at the given input port. [[grab]] can be used to retrieve the
   * element. After calling [[grab]] this method will return false.
   *
   * If this method returns true then [[hasBeenPulled]] will return false for that same port.
   */
  final protected def isAvailable[T](in: Inlet[T]): Boolean = {
    val connection = conn(in)

    val normalArrived = (conn(in).portState & (InReady | InFailed | InClosed)) == InReady

    // Fast path for active connection
    if (normalArrived) connection.slot.asInstanceOf[AnyRef] ne Empty
    else {
      // slow path on failure, closure, and cancellation
      if ((connection.portState & (InReady | InClosed | InFailed)) == (InReady | InClosed))
        connection.slot match {
          case Empty | _ @(_: Cancelled) => false // cancelled (element is discarded when cancelled)
          case _                         => true // completed but element still there to grab
        } else if ((connection.portState & (InReady | InFailed)) == (InReady | InFailed))
        connection.slot match {
          case Failed(_, elem) => elem.asInstanceOf[AnyRef] ne Empty // failed but element still there to grab
          case _               => false
        } else false
    }
  }

  /**
   * Indicates whether the port has been closed. A closed port cannot be pulled.
   */
  final protected def isClosed[T](in: Inlet[T]): Boolean = (conn(in).portState & InClosed) != 0

  /**
   * Emits an element through the given output port. Calling this method twice before a [[pull]] has been arrived
   * will fail. There can be only one outstanding push request at any given time. The method [[isAvailable]] can be
   * used to check if the port is ready to be pushed or not.
   */
  final protected def push[T](out: Outlet[T], elem: T): Unit = {
    val connection = conn(out)
    val it = interpreter
    val portState = connection.portState

    connection.portState = portState ^ PushStartFlip

    if ((portState & (OutReady | OutClosed | InClosed)) == OutReady && (elem != null)) {
      connection.slot = elem
      it.chasePush(connection)
    } else {
      // Restore state for the error case
      connection.portState = portState

      // Detailed error information should not add overhead to the hot path
      ReactiveStreamsCompliance.requireNonNullElement(elem)
      if (isClosed(out)) throw new IllegalArgumentException(s"Cannot push closed port ($out)")
      if (!isAvailable(out))
        throw new IllegalArgumentException(s"Cannot push port ($out) twice, or before it being pulled")

      // No error, just InClosed caused the actual pull to be ignored, but the status flag still needs to be flipped
      connection.portState = portState ^ PushStartFlip
    }
  }

  /**
   * Controls whether this operator shall shut down when all its ports are closed, which
   * is the default. In order to have it keep going past that point this method needs
   * to be called with a `true` argument before all ports are closed, and afterwards
   * it will not be closed until this method is called with a `false` argument or the
   * operator is terminated via `completeStage()` or `failStage()`.
   */
  final protected def setKeepGoing(enabled: Boolean): Unit =
    interpreter.setKeepGoing(this, enabled)

  /**
   * Signals that there will be no more elements emitted on the given port.
   */
  final protected def complete[T](out: Outlet[T]): Unit =
    getHandler(out) match {
      case e: Emitting[T @unchecked] => e.addFollowUp(new EmittingCompletion[T](e.out, e.previous))
      case _                         => interpreter.complete(conn(out))
    }

  /**
   * Signals failure through the given port.
   */
  final protected def fail[T](out: Outlet[T], ex: Throwable): Unit = interpreter.fail(conn(out), ex)

  /**
   * Automatically invokes [[cancel]] or [[complete]] on all the input or output ports that have been called,
   * then marks the operator as stopped.
   */
  final def completeStage(): Unit =
    internalCompleteStage(SubscriptionWithCancelException.StageWasCompleted, OptionVal.None)

  // Variable used from `OutHandler.onDownstreamFinish` to carry over cancellation cause in cases where
  // `OutHandler` implementations call `super.onDownstreamFinished()`.
  /**
   * INTERNAL API
   */
  @InternalApi private[stream] var lastCancellationCause: Throwable = _

  /**
   * Automatically invokes [[cancel]] or [[complete]] on all the input or output ports that have been called,
   * then marks the stage as stopped.
   */
  final def cancelStage(cause: Throwable): Unit =
    internalCancelStage(cause, attributes.mandatoryAttribute[Attributes.CancellationStrategy].strategy)

  @tailrec
  private def internalCancelStage(cause: Throwable, strategy: Attributes.CancellationStrategy.Strategy): Unit = {
    import Attributes.CancellationStrategy._
    import SubscriptionWithCancelException._
    strategy match {
      case CompleteStage => internalCompleteStage(cause, OptionVal.None)
      case FailStage     => internalCompleteStage(cause, OptionVal.Some(cause))
      case PropagateFailure =>
        cause match {
          case NoMoreElementsNeeded | StageWasCompleted => internalCompleteStage(cause, OptionVal.None)
          case _                                        => internalCompleteStage(cause, OptionVal.Some(cause))
        }
      case AfterDelay(_, andThen) =>
        // delay handled at the stage that sends the delay. See `def cancel(in, cause)`.
        internalCancelStage(cause, andThen)
    }
  }

  /**
   * Automatically invokes [[cancel]] or [[fail]] on all the input or output ports that have been called,
   * then marks the operator as stopped.
   */
  final def failStage(ex: Throwable): Unit = internalCompleteStage(ex, OptionVal.Some(ex))

  /**
   * Cancels all incoming ports with the given cause. Then, depending on whether `optionalFailureCause` is
   * defined, completes or fails the outgoing ports.
   */
  private def internalCompleteStage(cancelCause: Throwable, optionalFailureCause: OptionVal[Throwable]): Unit = {
    var i = 0
    while (i < portToConn.length) {
      if (i < inCount)
        cancel(portToConn(i), cancelCause) // call through GraphStage.cancel to apply delay if applicable
      else if (optionalFailureCause.isDefined)
        interpreter.fail(portToConn(i), optionalFailureCause.get)
      else
        handlers(i) match {
          case e: Emitting[Any @unchecked] => e.addFollowUp(new EmittingCompletion[Any](e.out, e.previous))
          case _                           => interpreter.complete(portToConn(i))
        }
      i += 1
    }
    cleanUpSubstreams(optionalFailureCause)
    setKeepGoing(false)
  }

  private def cleanUpSubstreams(optionalFailureCause: OptionVal[Throwable]): Unit = {
    _subInletsAndOutlets.foreach {
      case inlet: SubSinkInlet[_] =>
        val subSink = inlet.sink.asInstanceOf[SubSink[_]]
        optionalFailureCause match {
          case OptionVal.Some(cause) => subSink.cancelSubstream(cause)
          case _                     => subSink.cancelSubstream()
        }
      case outlet: SubSourceOutlet[_] =>
        val subSource = outlet.source.asInstanceOf[SubSource[_]]
        optionalFailureCause match {
          case OptionVal.Some(cause) => subSource.failSubstream(cause)
          case _                     => subSource.completeSubstream()
        }
      case wat =>
        throw new IllegalStateException(
          s"Stage _subInletsAndOutlets contained unexpected element of type ${wat.getClass.toString}")
    }
    _subInletsAndOutlets = Set.empty
  }

  /**
   * Return true if the given output port is ready to be pushed.
   */
  final def isAvailable[T](out: Outlet[T]): Boolean =
    (conn(out).portState & (OutReady | OutClosed)) == OutReady

  /**
   * Indicates whether the port has been closed. A closed port cannot be pushed.
   */
  final protected def isClosed[T](out: Outlet[T]): Boolean = (conn(out).portState & OutClosed) != 0

  /**
   * Read a number of elements from the given inlet and continue with the given function,
   * suspending execution if necessary. This action replaces the [[InHandler]]
   * for the given inlet if suspension is needed and reinstalls the current
   * handler upon receiving the last `onPush()` signal.
   *
   * If upstream closes before N elements have been read,
   * the `onClose` function is invoked with the elements which were read.
   */
  final protected def readN[T](in: Inlet[T], n: Int)(andThen: Seq[T] => Unit, onClose: Seq[T] => Unit): Unit =
    //FIXME `onClose` is a poor name for `onComplete` rename this at the earliest possible opportunity
    if (n < 0) throw new IllegalArgumentException("cannot read negative number of elements")
    else if (n == 0) andThen(Nil)
    else {
      val result = new Array[AnyRef](n).asInstanceOf[Array[T]]
      var pos = 0

      if (isAvailable(in)) { //If we already have data available, then shortcircuit and read the first
        result(pos) = grab(in)
        pos += 1
      }

      if (n != pos) { // If we aren't already done
        requireNotReading(in)
        if (!hasBeenPulled(in)) pull(in)
        setHandler(in, new Reading(in, n - pos, getHandler(in))((elem: T) => {
          result(pos) = elem
          pos += 1
          if (pos == n) andThen(result.toSeq)
        }, () => onClose(result.take(pos).toSeq)))
      } else andThen(result.toSeq)
    }

  /**
   * Java API: Read a number of elements from the given inlet and continue with the given function,
   * suspending execution if necessary. This action replaces the [[InHandler]]
   * for the given inlet if suspension is needed and reinstalls the current
   * handler upon receiving the last `onPush()` signal (before invoking the `andThen` function).
   */
  final protected def readN[T](
      in: Inlet[T],
      n: Int,
      andThen: Procedure[java.util.List[T]],
      onClose: Procedure[java.util.List[T]]): Unit = {
    //FIXME `onClose` is a poor name for `onComplete` rename this at the earliest possible opportunity
    import scala.jdk.CollectionConverters._
    readN(in, n)(seq => andThen(seq.asJava), seq => onClose(seq.asJava))
  }

  /**
   * Read an element from the given inlet and continue with the given function,
   * suspending execution if necessary. This action replaces the [[InHandler]]
   * for the given inlet if suspension is needed and reinstalls the current
   * handler upon receiving the `onPush()` signal (before invoking the `andThen` function).
   */
  final protected def read[T](in: Inlet[T])(andThen: T => Unit, onClose: () => Unit): Unit = {
    if (isAvailable(in)) {
      val elem = grab(in)
      andThen(elem)
    } else if (isClosed(in)) {
      onClose()
    } else {
      requireNotReading(in)
      if (!hasBeenPulled(in)) pull(in)
      setHandler(in, new Reading(in, 1, getHandler(in))(andThen, onClose))
    }
  }

  /**
   * Java API: Read an element from the given inlet and continue with the given function,
   * suspending execution if necessary. This action replaces the [[InHandler]]
   * for the given inlet if suspension is needed and reinstalls the current
   * handler upon receiving the `onPush()` signal (before invoking the `andThen` function).
   */
  final protected def read[T](in: Inlet[T], andThen: Procedure[T], onClose: Effect): Unit = {
    read(in)(andThen.apply, onClose.apply _)
  }

  /**
   * Abort outstanding (suspended) reading for the given inlet, if there is any.
   * This will reinstall the replaced handler that was in effect before the `read`
   * call.
   */
  final protected def abortReading(in: Inlet[_]): Unit =
    getHandler(in) match {
      case r: Reading[_] =>
        setHandler(in, r.previous)
      case _ =>
    }

  private def requireNotReading(in: Inlet[_]): Unit =
    if (getHandler(in).isInstanceOf[Reading[_]])
      throw new IllegalStateException("already reading on inlet " + in)

  /**
   * Caution: for n == 1 andThen is called after resetting the handler, for
   * other values it is called without resetting the handler. n MUST be positive.
   */
  // can't be final because of SI-4440
  private class Reading[T](in: Inlet[T], private var n: Int, val previous: InHandler)(
      andThen: T => Unit,
      onComplete: () => Unit)
      extends InHandler {
    require(n > 0, "number of elements to read must be positive!")

    override def onPush(): Unit = {
      val elem = grab(in)
      n -= 1

      if (n > 0) pull(in)
      else setHandler(in, previous)

      andThen(elem)
    }

    override def onUpstreamFinish(): Unit = {
      setHandler(in, previous)
      onComplete()
      previous.onUpstreamFinish()
    }

    override def onUpstreamFailure(ex: Throwable): Unit = {
      setHandler(in, previous)
      previous.onUpstreamFailure(ex)
    }
  }

  /**
   * Emit a sequence of elements through the given outlet and continue with the given thunk
   * afterwards, suspending execution if necessary.
   * This action replaces the [[OutHandler]] for the given outlet if suspension
   * is needed and reinstalls the current handler upon receiving an `onPull()`
   * signal (before invoking the `andThen` function).
   */
  final protected def emitMultiple[T](out: Outlet[T], elems: immutable.Iterable[T], andThen: () => Unit): Unit =
    emitMultiple(out, elems.iterator, andThen)

  /**
   * Emit a sequence of elements through the given outlet, suspending execution if necessary.
   * This action replaces the [[OutHandler]] for the given outlet if suspension
   * is needed and reinstalls the current handler upon receiving an `onPull()`
   * signal.
   */
  final protected def emitMultiple[T](out: Outlet[T], elems: immutable.Iterable[T]): Unit =
    emitMultiple(out, elems, DoNothing)

  /**
   * Java API
   *
   * Emit a sequence of elements through the given outlet, suspending execution if necessary.
   * This action replaces the [[AbstractOutHandler]] for the given outlet if suspension
   * is needed and reinstalls the current handler upon receiving an `onPull()`
   * signal.
   */
  final protected def emitMultiple[T](out: Outlet[T], elems: java.util.Iterator[T]): Unit = {
    import scala.jdk.CollectionConverters._
    emitMultiple(out, elems.asScala, DoNothing)
  }

  /**
   * Java API
   *
   * Emit a sequence of elements through the given outlet, suspending execution if necessary.
   * This action replaces the [[AbstractOutHandler]] for the given outlet if suspension
   * is needed and reinstalls the current handler upon receiving an `onPull()`
   * signal.
   */
  final protected def emitMultiple[T](out: Outlet[T], elems: java.util.Iterator[T], andThen: Effect): Unit = {
    import scala.jdk.CollectionConverters._
    emitMultiple(out, elems.asScala, andThen.apply _)
  }

  /**
   * Emit a sequence of elements through the given outlet and continue with the given thunk
   * afterwards, suspending execution if necessary.
   * This action replaces the [[OutHandler]] for the given outlet if suspension
   * is needed and reinstalls the current handler upon receiving an `onPull()`
   * signal (before invoking the `andThen` function).
   */
  final protected def emitMultiple[T](out: Outlet[T], elems: Iterator[T], andThen: () => Unit): Unit =
    if (elems.hasNext) {
      if (isAvailable(out)) {
        push(out, elems.next())
        if (elems.hasNext)
          setOrAddEmitting(out, new EmittingIterator[T](out, elems, getNonEmittingHandler(out), andThen))
        else andThen()
      } else {
        setOrAddEmitting(out, new EmittingIterator[T](out, elems, getNonEmittingHandler(out), andThen))
      }
    } else andThen()

  /**
   * Emit a sequence of elements through the given outlet, suspending execution if necessary.
   * This action replaces the [[OutHandler]] for the given outlet if suspension
   * is needed and reinstalls the current handler upon receiving an `onPull()`
   * signal.
   */
  final protected def emitMultiple[T](out: Outlet[T], elems: Iterator[T]): Unit = emitMultiple(out, elems, DoNothing)

  /**
   * Emit an element through the given outlet and continue with the given thunk
   * afterwards, suspending execution if necessary.
   * This action replaces the [[OutHandler]] for the given outlet if suspension
   * is needed and reinstalls the current handler upon receiving an `onPull()`
   * signal (before invoking the `andThen` function).
   */
  final protected def emit[T](out: Outlet[T], elem: T, andThen: () => Unit): Unit =
    if (isAvailable(out)) {
      push(out, elem)
      andThen()
    } else {
      setOrAddEmitting(out, new EmittingSingle[T](out, elem, getNonEmittingHandler(out), andThen))
    }

  /**
   * Emit an element through the given outlet, suspending execution if necessary.
   * This action replaces the [[OutHandler]] for the given outlet if suspension
   * is needed and reinstalls the current handler upon receiving an `onPull()`
   * signal.
   */
  final protected def emit[T](out: Outlet[T], elem: T): Unit = emit(out, elem, DoNothing)

  final protected def emit[T](out: Outlet[T], elem: T, andThen: Effect): Unit = {
    emit(out, elem, andThen.apply _)
  }

  /**
   * Abort outstanding (suspended) emissions for the given outlet, if there are any.
   * This will reinstall the replaced handler that was in effect before the `emit`
   * call.
   */
  final protected def abortEmitting(out: Outlet[_]): Unit =
    getHandler(out) match {
      case e: Emitting[_] => setHandler(out, e.previous)
      case _              =>
    }

  private def setOrAddEmitting[T](out: Outlet[T], next: Emitting[T]): Unit =
    getHandler(out) match {
      case e: Emitting[_] => e.asInstanceOf[Emitting[T]].addFollowUp(next)
      case _              => setHandler(out, next)
    }

  private abstract class Emitting[T](val out: Outlet[T], val previous: OutHandler, andThen: () => Unit)
      extends OutHandler {
    private var followUps: Emitting[T] = _
    private var followUpsTail: Emitting[T] = _

    protected def followUp(): Unit = {
      setHandler(out, previous)
      andThen()
      if (followUps != null) {

        /*
         * If (while executing andThen() callback) handler was changed to new emitting,
         * we should add it to the end of emission queue
         */
        val currentHandler = getHandler(out)
        if (currentHandler.isInstanceOf[Emitting[_]])
          addFollowUp(currentHandler.asInstanceOf[Emitting[T]])

        val next = dequeue()
        if (next.isInstanceOf[EmittingCompletion[_]]) {

          /*
           * If next element is emitting completion and there are some elements after it,
           * we to need pass them before completion
           */
          if (next.followUps != null) {
            setHandler(out, dequeueHeadAndAddToTail(next))
          } else {
            complete(out)
          }
        } else {
          setHandler(out, next)
        }
      }
    }

    def addFollowUp(e: Emitting[T]): Unit =
      if (followUps == null) {
        followUps = e
        followUpsTail = e
      } else {
        followUpsTail.followUps = e
        followUpsTail = e
      }

    private def dequeueHeadAndAddToTail(head: Emitting[T]): Emitting[T] = {
      val next = head.dequeue()
      next.addFollowUp(head)
      head.followUps = null
      head.followUpsTail = null
      next
    }

    /**
     * Dequeue `this` from the head of the queue, meaning that this object will
     * not be retained (setHandler will install the followUp). For this reason
     * the followUpsTail knowledge needs to be passed on to the next runner.
     */
    private def dequeue(): Emitting[T] = {
      val ret = followUps
      ret.followUpsTail = followUpsTail
      ret
    }

    override def onDownstreamFinish(cause: Throwable): Unit = previous.onDownstreamFinish(cause)
  }

  private class EmittingSingle[T](_out: Outlet[T], elem: T, _previous: OutHandler, _andThen: () => Unit)
      extends Emitting[T](_out, _previous, _andThen) {

    override def onPull(): Unit = {
      push(out, elem)
      followUp()
    }
  }

  private class EmittingIterator[T](_out: Outlet[T], elems: Iterator[T], _previous: OutHandler, _andThen: () => Unit)
      extends Emitting[T](_out, _previous, _andThen) {

    override def onPull(): Unit = {
      push(out, elems.next())
      if (!elems.hasNext) {
        followUp()
      }
    }
  }

  private class EmittingCompletion[T](_out: Outlet[T], _previous: OutHandler)
      extends Emitting[T](_out, _previous, DoNothing) {
    override def onPull(): Unit = complete(out)
  }

  /**
   * Install a handler on the given inlet that emits received elements on the
   * given outlet before pulling for more data. `doFinish` and `doFail` control whether
   * completion or failure of the given inlet shall lead to operator termination or not.
   * `doPull` instructs to perform one initial pull on the `from` port.
   */
  final protected def passAlong[Out, In <: Out](
      from: Inlet[In],
      to: Outlet[Out],
      doFinish: Boolean = true,
      doFail: Boolean = true,
      doPull: Boolean = false): Unit = {
    class PassAlongHandler extends InHandler with (() => Unit) {
      override def apply(): Unit = tryPull(from)

      override def onPush(): Unit = {
        val elem = grab(from)
        emit(to, elem, this)
      }

      override def onUpstreamFinish(): Unit = if (doFinish) completeStage()

      override def onUpstreamFailure(ex: Throwable): Unit = if (doFail) failStage(ex)
    }
    val ph = new PassAlongHandler
    if (_interpreter != null) {
      if (isAvailable(from)) emit(to, grab(from), ph)
      if (doFinish && isClosed(from)) completeStage()
    }
    setHandler(from, ph)
    if (doPull) tryPull(from)
  }

  /**
   * Obtain a callback object that can be used asynchronously to re-enter the
   * current [[GraphStage]] with an asynchronous notification. The [[invoke]] method of the returned
   * [[AsyncCallback]] is safe to be called from other threads. It will in the background thread-safely
   * delegate to the passed callback function. I.e. [[invoke]] will be called by other thread and
   * the passed handler will be invoked eventually in a thread-safe way by the execution environment.
   *
   * In case stream is not yet materialized [[AsyncCallback]] will buffer events until stream is available.
   *
   * [[AsyncCallback.invokeWithFeedback]] has an internal promise that will be failed if event cannot be processed
   * due to stream completion.
   *
   * To be thread safe this method must only be called from either the constructor of the graph operator during
   * materialization or one of the methods invoked by the graph operator machinery, such as `onPush` and `onPull`.
   *
   * This object can be cached and reused within the same [[GraphStageLogic]].
   */
  final def getAsyncCallback[T](handler: T => Unit): AsyncCallback[T] = {
    val callback = new ConcurrentAsyncCallback[T](handler)
    if (_interpreter != null) callback.onStart()
    else callbacksWaitingForInterpreter = callback :: callbacksWaitingForInterpreter
    callback
  }

  /**
   * ConcurrentAsyncCallback allows to call [[invoke]] and [[invokeWithPromise]] with event attribute.
   * This event will be sent to the stream and the corresponding handler will be called with this attribute in thread-safe manner.
   *
   * State of this object can be changed both "internally" by the owning GraphStage or by the "external world" (e.g. other threads).
   * Specifically, calls to this class can be made:
   * * From the owning [[GraphStage]], to [[onStart]] - when materialization is finished and to [[onStop]] -
   * because the operator is about to stop or fail.
   * * "Real world" calls [[invoke]] and [[invokeWithFeedback]]. These methods have synchronization
   *   with class state that reflects the stream state
   *
   * onStart sends all events that were buffered while stream was materializing.
   * In case "Real world" added more events while initializing, onStart checks for more events in buffer when exiting and
   * resend new events
   *
   * Once class is in `Initialized` state - all "Real world" calls of [[invoke]] and [[invokeWithFeedback]] are running
   * as is - without blocking each other.
   *
   * [[GraphStage]] is called [[onStop]] when stream is wrapping down. onStop fails all futures for events that have not yet processed
   * [[onStop]] puts class in `Completed` state
   * "Real world" calls of [[invokeWithFeedback]] always return failed promises for `Completed` state
   */
  private final class ConcurrentAsyncCallback[T](handler: T => Unit)
      extends AtomicReference[State[T]](NoPendingEvents)
      with AsyncCallback[T] {
    import ConcurrentAsyncCallbackState._

    // is called from the owning [[GraphStage]]
    @tailrec
    private[stage] def onStart(): Unit = {
      // dispatch callbacks that have been queued before the interpreter was started
      (getAndSet(NoPendingEvents): @unchecked) match {
        case Pending(l) => if (l.nonEmpty) l.reverse.foreach(evt => onAsyncInput(evt.e, evt.handlingPromise))
        case s          => throw new IllegalStateException(s"Unexpected callback state [$s]")
      }

      // in the meantime more callbacks might have been queued (we keep queueing them to ensure order)
      if (!compareAndSet(NoPendingEvents, Initialized))
        // state guaranteed to be still Pending
        onStart()
    }

    // external call
    override def invokeWithFeedback(event: T): Future[Done] = {
      val promise: Promise[Done] = Promise[Done]()

      /*
       * Add this promise to the owning logic, so it can be completed afterPostStop if it was never handled otherwise.
       * Returns whether the logic is still running.
       */
      def addToWaiting(): Boolean = {
        val previous = asyncCallbacksInProgress.get()
        if (previous != null) { // not stopped
          previous.add(promise)

          // Need to read that again to make sure the stage hasn't been stopped in the meantime and the cleanup
          // process is already running.
          // If the cleanup process is already running (ref eq null) the promise needs to be dropped
          asyncCallbacksInProgress.get() ne null
        } else // logic was already stopped
          false
      }

      if (addToWaiting()) {
        invokeWithPromise(event, promise)
        promise.future
      } else
        Future.failed(streamDetachedException)
    }

    //external call
    override def invoke(event: T): Unit = invokeWithPromise(event, NoPromise)

    // external call
    override def invokeWithFeedbackCS(event: T): CompletionStage[Done] = invokeWithFeedback(event).asJava

    @tailrec
    private def invokeWithPromise(event: T, promise: Promise[Done]): Unit =
      get() match {
        case Initialized =>
          // started - can just dispatch async message to interpreter
          onAsyncInput(event, promise)

        case list @ Pending(l: List[Event[T]]) =>
          // not started yet
          if (!compareAndSet(list, Pending[T](Event[T](event, promise) :: l)))
            invokeWithPromise(event, promise)
      }

    private def onAsyncInput(event: T, promise: Promise[Done]): Unit =
      interpreter.onAsyncInput(GraphStageLogic.this, event, promise, handler.asInstanceOf[Any => Unit])
  }

  /**
   * Java API: Obtain a callback object that can be used asynchronously to re-enter the
   * current [[GraphStage]] with an asynchronous notification. The [[invoke]] method of the returned
   * [[AsyncCallback]] is safe to be called from other threads. It will in the background thread-safely
   * delegate to the passed callback function. I.e. [[invoke]] will be called by other thread and
   * the passed handler will be invoked eventually in a thread-safe way by the execution environment.
   *
   * [[AsyncCallback.invokeWithFeedback]] has an internal promise that will be failed if event cannot be processed due to stream completion.
   *
   * This object can be cached and reused within the same [[GraphStageLogic]].
   */
  final protected def createAsyncCallback[T](handler: Procedure[T]): AsyncCallback[T] =
    getAsyncCallback(handler.apply)

  private var callbacksWaitingForInterpreter: List[ConcurrentAsyncCallback[_]] = Nil
  // is used for two purposes: keep track of running callbacks and signal that the
  // stage has stopped to fail incoming async callback invocations by being set to null
  private val asyncCallbacksInProgress: AtomicReference[java.util.Set[Promise[Done]]] =
    new AtomicReference(ConcurrentHashMap.newKeySet[Promise[Done]]())

  private var _stageActor: StageActor = _
  final def stageActor: StageActor = _stageActor match {
    case null => throw StageActorRefNotInitializedException()
    case ref  => ref
  }

  // keep track of created SubSinkInlets and SubSourceOutlets to make sure we do not leak them
  // when this stage completes/fails, not threadsafe only accessed from stream machinery callbacks etc.
  private var _subInletsAndOutlets: Set[AnyRef] = Set.empty

  private def created(inlet: SubSinkInlet[_]): Unit =
    _subInletsAndOutlets += inlet

  private def completedOrFailed(inlet: SubSinkInlet[_]): Unit =
    _subInletsAndOutlets -= inlet

  private def created(outlet: SubSourceOutlet[_]): Unit =
    _subInletsAndOutlets += outlet

  private def completedOrFailed(outlet: SubSourceOutlet[_]): Unit =
    _subInletsAndOutlets -= outlet

  /**
   * Initialize a [[StageActorRef]] which can be used to interact with from the outside world "as-if" an [[Actor]].
   * The messages are looped through the [[getAsyncCallback]] mechanism of [[GraphStage]] so they are safe to modify
   * internal state of this operator.
   *
   * This method must (the earliest) be called after the [[GraphStageLogic]] constructor has finished running,
   * for example from the [[preStart]] callback the graph operator logic provides.
   *
   * Created [[StageActorRef]] to get messages and watch other actors in synchronous way.
   *
   * The [[StageActorRef]]'s lifecycle is bound to the operator, in other words when the operator is finished,
   * the Actor will be terminated as well. The entity backing the [[StageActorRef]] is not a real Actor,
   * but the [[GraphStageLogic]] itself, therefore it does not react to [[PoisonPill]].
   *
   * To be thread safe this method must only be called from either the constructor of the graph operator during
   * materialization or one of the methods invoked by the graph operator machinery, such as `onPush` and `onPull`.
   *
   * @param receive callback that will be called upon receiving of a message by this special Actor
   * @return minimal actor with watch method
   */
  final protected def getStageActor(receive: ((ActorRef, Any)) => Unit): StageActor =
    getEagerStageActor(interpreter.materializer)(receive)

  /**
   * INTERNAL API
   *
   * To be thread safe this method must only be called from either the constructor of the graph operator during
   * materialization or one of the methods invoked by the graph operator machinery, such as `onPush` and `onPull`.
   */
  @InternalApi
  protected[akka] def getEagerStageActor(eagerMaterializer: Materializer)(
      receive: ((ActorRef, Any)) => Unit): StageActor =
    _stageActor match {
      case null =>
        _stageActor = new StageActor(eagerMaterializer, getAsyncCallback _, receive, stageActorName)
        _stageActor
      case existing =>
        existing.become(receive)
        existing
    }

  /**
   * Override and return a name to be given to the StageActor of this operator.
   *
   * This method will be only invoked and used once, during the first [[getStageActor]]
   * invocation whichc reates the actor, since subsequent `getStageActors` calls function
   * like `become`, rather than creating new actors.
   *
   * Returns an empty string by default, which means that the name will a unique generated String (e.g. "$$a").
   */
  protected def stageActorName: String = ""

  // Internal hooks to avoid reliance on user calling super in preStart
  /** INTERNAL API */
  protected[stream] def beforePreStart(): Unit = {
    callbacksWaitingForInterpreter.foreach(_.onStart())
    callbacksWaitingForInterpreter = Nil
  }

  // Internal hooks to avoid reliance on user calling super in postStop
  /** INTERNAL API */
  protected[stream] def afterPostStop(): Unit = {
    if (_stageActor ne null) {
      _stageActor.stop()
      _stageActor = null
    }

    import scala.jdk.CollectionConverters._
    // make sure any invokeWithFeedback after this fails fast
    // and fail current outstanding invokeWithFeedback promises
    val inProgress =
      asyncCallbacksInProgress
        .getAndSet(null) // remove reference to signify that we are in the process of shutting down
        .asScala
    if (inProgress.nonEmpty) {
      val exception = streamDetachedException
      inProgress.foreach(_.tryFailure(exception))
    }
    cleanUpSubstreams(OptionVal.None)
  }

  /** Called from interpreter thread by GraphInterpreter.runAsyncInput */
  private[stream] def onFeedbackDispatched(p: Promise[Done]): Unit =
    asyncCallbacksInProgress.get() match {
      case null => // already finished, nothing to do here
      case x    => x.remove(p)
    }

  private def streamDetachedException =
    new StreamDetachedException(s"Stage with GraphStageLogic ${this} stopped before async invocation was processed")

  /**
   * Invoked before any external events are processed, at the startup of the operator.
   */
  @throws(classOf[Exception])
  def preStart(): Unit = ()

  /**
   * Invoked after processing of external events stopped because the operator is about to stop or fail.
   */
  @throws(classOf[Exception])
  def postStop(): Unit = ()

  /**
   * INTERNAL API
   *
   * This allows the dynamic creation of an Inlet for a GraphStage which is
   * connected to a Sink that is available for materialization (e.g. using
   * the `subFusingMaterializer`). Completion, cancellation and failure of the
   * parent operator is automatically delegated to instances of `SubSinkInlet` to avoid resource leaks.
   *
   * To be thread safe this method must only be called from either the constructor of the graph operator during
   * materialization or one of the methods invoked by the graph operator machinery, such as `onPush` and `onPull`.
   */
  class SubSinkInlet[T](name: String) {
    import ActorSubscriberMessage._

    private var handler: InHandler = _
    private var elem: T = null.asInstanceOf[T]
    private var closed = false
    private var pulled = false

    private val _sink = new SubSink[T](name, getAsyncCallback[ActorSubscriberMessage] { msg =>
      if (!closed) msg match {
        case OnNext(e) =>
          elem = e.asInstanceOf[T]
          pulled = false
          handler.onPush()
        case OnComplete =>
          closed = true
          handler.onUpstreamFinish()
          GraphStageLogic.this.completedOrFailed(this)
        case OnError(ex) =>
          closed = true
          handler.onUpstreamFailure(ex)
          GraphStageLogic.this.completedOrFailed(this)
      }
    }.invoke _)

    GraphStageLogic.this.created(this)

    def sink: Graph[SinkShape[T], NotUsed] = _sink

    def setHandler(handler: InHandler): Unit = this.handler = handler

    def isAvailable: Boolean = elem != null

    def isClosed: Boolean = closed

    def hasBeenPulled: Boolean = pulled && !isClosed

    def grab(): T = {
      if (elem == null)
        throw new IllegalArgumentException(s"cannot grab element from port ($this) when data have not yet arrived")
      val ret = elem
      elem = null.asInstanceOf[T]
      ret
    }

    def pull(): Unit = {
      if (pulled) throw new IllegalArgumentException(s"cannot pull port ($this) twice")
      if (closed) throw new IllegalArgumentException(s"cannot pull closed port ($this)")
      pulled = true
      _sink.pullSubstream()
    }

    def cancel(): Unit = {
      cancel(SubscriptionWithCancelException.NoMoreElementsNeeded)
    }
    def cancel(cause: Throwable): Unit = {
      closed = true
      _sink.cancelSubstream(cause)
      GraphStageLogic.this.completedOrFailed(this)
    }

    override def toString = s"SubSinkInlet($name)"
  }

  /**
   * INTERNAL API
   *
   * This allows the dynamic creation of an Outlet for a GraphStage which is
   * connected to a Source that is available for materialization (e.g. using
   * the `subFusingMaterializer`). Completion, cancellation and failure of the
   * parent operator is automatically delegated to instances of `SubSourceOutlet`
   * to avoid resource leaks.
   *
   * Even so it is good practice to use the `timeout` method to cancel this
   * Outlet in case the corresponding Source is not materialized within a
   * given time limit, see e.g. ActorMaterializerSettings.
   *
   * To be thread safe this method must only be called from either the constructor of the graph operator during
   * materialization or one of the methods invoked by the graph operator machinery, such as `onPush` and `onPull`.
   */
  class SubSourceOutlet[T](name: String) {

    private var handler: OutHandler = _
    private var available = false
    private var closed = false

    private val callback = getAsyncCallback[SubSink.Command] {
      case SubSink.RequestOne =>
        if (!closed) {
          available = true
          handler.onPull()
        }
      case SubSink.Cancel(cause) =>
        if (!closed) {
          available = false
          closed = true
          handler.onDownstreamFinish(cause)
          GraphStageLogic.this.completedOrFailed(this)
        }
    }

    private val _source = new SubSource[T](name, callback)
    GraphStageLogic.this.created(this)

    /**
     * Set the source into timed-out mode if it has not yet been materialized.
     */
    def timeout(d: FiniteDuration): Unit =
      if (_source.timeout(d)) closed = true

    /**
     * Get the Source for this dynamic output port.
     */
    def source: Graph[SourceShape[T], NotUsed] = _source

    /**
     * Set OutHandler for this dynamic output port; this needs to be done before
     * the first substream callback can arrive.
     */
    def setHandler(handler: OutHandler): Unit = this.handler = handler

    /**
     * Returns `true` if this output port can be pushed.
     */
    def isAvailable: Boolean = available

    /**
     * Returns `true` if this output port is closed, but caution
     * THIS WORKS DIFFERENTLY THAN THE NORMAL isClosed(out).
     * Due to possibly asynchronous shutdown it may not return
     * `true` immediately after `complete()` or `fail()` have returned.
     */
    def isClosed: Boolean = closed

    /**
     * Push to this output port.
     */
    def push(elem: T): Unit = {
      if (!isAvailable) throw new IllegalArgumentException(s"Cannot push port ($this) twice, or before it being pulled")
      available = false
      _source.pushSubstream(elem)
    }

    /**
     * Complete this output port.
     */
    def complete(): Unit = {
      available = false
      closed = true
      _source.completeSubstream()
      GraphStageLogic.this.completedOrFailed(this)
    }

    /**
     * Fail this output port.
     */
    def fail(ex: Throwable): Unit = {
      available = false
      closed = true
      _source.failSubstream(ex)
      GraphStageLogic.this.completedOrFailed(this)
    }

    override def toString = s"SubSourceOutlet($name)"
  }

  override def toString: String =
    attributes.get[Attributes.Name] match {
      case Some(name) =>
        attributes.get[SourceLocation] match {
          case Some(location) => s"${getClass.getName}-${name.n}(${location.locationName})"
          case None           => s"${getClass.getName}-${name.n}"
        }

      case None => getClass.getName
    }

}

/**
 * An asynchronous callback holder that is attached to a [[GraphStageLogic]].
 * Initializing [[AsyncCallback#invoke]] will eventually lead to the registered handler
 * being called.
 *
 * This holder has the same lifecycle as a stream and cannot be used before
 * materialization is done.
 *
 * Typical use cases are exchanging messages between stream and substreams or invoking from external world sending
 * event to a stream
 *
 * Not for user extension
 */
@DoNotInherit
trait AsyncCallback[T] {

  /**
   * Dispatch an asynchronous notification. This method is thread-safe and
   * may be invoked from external execution contexts.
   *
   * For cases where it is important to know if the notification was ever processed or not
   * see [[AsyncCallback#invokeWithFeedback]]
   */
  def invoke(t: T): Unit

  /**
   * Scala API: Dispatch an asynchronous notification. This method is thread-safe and
   * may be invoked from external execution contexts.
   *
   * The method returns directly and the returned future is then completed once the event
   * has been handled by the operator, if the event triggers an exception from the handler the future
   * is failed with that exception and finally if the operator was stopped before the event has been
   * handled the future is failed with `StreamDetachedException`.
   *
   * The handling of the returned future incurs a slight overhead, so for cases where it does not matter
   * to the invoking logic see [[AsyncCallback#invoke]]
   */
  def invokeWithFeedback(t: T): Future[Done]

  /**
   * Java API: Dispatch an asynchronous notification. This method is thread-safe and
   * may be invoked from external execution contexts.
   *
   * The method returns directly and the returned future is then completed once the event
   * has been handled by the operator, if the event triggers an exception from the handler the future
   * is failed with that exception and finally if the operator was stopped before the event has been
   * handled the future is failed with `StreamDetachedException`.
   *
   * The handling of the returned future incurs a slight overhead, so for cases where it does not matter
   * to the invoking logic see [[AsyncCallback#invoke]]
   */
  def invokeWithFeedbackCS(t: T): CompletionStage[Done]
}

/**
 * Provides timer related facilities to a [[GraphStageLogic]].
 *
 * To be thread safe the methods of this class must only be called from either the constructor of the graph operator during
 * materialization or one of the methods invoked by the graph operator machinery, such as `onPush` and `onPull`.
 */
abstract class TimerGraphStageLogic(_shape: Shape) extends GraphStageLogic(_shape) {
  import TimerMessages._

  private val keyToTimers = mutable.Map[Any, Timer]()
  private val timerIdGen = Iterator.from(1)

  private var _timerAsyncCallback: AsyncCallback[Scheduled] = _
  private def getTimerAsyncCallback: AsyncCallback[Scheduled] = {
    if (_timerAsyncCallback eq null)
      _timerAsyncCallback = getAsyncCallback(onInternalTimer)

    _timerAsyncCallback
  }

  private def onInternalTimer(scheduled: Scheduled): Unit = {
    val Id = scheduled.timerId
    val timerKey = scheduled.timerKey
    keyToTimers.get(timerKey) match {
      case Some(Timer(Id, _)) =>
        if (!scheduled.repeating) keyToTimers -= timerKey
        onTimer(timerKey)
      case _ =>
    }
  }

  /**
   * Will be called when the scheduled timer is triggered.
   *
   * @param timerKey key of the scheduled timer
   */
  @throws(classOf[Exception])
  protected def onTimer(timerKey: Any): Unit = ()

  // Internal hooks to avoid reliance on user calling super in postStop
  protected[stream] override def afterPostStop(): Unit = {
    super.afterPostStop()
    if (keyToTimers ne null) {
      keyToTimers.foreach { case (_, Timer(_, task)) => task.cancel() }
      keyToTimers.clear()
    }
  }

  /**
   * Schedule timer to call [[#onTimer]] after given delay.
   * Any existing timer with the same key will automatically be canceled before
   * adding the new timer.
   */
  final protected def scheduleOnce(timerKey: Any, delay: FiniteDuration): Unit = {
    cancelTimer(timerKey)
    val id = timerIdGen.next()
    val callback = getTimerAsyncCallback
    val task =
      interpreter.materializer.scheduleOnce(delay, () => callback.invoke(Scheduled(timerKey, id, repeating = false)))
    keyToTimers(timerKey) = Timer(id, task)
  }

  /**
   * Schedule timer to call [[#onTimer]] after given delay.
   * Any existing timer with the same key will automatically be canceled before
   * adding the new timer.
   */
  final protected def scheduleOnce(timerKey: Any, delay: java.time.Duration): Unit = {
    import scala.jdk.DurationConverters._
    scheduleOnce(timerKey, delay.toScala)
  }

  /**
   * Schedule timer to call [[#onTimer]] periodically with the given `delay` after the specified
   * initial delay.
   * Any existing timer with the same key will automatically be canceled before
   * adding the new timer.
   */
  final protected def scheduleWithFixedDelay(
      timerKey: Any,
      initialDelay: FiniteDuration,
      delay: FiniteDuration): Unit = {
    cancelTimer(timerKey)
    val id = timerIdGen.next()
    val callback = getTimerAsyncCallback
    val task = interpreter.materializer.scheduleWithFixedDelay(
      initialDelay,
      delay,
      () => callback.invoke(Scheduled(timerKey, id, repeating = true)))
    keyToTimers(timerKey) = Timer(id, task)
  }

  /**
   * Schedule timer to call [[#onTimer]] periodically with the given `delay` after the specified
   * initial delay.
   * Any existing timer with the same key will automatically be canceled before
   * adding the new timer.
   */
  final protected def scheduleWithFixedDelay(
      timerKey: Any,
      initialDelay: java.time.Duration,
      interval: java.time.Duration): Unit = {
    import scala.jdk.DurationConverters._
    scheduleWithFixedDelay(timerKey, initialDelay.toScala, interval.toScala)
  }

  /**
   * Schedule timer to call [[#onTimer]] periodically with the given `interval` after the specified
   * initial delay.
   * Any existing timer with the same key will automatically be canceled before
   * adding the new timer.
   */
  final protected def scheduleAtFixedRate(
      timerKey: Any,
      initialDelay: FiniteDuration,
      interval: FiniteDuration): Unit = {
    cancelTimer(timerKey)
    val id = timerIdGen.next()
    val callback = getTimerAsyncCallback
    val task = interpreter.materializer.scheduleAtFixedRate(
      initialDelay,
      interval,
      () => callback.invoke(Scheduled(timerKey, id, repeating = true)))
    keyToTimers(timerKey) = Timer(id, task)
  }

  /**
   * Schedule timer to call [[#onTimer]] periodically with the given `interval` after the specified
   * initial delay.
   * Any existing timer with the same key will automatically be canceled before
   * adding the new timer.
   */
  final protected def scheduleAtFixedRate(
      timerKey: Any,
      initialDelay: java.time.Duration,
      interval: java.time.Duration): Unit = {
    import scala.jdk.DurationConverters._
    scheduleAtFixedRate(timerKey, initialDelay.toScala, interval.toScala)
  }

  /**
   * Schedule timer to call [[#onTimer]] periodically with the given interval after the specified
   * initial delay.
   * Any existing timer with the same key will automatically be canceled before
   * adding the new timer.
   */
  @deprecated(
    "Use scheduleWithFixedDelay or scheduleAtFixedRate instead. This has the same semantics as " +
    "scheduleAtFixedRate, but scheduleWithFixedDelay is often preferred.",
    since = "2.6.0")
  final protected def schedulePeriodicallyWithInitialDelay(
      timerKey: Any,
      initialDelay: FiniteDuration,
      interval: FiniteDuration): Unit =
    scheduleAtFixedRate(timerKey, initialDelay, interval)

  /**
   * Schedule timer to call [[#onTimer]] periodically with the given interval after the specified
   * initial delay.
   * Any existing timer with the same key will automatically be canceled before
   * adding the new timer.
   */
  @deprecated(
    "Use scheduleWithFixedDelay or scheduleAtFixedRate instead. This has the same semantics as " +
    "scheduleAtFixedRate, but scheduleWithFixedDelay is often preferred.",
    since = "2.6.0")
  final protected def schedulePeriodicallyWithInitialDelay(
      timerKey: Any,
      initialDelay: java.time.Duration,
      interval: java.time.Duration): Unit = {
    import scala.jdk.DurationConverters._
    schedulePeriodicallyWithInitialDelay(timerKey, initialDelay.toScala, interval.toScala)
  }

  /**
   * Schedule timer to call [[#onTimer]] periodically with the given interval.
   * Any existing timer with the same key will automatically be canceled before
   * adding the new timer.
   */
  @deprecated(
    "Use scheduleWithFixedDelay or scheduleAtFixedRate instead. This has the same semantics as " +
    "scheduleAtFixedRate, but scheduleWithFixedDelay is often preferred.",
    since = "2.6.0")
  final protected def schedulePeriodically(timerKey: Any, interval: FiniteDuration): Unit =
    schedulePeriodicallyWithInitialDelay(timerKey, interval, interval)

  /**
   * Schedule timer to call [[#onTimer]] periodically with the given interval.
   * Any existing timer with the same key will automatically be canceled before
   * adding the new timer.
   */
  @deprecated(
    "Use scheduleWithFixedDelay or scheduleAtFixedRate instead. This has the same semantics as " +
    "scheduleAtFixedRate, but scheduleWithFixedDelay is often preferred.",
    since = "2.6.0")
  final protected def schedulePeriodically(timerKey: Any, interval: java.time.Duration): Unit = {
    import scala.jdk.DurationConverters._
    schedulePeriodically(timerKey, interval.toScala)
  }

  /**
   * Cancel timer, ensuring that the [[#onTimer]] is not subsequently called.
   *
   * @param timerKey key of the timer to cancel
   */
  final protected def cancelTimer(timerKey: Any): Unit =
    keyToTimers.get(timerKey).foreach { t =>
      t.task.cancel()
      keyToTimers -= timerKey
    }

  /**
   * Inquire whether the timer is still active. Returns true unless the
   * timer does not exist, has previously been canceled or if it was a
   * single-shot timer that was already triggered.
   */
  final protected def isTimerActive(timerKey: Any): Boolean = keyToTimers contains timerKey

}

/** Java API: [[GraphStageLogic]] with [[StageLogging]]. */
abstract class GraphStageLogicWithLogging(_shape: Shape) extends GraphStageLogic(_shape) with StageLogging

/** Java API: [[TimerGraphStageLogic]] with [[StageLogging]]. */
abstract class TimerGraphStageLogicWithLogging(_shape: Shape) extends TimerGraphStageLogic(_shape) with StageLogging

/**
 * Collection of callbacks for an input port of a [[GraphStage]]
 */
trait InHandler {

  /**
   * Called when the input port has a new element available. The actual element can be retrieved via the
   * [[GraphStageLogic.grab]] method.
   */
  @throws(classOf[Exception])
  def onPush(): Unit

  /**
   * Called when the input port is finished. After this callback no other callbacks will be called for this port.
   */
  @throws(classOf[Exception])
  def onUpstreamFinish(): Unit = GraphInterpreter.currentInterpreter.activeStage.completeStage()

  /**
   * Called when the input port has failed. After this callback no other callbacks will be called for this port.
   */
  @throws(classOf[Exception])
  def onUpstreamFailure(ex: Throwable): Unit = GraphInterpreter.currentInterpreter.activeStage.failStage(ex)
}

/**
 * Collection of callbacks for an output port of a [[GraphStage]]
 */
trait OutHandler {

  /**
   * Called when the output port has received a pull, and therefore ready to emit an element, i.e. [[GraphStageLogic.push]]
   * is now allowed to be called on this port.
   */
  @throws(classOf[Exception])
  def onPull(): Unit

  /**
   * Called when the output port will no longer accept any new elements. After this callback no other callbacks will
   * be called for this port.
   */
  @throws(classOf[Exception])
  @deprecatedOverriding("Override `def onDownstreamFinish(cause: Throwable)`, instead.", since = "2.6.0") // warns when overriding
  @deprecated("Call onDownstreamFinish with a cancellation cause.", since = "2.6.0") // warns when calling
  def onDownstreamFinish(): Unit = {
    val thisStage = GraphInterpreter.currentInterpreter.activeStage
    require(
      thisStage.lastCancellationCause ne null,
      "onDownstreamFinish() must not be called without a cancellation cause")
    thisStage.cancelStage(thisStage.lastCancellationCause)
  }

  /**
   * Called when the output port will no longer accept any new elements. After this callback no other callbacks will
   * be called for this port.
   */
  @throws(classOf[Exception])
  def onDownstreamFinish(cause: Throwable): Unit = {
    val thisStage = GraphInterpreter.currentInterpreter.activeStage
    try {
      require(cause ne null, "Cancellation cause must not be null")
      require(thisStage.lastCancellationCause eq null, "onDownstreamFinish(cause) must not be called recursively")
      thisStage.lastCancellationCause = cause
      (onDownstreamFinish(): @nowarn("msg=deprecated")) // if not overridden, call old deprecated variant
    } finally thisStage.lastCancellationCause = null
  }
}

/**
 * Java API: callbacks for an input port where termination logic is predefined
 * (completing when upstream completes, failing when upstream fails).
 */
abstract class AbstractInHandler extends InHandler

/**
 * Java API: callbacks for an output port where termination logic is predefined
 * (completing when downstream cancels).
 */
abstract class AbstractOutHandler extends OutHandler

/**
 * Java API: callback combination for output and input ports where termination logic is predefined
 * (completing when upstream completes, failing when upstream fails, completing when downstream cancels).
 */
abstract class AbstractInOutHandler extends InHandler with OutHandler
