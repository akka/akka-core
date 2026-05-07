/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.Promise
import scala.util.control.NonFatal

import akka.Done
import akka.actor.ActorRef
import akka.annotation.{ InternalApi, InternalStableApi }
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.stream._
import akka.stream.Attributes.LogLevels
import akka.stream.snapshot._
import akka.stream.stage._

/**
 * INTERNAL API
 *
 * (See the class for the documentation of the internals)
 */
@InternalApi private[akka] object GraphInterpreter {

  /**
   * Compile time constant, enable it for debug logging to the console.
   */
  final val Debug = false

  final val NoEvent = null

  final val InReady = 1
  final val Pulling = 2
  final val Pushing = 4
  final val OutReady = 8

  final val InClosed = 16
  final val OutClosed = 32
  final val InFailed = 64

  final val PullStartFlip = 3 // 0011
  final val PullEndFlip = 10 // 1010
  final val PushStartFlip = 12 //1100
  final val PushEndFlip = 5 //0101

  final val KeepGoingFlag = 0x4000000
  final val KeepGoingMask = 0x3ffffff

  /**
   * Marker object that indicates that a port holds no element since it was already grabbed. The port is still pullable,
   * but there is no more element to grab.
   */
  case object Empty

  /** Marker class that indicates that a port was failed with a given cause and a potential outstanding element */
  final case class Failed(ex: Throwable, previousElem: Any)

  /** Marker class that indicates that a port was cancelled with a given cause */
  final case class Cancelled(cause: Throwable)

  abstract class UpstreamBoundaryStageLogic[T] extends GraphStageLogic(inCount = 0, outCount = 1) {
    def out: Outlet[T]
  }

  abstract class DownstreamBoundaryStageLogic[T] extends GraphStageLogic(inCount = 1, outCount = 0) {
    def in: Inlet[T]
  }

  val singleNoAttribute: Array[Attributes] = Array(Attributes.none)

  /**
   * INERNAL API
   *
   * Contains all the necessary information for the GraphInterpreter to be able to implement a connection
   * between an output and input ports.
   *
   * @param id Identifier of the connection.
   * @param inOwner The operator logic that corresponds to the input side of the connection.
   * @param outOwner The operator logic that corresponds to the output side of the connection.
   * @param inHandler The handler that contains the callback for input events.
   * @param outHandler The handler that contains the callback for output events.
   */
  @InternalStableApi
  final class Connection(
      var id: Int,
      var inOwner: GraphStageLogic,
      var outOwner: GraphStageLogic,
      var inHandler: InHandler,
      var outHandler: OutHandler) {

    /** See [[GraphInterpreter]] about possible states */
    var portState: Int = InReady

    /**
     * Can either be
     *  * an in-flight element
     *  * a failure (with an optional in-flight element), if elem.isInstanceOf[Failed]
     *  * a cancellation cause, if elem.isInstanceOf[Cancelled]
     */
    var slot: Any = Empty
  }

  private val _currentInterpreter = new ThreadLocal[Array[AnyRef]] {
    /*
     * Using an Object-array avoids holding on to the GraphInterpreter class
     * when this accidentally leaks onto threads that are not stopped when this
     * class should be unloaded.
     */
    override def initialValue: Array[AnyRef] = new Array(1)
  }

  /**
   * INTERNAL API
   */
  private[akka] def currentInterpreter: GraphInterpreter =
    _currentInterpreter.get()(0).asInstanceOf[GraphInterpreter].nonNull
  // nonNull is just a debug helper to find nulls more timely

  /**
   * INTERNAL API
   */
  private[akka] def currentInterpreterOrNull: GraphInterpreter =
    _currentInterpreter.get()(0).asInstanceOf[GraphInterpreter]

}

/**
 * INTERNAL API
 *
 * From an external viewpoint, the GraphInterpreter takes an assembly of graph processing stages and provides facilities
 * to execute and interact with this assembly.
 *
 * The lifecycle of the Interpreter is roughly the following:
 *  - [[init]] is called
 *  - [[execute]] is called whenever there is need for execution, providing an upper limit on the processed events
 *  - [[finish]] is called before the interpreter is disposed, preferably after [[isCompleted]] returned true, although
 *    in abort cases this is not strictly necessary
 *
 * The [[execute]] method of the interpreter accepts an upper bound on the events it will process. After this limit
 * is reached or there are no more pending events to be processed, the call returns. It is possible to inspect
 * if there are unprocessed events left via the [[isSuspended]] method. [[isCompleted]] returns true once all operators
 * reported completion inside the interpreter.
 *
 * The internal architecture of the interpreter is based on the usage of arrays and optimized for reducing allocations
 * on the hot paths.
 *
 * One of the basic abstractions inside the interpreter is the [[akka.stream.impl.fusing.GraphInterpreter.Connection]].
 * A connection represents an output-input port pair (an analogue for a connected RS Publisher-Subscriber pair).
 * The Connection object contains all the necessary data for the interpreter to pass elements, demand, completion
 * or errors across the Connection.
 * In particular
 *  - portStates contains a bitfield that tracks the states of the ports (output-input) corresponding to this
 *    connection. This bitfield is used to decode the event that is in-flight.
 *  - connectionSlot contains a potential element or exception that accompanies the
 *    event encoded in the portStates bitfield
 *  - inHandler contains the [[InHandler]] instance that handles the events corresponding
 *    to the input port of the connection
 *  - outHandler contains the [[OutHandler]] instance that handles the events corresponding
 *    to the output port of the connection
 *
 * On top of the Connection table there is an eventQueue, represented as a circular buffer of Connections. The queue
 * contains the Connections that have pending events to be processed. The pending event itself is encoded
 * in the portState bitfield of the Connection. This implies that there can be only one event in flight for a given
 * Connection, which is true in almost all cases, except a complete-after-push or fail-after-push which has to
 * be decoded accordingly.
 *
 * The layout of the portState bitfield is the following:
 *
 *             |- state machn.-| Only one bit is hot among these bits
 *  64  32  16 | 8   4   2   1 |
 * +---+---+---|---+---+---+---|
 *   |   |   |   |   |   |   |
 *   |   |   |   |   |   |   |  From the following flags only one is active in any given time. These bits encode
 *   |   |   |   |   |   |   |  state machine states, and they are "moved" around using XOR masks to keep other bits
 *   |   |   |   |   |   |   |  intact.
 *   |   |   |   |   |   |   |
 *   |   |   |   |   |   |   +- InReady:  The input port is ready to be pulled
 *   |   |   |   |   |   +----- Pulling:  A pull is active, but have not arrived yet (queued)
 *   |   |   |   |   +--------- Pushing:  A push is active, but have not arrived yet (queued)
 *   |   |   |   +------------- OutReady: The output port is ready to be pushed
 *   |   |   |
 *   |   |   +----------------- InClosed:  The input port is closed and will not receive any events.
 *   |   |                                 A push might be still in flight which will be then processed first.
 *   |   +--------------------- OutClosed: The output port is closed and will not receive any events.
 *   +------------------------- InFailed:  Always set in conjunction with InClosed. Indicates that the close event
 *                                         is a failure
 *
 * Sending an event is usually the following sequence:
 *  - An action is requested by an operator logic (push, pull, complete, etc.)
 *  - the state machine in portStates is transitioned from a ready state to a pending event
 *  - the affected Connection is enqueued
 *
 * Receiving an event is usually the following sequence:
 *  - the Connection to be processed is dequeued
 *  - the type of the event is determined from the bits set on portStates
 *  - the state machine in portStates is transitioned to a ready state
 *  - using the inHandlers/outHandlers table the corresponding callback is called on the operator logic.
 *
 * Because of the FIFO construction of the queue the interpreter is fair, i.e. a pending event is always executed
 * after a bounded number of other events. This property, together with suspendability means that even infinite cycles can
 * be modeled, or even dissolved (if preempted and a "stealing" external event is injected; for example the non-cycle
 * edge of a balance is pulled, dissolving the original cycle).
 */
@InternalApi private[akka] final class GraphInterpreter(
    val materializer: Materializer,
    val log: LoggingAdapter,
    val logics: Array[GraphStageLogic], // Array of stage logics
    val connections: Array[GraphInterpreter.Connection],
    val onAsyncInput: (GraphStageLogic, Any, Promise[Done], (Any) => Unit) => Unit,
    val fuzzingMode: Boolean,
    val context: ActorRef) {
  import GraphInterpreter._

  private[this] val ChaseLimit = if (fuzzingMode) 0 else 16

  /**
   * INTERNAL API
   */
  @InternalApi private[stream] var activeStage: GraphStageLogic = _

  // The number of currently running stages. Once this counter reaches zero, the interpreter is considered to be
  // completed
  private[this] var runningStages = logics.length

  // Counts how many active connections a stage has. Once it reaches zero, the stage is automatically stopped.
  private[this] val shutdownCounter = Array.tabulate(logics.length) { i =>
    logics(i).handlers.length
  }

  private[this] var _subFusingMaterializer: Materializer = _
  private[this] lazy val defaultErrorReportingLogLevel = LogLevels.defaultErrorLevel(materializer.system)

  def subFusingMaterializer: Materializer = _subFusingMaterializer

  // An event queue implemented as a circular buffer
  // FIXME: This calculates the maximum size ever needed, but most assemblies can run on a smaller queue
  private[this] val eventQueue = new Array[Connection](1 << (32 - Integer.numberOfLeadingZeros(connections.length - 1)))
  private[this] val mask = eventQueue.length - 1
  private[this] var queueHead: Int = 0
  private[this] var queueTail: Int = 0

  private[this] var chaseCounter = 0 // the first events in preStart blocks should be not chased
  private[this] var chasedPush: Connection = NoEvent
  private[this] var chasedPull: Connection = NoEvent

  private def queueStatus: String = {
    val contents = (queueHead until queueTail).map(idx => {
      val conn = eventQueue(idx & mask)
      conn
    })
    s"(${eventQueue.length}, $queueHead, $queueTail)(${contents.mkString(", ")})"
  }
  private[this] var _Name: String = _
  def Name: String =
    if (_Name eq null) {
      _Name = f"${System.identityHashCode(this)}%08X"
      _Name
    } else _Name

  /**
   * INTERNAL API
   */
  @InternalApi private[stream] def nonNull: GraphInterpreter = this

  /**
   * Dynamic handler changes are communicated from a GraphStageLogic by this method.
   */
  def setHandler(connection: Connection, handler: InHandler): Unit = {
    if (Debug) println(s"$Name SETHANDLER ${inOwnerName(connection)} (in) $handler")
    connection.inHandler = handler
  }

  /**
   * Dynamic handler changes are communicated from a GraphStageLogic by this method.
   */
  def setHandler(connection: Connection, handler: OutHandler): Unit = {
    if (Debug) println(s"$Name SETHANDLER ${outOwnerName(connection)} (out) $handler")
    connection.outHandler = handler
  }

  /**
   * Returns true if there are pending unprocessed events in the event queue.
   */
  def isSuspended: Boolean = queueHead != queueTail

  /**
   * Returns true if there are no more running operators and pending events.
   */
  def isCompleted: Boolean = runningStages == 0 && !isSuspended

  /**
   * Initializes the states of all the operator logics by calling preStart().
   * The passed-in materializer is intended to be a SubFusingActorMaterializer
   * that avoids creating new Actors when operators materialize sub-flows. If no
   * such materializer is available, passing in `null` will reuse the normal
   * materializer for the GraphInterpreter—fusing is only an optimization.
   */
  def init(subMat: Materializer): Unit = {
    _subFusingMaterializer = if (subMat == null) materializer else subMat
    var i = 0
    while (i < logics.length) {
      val logic = logics(i)
      logic.interpreter = this
      try {
        logic.beforePreStart()
        logic.preStart()
      } catch {
        case NonFatal(e) =>
          log.error(e, "Error during preStart in [{}]: {}", logic.toString, e.getMessage)
          logic.failStage(e)
      }
      afterStageHasRun(logic)
      i += 1
    }
  }

  /**
   * Finalizes the state of all operators by calling postStop() (if necessary).
   */
  def finish(): Unit = {
    var i = 0
    while (i < logics.length) {
      val logic = logics(i)
      if (!isStageCompleted(logic)) finalizeStage(logic)
      i += 1
    }
  }

  // Debug name for a connections input part
  private def inOwnerName(connection: Connection): String = connection.inOwner.toString

  // Debug name for a connections output part
  private def outOwnerName(connection: Connection): String = connection.outOwner.toString

  // Debug name for a connections input part
  private def inLogicName(connection: Connection): String = logics(connection.inOwner.stageId).toString

  // Debug name for a connections output part
  private def outLogicName(connection: Connection): String = logics(connection.outOwner.stageId).toString

  private def shutdownCounters: String =
    shutdownCounter.map(x => if (x >= KeepGoingFlag) s"${x & KeepGoingMask}(KeepGoing)" else x.toString).mkString(",")

  /**
   * Executes pending events until the given limit is met. If there were remaining events, isSuspended will return
   * true.
   */
  def execute(eventLimit: Int): Int = {
    if (Debug)
      println(s"$Name ---------------- EXECUTE $queueStatus (running=$runningStages, shutdown=$shutdownCounters)")
    val currentInterpreterHolder = _currentInterpreter.get()
    val previousInterpreter = currentInterpreterHolder(0)
    currentInterpreterHolder(0) = this
    var eventsRemaining = eventLimit
    try {
      while (eventsRemaining > 0 && queueTail != queueHead) {
        val connection = dequeue()
        eventsRemaining -= 1
        chaseCounter = math.min(ChaseLimit, eventsRemaining)

        def reportStageError(e: Throwable): Unit = {
          if (activeStage == null) throw e
          else {
            val logAt: Logging.LogLevel = activeStage.attributes.get[LogLevels] match {
              case Some(levels) => levels.onFailure
              case None         => defaultErrorReportingLogLevel
            }
            logAt match {
              case Logging.ErrorLevel =>
                log.error(e, "Error in stage [{}]: {}", activeStage.toString, e.getMessage)
              case Logging.WarningLevel =>
                log.warning(e, "Error in stage [{}]: {}", activeStage.toString, e.getMessage)
              case Logging.InfoLevel =>
                log.info("Error in stage [{}]: {}", activeStage.toString, e.getMessage)
              case Logging.DebugLevel =>
                log.debug("Error in stage [{}]: {}", activeStage.toString, e.getMessage)
              case _ => // Off, nop
            }
            activeStage.failStage(e)

            // Abort chasing
            chaseCounter = 0
            if (chasedPush ne NoEvent) {
              enqueue(chasedPush)
              chasedPush = NoEvent
            }
            if (chasedPull ne NoEvent) {
              enqueue(chasedPull)
              chasedPull = NoEvent
            }
          }
        }

        /*
         * This is the "normal" event processing code which dequeues directly from the internal event queue. Since
         * most execution paths tend to produce either a Push that will be propagated along a longer chain we take
         * extra steps below to make this more efficient.
         */
        try processEvent(connection)
        catch {
          case NonFatal(e) => reportStageError(e)
        }
        var wasFinalized = afterStageHasRun(activeStage)

        /*
         * "Event chasing" optimization follows from here. This optimization works under the assumption that a Push or
         * Pull is very likely immediately followed by another Push/Pull. The difference from the "normal" event
         * dispatch is that chased events are never touching the event queue, they use a "streamlined" execution path
         * instead. Looking at the scenario of a Push, the following events will happen.
         *  - "normal" dispatch executes an onPush event
         *  - stage eventually calls push()
         *  - code inside the push() method checks the validity of the call, and also if it can be safely ignored
         *    (because the target stage already completed we just have not been notified yet)
         *  - if the upper limit of ChaseLimit has not been reached, then the Connection is put into the chasedPush
         *    variable
         *  - the loop below immediately captures this push and dispatches it
         *
         * What is saved by this optimization is three steps:
         *  - no need to enqueue the Connection in the queue (array), it ends up in a simple variable, reducing
         *    pressure on array load-store
         *  - no need to dequeue the Connection from the queue, similar to above
         *  - no need to decode the event, we know it is a Push already
         *  - no need to check for validity of the event because we already checked at the push() call, and there
         *    can be no concurrent events interleaved unlike with the normal dispatch (think about a cancel() that is
         *    called in the target stage just before the onPush() arrives). This avoids unnecessary branching.
         */

        // Chasing PUSH events
        while (chasedPush != NoEvent) {
          val connection = chasedPush
          chasedPush = NoEvent
          try processPush(connection)
          catch {
            case NonFatal(e) => reportStageError(e)
          }
          if (!wasFinalized)
            wasFinalized = afterStageHasRun(activeStage)
        }

        // Chasing PULL events
        while (chasedPull != NoEvent) {
          val connection = chasedPull
          chasedPull = NoEvent
          try processPull(connection)
          catch {
            case NonFatal(e) => reportStageError(e)
          }
          if (!wasFinalized)
            wasFinalized = afterStageHasRun(activeStage)
        }

        if (chasedPush != NoEvent) {
          enqueue(chasedPush)
          chasedPush = NoEvent
        }

      }
      // Event *must* be enqueued while not in the execute loop (events enqueued from external, possibly async events)
      chaseCounter = 0
    } finally {
      currentInterpreterHolder(0) = previousInterpreter
    }
    if (Debug) println(s"$Name ---------------- $queueStatus (running=$runningStages, shutdown=$shutdownCounters)")
    // TODO: deadlock detection
    eventsRemaining
  }

  @InternalStableApi
  def runAsyncInput(logic: GraphStageLogic, evt: Any, promise: Promise[Done], handler: (Any) => Unit): Unit =
    if (!isStageCompleted(logic)) {
      if (GraphInterpreter.Debug) println(s"$Name ASYNC $evt ($handler) [$logic]")
      val currentInterpreterHolder = _currentInterpreter.get()
      val previousInterpreter = currentInterpreterHolder(0)
      currentInterpreterHolder(0) = this
      try {
        activeStage = logic
        try {
          handler(evt)
          if (promise ne GraphStageLogic.NoPromise) {
            promise.success(Done)
            logic.onFeedbackDispatched(promise)
          }
        } catch {
          case NonFatal(ex) =>
            if (promise ne GraphStageLogic.NoPromise) {
              promise.failure(ex)
              logic.onFeedbackDispatched(promise)
            }
            logic.failStage(ex)
        }
        afterStageHasRun(logic)
      } finally currentInterpreterHolder(0) = previousInterpreter
    }

  // Decodes and processes a single event for the given connection
  @InternalStableApi
  private def processEvent(connection: Connection): Unit = {

    // this must be the state after returning without delivering any signals, to avoid double-finalization of some unlucky stage
    // (this can happen if a stage completes voluntarily while connection close events are still queued)
    activeStage = null
    val code = connection.portState

    // Manual fast decoding, fast paths are PUSH and PULL
    //   PUSH
    if ((code & (Pushing | InClosed | OutClosed)) == Pushing) {
      processPush(connection)

      // PULL
    } else if ((code & (Pulling | OutClosed | InClosed)) == Pulling) {
      processPull(connection)

      // CANCEL
    } else if ((code & (OutClosed | InClosed)) == InClosed) {
      activeStage = connection.outOwner
      if (Debug)
        println(
          s"$Name CANCEL ${inOwnerName(connection)} -> ${outOwnerName(connection)} (${connection.outHandler}) [${outLogicName(connection)}]")
      connection.portState |= OutClosed
      completeConnection(connection.outOwner.stageId)
      val cause = connection.slot.asInstanceOf[Cancelled].cause
      connection.slot = Empty
      connection.outHandler.onDownstreamFinish(cause)
    } else if ((code & (OutClosed | InClosed)) == OutClosed) {
      // COMPLETIONS

      if ((code & Pushing) == 0) {
        // Normal completion (no push pending)
        if (Debug)
          println(
            s"$Name COMPLETE ${outOwnerName(connection)} -> ${inOwnerName(connection)} (${connection.inHandler}) [${inLogicName(connection)}]")
        connection.portState |= InClosed
        activeStage = connection.inOwner
        completeConnection(connection.inOwner.stageId)
        if ((connection.portState & InFailed) == 0) connection.inHandler.onUpstreamFinish()
        else connection.inHandler.onUpstreamFailure(connection.slot.asInstanceOf[Failed].ex)
      } else {
        // Push is pending, first process push, then re-enqueue closing event
        processPush(connection)
        enqueue(connection)
      }

    }
  }

  @InternalStableApi
  private def processPush(connection: Connection): Unit = {
    if (Debug)
      println(
        s"$Name PUSH ${outOwnerName(connection)} -> ${inOwnerName(connection)}, ${connection.slot} (${connection.inHandler}) [${inLogicName(connection)}]")
    activeStage = connection.inOwner
    connection.portState ^= PushEndFlip
    connection.inHandler.onPush()
  }

  @InternalStableApi
  private def processPull(connection: Connection): Unit = {
    if (Debug)
      println(
        s"$Name PULL ${inOwnerName(connection)} -> ${outOwnerName(connection)} (${connection.outHandler}) [${outLogicName(connection)}]")
    activeStage = connection.outOwner
    connection.portState ^= PullEndFlip
    connection.outHandler.onPull()
  }

  private def dequeue(): Connection = {
    val idx = queueHead & mask
    if (fuzzingMode) {
      val swapWith = (ThreadLocalRandom.current.nextInt(queueTail - queueHead) + queueHead) & mask
      val ev = eventQueue(swapWith)
      eventQueue(swapWith) = eventQueue(idx)
      eventQueue(idx) = ev
    }
    val elem = eventQueue(idx)
    eventQueue(idx) = NoEvent
    queueHead += 1
    elem
  }

  def enqueue(connection: Connection): Unit = {
    if (Debug)
      if (queueTail - queueHead > mask)
        new Exception(s"$Name internal queue full ($queueStatus) + $connection").printStackTrace()
    eventQueue(queueTail & mask) = connection
    queueTail += 1
  }

  def afterStageHasRun(logic: GraphStageLogic): Boolean =
    if (isStageCompleted(logic)) {
      runningStages -= 1
      finalizeStage(logic)
      true
    } else {
      false
    }

  // Returns true if the given stage is already completed
  def isStageCompleted(stage: GraphStageLogic): Boolean = stage != null && shutdownCounter(stage.stageId) == 0

  // Register that a connection in which the given stage participated has been completed and therefore the stage
  // itself might stop, too.
  private def completeConnection(stageId: Int): Unit = {
    val activeConnections = shutdownCounter(stageId)
    if (activeConnections > 0) shutdownCounter(stageId) = activeConnections - 1
  }

  private[stream] def setKeepGoing(logic: GraphStageLogic, enabled: Boolean): Unit =
    if (enabled) shutdownCounter(logic.stageId) |= KeepGoingFlag
    else shutdownCounter(logic.stageId) &= KeepGoingMask

  @InternalStableApi
  private[stream] def finalizeStage(logic: GraphStageLogic): Unit = {
    try {
      logic.postStop()
      logic.afterPostStop()
    } catch {
      case NonFatal(e) =>
        log.error(e, s"Error during postStop in [{}]: {}", logic.toString, e.getMessage)
    }
  }

  private[stream] def chasePush(connection: Connection): Unit = {
    if (chaseCounter > 0 && chasedPush == NoEvent) {
      chaseCounter -= 1
      chasedPush = connection
    } else enqueue(connection)
  }

  private[stream] def chasePull(connection: Connection): Unit = {
    if (chaseCounter > 0 && chasedPull == NoEvent) {
      chaseCounter -= 1
      chasedPull = connection
    } else enqueue(connection)
  }

  private[stream] def complete(connection: Connection): Unit = {
    val currentState = connection.portState
    if (Debug) println(s"$Name   complete($connection) [$currentState]")
    connection.portState = currentState | OutClosed

    // Push-Close needs special treatment, cannot be chased, convert back to ordinary event
    if (chasedPush == connection) {
      chasedPush = NoEvent
      enqueue(connection)
    } else if ((currentState & (InClosed | Pushing | Pulling | OutClosed)) == 0) enqueue(connection)

    if ((currentState & OutClosed) == 0) completeConnection(connection.outOwner.stageId)
  }

  @InternalStableApi
  private[stream] def fail(connection: Connection, ex: Throwable): Unit = {
    val currentState = connection.portState
    if (Debug) println(s"$Name   fail($connection, $ex) [$currentState]")
    connection.portState = currentState | OutClosed
    if ((currentState & (InClosed | OutClosed)) == 0) {
      connection.portState = currentState | (OutClosed | InFailed)
      connection.slot = Failed(ex, connection.slot)
      if ((currentState & (Pulling | Pushing)) == 0) enqueue(connection)
      else if (chasedPush eq connection) {
        // Abort chasing so Failure is not lost (chasing does NOT decode the event but assumes it to be a PUSH
        // but we just changed the event!)
        chasedPush = NoEvent
        enqueue(connection)
      }
    }
    if ((currentState & OutClosed) == 0) completeConnection(connection.outOwner.stageId)
  }

  @InternalStableApi
  private[stream] def cancel(connection: Connection, cause: Throwable): Unit = {
    val currentState = connection.portState
    if (Debug) println(s"$Name   cancel($connection) [$currentState]")
    connection.portState = currentState | InClosed
    if ((currentState & OutClosed) == 0) {
      connection.slot = Cancelled(cause)
      if ((currentState & (Pulling | Pushing | InClosed)) == 0) enqueue(connection)
      else if (chasedPull eq connection) {
        // Abort chasing so Cancel is not lost (chasing does NOT decode the event but assumes it to be a PULL
        // but we just changed the event!)
        chasedPull = NoEvent
        enqueue(connection)
      }
    }
    if ((currentState & InClosed) == 0) completeConnection(connection.inOwner.stageId)
  }

  /**
   * Debug utility to dump the "waits-on" relationships in an AST format for rendering in some suitable format for
   * analysis of deadlocks.
   *
   * Only invoke this after the interpreter completely settled, otherwise the results might be off. This is a very
   * simplistic tool, make sure you are understanding what you are doing and then it will serve you well.
   */
  def toSnapshot: RunningInterpreter = {

    val logicSnapshots = logics.zipWithIndex.map {
      case (logic, idx) =>
        LogicSnapshotImpl(idx, logic.toString, logic.attributes)
    }
    val logicIndexes = logics.zipWithIndex.map { case (stage, idx) => stage -> idx }.toMap
    val connectionSnapshots = connections.filter(_ != null).map { connection =>
      ConnectionSnapshotImpl(
        connection.id,
        logicSnapshots(logicIndexes(connection.inOwner)),
        logicSnapshots(logicIndexes(connection.outOwner)),
        connection.portState match {
          case InReady | Pushing                                           => ConnectionSnapshot.ShouldPull
          case OutReady | Pulling                                          => ConnectionSnapshot.ShouldPush
          case x if (x & (InClosed | OutClosed)) == (InClosed | OutClosed) =>
            // At least one side of the connection is closed: we show it as closed
            ConnectionSnapshot.Closed
          case _ =>
            // This should not be possible: connection alive and both push and pull enqueued but not received
            throw new IllegalStateException(s"Unexpected connection state for $connection: ${connection.portState}")

        })
    }

    val stoppedStages: List[LogicSnapshot] = shutdownCounter.zipWithIndex.collect {
      case (activeConnections, idx) if activeConnections < 1 => logicSnapshots(idx)
    }.toList

    RunningInterpreterImpl(
      logicSnapshots.toVector,
      connectionSnapshots.toVector,
      queueStatus,
      runningStages,
      stoppedStages)
  }
}
