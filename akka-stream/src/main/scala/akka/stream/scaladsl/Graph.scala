/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.SplittableRandom

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.{ immutable, mutable }
import scala.concurrent.Promise
import scala.util.control.{ NoStackTrace, NonFatal }

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.impl._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.fusing.GraphStages
import akka.stream.scaladsl.Partition.PartitionOutOfBoundsException
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.util.ConstantFun

/**
 * INTERNAL API
 *
 * The implementation of a graph with an arbitrary shape.
 */
private[stream] final class GenericGraph[S <: Shape, Mat](
    override val shape: S,
    override val traversalBuilder: TraversalBuilder)
    extends Graph[S, Mat] { outer =>

  override def toString: String = s"GenericGraph($shape)"

  override def withAttributes(attr: Attributes): Graph[S, Mat] =
    new GenericGraphWithChangedAttributes(shape, traversalBuilder, attr)

  def mapMaterializedValue[Mat2](f: Mat => Mat2): GenericGraph[S, Mat2] =
    new GenericGraph(shape, traversalBuilder.transformMat(f))
}

/**
 * INTERNAL API
 *
 * The implementation of a graph with an arbitrary shape with changed attributes. Changing attributes again
 * prevents building up a chain of changes.
 */
private[stream] final class GenericGraphWithChangedAttributes[S <: Shape, Mat](
    override val shape: S,
    originalTraversalBuilder: TraversalBuilder,
    newAttributes: Attributes)
    extends Graph[S, Mat] { outer =>

  private[stream] def traversalBuilder: TraversalBuilder = originalTraversalBuilder.setAttributes(newAttributes)

  override def toString: String = s"GenericGraphWithChangedAttributes($shape)"

  override def withAttributes(attr: Attributes): Graph[S, Mat] =
    new GenericGraphWithChangedAttributes(shape, originalTraversalBuilder, attr)
}

object Merge {

  /**
   * Create a new `Merge` with the specified number of input ports.
   *
   * @param inputPorts number of input ports
   * @param eagerComplete if true, the merge will complete as soon as one of its inputs completes.
   */
  def apply[T](inputPorts: Int, eagerComplete: Boolean = false): Merge[T] = new Merge(inputPorts, eagerComplete)

}

/**
 * Merge several streams, taking elements as they arrive from input streams
 * (picking randomly when several have elements ready).
 *
 * '''Emits when''' one of the inputs has an element available
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' all upstreams complete (eagerComplete=false) or one upstream completes (eagerComplete=true), default value is `false`
 *
 * '''Cancels when''' downstream cancels
 */
final class Merge[T](val inputPorts: Int, val eagerComplete: Boolean) extends GraphStage[UniformFanInShape[T, T]] {
  // one input might seem counter intuitive but saves us from special handling in other places
  require(inputPorts >= 1, "A Merge must have one or more input ports")

  val in: immutable.IndexedSeq[Inlet[T]] = Vector.tabulate(inputPorts)(i => Inlet[T]("Merge.in" + i))
  val out: Outlet[T] = Outlet[T]("Merge.out")
  override def initialAttributes = DefaultAttributes.merge
  override val shape: UniformFanInShape[T, T] = UniformFanInShape(out, in: _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {

      private val pendingQueue = FixedSizeBuffer[Inlet[T]](inputPorts)
      private def pending: Boolean = pendingQueue.nonEmpty

      private var runningUpstreams = inputPorts
      private def upstreamsClosed = runningUpstreams == 0

      override def preStart(): Unit = {
        var ix = 0
        while (ix < in.size) {
          tryPull(in(ix))
          ix += 1
        }
      }

      @tailrec
      private def dequeueAndDispatch(): Unit = {
        val in = pendingQueue.dequeue()
        if (in == null) {
          // in is null if we reached the end of the queue
          if (upstreamsClosed) completeStage()
        } else if (isAvailable(in)) {
          push(out, grab(in))
          if (upstreamsClosed && !pending) completeStage()
          else tryPull(in)
        } else {
          // in was closed after being enqueued
          // try next in queue
          dequeueAndDispatch()
        }
      }

      var ix = 0
      while (ix < in.size) {
        val i = in(ix)
        ix += 1

        setHandler(
          i,
          new InHandler {
            override def onPush(): Unit = {
              if (isAvailable(out)) {
                // isAvailable(out) implies !pending
                // -> grab and push immediately
                push(out, grab(i))
                tryPull(i)
              } else pendingQueue.enqueue(i)
            }

            override def onUpstreamFinish() =
              if (eagerComplete) {
                var ix2 = 0
                while (ix2 < in.size) {
                  cancel(in(ix2))
                  ix2 += 1
                }
                runningUpstreams = 0
                if (!pending) completeStage()
              } else {
                runningUpstreams -= 1
                if (upstreamsClosed && !pending) completeStage()
              }
          })
      }

      override def onPull(): Unit = {
        if (pending)
          dequeueAndDispatch()
      }

      setHandler(out, this)
    }

  override def toString = "Merge"
}

object MergePreferred {
  import FanInShape._
  final class MergePreferredShape[T](val secondaryPorts: Int, _init: Init[T])
      extends UniformFanInShape[T, T](secondaryPorts, _init) {
    def this(secondaryPorts: Int, name: String) = this(secondaryPorts, Name[T](name))
    override protected def construct(init: Init[T]): FanInShape[T] = new MergePreferredShape(secondaryPorts, init)
    override def deepCopy(): MergePreferredShape[T] = super.deepCopy().asInstanceOf[MergePreferredShape[T]]

    val preferred = newInlet[T]("preferred")
  }

  /**
   * Create a new `MergePreferred` with the specified number of secondary input ports.
   *
   * @param secondaryPorts number of secondary input ports
   * @param eagerComplete if true, the merge will complete as soon as one of its inputs completes.
   */
  def apply[T](secondaryPorts: Int, eagerComplete: Boolean = false): MergePreferred[T] =
    new MergePreferred(secondaryPorts, eagerComplete)
}

/**
 * Merge several streams, taking elements as they arrive from input streams
 * (picking from preferred when several have elements ready).
 *
 * A `MergePreferred` has one `out` port, one `preferred` input port and 1 or more secondary `in` ports.
 *
 * '''Emits when''' one of the inputs has an element available, preferring
 * a specified input if multiple have elements available
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' all upstreams complete (eagerComplete=false) or one upstream completes (eagerComplete=true), default value is `false`
 *
 * '''Cancels when''' downstream cancels
 */
final class MergePreferred[T](val secondaryPorts: Int, val eagerComplete: Boolean)
    extends GraphStage[MergePreferred.MergePreferredShape[T]] {
  require(secondaryPorts >= 1, "A MergePreferred must have 1 or more secondary input ports")

  override def initialAttributes = DefaultAttributes.mergePreferred
  override val shape: MergePreferred.MergePreferredShape[T] =
    new MergePreferred.MergePreferredShape(secondaryPorts, "MergePreferred")

  def in(id: Int): Inlet[T] = shape.in(id)
  def out: Outlet[T] = shape.out
  def preferred: Inlet[T] = shape.preferred

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var openInputs = secondaryPorts + 1
    def onComplete(): Unit = {
      openInputs -= 1
      if (eagerComplete || openInputs == 0) completeStage()
    }

    override def preStart(): Unit = {
      //while initializing this `MergePreferredShape`, the `preferred` port gets added to `inlets` by side-effect.
      shape.inlets.foreach(tryPull)
    }

    setHandler(out, eagerTerminateOutput)

    val pullMe = Array.tabulate(secondaryPorts)(i => {
      val port = in(i)
      () => tryPull(port)
    })

    /*
     * This determines the unfairness of the merge:
     * - at 1 the preferred will grab 40% of the bandwidth against three equally fast secondaries
     * - at 2 the preferred will grab almost all bandwidth against three equally fast secondaries
     * (measured with eventLimit=1 in the GraphInterpreter, so may not be accurate)
     */
    val maxEmitting = 2
    var preferredEmitting = 0

    setHandler(
      preferred,
      new InHandler {
        override def onUpstreamFinish(): Unit = onComplete()
        override def onPush(): Unit =
          if (preferredEmitting == maxEmitting) () // blocked
          else emitPreferred()

        def emitPreferred(): Unit = {
          preferredEmitting += 1
          emit(out, grab(preferred), emitted)
          tryPull(preferred)
        }

        val emitted = () => {
          preferredEmitting -= 1
          if (isAvailable(preferred)) emitPreferred()
          else if (preferredEmitting == 0) emitSecondary()
        }

        def emitSecondary(): Unit = {
          var i = 0
          while (i < secondaryPorts) {
            val port = in(i)
            if (isAvailable(port)) emit(out, grab(port), pullMe(i))
            i += 1
          }
        }
      })

    var i = 0
    while (i < secondaryPorts) {
      val port = in(i)
      val pullPort = pullMe(i)
      setHandler(
        port,
        new InHandler {
          override def onPush(): Unit = {
            if (preferredEmitting > 0) () // blocked
            else {
              emit(out, grab(port), pullPort)
            }
          }
          override def onUpstreamFinish(): Unit = onComplete()
        })
      i += 1
    }

  }
}

object MergePrioritized {

  /**
   * Create a new `MergePrioritized` with specified number of input ports.
   *
   * @param priorities priorities of the input ports
   * @param eagerComplete if true, the merge will complete as soon as one of its inputs completes.
   */
  def apply[T](priorities: Seq[Int], eagerComplete: Boolean = false): GraphStage[UniformFanInShape[T, T]] =
    new MergePrioritized(priorities, eagerComplete)
}

/**
 * Merge several streams, taking elements as they arrive from input streams
 * (picking from prioritized once when several have elements ready).
 *
 * A `MergePrioritized` has one `out` port, one or more input port with their priorities.
 *
 * '''Emits when''' one of the inputs has an element available, preferring
 * a input based on its priority if multiple have elements available
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' all upstreams complete (eagerComplete=false) or one upstream completes (eagerComplete=true), default value is `false`
 *
 * '''Cancels when''' downstream cancels
 */
final class MergePrioritized[T] private (val priorities: Seq[Int], val eagerComplete: Boolean)
    extends GraphStage[UniformFanInShape[T, T]] {
  require(priorities.nonEmpty, "A Merge must have one or more input ports")
  require(priorities.forall(_ > 0), "Priorities should be positive integers")

  val in: immutable.IndexedSeq[Inlet[T]] = Vector.tabulate(priorities.size)(i => Inlet[T]("MergePrioritized.in" + i))
  val out: Outlet[T] = Outlet[T]("MergePrioritized.out")
  override def initialAttributes: Attributes = DefaultAttributes.mergePrioritized
  override val shape: UniformFanInShape[T, T] = UniformFanInShape(out, in: _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private var runningUpstreams = priorities.size
      private val randomGen = new SplittableRandom

      override def preStart(): Unit = in.foreach(tryPull)

      in.foreach { inlet =>
        setHandler(
          inlet,
          new InHandler {
            override def onPush(): Unit = {
              if (isAvailable(out) && !hasOtherInletAvailable(inlet)) {
                push(out, grab(inlet))
                tryPull(inlet)
              }
            }

            override def onUpstreamFinish(): Unit = {
              if (eagerComplete) {
                in.foreach(cancel(_))
                runningUpstreams = 0
                if (!hasPending) completeStage()
              } else {
                runningUpstreams -= 1
                if (upstreamsClosed && !hasPending) completeStage()
              }
            }
          })
      }

      override def onPull(): Unit = {
        val in = select()
        if (in ne null) {
          push(out, grab(in))
          if (upstreamsClosed && !hasPending)
            completeStage()
          else
            tryPull(in)
        }
      }

      setHandler(out, this)

      private def hasPending: Boolean = in.exists(inlet => isAvailable(inlet))

      private def hasOtherInletAvailable(excludeInlet: Inlet[T]): Boolean =
        in.exists(inlet => (inlet ne excludeInlet) && isAvailable(inlet))

      private def upstreamsClosed: Boolean = runningUpstreams == 0

      private def select(): Inlet[T] = {
        var tp = 0
        var ix = 0

        while (ix < in.length) {
          if (isAvailable(in(ix))) {
            tp += priorities(ix)
          }
          ix += 1
        }

        if (tp == 0) {
          // no inlets are available
          null.asInstanceOf[Inlet[T]]
        } else {
          var r = randomGen.nextInt(tp)
          var next: Inlet[T] = null
          ix = 0

          while (ix < in.length && next == null) {
            if (isAvailable(in(ix))) {
              r -= priorities(ix)
              if (r < 0) next = in(ix)
            }
            ix += 1
          }

          next
        }
      }
    }

  override def toString = "MergePrioritized"
}

object Interleave {

  /**
   * Create a new `Interleave` with the specified number of input ports and given size of elements
   * to take from each input.
   *
   * @param inputPorts number of input ports
   * @param segmentSize number of elements to send downstream before switching to next input port
   * @param eagerClose if true, interleave completes upstream if any of its upstream completes.
   */
  def apply[T](
      inputPorts: Int,
      segmentSize: Int,
      eagerClose: Boolean = false): Graph[UniformFanInShape[T, T], NotUsed] =
    GraphStages.withDetachedInputs(new Interleave[T](inputPorts, segmentSize, eagerClose))
}

/**
 * Interleave represents deterministic merge which takes N elements per input stream,
 * in-order of inputs, emits them downstream and then cycles/"wraps-around" the inputs.
 *
 * '''Emits when''' element is available from current input (depending on phase)
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' all upstreams complete (eagerClose=false) or one upstream completes (eagerClose=true)
 *
 * '''Cancels when''' downstream cancels
 *
 */
final class Interleave[T](val inputPorts: Int, val segmentSize: Int, val eagerClose: Boolean)
    extends GraphStage[UniformFanInShape[T, T]] {
  require(inputPorts > 1, "input ports must be > 1")
  require(segmentSize > 0, "segmentSize must be > 0")

  val in: immutable.IndexedSeq[Inlet[T]] = Vector.tabulate(inputPorts)(i => Inlet[T]("Interleave.in" + i))
  val out: Outlet[T] = Outlet[T]("Interleave.out")
  override val shape: UniformFanInShape[T, T] = UniformFanInShape(out, in: _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private var counter = 0
      private var currentUpstreamIndex = 0
      private var runningUpstreams = inputPorts
      private def upstreamsClosed = runningUpstreams == 0
      private def currentUpstream = in(currentUpstreamIndex)

      private def switchToNextInput(): Unit = {
        @tailrec
        def nextInletIndex(index: Int): Int = {
          val successor = index + 1 match {
            case `inputPorts` => 0
            case x            => x
          }
          if (!isClosed(in(successor))) successor
          else {
            if (successor != currentUpstreamIndex) nextInletIndex(successor)
            else {
              completeStage()
              0 // return dummy/min value to exit stage logic gracefully
            }
          }
        }
        counter = 0
        currentUpstreamIndex = nextInletIndex(currentUpstreamIndex)
      }

      in.foreach { i =>
        setHandler(
          i,
          new InHandler {
            override def onPush(): Unit = {
              push(out, grab(i))
              counter += 1
              if (counter == segmentSize) switchToNextInput()
            }

            override def onUpstreamFinish(): Unit = {
              if (!eagerClose) {
                runningUpstreams -= 1
                if (!upstreamsClosed) {
                  if (i == currentUpstream) {
                    switchToNextInput()
                    if (isAvailable(out)) pull(currentUpstream)
                  }
                } else completeStage()
              } else completeStage()
            }
          })
      }

      def onPull(): Unit =
        if (!hasBeenPulled(currentUpstream)) tryPull(currentUpstream)

      setHandler(out, this)
    }

  override def toString = "Interleave"
}

/**
 * Merge two pre-sorted streams such that the resulting stream is sorted.
 *
 * '''Emits when''' both inputs have an element available
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' all upstreams complete
 *
 * '''Cancels when''' downstream cancels
 */
final class MergeSorted[T: Ordering] extends GraphStage[FanInShape2[T, T, T]] {
  private val left = Inlet[T]("left")
  private val right = Inlet[T]("right")
  private val out = Outlet[T]("out")

  override val shape = new FanInShape2(left, right, out)

  override def createLogic(attr: Attributes) = new GraphStageLogic(shape) {
    import Ordering.Implicits._
    setHandler(left, ignoreTerminateInput)
    setHandler(right, ignoreTerminateInput)
    setHandler(out, eagerTerminateOutput)

    var other: T = _
    def nullOut(): Unit = other = null.asInstanceOf[T]

    def dispatch(l: T, r: T): Unit =
      if (l < r) {
        other = r; emit(out, l, readL)
      } else {
        other = l; emit(out, r, readR)
      }

    val dispatchR = dispatch(other, _: T)
    val dispatchL = dispatch(_: T, other)
    val passR = () => emit(out, other, () => { nullOut(); passAlong(right, out, doPull = true) })
    val passL = () => emit(out, other, () => { nullOut(); passAlong(left, out, doPull = true) })
    val readR = () => read(right)(dispatchR, passL)
    val readL = () => read(left)(dispatchL, passR)

    override def preStart(): Unit = {
      // all fan-in stages need to eagerly pull all inputs to get cycles started
      pull(right)
      read(left)(l => {
        other = l
        readR()
      }, () => passAlong(right, out))
    }
  }
}

object Broadcast {

  /**
   * Create a new `Broadcast` with the specified number of output ports.
   *
   * @param outputPorts number of output ports
   * @param eagerCancel if true, broadcast cancels upstream if any of its downstreams cancel.
   */
  def apply[T](outputPorts: Int, eagerCancel: Boolean = false): Broadcast[T] =
    new Broadcast(outputPorts, eagerCancel)
}

/**
 * Fan-out the stream to several streams emitting each incoming upstream element to all downstream consumers.
 * It will not shut down until the subscriptions for at least two downstream subscribers have been established.
 *
 * '''Emits when''' all of the outputs stops backpressuring and there is an input element available
 *
 * '''Backpressures when''' any of the outputs backpressure
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when'''
 *   If eagerCancel is enabled: when any downstream cancels; otherwise: when all downstreams cancel
 *
 */
final class Broadcast[T](val outputPorts: Int, val eagerCancel: Boolean) extends GraphStage[UniformFanOutShape[T, T]] {
  // one output might seem counter intuitive but saves us from special handling in other places
  require(outputPorts >= 1, "A Broadcast must have one or more output ports")
  val in: Inlet[T] = Inlet[T]("Broadcast.in")
  val out: immutable.IndexedSeq[Outlet[T]] = Vector.tabulate(outputPorts)(i => Outlet[T]("Broadcast.out" + i))
  override def initialAttributes = DefaultAttributes.broadcast
  override val shape: UniformFanOutShape[T, T] = UniformFanOutShape(in, out: _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler {
      private var pendingCount = outputPorts
      private val pending = Array.fill[Boolean](outputPorts)(true)
      private var downstreamsRunning = outputPorts

      def onPush(): Unit = {
        pendingCount = downstreamsRunning
        val elem = grab(in)

        val size = out.size
        var idx = 0
        while (idx < size) {
          val o = out(idx)
          if (!isClosed(o)) {
            push(o, elem)
            pending(idx) = true
          }
          idx += 1
        }
      }

      setHandler(in, this)

      private def tryPull(): Unit =
        if (pendingCount == 0 && !hasBeenPulled(in)) pull(in)

      {
        val size = out.size
        var idx = 0
        while (idx < size) {
          val o = out(idx)
          val i = idx // close over val
          setHandler(
            o,
            new OutHandler {
              override def onPull(): Unit = {
                pending(i) = false
                pendingCount -= 1
                tryPull()
              }

              override def onDownstreamFinish(cause: Throwable) = {
                if (eagerCancel) cancelStage(cause)
                else {
                  downstreamsRunning -= 1
                  if (downstreamsRunning == 0) cancelStage(cause)
                  else if (pending(i)) {
                    pending(i) = false
                    pendingCount -= 1
                    tryPull()
                  }
                }
              }
            })
          idx += 1
        }
      }

    }

  override def toString = "Broadcast"

}

object WireTap {
  private val singleton = new WireTap[Nothing]

  /**
   * @see [[WireTap]]
   */
  def apply[T](): WireTap[T] = singleton.asInstanceOf[WireTap[T]]
}

/**
 * Fan-out the stream to two output streams - a 'main' and a 'tap' one. Each incoming element is emitted
 * to the 'main' output; elements are also emitted to the 'tap' output if there is demand;
 * otherwise they are dropped.
 *
 * '''Emits when''' element is available and demand exists from the 'main' output; the element will
 * also be sent to the 'tap' output if there is demand.
 *
 * '''Backpressures when''' the 'main' output backpressures
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when''' the 'main' output cancels
 *
 */
@InternalApi
private[stream] final class WireTap[T] extends GraphStage[FanOutShape2[T, T, T]] {
  val in: Inlet[T] = Inlet[T]("WireTap.in")
  val outMain: Outlet[T] = Outlet[T]("WireTap.outMain")
  val outTap: Outlet[T] = Outlet[T]("WireTap.outTap")
  override def initialAttributes: Attributes = DefaultAttributes.wireTap
  override val shape: FanOutShape2[T, T, T] = new FanOutShape2(in, outMain, outTap)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var pendingTap: Option[T] = None

      override def onPush(): Unit = {
        val elem = grab(in)
        push(outMain, elem)
        if (isAvailable(outTap)) {
          push(outTap, elem)
        } else {
          pendingTap = Some(elem)
        }
      }
      override def onPull(): Unit = {
        pull(in)
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        cancelStage(cause)
      }

      // The 'tap' output can neither backpressure, nor cancel, the stage.
      setHandler(
        outTap,
        new OutHandler {
          override def onPull() = {
            pendingTap match {
              case Some(elem) =>
                push(outTap, elem)
                pendingTap = None
              case None => // no pending element to emit
            }
          }

          override def onDownstreamFinish(cause: Throwable): Unit = {
            setHandler(in, new InHandler {
              override def onPush() = {
                push(outMain, grab(in))
              }
            })
            // Allow any outstanding element to be garbage-collected
            pendingTap = None
          }
        })

      setHandlers(in, outMain, this)

    }
  override def toString = "WireTap"
}

object Partition {
  // FIXME make `PartitionOutOfBoundsException` a `final` class when possible
  case class PartitionOutOfBoundsException(msg: String) extends IndexOutOfBoundsException(msg) with NoStackTrace

  /**
   * Create a new `Partition` operator with the specified input type. This method sets `eagerCancel` to `false`.
   * To specify a different value for the `eagerCancel` parameter, then instantiate Partition using the constructor.
   *
   * If `eagerCancel` is true, partition cancels upstream if any of its downstreams cancel, if false, when all have cancelled.
   *
   * @param outputPorts number of output ports
   * @param partitioner function deciding which output each element will be targeted
   */ // FIXME BC add `eagerCancel: Boolean = false` parameter
  def apply[T](outputPorts: Int, partitioner: T => Int): Partition[T] =
    new Partition(outputPorts, partitioner, eagerCancel = false)
}

/**
 * Fan-out the stream to several streams. emitting an incoming upstream element to one downstream consumer according
 * to the partitioner function applied to the element
 *
 * Adheres to the [[ActorAttributes.SupervisionStrategy]] attribute.
 *
 * '''Emits when''' emits when an element is available from the input and the chosen output has demand
 *
 * '''Backpressures when''' the currently chosen output back-pressures
 *
 * '''Completes when''' upstream completes and no output is pending
 *
 * '''Cancels when''' all downstreams have cancelled (eagerCancel=false) or one downstream cancels (eagerCancel=true)
 */
final class Partition[T](val outputPorts: Int, val partitioner: T => Int, val eagerCancel: Boolean)
    extends GraphStage[UniformFanOutShape[T, T]] {

  val in: Inlet[T] = Inlet[T]("Partition.in")
  val out: Seq[Outlet[T]] = Seq.tabulate(outputPorts)(i => Outlet[T]("Partition.out" + i)) // FIXME BC make this immutable.IndexedSeq as type + Vector as concrete impl
  override val shape: UniformFanOutShape[T, T] = UniformFanOutShape[T, T](in, out: _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler {
      lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
      private var outPendingElem: Any = null
      private var outPendingIdx: Int = _
      private var downstreamRunning = outputPorts

      def onPush() = {
        val elem = grab(in)
        val idx =
          try {
            val i = partitioner(elem)
            if (i < 0 || i >= outputPorts)
              throw PartitionOutOfBoundsException(
                s"partitioner must return an index in the range [0,${outputPorts - 1}]. returned: [$i] for input " +
                s"[${elem.getClass.getName}].")
            else i
          } catch {
            case NonFatal(ex) =>
              decider(ex) match {
                case Supervision.Stop    => failStage(ex)
                case Supervision.Restart => pull(in)
                case Supervision.Resume  => pull(in)
              }
              Int.MinValue
          }

        if (idx != Int.MinValue) {
          if (!isClosed(out(idx))) {
            if (isAvailable(out(idx))) {
              push(out(idx), elem)
              pullIfAnyOutIsAvailable()
            } else {
              outPendingElem = elem
              outPendingIdx = idx
            }

          } else
            pullIfAnyOutIsAvailable()
        }
      }

      private def pullIfAnyOutIsAvailable(): Unit = {
        if (out.exists(isAvailable(_)))
          pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        if (outPendingElem == null) completeStage()
      }

      setHandler(in, this)

      out.iterator.zipWithIndex.foreach {
        case (o, idx) =>
          setHandler(
            o,
            new OutHandler {
              override def onPull() = {
                if (outPendingElem != null) {
                  val elem = outPendingElem.asInstanceOf[T]
                  if (idx == outPendingIdx) {
                    push(o, elem)
                    outPendingElem = null
                    if (isClosed(in))
                      completeStage()
                    else if (!hasBeenPulled(in))
                      pull(in)
                  }
                } else if (!hasBeenPulled(in))
                  pull(in)
              }

              override def onDownstreamFinish(cause: Throwable) =
                if (eagerCancel) cancelStage(cause)
                else {
                  downstreamRunning -= 1
                  if (downstreamRunning == 0)
                    cancelStage(cause)
                  else if (outPendingElem != null && idx == outPendingIdx) {
                    outPendingElem = null
                    if (isClosed(in))
                      cancelStage(cause)
                    else if (!hasBeenPulled(in))
                      pull(in)
                  }
                }
            })
      }
    }

  override def toString = s"Partition($outputPorts)"
}

object Balance {

  /**
   * Create a new `Balance` with the specified number of output ports. This method sets `eagerCancel` to `false`.
   * To specify a different value for the `eagerCancel` parameter, then instantiate Balance using the constructor.
   *
   * If `eagerCancel` is true, balance cancels upstream if any of its downstreams cancel, if false, when all have cancelled.
   *
   * @param outputPorts number of output ports
   * @param waitForAllDownstreams if you use `waitForAllDownstreams = true` it will not start emitting
   *   elements to downstream outputs until all of them have requested at least one element,
   *   default value is `false`
   */
  def apply[T](outputPorts: Int, waitForAllDownstreams: Boolean = false): Balance[T] =
    new Balance(outputPorts, waitForAllDownstreams, eagerCancel = false)
}

/**
 * Fan-out the stream to several streams. Each upstream element is emitted to the first available downstream consumer.
 * It will not shut down until the subscriptions
 * for at least two downstream subscribers have been established.
 *
 * A `Balance` has one `in` port and 2 or more `out` ports.
 *
 * '''Emits when''' any of the outputs stops backpressuring; emits the element to the first available output
 *
 * '''Backpressures when''' all of the outputs backpressure
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when''' If eagerCancel is enabled: when any downstream cancels; otherwise: when all downstreams cancel
 */
final class Balance[T](val outputPorts: Int, val waitForAllDownstreams: Boolean, val eagerCancel: Boolean)
    extends GraphStage[UniformFanOutShape[T, T]] {
  // one output might seem counter intuitive but saves us from special handling in other places
  require(outputPorts >= 1, "A Balance must have one or more output ports")

  val in: Inlet[T] = Inlet[T]("Balance.in")
  val out: immutable.IndexedSeq[Outlet[T]] = Vector.tabulate(outputPorts)(i => Outlet[T]("Balance.out" + i))
  override def initialAttributes = DefaultAttributes.balance
  override val shape: UniformFanOutShape[T, T] = UniformFanOutShape[T, T](in, out: _*)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler {
      private val pendingQueue = FixedSizeBuffer[Outlet[T]](outputPorts)
      private def noPending: Boolean = pendingQueue.isEmpty

      private var needDownstreamPulls: Int = if (waitForAllDownstreams) outputPorts else 0
      private var downstreamsRunning: Int = outputPorts

      @tailrec
      private def dequeueAndDispatch(): Unit = {
        val out = pendingQueue.dequeue()
        // out is null if depleted pendingQueue without reaching
        // an out that is not closed, in which case we just return
        if (out ne null) {
          if (!isClosed(out)) {
            push(out, grab(in))
            if (!noPending) pull(in)
          } else if (!noPending) {
            // if they are pending outlets, try to find one output that isn't closed
            dequeueAndDispatch()
          }
        }
      }

      def onPush(): Unit = dequeueAndDispatch()
      setHandler(in, this)

      out.foreach { o =>
        setHandler(
          o,
          new OutHandler {
            private var hasPulled = false

            override def onPull(): Unit = {
              if (!hasPulled) {
                hasPulled = true
                if (needDownstreamPulls > 0) needDownstreamPulls -= 1
              }

              if (needDownstreamPulls == 0) {
                if (isAvailable(in)) {
                  if (noPending) {
                    push(o, grab(in))
                  }
                } else {
                  if (!hasBeenPulled(in)) pull(in)
                  pendingQueue.enqueue(o)
                }
              } else pendingQueue.enqueue(o)
            }

            override def onDownstreamFinish(cause: Throwable) = {
              if (eagerCancel) cancelStage(cause)
              else {
                downstreamsRunning -= 1
                if (downstreamsRunning == 0) cancelStage(cause)
                else if (!hasPulled && needDownstreamPulls > 0) {
                  needDownstreamPulls -= 1
                  if (needDownstreamPulls == 0 && !hasBeenPulled(in)) pull(in)
                }
              }
            }
          })
      }
    }

  override def toString = "Balance"
}

object Zip {

  /**
   * Create a new `Zip`.
   */
  def apply[A, B](): Zip[A, B] = new Zip()
}

/**
 * Combine the elements of 2 streams into a stream of tuples.
 *
 * A `Zip` has a `left` and a `right` input port and one `out` port
 *
 * '''Emits when''' all of the inputs has an element available
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' any upstream completes
 *
 * '''Cancels when''' downstream cancels
 */
final class Zip[A, B] extends ZipWith2[A, B, (A, B)](Tuple2.apply) {
  override def toString = "Zip"
}

object ZipLatest {

  /**
   * Create a new `ZipLatest`.
   */
  def apply[A, B](): ZipLatest[A, B] = new ZipLatest()

  /**
   * Create a new `ZipLatest`.
   */
  def apply[A, B](eagerComplete: Boolean): ZipLatest[A, B] = new ZipLatest(eagerComplete)
}

/**
 * Combine the elements of 2 streams into a stream of tuples, picking always the latest element of each.
 *
 * A `ZipLatest` has a `left` and a `right` input port and one `out` port.
 *
 * No element is emitted until at least one element from each Source becomes available.
 *
 * '''Emits when''' all of the inputs have at least an element available, and then each time an element becomes
 * *   available on either of the inputs
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' any upstream completes if `eagerComplete` is enabled or wait for all upstreams to complete
 *
 * '''Cancels when''' downstream cancels
 */
final class ZipLatest[A, B](eagerComplete: Boolean) extends ZipLatestWith2[A, B, (A, B)](Tuple2.apply, eagerComplete) {

  def this() = this(eagerComplete = true)

  override def toString = "ZipLatest"
}

/**
 * Combine the elements of multiple streams into a stream of combined elements using a combiner function.
 *
 * '''Emits when''' all of the inputs has an element available
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' any upstream completes
 *
 * '''Cancels when''' downstream cancels
 */
object ZipWith extends ZipWithApply

/**
 * Combine the elements of multiple streams into a stream of combined elements using a combiner function,
 * picking always the latest of the elements of each source. The combined stream completes immediately if
 * some upstreams have already completed while some upstreams did not emitted any value yet.
 * If all upstreams produced some value and the optional parameter `eagerComplete` is true (default),
 * the combined stream completes when any of the upstreams completes, otherwise, the combined stream
 * will wait for all upstreams to complete.
 *
 * No element is emitted until at least one element from each Source becomes available. Whenever a new
 * element appears, the zipping function is invoked with a tuple containing the new element
 * and the other last seen elements.
 *
 *   '''Emits when''' all of the inputs have at least an element available, and then each time an element becomes
 *   available on either of the inputs
 *
 *   '''Backpressures when''' downstream backpressures
 *
 *   '''Completes when''' any of the upstreams completes
 *
 *   '''Cancels when''' downstream cancels
 */
object ZipLatestWith extends ZipLatestWithApply

/**
 * Takes a stream of pair elements and splits each pair to two output streams.
 *
 * An `Unzip` has one `in` port and one `left` and one `right` output port.
 *
 * '''Emits when''' all of the outputs stop backpressuring and there is an input element available
 *
 * '''Backpressures when''' any of the outputs backpressure
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when''' any downstream cancels
 */
object Unzip {

  /**
   * Create a new `Unzip`.
   */
  def apply[A, B](): Unzip[A, B] = new Unzip()
}

/**
 * Takes a stream of pair elements and splits each pair to two output streams.
 *
 * An `Unzip` has one `in` port and one `left` and one `right` output port.
 *
 * '''Emits when''' all of the outputs stop backpressuring and there is an input element available
 *
 * '''Backpressures when''' any of the outputs backpressure
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when''' any downstream cancels
 */
final class Unzip[A, B]() extends UnzipWith2[(A, B), A, B](ConstantFun.scalaIdentityFunction) {
  override def toString = "Unzip"
}

/**
 * Transforms each element of input stream into multiple streams using a splitter function.
 *
 * '''Emits when''' all of the outputs stop backpressuring and there is an input element available
 *
 * '''Backpressures when''' any of the outputs backpressure
 *
 * '''Completes when''' upstream completes
 *
 * '''Cancels when''' any downstream cancels
 */
object UnzipWith extends UnzipWithApply

object ZipN {

  /**
   * Create a new `ZipN`.
   */
  def apply[A](n: Int) = new ZipN[A](n)
}

/**
 * Combine the elements of multiple streams into a stream of sequences.
 *
 * A `ZipN` has a `n` input ports and one `out` port
 *
 * '''Emits when''' all of the inputs has an element available
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' any upstream completes
 *
 * '''Cancels when''' downstream cancels
 */
final class ZipN[A](n: Int) extends ZipWithN[A, immutable.Seq[A]](ConstantFun.scalaIdentityFunction)(n) {
  override def initialAttributes = DefaultAttributes.zipN
  override def toString = "ZipN"
}

object ZipWithN {

  /**
   * Create a new `ZipWithN`.
   */
  def apply[A, O](zipper: immutable.Seq[A] => O)(n: Int) = new ZipWithN[A, O](zipper)(n)
}

/**
 * Combine the elements of multiple streams into a stream of sequences using a combiner function.
 *
 * A `ZipWithN` has a `n` input ports and one `out` port
 *
 * '''Emits when''' all of the inputs has an element available
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' any upstream completes
 *
 * '''Cancels when''' downstream cancels
 */
class ZipWithN[A, O](zipper: immutable.Seq[A] => O)(n: Int) extends GraphStage[UniformFanInShape[A, O]] {
  override def initialAttributes = DefaultAttributes.zipWithN
  override val shape = new UniformFanInShape[A, O](n)
  def out: Outlet[O] = shape.out

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      var pending = 0
      // Without this field the completion signalling would take one extra pull
      var willShutDown = false

      private val contextPropagation = ContextPropagation()

      val grabInlet = grab[A] _
      val pullInlet = pull[A] _

      private def pushAll(): Unit = {
        contextPropagation.resumeContext()
        push(out, zipper(shape.inlets.map(grabInlet)))
        if (willShutDown) completeStage()
        else shape.inlets.foreach(pullInlet)
      }

      override def preStart(): Unit = {
        shape.inlets.foreach(pullInlet)
      }

      shape.inlets.zipWithIndex.foreach {
        case (in, i) =>
          setHandler(in, new InHandler {
            override def onPush(): Unit = {
              // Only one context can be propagated. Picked the first element as an arbitrary but deterministic choice.
              if (i == 0) contextPropagation.suspendContext()
              pending -= 1
              if (pending == 0) pushAll()
            }

            override def onUpstreamFinish(): Unit = {
              if (!isAvailable(in)) completeStage()
              willShutDown = true
            }
          })
      }

      def onPull(): Unit = {
        pending += n
        if (pending == 0) pushAll()
      }

      setHandler(out, this)
    }

  override def toString = "ZipWithN"
}

object Concat {

  // two streams is so common that we can re-use a single instance to avoid some allocations
  private val _concatTwo = new Concat[Any](2)
  private def concatTwo[T]: GraphStage[UniformFanInShape[T, T]] =
    _concatTwo.asInstanceOf[GraphStage[UniformFanInShape[T, T]]]

  /**
   * Create a new `Concat`. Note that this for historical reasons creates a "detached" Concat which
   * will eagerly pull each input on materialization and act as a one element buffer for each input.
   */
  def apply[T](inputPorts: Int = 2): Graph[UniformFanInShape[T, T], NotUsed] =
    apply(inputPorts, detachedInputs = true)

  /**
   * Create a new `Concat` operator that will concatenate two or more streams.
   * @param inputPorts The number of fan-in input ports
   * @param detachedInputs If the ports should be detached (eagerly pull both inputs) useful to avoid deadlocks in graphs with loops
   * @return
   */
  def apply[T](inputPorts: Int, detachedInputs: Boolean): Graph[UniformFanInShape[T, T], NotUsed] = {
    val concat = {
      if (inputPorts == 2) concatTwo[T]
      else new Concat[T](inputPorts)
    }

    if (detachedInputs) GraphStages.withDetachedInputs(concat)
    else concat
  }

}

/**
 * Takes multiple streams and outputs one stream formed from the input streams
 * by first emitting all of the elements from the first stream and then emitting
 * all of the elements from the second stream, etc.
 *
 * A `Concat` has one `first` port, one `second` port and one `out` port.
 *
 * '''Emits when''' the current stream has an element available; if the current input completes, it tries the next one
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' all upstreams complete
 *
 * '''Cancels when''' downstream cancels
 */
final class Concat[T](val inputPorts: Int) extends GraphStage[UniformFanInShape[T, T]] {
  require(inputPorts > 1, "A Concat must have more than 1 input ports")
  val in: immutable.IndexedSeq[Inlet[T]] = Vector.tabulate(inputPorts)(i => Inlet[T]("Concat.in" + i))
  val out: Outlet[T] = Outlet[T]("Concat.out")
  override def initialAttributes = DefaultAttributes.concat
  override val shape: UniformFanInShape[T, T] = UniformFanInShape(out, in: _*)

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) with OutHandler {
    var activeStream: Int = 0

    {
      var idxx = 0
      val size = in.size
      while (idxx < size) {
        val i = in(idxx)
        val idx = idxx // close over val
        setHandler(
          i,
          new InHandler {
            override def onPush() = {
              push(out, grab(i))
            }

            override def onUpstreamFinish() = {
              if (idx == activeStream) {
                activeStream += 1
                // Skip closed inputs
                while (activeStream < inputPorts && isClosed(in(activeStream))) activeStream += 1
                if (activeStream == inputPorts) completeStage()
                else if (isAvailable(out)) pull(in(activeStream))
              }
            }
          })
        idxx += 1
      }
    }

    def onPull() = pull(in(activeStream))

    setHandler(out, this)
  }

  override def toString: String = s"Concat($inputPorts)"
}

object OrElse {
  private val singleton = new OrElse[Nothing]

  /**
   * @see [[OrElse]]
   */
  def apply[T]() = singleton.asInstanceOf[OrElse[T]]
}

/**
 * Takes two streams and passes the first through, the secondary stream is only passed
 * through if the primary stream completes without passing any elements through. When
 * the first element is passed through from the primary the secondary is cancelled.
 * Both incoming streams are materialized when the operator is materialized.
 *
 * On errors the operator is failed regardless of source of the error.
 *
 * '''Emits when''' element is available from primary stream or the primary stream closed without emitting any elements and an element
 *                  is available from the secondary stream
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' the primary stream completes after emitting at least one element, when the primary stream completes
 *                      without emitting and the secondary stream already has completed or when the secondary stream completes
 *
 * '''Cancels when''' downstream cancels
 */
@InternalApi
private[stream] final class OrElse[T] extends GraphStage[UniformFanInShape[T, T]] {
  val primary = Inlet[T]("OrElse.primary")
  val secondary = Inlet[T]("OrElse.secondary")
  val out = Outlet[T]("OrElse.out")

  override val shape: UniformFanInShape[T, T] = UniformFanInShape(out, primary, secondary)

  override protected def initialAttributes: Attributes = DefaultAttributes.orElse

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler with InHandler {

      private[this] var currentIn = primary
      private[this] var primaryPushed = false

      override def onPull(): Unit = {
        pull(currentIn)
      }

      // for the primary inHandler
      override def onPush(): Unit = {
        if (!primaryPushed) {
          primaryPushed = true
          cancel(secondary)
        }
        val elem = grab(primary)
        push(out, elem)
      }

      // for the primary inHandler
      override def onUpstreamFinish(): Unit = {
        if (!primaryPushed && !isClosed(secondary)) {
          currentIn = secondary
          if (isAvailable(out)) pull(secondary)
        } else {
          completeStage()
        }
      }

      setHandler(secondary, new InHandler {
        override def onPush(): Unit = {
          push(out, grab(secondary))
        }

        override def onUpstreamFinish(): Unit = {
          if (isClosed(primary)) completeStage()
        }
      })

      setHandlers(primary, out, this)
    }

  override def toString: String = s"OrElse"

}

object MergeSequence {

  private case class Pushed[T](in: Inlet[T], sequence: Long, elem: T)

  private implicit def ordering[T]: Ordering[Pushed[T]] = Ordering.by[Pushed[T], Long](_.sequence).reverse

  /** @see [[MergeSequence]] **/
  def apply[T](inputPorts: Int = 2)(extractSequence: T => Long): Graph[UniformFanInShape[T, T], NotUsed] =
    GraphStages.withDetachedInputs(new MergeSequence[T](inputPorts)(extractSequence))
}

/**
 * Takes multiple streams whose elements in aggregate have a defined linear
 * sequence with difference 1, starting at 0, and outputs a single stream
 * containing these elements, in order. That is, given a set of input streams
 * with combined elements *e<sub>k</sub>*:
 *
 * *e<sub>0</sub>*, *e<sub>1</sub>*, *e<sub>2</sub>*, ..., *e<sub>n</sub>*
 *
 * This will output a stream ordered by *k*.
 *
 * The elements in the input streams must already be sorted according to the
 * sequence. The input streams do not need to be linear, but the aggregate
 * stream must be linear, no element *k* may be skipped or duplicated, either
 * of these conditions will cause the stream to fail.
 *
 * The typical use case for this is to merge a partitioned stream back
 * together while maintaining order. This can be achieved by first using
 * `zipWithIndex` on the input stream, then partitioning using a
 * [[Partition]] fanout, and then maintaining the index through the processing
 * of each partition before bringing together with this stage.
 *
 * '''Emits when''' one of the upstreams has the next expected element in the
 * sequence available.
 *
 * '''Backpressures when''' downstream backpressures
 *
 * '''Completes when''' all upstreams complete
 *
 * '''Cancels when''' downstream cancels
 */
final class MergeSequence[T](val inputPorts: Int)(extractSequence: T => Long)
    extends GraphStage[UniformFanInShape[T, T]] {
  require(inputPorts > 1, "A MergeSequence must have more than 1 input ports")
  private val in: IndexedSeq[Inlet[T]] = Vector.tabulate(inputPorts)(i => Inlet[T]("MergeSequence.in" + i))
  private val out: Outlet[T] = Outlet("MergeSequence.out")
  override val shape: UniformFanInShape[T, T] = UniformFanInShape(out, in: _*)

  import MergeSequence._

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {
      private var nextSequence = 0L
      private val available = mutable.PriorityQueue.empty[Pushed[T]]
      private var complete = 0

      setHandler(out, this)

      in.zipWithIndex.foreach {
        case (inPort, idx) =>
          setHandler(
            inPort,
            new InHandler {
              override def onPush(): Unit = {
                val elem = grab(inPort)
                val sequence = extractSequence(elem)
                if (sequence < nextSequence) {
                  failStage(
                    new IllegalStateException(s"Sequence regression from $nextSequence to $sequence on port $idx"))
                } else if (sequence == nextSequence && isAvailable(out)) {
                  push(out, elem)
                  tryPull(inPort)
                  nextSequence += 1
                } else {
                  available.enqueue(Pushed(inPort, sequence, elem))
                  detectMissedSequence()
                }
              }

              override def onUpstreamFinish(): Unit = {
                complete += 1
                if (complete == inputPorts && available.isEmpty) {
                  completeStage()
                } else {
                  detectMissedSequence()
                }
              }
            })
      }

      def onPull(): Unit =
        if (available.nonEmpty && available.head.sequence == nextSequence) {
          val pushed = available.dequeue()
          push(out, pushed.elem)
          if (complete == inputPorts && available.isEmpty) {
            completeStage()
          } else {
            if (available.nonEmpty && available.head.sequence == nextSequence) {
              failStage(
                new IllegalStateException(
                  s"Duplicate sequence $nextSequence on ports ${pushed.in} and ${available.head.in}"))
            }
            tryPull(pushed.in)
            nextSequence += 1
          }
        } else {
          detectMissedSequence()
        }

      private def detectMissedSequence(): Unit =
        // Cheap to calculate, but doesn't give the right answer, because there might be input ports
        // that are both complete and still have one last buffered element.
        if (isAvailable(out) && available.size + complete >= inputPorts) {
          // So in the event that this was true we count the number of ports that we have elements buffered for that
          // are not yet closed, and add that to the complete ones, to see if we're in a dead lock.
          if (available.count(pushed => !isClosed(pushed.in)) + complete == inputPorts) {
            failStage(
              new IllegalStateException(
                s"Expected sequence $nextSequence, but all input ports have pushed or are complete, " +
                "but none have pushed the next sequence number. Pushed sequences: " +
                available.toVector.map(p => s"${p.in}: ${p.sequence}").mkString(", ")))
          }
        }

      override def preStart(): Unit =
        in.foreach(pull)
    }

  override def toString: String = s"MergeSequence($inputPorts)"
}

object GraphDSL extends GraphApply {

  /**
   * Creates a new [[Graph]] by importing the given graph list `graphs` and passing their [[Shape]]s
   * along with the [[GraphDSL.Builder]] to the given create function.
   */
  def create[S <: Shape, IS <: Shape, Mat](graphs: immutable.Seq[Graph[IS, Mat]])(
      buildBlock: GraphDSL.Builder[immutable.Seq[Mat]] => immutable.Seq[IS] => S): Graph[S, immutable.Seq[Mat]] = {
    require(graphs.nonEmpty, "The input list must have one or more Graph elements")
    val builder = new GraphDSL.Builder
    val toList = (m1: Mat) => Seq(m1)
    val combine = (s: Seq[Mat], m2: Mat) => s :+ m2
    val sListH = builder.add(graphs.head, toList)
    val sListT = graphs.tail.map(g => builder.add(g, combine))
    val s = buildBlock(builder)(immutable.Seq(sListH) ++ sListT)

    new GenericGraph(s, builder.result(s))
  }

  class Builder[+M] private[stream] () {
    private val unwiredIns = new mutable.HashSet[Inlet[_]]()
    private val unwiredOuts = new mutable.HashSet[Outlet[_]]()

    private var traversalBuilderInProgress: TraversalBuilder = TraversalBuilder.empty()

    /**
     * INTERNAL API
     */
    private[GraphDSL] def addEdge[T, U >: T](from: Outlet[T], to: Inlet[U]): Unit =
      try {
        traversalBuilderInProgress = traversalBuilderInProgress.wire(from, to)
        unwiredIns -= to
        unwiredOuts -= from
      } catch {
        case NonFatal(ex) =>
          if (!traversalBuilderInProgress.isUnwired(from))
            throw new IllegalArgumentException(s"[${from.s}] is already connected")
          else if (!traversalBuilderInProgress.isUnwired(to))
            throw new IllegalArgumentException(s"[${to.s}] is already connected")
          else throw ex
      }

    /**
     * Import a graph into this module, performing a deep copy, discarding its
     * materialized value and returning the copied Ports that are now to be
     * connected.
     */
    def add[S <: Shape](graph: Graph[S, _]): S = {
      val newShape = graph.shape.deepCopy()
      traversalBuilderInProgress = traversalBuilderInProgress.add(graph.traversalBuilder, newShape, Keep.left)

      unwiredIns ++= newShape.inlets
      unwiredOuts ++= newShape.outlets

      newShape.asInstanceOf[S]
    }

    /**
     * INTERNAL API.
     *
     * This is only used by the materialization-importing apply methods of Source,
     * Flow, Sink and Graph.
     */
    private[stream] def add[S <: Shape, A](graph: Graph[S, _], transform: (A) => Any): S = {
      val newShape = graph.shape.deepCopy()
      traversalBuilderInProgress =
        traversalBuilderInProgress.add(graph.traversalBuilder.transformMat(transform), newShape, Keep.right)

      unwiredIns ++= newShape.inlets
      unwiredOuts ++= newShape.outlets

      newShape.asInstanceOf[S]
    }

    /**
     * INTERNAL API.
     *
     * This is only used by the materialization-importing apply methods of Source,
     * Flow, Sink and Graph.
     */
    private[stream] def add[S <: Shape, A, B](graph: Graph[S, _], combine: (A, B) => Any): S = {
      val newShape = graph.shape.deepCopy()
      traversalBuilderInProgress = traversalBuilderInProgress.add(graph.traversalBuilder, newShape, combine)

      unwiredIns ++= newShape.inlets
      unwiredOuts ++= newShape.outlets

      newShape.asInstanceOf[S]
    }

    /**
     * Returns an [[Outlet]] that gives access to the materialized value of this graph. Once the graph is materialized
     * this outlet will emit exactly one element which is the materialized value. It is possible to expose this
     * outlet as an externally accessible outlet of a [[Source]], [[Sink]], [[Flow]] or [[BidiFlow]].
     *
     * It is possible to call this method multiple times to get multiple [[Outlet]] instances if necessary. All of
     * the outlets will emit the materialized value.
     *
     * Be careful to not to feed the result of this outlet to an operator that produces the materialized value itself (for
     * example to a [[Sink#fold]] that contributes to the materialized value) since that might lead to an unresolvable
     * dependency cycle.
     *
     * @return The outlet that will emit the materialized value.
     */
    def materializedValue: Outlet[M @uncheckedVariance] =
      add(Source.maybe[M], { (prev: M, prom: Promise[Option[M]]) =>
        prom.success(Some(prev)); prev
      }).out

    private[GraphDSL] def traversalBuilder: TraversalBuilder = traversalBuilderInProgress

    private[stream] def result(resultShape: Shape): TraversalBuilder = {
      def errorString[T](expectedSet: Set[T], actualSet: Set[T], tag: String)(format: T => String): String =
        if (expectedSet != actualSet) {
          val diff1 = expectedSet.diff(actualSet)
          val diff2 = actualSet.diff(expectedSet)

          val forwardMessage =
            if (diff1.isEmpty) ""
            else
              s" $tag [${diff1.map(format).mkString(", ")}] were returned in the resulting shape but were already connected."

          val backwardMessage =
            if (diff2.isEmpty) ""
            else
              s" $tag [${diff2.map(format).mkString(", ")}] were not returned in the resulting shape and not connected."

          forwardMessage + backwardMessage
        } else ""

      if (resultShape.inlets.toSet != unwiredIns || resultShape.outlets.toSet != unwiredOuts) {
        val inletError = errorString(resultShape.inlets.toSet, unwiredIns.toSet, "Inlets")(_.s)
        val outletError = errorString(resultShape.outlets.toSet, unwiredOuts.toSet, "Outlets")(_.s)
        throw new IllegalStateException(s"Illegal GraphDSL usage.$inletError$outletError")
      }

      traversalBuilderInProgress
    }

    /** Converts this Scala DSL element to it's Java DSL counterpart. */
    def asJava: javadsl.GraphDSL.Builder[M] = new javadsl.GraphDSL.Builder()(this)
  }

  object Implicits {

    @tailrec
    private[stream] def findOut[I, O](b: Builder[_], junction: UniformFanOutShape[I, O], n: Int): Outlet[O] = {
      if (n == junction.outlets.length)
        throw new IllegalArgumentException(s"no more outlets free on $junction")
      else if (!b.traversalBuilder.isUnwired(junction.out(n))) findOut(b, junction, n + 1)
      else junction.out(n)
    }

    @tailrec
    private[stream] def findIn[I, O](b: Builder[_], junction: UniformFanInShape[I, O], n: Int): Inlet[I] = {
      if (n == junction.inlets.length)
        throw new IllegalArgumentException(s"no more inlets free on $junction")
      else if (!b.traversalBuilder.isUnwired(junction.in(n))) findIn(b, junction, n + 1)
      else junction.in(n)
    }

    sealed trait CombinerBase[+T] extends Any {
      def importAndGetPort(b: Builder[_]): Outlet[T @uncheckedVariance]

      def ~>[U >: T](to: Inlet[U])(implicit b: Builder[_]): Unit =
        b.addEdge(importAndGetPort(b), to)

      def ~>[Out](via: Graph[FlowShape[T, Out], Any])(implicit b: Builder[_]): PortOps[Out] = {
        val s = b.add(via)
        b.addEdge(importAndGetPort(b), s.in)
        s.out
      }

      def ~>[Out](junction: UniformFanInShape[T, Out])(implicit b: Builder[_]): PortOps[Out] = {
        def bind(n: Int): Unit = {
          if (n == junction.inlets.length)
            throw new IllegalArgumentException(s"no more inlets free on $junction")
          else if (!b.traversalBuilder.isUnwired(junction.in(n))) bind(n + 1)
          else b.addEdge(importAndGetPort(b), junction.in(n))
        }
        bind(0)
        junction.out
      }

      def ~>[Out](junction: UniformFanOutShape[T, Out])(implicit b: Builder[_]): PortOps[Out] = {
        b.addEdge(importAndGetPort(b), junction.in)
        try findOut(b, junction, 0)
        catch {
          case e: IllegalArgumentException => new DisabledPortOps(e.getMessage)
        }
      }

      def ~>[Out](flow: FlowShape[T, Out])(implicit b: Builder[_]): PortOps[Out] = {
        b.addEdge(importAndGetPort(b), flow.in)
        flow.out
      }

      def ~>(to: Graph[SinkShape[T], _])(implicit b: Builder[_]): Unit =
        b.addEdge(importAndGetPort(b), b.add(to).in)

      def ~>(to: SinkShape[T])(implicit b: Builder[_]): Unit =
        b.addEdge(importAndGetPort(b), to.in)
    }

    sealed trait ReverseCombinerBase[T] extends Any {
      def importAndGetPortReverse(b: Builder[_]): Inlet[T]

      def <~[U <: T](from: Outlet[U])(implicit b: Builder[_]): Unit =
        b.addEdge(from, importAndGetPortReverse(b))

      def <~[In](via: Graph[FlowShape[In, T], _])(implicit b: Builder[_]): ReversePortOps[In] = {
        val s = b.add(via)
        b.addEdge(s.out, importAndGetPortReverse(b))
        s.in
      }

      def <~[In](junction: UniformFanOutShape[In, T])(implicit b: Builder[_]): ReversePortOps[In] = {
        def bind(n: Int): Unit = {
          if (n == junction.outlets.length)
            throw new IllegalArgumentException(s"no more outlets free on $junction")
          else if (!b.traversalBuilder.isUnwired(junction.out(n))) bind(n + 1)
          else b.addEdge(junction.out(n), importAndGetPortReverse(b))
        }
        bind(0)
        junction.in
      }

      def <~[In](junction: UniformFanInShape[In, T])(implicit b: Builder[_]): ReversePortOps[In] = {
        b.addEdge(junction.out, importAndGetPortReverse(b))
        try findIn(b, junction, 0)
        catch {
          case e: IllegalArgumentException => new DisabledReversePortOps(e.getMessage)
        }
      }

      def <~[In](flow: FlowShape[In, T])(implicit b: Builder[_]): ReversePortOps[In] = {
        b.addEdge(flow.out, importAndGetPortReverse(b))
        flow.in
      }

      def <~(from: Graph[SourceShape[T], _])(implicit b: Builder[_]): Unit =
        b.addEdge(b.add(from).out, importAndGetPortReverse(b))

      def <~(from: SourceShape[T])(implicit b: Builder[_]): Unit =
        b.addEdge(from.out, importAndGetPortReverse(b))
    }

    // Although Mat is always Unit, it cannot be removed as a type parameter, otherwise the "override type"
    // won't work below
    trait PortOps[+Out] extends FlowOps[Out, NotUsed] with CombinerBase[Out] {
      override type Repr[+O] = PortOps[O]
      override type Closed = Unit
      def outlet: Outlet[Out @uncheckedVariance]
    }

    private class PortOpsImpl[+Out](override val outlet: Outlet[Out @uncheckedVariance], b: Builder[_])
        extends PortOps[Out] {

      override def withAttributes(attr: Attributes): Repr[Out] = throw settingAttrNotSupported
      override def addAttributes(attr: Attributes): Repr[Out] = throw settingAttrNotSupported
      override def named(name: String): Repr[Out] = throw settingAttrNotSupported
      override def async: Repr[Out] = throw settingAttrNotSupported

      private def settingAttrNotSupported =
        new UnsupportedOperationException("Cannot set attributes on chained ops from a junction output port")

      override def importAndGetPort(b: Builder[_]): Outlet[Out @uncheckedVariance] = outlet

      override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): Repr[T] =
        super.~>(flow)(b)

      def to[Mat2](sink: Graph[SinkShape[Out], Mat2]): Closed = {
        super.~>(sink)(b)
      }
    }

    private class DisabledPortOps[Out](msg: String) extends PortOpsImpl[Out](null, null) {
      override def importAndGetPort(b: Builder[_]): Outlet[Out] = throw new IllegalArgumentException(msg)

      override def via[T, Mat2](flow: Graph[FlowShape[Out, T], Mat2]): Repr[T] =
        throw new IllegalArgumentException(msg)
    }

    implicit class ReversePortOps[In](val inlet: Inlet[In]) extends ReverseCombinerBase[In] {
      override def importAndGetPortReverse(b: Builder[_]): Inlet[In] = inlet
    }

    final class DisabledReversePortOps[In](msg: String) extends ReversePortOps[In](null) {
      override def importAndGetPortReverse(b: Builder[_]): Inlet[In] = throw new IllegalArgumentException(msg)
    }

    implicit final class FanInOps[In, Out](val j: UniformFanInShape[In, Out])
        extends AnyVal
        with CombinerBase[Out]
        with ReverseCombinerBase[In] {
      override def importAndGetPort(b: Builder[_]): Outlet[Out] = j.out
      override def importAndGetPortReverse(b: Builder[_]): Inlet[In] = findIn(b, j, 0)
    }

    implicit final class FanOutOps[In, Out](val j: UniformFanOutShape[In, Out])
        extends AnyVal
        with ReverseCombinerBase[In] {
      override def importAndGetPortReverse(b: Builder[_]): Inlet[In] = j.in
    }

    implicit final class SinkArrow[T](val s: Graph[SinkShape[T], _]) extends AnyVal with ReverseCombinerBase[T] {
      override def importAndGetPortReverse(b: Builder[_]): Inlet[T] = b.add(s).in
    }

    implicit final class SinkShapeArrow[T](val s: SinkShape[T]) extends AnyVal with ReverseCombinerBase[T] {
      override def importAndGetPortReverse(b: Builder[_]): Inlet[T] = s.in
    }

    implicit final class FlowShapeArrow[I, O](val f: FlowShape[I, O]) extends AnyVal with ReverseCombinerBase[I] {
      override def importAndGetPortReverse(b: Builder[_]): Inlet[I] = f.in

      def <~>[I2, O2, Mat](bidi: Graph[BidiShape[O, O2, I2, I], Mat])(
          implicit b: Builder[_]): BidiShape[O, O2, I2, I] = {
        val shape = b.add(bidi)
        b.addEdge(f.out, shape.in1)
        b.addEdge(shape.out2, f.in)
        shape
      }

      def <~>[I2, O2](bidi: BidiShape[O, O2, I2, I])(implicit b: Builder[_]): BidiShape[O, O2, I2, I] = {
        b.addEdge(f.out, bidi.in1)
        b.addEdge(bidi.out2, f.in)
        bidi
      }

      def <~>[M](flow: Graph[FlowShape[O, I], M])(implicit b: Builder[_]): Unit = {
        val shape = b.add(flow)
        b.addEdge(shape.out, f.in)
        b.addEdge(f.out, shape.in)
      }
    }

    implicit final class FlowArrow[I, O, M](val f: Graph[FlowShape[I, O], M]) extends AnyVal {
      def <~>[I2, O2, Mat](bidi: Graph[BidiShape[O, O2, I2, I], Mat])(
          implicit b: Builder[_]): BidiShape[O, O2, I2, I] = {
        val shape = b.add(bidi)
        val flow = b.add(f)
        b.addEdge(flow.out, shape.in1)
        b.addEdge(shape.out2, flow.in)
        shape
      }

      def <~>[I2, O2](bidi: BidiShape[O, O2, I2, I])(implicit b: Builder[_]): BidiShape[O, O2, I2, I] = {
        val flow = b.add(f)
        b.addEdge(flow.out, bidi.in1)
        b.addEdge(bidi.out2, flow.in)
        bidi
      }

      def <~>[M2](flow: Graph[FlowShape[O, I], M2])(implicit b: Builder[_]): Unit = {
        val shape = b.add(flow)
        val ff = b.add(f)
        b.addEdge(shape.out, ff.in)
        b.addEdge(ff.out, shape.in)
      }
    }

    implicit final class BidiFlowShapeArrow[I1, O1, I2, O2](val bidi: BidiShape[I1, O1, I2, O2]) extends AnyVal {
      def <~>[I3, O3](other: BidiShape[O1, O3, I3, I2])(implicit b: Builder[_]): BidiShape[O1, O3, I3, I2] = {
        b.addEdge(bidi.out1, other.in1)
        b.addEdge(other.out2, bidi.in2)
        other
      }

      def <~>[I3, O3, M](otherFlow: Graph[BidiShape[O1, O3, I3, I2], M])(
          implicit b: Builder[_]): BidiShape[O1, O3, I3, I2] = {
        val other = b.add(otherFlow)
        b.addEdge(bidi.out1, other.in1)
        b.addEdge(other.out2, bidi.in2)
        other
      }

      def <~>(flow: FlowShape[O1, I2])(implicit b: Builder[_]): Unit = {
        b.addEdge(bidi.out1, flow.in)
        b.addEdge(flow.out, bidi.in2)
      }

      def <~>[M](f: Graph[FlowShape[O1, I2], M])(implicit b: Builder[_]): Unit = {
        val flow = b.add(f)
        b.addEdge(bidi.out1, flow.in)
        b.addEdge(flow.out, bidi.in2)
      }
    }

    import scala.language.implicitConversions

    implicit def port2flow[T](from: Outlet[T])(implicit b: Builder[_]): PortOps[T] =
      new PortOpsImpl(from, b)

    implicit def fanOut2flow[I, O](j: UniformFanOutShape[I, O])(implicit b: Builder[_]): PortOps[O] =
      new PortOpsImpl(findOut(b, j, 0), b)

    implicit def flow2flow[I, O](f: FlowShape[I, O])(implicit b: Builder[_]): PortOps[O] =
      new PortOpsImpl(f.out, b)

    implicit final class SourceArrow[T](val s: Graph[SourceShape[T], _]) extends AnyVal with CombinerBase[T] {
      override def importAndGetPort(b: Builder[_]): Outlet[T] = b.add(s).out
    }

    implicit final class SourceShapeArrow[T](val s: SourceShape[T]) extends AnyVal with CombinerBase[T] {
      override def importAndGetPort(b: Builder[_]): Outlet[T] = s.out
    }
  }
}
