/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.ActorAttributes.StreamSubscriptionTimeout
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Attributes.SourceLocation
import akka.stream._
import akka.stream.impl.{
  ActorSubscriberMessage,
  FailedSource,
  JavaStreamSource,
  SubscriptionTimeoutException,
  TraversalBuilder,
  Buffer => BufferImpl
}
import akka.stream.impl.ActorSubscriberMessage.OnError
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.fusing.GraphStages.{ FutureSource, IterableSource, SingleSource }
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.OptionVal

import scala.jdk.CollectionConverters._
import java.util.Collections
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.{ nowarn, tailrec }
import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration
import scala.util.{ Failure, Try }
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class FlattenMerge[T, M](val breadth: Int)
    extends GraphStage[FlowShape[Graph[SourceShape[T], M], T]] {
  require(breadth >= 1, "breadth should >= 1")
  private val in = Inlet[Graph[SourceShape[T], M]]("flatten.in")
  private val out = Outlet[T]("flatten.out")

  override def initialAttributes = DefaultAttributes.flattenMerge
  override val shape = FlowShape(in, out)

  override def createLogic(enclosingAttributes: Attributes) =
    new GraphStageLogic(shape) with OutHandler with InHandler {
      var sources = Set.empty[SubSinkInlet[T]]
      var pendingSingleSources = 0
      def activeSources = sources.size + pendingSingleSources

      // To be able to optimize for SingleSource without materializing them the queue may hold either
      // SubSinkInlet[T] or SingleSource
      var queue: BufferImpl[AnyRef] = _

      override def preStart(): Unit = queue = BufferImpl(breadth, enclosingAttributes)

      def pushOut(): Unit = {
        queue.dequeue() match {
          case src: SubSinkInlet[T] @unchecked =>
            push(out, src.grab())
            if (!src.isClosed) src.pull()
            else removeSource(src)
          case single: SingleSource[T] @unchecked =>
            push(out, single.elem)
            removeSource(single)
          case other =>
            throw new IllegalStateException(s"Unexpected source type in queue: '${other.getClass}'")
        }
      }

      override def onPush(): Unit = {
        val source = grab(in)
        addSource(source)
        if (activeSources < breadth) tryPull(in)
      }

      override def onUpstreamFinish(): Unit = if (activeSources == 0) completeStage()
      override def onPull(): Unit = {
        pull(in)
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            // could be unavailable due to async input having been executed before this notification
            if (queue.nonEmpty && isAvailable(out)) pushOut()
          }
        })
      }

      setHandlers(in, out, this)

      def addSource(source: Graph[SourceShape[T], M]): Unit = {
        // If it's a SingleSource or wrapped such we can push the element directly instead of materializing it.
        // Have to use AnyRef because of OptionVal null value.
        TraversalBuilder.getSingleSource(source.asInstanceOf[Graph[SourceShape[AnyRef], M]]) match {
          case OptionVal.Some(single) =>
            if (isAvailable(out) && queue.isEmpty) {
              push(out, single.elem.asInstanceOf[T])
            } else {
              queue.enqueue(single)
              pendingSingleSources += 1
            }
          case _ =>
            val sinkIn = new SubSinkInlet[T]("FlattenMergeSink")
            sinkIn.setHandler(new InHandler {
              override def onPush(): Unit = {
                if (isAvailable(out)) {
                  push(out, sinkIn.grab())
                  sinkIn.pull()
                } else {
                  queue.enqueue(sinkIn)
                }
              }
              override def onUpstreamFinish(): Unit = if (!sinkIn.isAvailable) removeSource(sinkIn)
            })
            sinkIn.pull()
            sources += sinkIn
            val graph = Source.fromGraph(source).to(sinkIn.sink)
            interpreter.subFusingMaterializer.materialize(graph, defaultAttributes = enclosingAttributes)
        }
      }

      def removeSource(src: AnyRef): Unit = {
        val pullSuppressed = activeSources == breadth
        src match {
          case sub: SubSinkInlet[T] @unchecked =>
            sources -= sub
          case _: SingleSource[_] =>
            pendingSingleSources -= 1
          case other => throw new IllegalArgumentException(s"Unexpected source type: '${other.getClass}'")
        }
        if (pullSuppressed) tryPull(in)
        if (activeSources == 0 && isClosed(in)) completeStage()
      }

      override def postStop(): Unit = sources.foreach(_.cancel())

    }

  override def toString: String = s"FlattenMerge($breadth)"
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object FlattenConcat {
  private sealed abstract class InflightSource[T] {
    def hasNext: Boolean
    def next(): T
    def tryPull(): Unit
    def cancel(cause: Throwable): Unit
    def isClosed: Boolean
    def hasFailed: Boolean = failure.isDefined
    def failure: Option[Throwable] = None
    def materialize(): Unit = ()
  }

  private final class InflightIteratorSource[T](iterator: Iterator[T]) extends InflightSource[T] {
    override def hasNext: Boolean = iterator.hasNext
    override def next(): T = iterator.next()
    override def tryPull(): Unit = ()
    override def cancel(cause: Throwable): Unit = ()
    override def isClosed: Boolean = !hasNext
  }

  private final class InflightCompletedFutureSource[T](result: Try[T]) extends InflightSource[T] {
    private var _hasNext = result.isSuccess
    override def hasNext: Boolean = _hasNext
    override def next(): T = {
      if (_hasNext) {
        _hasNext = false
        result.get
      } else throw new NoSuchElementException("next called after completion")
    }
    override def hasFailed: Boolean = result.isFailure
    override def failure: Option[Throwable] = result.failed.toOption
    override def tryPull(): Unit = ()
    override def cancel(cause: Throwable): Unit = ()
    override def isClosed: Boolean = true
  }

  private final class InflightPendingFutureSource[T](cb: InflightSource[T] => Unit)
      extends InflightSource[T]
      with (Try[T] => Unit) {
    private var result: Try[T] = MapAsync.NotYetThere
    private var consumed = false
    override def apply(result: Try[T]): Unit = {
      this.result = result
      cb(this)
    }
    override def hasNext: Boolean = (result ne MapAsync.NotYetThere) && !consumed && result.isSuccess
    override def next(): T = {
      if (!consumed) {
        consumed = true
        result.get
      } else throw new NoSuchElementException("next called after completion")
    }
    override def hasFailed: Boolean = (result ne MapAsync.NotYetThere) && result.isFailure
    override def failure: Option[Throwable] = if (result eq MapAsync.NotYetThere) None else result.failed.toOption
    override def tryPull(): Unit = ()
    override def cancel(cause: Throwable): Unit = ()
    override def isClosed: Boolean = consumed || hasFailed
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class FlattenConcat[T, M](parallelism: Int)
    extends GraphStage[FlowShape[Graph[SourceShape[T], M], T]] {
  require(parallelism >= 1, "parallelism should >= 1")
  private val in = Inlet[Graph[SourceShape[T], M]]("flattenConcat.in")
  private val out = Outlet[T]("flattenConcat.out")

  override def initialAttributes: Attributes = DefaultAttributes.flattenConcat
  override val shape: FlowShape[Graph[SourceShape[T], M], T] = FlowShape(in, out)
  override def createLogic(enclosingAttributes: Attributes) = {
    final object FlattenConcatLogic extends GraphStageLogic(shape) with InHandler with OutHandler {
      import FlattenConcat._
      // InflightSource[T] or SingleSource[T]
      // AnyRef here to avoid lift the SingleSource[T] to InflightSource[T]
      private var queue: BufferImpl[AnyRef] = _
      private val invokeCb: InflightSource[T] => Unit =
        getAsyncCallback[InflightSource[T]](futureSourceCompleted).invoke

      override def preStart(): Unit = queue = BufferImpl(parallelism, enclosingAttributes)

      private def futureSourceCompleted(futureSource: InflightSource[T]): Unit = {
        if (queue.peek() eq futureSource) {
          if (isAvailable(out) && futureSource.hasNext) {
            push(out, futureSource.next()) //TODO should filter out the `null` here?
            if (futureSource.isClosed) {
              handleCurrentSourceClosed(futureSource)
            }
          } else if (futureSource.isClosed) {
            handleCurrentSourceClosed(futureSource)
          }
        } // else just ignore, it will be picked up by onPull
      }

      override def onPush(): Unit = {
        addSource(grab(in))
        //must try pull after addSource to avoid queue overflow
        if (!queue.isFull) { // try to keep the maximum parallelism
          tryPull(in)
        }
      }

      override def onUpstreamFinish(): Unit = if (queue.isEmpty) completeStage()

      override def onUpstreamFailure(ex: Throwable): Unit = {
        super.onUpstreamFailure(ex)
        cancelInflightSources(SubscriptionWithCancelException.NoMoreElementsNeeded)
      }

      override def onPull(): Unit = {
        //purge if possible
        queue.peek() match {
          case src: SingleSource[T] @unchecked =>
            push(out, src.elem)
            removeSource()
          case src: InflightSource[T] @unchecked => pushOut(src)
          case null => //queue is empty
            if (!hasBeenPulled(in)) {
              tryPull(in)
            } else if (isClosed(in)) {
              completeStage()
            }
          case _ => throw new IllegalStateException("Should not reach here.")
        }
      }

      private def pushOut(src: InflightSource[T]): Unit = {
        if (src.hasNext) {
          push(out, src.next())
          if (src.isClosed) {
            handleCurrentSourceClosed(src)
          }
        } else if (src.isClosed) {
          handleCurrentSourceClosed(src)
        } else {
          src.tryPull()
        }
      }

      private def handleCurrentSourceClosed(source: InflightSource[T]): Unit = {
        source.failure match {
          case Some(cause) => onUpstreamFailure(cause)
          case None        => removeSource(source)
        }
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        super.onDownstreamFinish(cause)
        cancelInflightSources(cause)
      }

      private def cancelInflightSources(cause: Throwable): Unit = {
        if (queue.nonEmpty) {
          var source = queue.dequeue()
          while ((source ne null) && (source.isInstanceOf[InflightSource[T] @unchecked])) {
            source.asInstanceOf[InflightSource[T]].cancel(cause)
            source = queue.dequeue()
          }
        }
      }

      private def addSource(singleSource: SingleSource[T]): Unit = {
        if (isAvailable(out) && queue.isEmpty) {
          push(out, singleSource.elem)
        } else {
          queue.enqueue(singleSource)
        }
      }

      private def addSourceElements(iterator: Iterator[T]): Unit = {
        val inflightSource = new InflightIteratorSource[T](iterator)
        if (isAvailable(out) && queue.isEmpty) {
          if (inflightSource.hasNext) {
            push(out, inflightSource.next())
            if (inflightSource.hasNext) {
              queue.enqueue(inflightSource)
            }
          }
        } else {
          queue.enqueue(inflightSource)
        }
      }

      private def addCompletedFutureElem(elem: Try[T]): Unit = {
        if (isAvailable(out) && queue.isEmpty) {
          elem match {
            case scala.util.Success(value) => push(out, value)
            case scala.util.Failure(ex)    => onUpstreamFailure(ex)
          }
        } else {
          queue.enqueue(new InflightCompletedFutureSource(elem))
        }
      }

      private def addPendingFutureElem(future: Future[T]): Unit = {
        val inflightSource = new InflightPendingFutureSource[T](invokeCb)
        future.onComplete(inflightSource)(ExecutionContext.parasitic)
        queue.enqueue(inflightSource)
      }

      private def attachAndMaterializeSource(source: Graph[SourceShape[T], M]): Unit = {
        object inflightSource extends InflightSource[T] { self =>
          private val sinkIn = new SubSinkInlet[T]("FlattenConcatSink")
          private var upstreamFailure = Option.empty[Throwable]
          sinkIn.setHandler(new InHandler {
            override def onPush(): Unit = {
              if (isAvailable(out) && (queue.peek() eq self)) {
                push(out, sinkIn.grab())
              }
            }
            override def onUpstreamFinish(): Unit = if (!sinkIn.isAvailable) removeSource(self)
            override def onUpstreamFailure(ex: Throwable): Unit = {
              upstreamFailure = Some(ex)
              // if it's the current emitting source, fail the stage
              if (queue.peek() eq self) {
                super.onUpstreamFailure(ex)
              } // else just mark the source as failed
            }
          })

          final override def materialize(): Unit = {
            val graph = Source.fromGraph(source).to(sinkIn.sink)
            interpreter.subFusingMaterializer.materialize(graph, defaultAttributes = enclosingAttributes)
          }
          final override def cancel(cause: Throwable): Unit = sinkIn.cancel(cause)
          final override def hasNext: Boolean = sinkIn.isAvailable
          final override def isClosed: Boolean = sinkIn.isClosed
          final override def failure: Option[Throwable] = upstreamFailure
          final override def next(): T = sinkIn.grab()
          final override def tryPull(): Unit = if (!sinkIn.isClosed && !sinkIn.hasBeenPulled) sinkIn.pull()
        }
        if (isAvailable(out) && queue.isEmpty) {
          //this is the first one, pull
          inflightSource.tryPull()
        }
        queue.enqueue(inflightSource)
        inflightSource.materialize()
      }

      private def addSource(source: Graph[SourceShape[T], M]): Unit = {
        (TraversalBuilder.getValuePresentedSource(source): @nowarn("cat=lint-infer-any")) match {
          case OptionVal.Some(graph) =>
            graph match {
              case single: SingleSource[T] @unchecked => addSource(single)
              case futureSource: FutureSource[T] @unchecked =>
                val future = futureSource.future
                future.value match {
                  case Some(elem) => addCompletedFutureElem(elem)
                  case None       => addPendingFutureElem(future)
                }
              case iterable: IterableSource[T] @unchecked => addSourceElements(iterable.elements.iterator)
              case javaStream: JavaStreamSource[T, _] @unchecked =>
                addSourceElements(javaStream.open().iterator.asScala)
              case failed: FailedSource[T] @unchecked                       => addCompletedFutureElem(Failure(failed.failure))
              case maybeEmpty if TraversalBuilder.isEmptySource(maybeEmpty) => //Empty source is discarded
              case _                                                        => attachAndMaterializeSource(source)
            }
          case _ => attachAndMaterializeSource(source)
        }

      }

      private def removeSource(): Unit = {
        queue.dequeue()
        pullIfNeeded()
      }

      private def removeSource(source: InflightSource[T]): Unit = {
        if (source eq queue.peek()) {
          //only dequeue if it's the current emitting source
          queue.dequeue()
          pullIfNeeded()
        } //not the head source, just ignore
      }

      private def pullIfNeeded(): Unit = {
        if (isClosed(in)) {
          if (queue.isEmpty) {
            completeStage()
          } else {
            tryPullNextSourceInQueue()
          }
        } else {
          if (queue.nonEmpty) {
            tryPullNextSourceInQueue()
          }
          if (!hasBeenPulled(in)) {
            tryPull(in)
          }
        }
      }

      private def tryPullNextSourceInQueue(): Unit = {
        //pull the new emitting source
        val nextSource = queue.peek()
        if (nextSource.isInstanceOf[InflightSource[T] @unchecked]) {
          nextSource.asInstanceOf[InflightSource[T]].tryPull()
        }
      }

      setHandlers(in, out, this)
    }

    FlattenConcatLogic
  }

  override def toString: String = s"FlattenConcat($parallelism)"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class PrefixAndTail[T](val n: Int)
    extends GraphStage[FlowShape[T, (immutable.Seq[T], Source[T, NotUsed])]] {
  val in: Inlet[T] = Inlet("PrefixAndTail.in")
  val out: Outlet[(immutable.Seq[T], Source[T, NotUsed])] = Outlet("PrefixAndTail.out")
  override val shape: FlowShape[T, (immutable.Seq[T], Source[T, NotUsed])] = FlowShape(in, out)

  override def initialAttributes = DefaultAttributes.prefixAndTail

  private final class PrefixAndTailLogic(_shape: Shape, inheritedAttributes: Attributes)
      extends TimerGraphStageLogic(_shape)
      with OutHandler
      with InHandler {

    private var left = if (n < 0) 0 else n
    private var builder = Vector.newBuilder[T]
    builder.sizeHint(left)

    private var tailSource: SubSourceOutlet[T] = null

    private val SubscriptionTimer = "SubstreamSubscriptionTimer"

    override protected def onTimer(timerKey: Any): Unit = {
      val materializer = interpreter.materializer
      val StreamSubscriptionTimeout(timeout, mode) =
        inheritedAttributes.mandatoryAttribute[ActorAttributes.StreamSubscriptionTimeout]

      mode match {
        case StreamSubscriptionTimeoutTerminationMode.CancelTermination =>
          tailSource.timeout(timeout)
          if (tailSource.isClosed) completeStage()
        case StreamSubscriptionTimeoutTerminationMode.NoopTermination =>
        // do nothing
        case StreamSubscriptionTimeoutTerminationMode.WarnTermination =>
          materializer.logger.warning(
            "Substream subscription timeout triggered after {} in prefixAndTail({}).",
            timeout,
            n)
      }
    }

    private def prefixComplete = builder eq null

    private def subHandler = new OutHandler {
      override def onPull(): Unit = {
        setKeepGoing(false)
        cancelTimer(SubscriptionTimer)
        pull(in)
        tailSource.setHandler(new OutHandler {
          override def onPull(): Unit = pull(in)
        })
      }
    }

    private def openSubstream(): Source[T, NotUsed] = {
      val timeout = inheritedAttributes.mandatoryAttribute[ActorAttributes.StreamSubscriptionTimeout].timeout
      tailSource = new SubSourceOutlet[T]("TailSource")
      tailSource.setHandler(subHandler)
      setKeepGoing(true)
      scheduleOnce(SubscriptionTimer, timeout)
      builder = null
      Source.fromGraph(tailSource.source)
    }

    override def onPush(): Unit = {
      if (prefixComplete) {
        tailSource.push(grab(in))
      } else {
        builder += grab(in)
        left -= 1
        if (left == 0) {
          push(out, (builder.result(), openSubstream()))
          complete(out)
        } else pull(in)
      }
    }
    override def onPull(): Unit = {
      if (left == 0) {
        push(out, (Nil, openSubstream()))
        complete(out)
      } else pull(in)
    }

    override def onUpstreamFinish(): Unit = {
      if (!prefixComplete) {
        // This handles the unpulled out case as well
        val prefix = builder.result()
        builder = null // free for GC
        emit(out, (prefix, Source.empty), () => completeStage())
      } else {
        if (!tailSource.isClosed) tailSource.complete()
        completeStage()
      }
    }

    override def onUpstreamFailure(ex: Throwable): Unit = {
      if (prefixComplete) {
        if (!tailSource.isClosed) tailSource.fail(ex)
        completeStage()
      } else failStage(ex)
    }

    override def onDownstreamFinish(cause: Throwable): Unit = {
      if (!prefixComplete) cancelStage(cause)
      // Otherwise substream is open, ignore
    }

    setHandlers(in, out, this)
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new PrefixAndTailLogic(shape, inheritedAttributes)

  override def toString: String = s"PrefixAndTail($n)"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class GroupBy[T, K](
    val maxSubstreams: Int,
    val keyFor: T => K,
    val allowClosedSubstreamRecreation: Boolean = false)
    extends GraphStage[FlowShape[T, Source[T, NotUsed]]] {
  val in: Inlet[T] = Inlet("GroupBy.in")
  val out: Outlet[Source[T, NotUsed]] = Outlet("GroupBy.out")
  override val shape: FlowShape[T, Source[T, NotUsed]] = FlowShape(in, out)
  override def initialAttributes = DefaultAttributes.groupBy

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with OutHandler with InHandler {
      parent =>

      lazy val decider = inheritedAttributes.mandatoryAttribute[SupervisionStrategy].decider
      private val activeSubstreamsMap = new java.util.HashMap[Any, SubstreamSource]()
      private val closedSubstreams =
        if (allowClosedSubstreamRecreation) Collections.unmodifiableSet(Collections.emptySet[Any])
        else new java.util.HashSet[Any]()
      private val timeout: FiniteDuration =
        inheritedAttributes.mandatoryAttribute[ActorAttributes.StreamSubscriptionTimeout].timeout
      private var substreamWaitingToBePushed: Option[SubstreamSource] = None
      private var nextElementKey: K = null.asInstanceOf[K]
      private var nextElementValue: T = null.asInstanceOf[T]
      private var _nextId = 0
      private val substreamsJustStared = new java.util.HashSet[Any]()
      private var firstPushCounter: Int = 0

      private val tooManySubstreamsOpenException = new TooManySubstreamsOpenException

      private def nextId(): Long = { _nextId += 1; _nextId }

      private def hasNextElement = nextElementKey != null

      private def clearNextElement(): Unit = {
        nextElementKey = null.asInstanceOf[K]
        nextElementValue = null.asInstanceOf[T]
      }

      private def tryCompleteAll(): Boolean =
        if (activeSubstreamsMap.isEmpty || (!hasNextElement && firstPushCounter == 0)) {
          for (value <- activeSubstreamsMap.values().asScala) value.complete()
          completeStage()
          true
        } else false

      private def tryCancel(cause: Throwable): Boolean =
        // if there's no active substreams or there's only one but it's not been pushed yet
        if (activeSubstreamsMap.isEmpty || (activeSubstreamsMap.size == 1 && substreamWaitingToBePushed.isDefined)) {
          cancelStage(cause)
          true
        } else false

      private def fail(ex: Throwable): Unit = {
        for (value <- activeSubstreamsMap.values().asScala) value.fail(ex)
        failStage(ex)
      }

      private def needToPull: Boolean =
        !(hasBeenPulled(in) || isClosed(in) || hasNextElement || substreamWaitingToBePushed.nonEmpty)

      override def onPull(): Unit = {
        substreamWaitingToBePushed match {
          case Some(substreamSource) =>
            push(out, Source.fromGraph(substreamSource.source))
            scheduleOnce(substreamSource.key, timeout)
            substreamWaitingToBePushed = None
          case None =>
            if (hasNextElement) {
              val subSubstreamSource = activeSubstreamsMap.get(nextElementKey)
              if (subSubstreamSource.isAvailable) {
                subSubstreamSource.push(nextElementValue)
                clearNextElement()
              }
            } else if (!hasBeenPulled(in)) tryPull(in)
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = fail(ex)

      override def onUpstreamFinish(): Unit = if (!tryCompleteAll()) setKeepGoing(true)

      override def onDownstreamFinish(cause: Throwable): Unit = if (!tryCancel(cause)) setKeepGoing(true)

      override def onPush(): Unit =
        try {
          val elem = grab(in)
          val key = keyFor(elem)
          require(key != null, "Key cannot be null")
          val substreamSource = activeSubstreamsMap.get(key)
          if (substreamSource != null) {
            if (substreamSource.isAvailable) substreamSource.push(elem)
            else {
              nextElementKey = key
              nextElementValue = elem
            }
          } else {
            if (closedSubstreams.contains(key))
              pull(in)
            else if (activeSubstreamsMap.size + closedSubstreams.size == maxSubstreams)
              throw tooManySubstreamsOpenException
            else runSubstream(key, elem)
          }
        } catch {
          case NonFatal(ex) =>
            decider(ex) match {
              case Supervision.Stop                         => fail(ex)
              case Supervision.Resume | Supervision.Restart => if (!hasBeenPulled(in)) pull(in)
            }
        }

      private def runSubstream(key: K, value: T): Unit = {
        val substreamSource = new SubstreamSource("GroupBySource " + nextId(), key, value)
        activeSubstreamsMap.put(key, substreamSource)
        firstPushCounter += 1
        if (isAvailable(out)) {
          push(out, Source.fromGraph(substreamSource.source))
          scheduleOnce(key, timeout)
          substreamWaitingToBePushed = None
        } else {
          setKeepGoing(true)
          substreamsJustStared.add(substreamSource)
          substreamWaitingToBePushed = Some(substreamSource)
        }
      }

      override protected def onTimer(timerKey: Any): Unit = {
        val substreamSource = activeSubstreamsMap.get(timerKey)
        if (substreamSource != null) {
          if (!allowClosedSubstreamRecreation) {
            closedSubstreams.add(timerKey)
          }
          activeSubstreamsMap.remove(timerKey)
          if (isClosed(in)) tryCompleteAll()
        }
      }

      setHandlers(in, out, this)

      private class SubstreamSource(name: String, val key: K, var firstElement: T)
          extends SubSourceOutlet[T](name)
          with OutHandler {
        def firstPush(): Boolean = firstElement != null
        def hasNextForSubSource = hasNextElement && nextElementKey == key
        private def completeSubStream(): Unit = {
          complete()
          activeSubstreamsMap.remove(key)
          if (!allowClosedSubstreamRecreation) {
            closedSubstreams.add(key)
          }
        }

        private def tryCompleteHandler(): Unit = {
          if (parent.isClosed(in) && !hasNextForSubSource) {
            completeSubStream()
            tryCompleteAll()
          }
        }

        override def onPull(): Unit = {
          cancelTimer(key)
          if (firstPush()) {
            firstPushCounter -= 1
            push(firstElement)
            firstElement = null.asInstanceOf[T]
            substreamsJustStared.remove(this)
            if (substreamsJustStared.isEmpty) setKeepGoing(false)
          } else if (hasNextForSubSource) {
            push(nextElementValue)
            clearNextElement()
          } else if (needToPull) pull(in)

          tryCompleteHandler()
        }

        override def onDownstreamFinish(cause: Throwable): Unit = {
          if (hasNextElement && nextElementKey == key) clearNextElement()
          if (firstPush()) firstPushCounter -= 1
          completeSubStream()
          if (parent.isClosed(out)) tryCancel(cause)
          if (parent.isClosed(in)) tryCompleteAll() else if (needToPull) pull(in)
        }

        setHandler(this)
      }
    }

  override def toString: String = "GroupBy"

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object Split {
  sealed abstract class SplitDecision

  /** Splits before the current element. The current element will be the first element in the new substream. */
  case object SplitBefore extends SplitDecision

  /** Splits after the current element. The current element will be the last element in the current substream. */
  case object SplitAfter extends SplitDecision

  def when[T](
      p: T => Boolean,
      substreamCancelStrategy: SubstreamCancelStrategy): Graph[FlowShape[T, Source[T, NotUsed]], NotUsed] =
    new Split(Split.SplitBefore, p, substreamCancelStrategy)

  def after[T](
      p: T => Boolean,
      substreamCancelStrategy: SubstreamCancelStrategy): Graph[FlowShape[T, Source[T, NotUsed]], NotUsed] =
    new Split(Split.SplitAfter, p, substreamCancelStrategy)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class Split[T](
    val decision: Split.SplitDecision,
    val p: T => Boolean,
    val substreamCancelStrategy: SubstreamCancelStrategy)
    extends GraphStage[FlowShape[T, Source[T, NotUsed]]] {

  val in: Inlet[T] = Inlet("Split.in")
  val out: Outlet[Source[T, NotUsed]] = Outlet("Split.out")

  override val shape: FlowShape[T, Source[T, NotUsed]] = FlowShape(in, out)

  private val propagateSubstreamCancel = substreamCancelStrategy match {
    case SubstreamCancelStrategies.Propagate => true
    case SubstreamCancelStrategies.Drain     => false
  }

  override protected def initialAttributes: Attributes = DefaultAttributes.split and SourceLocation.forLambda(p)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler { parent =>
      import Split._

      private val SubscriptionTimer = "SubstreamSubscriptionTimer"

      private val timeout: FiniteDuration =
        inheritedAttributes.mandatoryAttribute[ActorAttributes.StreamSubscriptionTimeout].timeout
      private var substreamSource: SubSourceOutlet[T] = null
      private var substreamWaitingToBePushed = false
      private var substreamCancelled = false

      override def onPull(): Unit = {
        if (substreamSource eq null) {
          //can be already pulled from substream in case split after
          if (!hasBeenPulled(in)) pull(in)
        } else if (substreamWaitingToBePushed) pushSubstreamSource()
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        // If the substream is already cancelled or it has not been handed out, we can go away
        if ((substreamSource eq null) || substreamWaitingToBePushed || substreamCancelled) cancelStage(cause)
      }

      override def onPush(): Unit = {
        val handler = new SubstreamHandler
        val elem = grab(in)

        decision match {
          case SplitAfter if p(elem) =>
            push(out, Source.single(elem))
          // Next pull will come from the next substream that we will open
          case _ =>
            handler.firstElem = elem
        }

        handOver(handler)
      }
      override def onUpstreamFinish(): Unit = completeStage()

      // initial input handler
      setHandlers(in, out, this)

      private def handOver(handler: SubstreamHandler): Unit = {
        if (isClosed(out)) completeStage()
        else {
          substreamSource = new SubSourceOutlet[T]("SplitSource")
          substreamSource.setHandler(handler)
          substreamCancelled = false
          setHandler(in, handler)
          setKeepGoing(enabled = handler.hasInitialElement)

          if (isAvailable(out)) {
            if (decision == SplitBefore || handler.hasInitialElement) pushSubstreamSource() else pull(in)
          } else substreamWaitingToBePushed = true
        }
      }

      private def pushSubstreamSource(): Unit = {
        push(out, Source.fromGraph(substreamSource.source))
        scheduleOnce(SubscriptionTimer, timeout)
        substreamWaitingToBePushed = false
      }

      override protected def onTimer(timerKey: Any): Unit = substreamSource.timeout(timeout)

      private class SubstreamHandler extends InHandler with OutHandler {

        var firstElem: T = null.asInstanceOf[T]

        def hasInitialElement: Boolean = firstElem.asInstanceOf[AnyRef] ne null
        private var willCompleteAfterInitialElement = false

        // Substreams are always assumed to be pushable position when we enter this method
        private def closeThis(handler: SubstreamHandler, currentElem: T): Unit = {
          decision match {
            case SplitAfter =>
              if (!substreamCancelled) {
                substreamSource.push(currentElem)
                substreamSource.complete()
              }
            case SplitBefore =>
              handler.firstElem = currentElem
              if (!substreamCancelled) substreamSource.complete()
          }
        }

        override def onPull(): Unit = {
          cancelTimer(SubscriptionTimer)
          if (hasInitialElement) {
            substreamSource.push(firstElem)
            firstElem = null.asInstanceOf[T]
            setKeepGoing(false)
            if (willCompleteAfterInitialElement) {
              substreamSource.complete()
              completeStage()
            }
          } else pull(in)
        }

        override def onDownstreamFinish(cause: Throwable): Unit = {
          substreamCancelled = true
          if (isClosed(in) || propagateSubstreamCancel) {
            cancelStage(cause)
          } else {
            // Start draining
            if (!hasBeenPulled(in)) pull(in)
          }
        }

        override def onPush(): Unit = {
          val elem = grab(in)
          try {
            if (p(elem)) {
              val handler = new SubstreamHandler
              closeThis(handler, elem)
              if (decision == SplitBefore) handOver(handler)
              else {
                substreamSource = null
                setHandler(in, parent)
                pull(in)
              }
            } else {
              // Drain into the void
              if (substreamCancelled) pull(in)
              else substreamSource.push(elem)
            }
          } catch {
            case NonFatal(ex) => onUpstreamFailure(ex)
          }
        }

        override def onUpstreamFinish(): Unit =
          if (hasInitialElement) willCompleteAfterInitialElement = true
          else {
            substreamSource.complete()
            completeStage()
          }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          substreamSource.fail(ex)
          failStage(ex)
        }

      }
    }
  override def toString: String = "Split"

}

/**
 * INTERNAL API
 */
@InternalApi private[stream] object SubSink {
  sealed trait State

  /** Not yet materialized and no command has been scheduled */
  case object Uninitialized extends State

  /** A command was scheduled before materialization */
  sealed abstract class CommandScheduledBeforeMaterialization(val command: Command) extends State

  // preallocated instances for both commands
  /** A RequestOne command was scheduled before materialization */
  case object RequestOneScheduledBeforeMaterialization extends CommandScheduledBeforeMaterialization(RequestOne)

  /** A Cancel command was scheduled before materialization */
  case class CancelScheduledBeforeMaterialization(cause: Throwable)
      extends CommandScheduledBeforeMaterialization(Cancel(cause))

  /** Steady state: sink has been materialized, commands can be delivered through the callback */
  // Represented in unwrapped form as AsyncCallback[Command] directly to prevent a level of indirection
  // case class Materialized(callback: AsyncCallback[Command]) extends State

  sealed trait Command
  case object RequestOne extends Command
  case class Cancel(cause: Throwable) extends Command
}

/**
 * INTERNAL API
 */
@InternalApi private[stream] final class SubSink[T](name: String, externalCallback: ActorSubscriberMessage => Unit)
    extends GraphStage[SinkShape[T]] {
  import SubSink._

  private val in = Inlet[T](s"SubSink($name).in")

  override def initialAttributes = Attributes.name(s"SubSink($name)")
  override val shape = SinkShape(in)

  private val status = new AtomicReference[ /* State */ AnyRef](Uninitialized)

  def pullSubstream(): Unit = dispatchCommand(RequestOneScheduledBeforeMaterialization)
  def cancelSubstream(): Unit = cancelSubstream(SubscriptionWithCancelException.NoMoreElementsNeeded)
  def cancelSubstream(cause: Throwable): Unit = dispatchCommand(CancelScheduledBeforeMaterialization(cause))

  @tailrec
  private def dispatchCommand(newState: CommandScheduledBeforeMaterialization): Unit =
    status.get match {
      case /* Materialized */ callback: AsyncCallback[Command @unchecked] => callback.invoke(newState.command)
      case Uninitialized =>
        if (!status.compareAndSet(Uninitialized, newState))
          dispatchCommand(newState) // changed to materialized in the meantime

      case RequestOneScheduledBeforeMaterialization if newState.isInstanceOf[CancelScheduledBeforeMaterialization] =>
        // cancellation is allowed to replace pull
        if (!status.compareAndSet(RequestOneScheduledBeforeMaterialization, newState))
          dispatchCommand(RequestOneScheduledBeforeMaterialization)

      case _: CancelScheduledBeforeMaterialization if newState.isInstanceOf[CancelScheduledBeforeMaterialization] =>
      // already cancelled, just ignore the new cancel and keep the old cause

      case cmd: CommandScheduledBeforeMaterialization =>
        throw new IllegalStateException(
          s"${newState.command} on subsink($name) is illegal when ${cmd.command} is still pending")

      case _ => throw new RuntimeException() // won't happen, compiler exhaustiveness check pleaser
    }

  override def createLogic(attr: Attributes) = new GraphStageLogic(shape) with InHandler {
    // check for previous materialization eagerly so we fail with a more useful stacktrace
    private[this] val materializationException: OptionVal[IllegalStateException] =
      if (status.get.isInstanceOf[AsyncCallback[_]])
        OptionVal.Some(createMaterializedTwiceException())
      else
        OptionVal.None

    setHandler(in, this)

    override def onPush(): Unit = externalCallback(ActorSubscriberMessage.OnNext(grab(in)))
    override def onUpstreamFinish(): Unit = externalCallback(ActorSubscriberMessage.OnComplete)
    override def onUpstreamFailure(ex: Throwable): Unit = externalCallback(ActorSubscriberMessage.OnError(ex))

    @tailrec
    private def setCallback(callback: Command => Unit): Unit =
      status.get match {
        case Uninitialized =>
          if (!status.compareAndSet(Uninitialized, /* Materialized */ getAsyncCallback[Command](callback)))
            setCallback(callback)

        case cmd: CommandScheduledBeforeMaterialization =>
          if (status.compareAndSet(cmd, /* Materialized */ getAsyncCallback[Command](callback)))
            // between those two lines a new command might have been scheduled, but that will go through the
            // async interface, so that the ordering is still kept
            callback(cmd.command)
          else
            setCallback(callback)

        case _: /* Materialized */ AsyncCallback[Command @unchecked] =>
          failStage(materializationException.getOrElse(createMaterializedTwiceException()))

        case _ => throw new RuntimeException() // won't happen, compiler exhaustiveness check pleaser
      }

    override def preStart(): Unit =
      setCallback {
        case RequestOne    => tryPull(in)
        case Cancel(cause) => cancelStage(cause)
      }

    def createMaterializedTwiceException(): IllegalStateException =
      new IllegalStateException(s"Substream Sink($name) cannot be materialized more than once")
  }

  override def toString: String = name
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class SubSource[T](
    name: String,
    private[fusing] val externalCallback: AsyncCallback[SubSink.Command])
    extends GraphStage[SourceShape[T]] {
  import SubSink._

  val out: Outlet[T] = Outlet(s"SubSource($name).out")
  override def initialAttributes = Attributes.name(s"SubSource($name)")
  override val shape: SourceShape[T] = SourceShape(out)

  private val status = new AtomicReference[AnyRef]

  def pushSubstream(elem: T): Unit = status.get match {
    case f: AsyncCallback[Any] @unchecked => f.invoke(ActorSubscriberMessage.OnNext(elem))
    case _                                => throw new IllegalStateException("cannot push to uninitialized substream")
  }

  def completeSubstream(): Unit = status.get match {
    case f: AsyncCallback[Any] @unchecked => f.invoke(ActorSubscriberMessage.OnComplete)
    case null =>
      if (!status.compareAndSet(null, ActorSubscriberMessage.OnComplete))
        status.get.asInstanceOf[AsyncCallback[Any]].invoke(ActorSubscriberMessage.OnComplete)
    case OnError(_)                        => // already failed out, keep the exception as that happened first
    case ActorSubscriberMessage.OnComplete => // it was already completed
    case _                                 => throw new RuntimeException() // won't happen, compiler exhaustiveness check pleaser
  }

  def failSubstream(ex: Throwable): Unit = status.get match {
    case f: AsyncCallback[Any] @unchecked => f.invoke(ActorSubscriberMessage.OnError(ex))
    case null =>
      val failure = ActorSubscriberMessage.OnError(ex)
      if (!status.compareAndSet(null, failure))
        status.get.asInstanceOf[AsyncCallback[Any]].invoke(failure)
    case ActorSubscriberMessage.OnComplete => // it was already completed, ignore failure as completion happened first
    case OnError(_)                        => // already failed out, keep the exception as that happened first
    case _                                 => throw new RuntimeException() // won't happen, compiler exhaustiveness check pleaser
  }

  def timeout(d: FiniteDuration): Boolean =
    status.compareAndSet(
      null,
      ActorSubscriberMessage.OnError(
        new SubscriptionTimeoutException(s"Substream Source($name) has not been materialized in $d")))

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) with OutHandler {
    // check for previous materialization eagerly so we fail with a more useful stacktrace
    private[this] val materializationException: OptionVal[IllegalStateException] =
      if (status.get.isInstanceOf[AsyncCallback[_]])
        OptionVal.Some(createMaterializedTwiceException())
      else
        OptionVal.None

    setHandler(out, this)

    @tailrec private def setCB(cb: AsyncCallback[ActorSubscriberMessage]): Unit = {
      status.get match {
        case null                               => if (!status.compareAndSet(null, cb)) setCB(cb)
        case ActorSubscriberMessage.OnComplete  => completeStage()
        case ActorSubscriberMessage.OnError(ex) => failStage(ex)
        case _: AsyncCallback[_] =>
          failStage(materializationException.getOrElse(createMaterializedTwiceException()))
        case _ => throw new RuntimeException() // won't happen, compiler exhaustiveness check pleaser
      }
    }

    override def preStart(): Unit = {
      val ourOwnCallback = getAsyncCallback[ActorSubscriberMessage] {
        case ActorSubscriberMessage.OnNext(elem) => push(out, elem.asInstanceOf[T])
        case ActorSubscriberMessage.OnComplete   => completeStage()
        case ActorSubscriberMessage.OnError(ex)  => failStage(ex)
      }
      setCB(ourOwnCallback)
    }

    override def onPull(): Unit = externalCallback.invoke(RequestOne)
    override def onDownstreamFinish(cause: Throwable): Unit = externalCallback.invoke(Cancel(cause))

    def createMaterializedTwiceException(): IllegalStateException =
      new IllegalStateException(s"Substream Source($name) cannot be materialized more than once")
  }

  override def toString: String = name
}
