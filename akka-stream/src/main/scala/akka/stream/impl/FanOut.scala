/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.collection.immutable

import org.reactivestreams.Subscription

import akka.actor._
import akka.annotation.{ DoNotInherit, InternalApi }
import akka.stream.AbruptTerminationException
import akka.stream.ActorAttributes
import akka.stream.Attributes

/**
 * INTERNAL API
 */
@InternalApi private[akka] object FanOut {

  final case class SubstreamRequestMore(id: Int, demand: Long)
      extends DeadLetterSuppression
      with NoSerializationVerificationNeeded
  final case class SubstreamCancel(id: Int) extends DeadLetterSuppression with NoSerializationVerificationNeeded
  final case class SubstreamSubscribePending(id: Int)
      extends DeadLetterSuppression
      with NoSerializationVerificationNeeded

  class SubstreamSubscription(val parent: ActorRef, val id: Int) extends Subscription {
    override def request(elements: Long): Unit = parent ! SubstreamRequestMore(id, elements)
    override def cancel(): Unit = parent ! SubstreamCancel(id)
    override def toString = "SubstreamSubscription" + System.identityHashCode(this)
  }

  class FanoutOutputs(val id: Int, _impl: ActorRef, _pump: Pump) extends SimpleOutputs(_impl, _pump) {
    override def createSubscription(): Subscription = new SubstreamSubscription(actor, id)
  }

  final case class ExposedPublishers(publishers: immutable.Seq[ActorPublisher[Any]])
      extends DeadLetterSuppression
      with NoSerializationVerificationNeeded

  class OutputBunch(outputCount: Int, impl: ActorRef, pump: Pump) {
    private var bunchCancelled = false

    private val outputs = Array.tabulate(outputCount)(new FanoutOutputs(_, impl, pump))

    private val marked = new Array[Boolean](outputCount)
    private var markedCount = 0
    private val pending = new Array[Boolean](outputCount)
    private var markedPending = 0
    private val cancelled = new Array[Boolean](outputCount)
    private var markedCancelled = 0
    private val completed = new Array[Boolean](outputCount)
    private val errored = new Array[Boolean](outputCount)

    override def toString: String =
      s"""|OutputBunch
          |  marked:    ${marked.mkString(", ")}
          |  pending:   ${pending.mkString(", ")}
          |  errored:   ${errored.mkString(", ")}
          |  completed: ${completed.mkString(", ")}
          |  cancelled: ${cancelled.mkString(", ")}
          |    mark=$markedCount pend=$markedPending depl=$markedCancelled pref=$preferredId unmark=$unmarkCancelled""".stripMargin

    private var unmarkCancelled = true

    private var preferredId = 0

    def isPending(output: Int): Boolean = pending(output)

    def isCompleted(output: Int): Boolean = completed(output)

    def isCancelled(output: Int): Boolean = cancelled(output)

    def isErrored(output: Int): Boolean = errored(output)

    def complete(): Unit =
      if (!bunchCancelled) {
        bunchCancelled = true
        var i = 0
        while (i < outputs.length) {
          complete(i)
          i += 1
        }
      }

    def complete(output: Int) =
      if (!completed(output) && !errored(output) && !cancelled(output)) {
        outputs(output).complete()
        completed(output) = true
        unmarkOutput(output)
      }

    def cancel(e: Throwable): Unit =
      if (!bunchCancelled) {
        bunchCancelled = true
        var i = 0
        while (i < outputs.length) {
          error(i, e)
          i += 1
        }
      }

    def error(output: Int, e: Throwable): Unit =
      if (!errored(output) && !cancelled(output) && !completed(output)) {
        outputs(output).error(e)
        errored(output) = true
        unmarkOutput(output)
      }

    def markOutput(output: Int): Unit = {
      if (!marked(output)) {
        if (cancelled(output)) markedCancelled += 1
        if (pending(output)) markedPending += 1
        marked(output) = true
        markedCount += 1
      }
    }

    def unmarkOutput(output: Int): Unit = {
      if (marked(output)) {
        if (cancelled(output)) markedCancelled -= 1
        if (pending(output)) markedPending -= 1
        marked(output) = false
        markedCount -= 1
      }
    }

    def markAllOutputs(): Unit = {
      var i = 0
      while (i < outputCount) {
        markOutput(i)
        i += 1
      }
    }

    def unmarkAllOutputs(): Unit = {
      var i = 0
      while (i < outputCount) {
        unmarkOutput(i)
        i += 1
      }
    }

    def unmarkCancelledOutputs(enabled: Boolean): Unit = unmarkCancelled = enabled

    def idToEnqueue(): Int = {
      var id = preferredId
      while (!(marked(id) && pending(id))) {
        id += 1
        if (id == outputCount) id = 0
        require(id != preferredId, "Tried to enqueue without waiting for any demand")
      }
      id
    }

    def enqueue(id: Int, elem: Any): Unit = {
      val output = outputs(id)
      output.enqueueOutputElement(elem)
      if (!output.demandAvailable) {
        if (marked(id)) markedPending -= 1
        pending(id) = false
      }
    }

    def enqueueMarked(elem: Any): Unit = {
      var id = 0
      while (id < outputCount) {
        if (marked(id)) enqueue(id, elem)
        id += 1
      }
    }

    def idToEnqueueAndYield(): Int = {
      val id = idToEnqueue()
      preferredId = id + 1
      if (preferredId == outputCount) preferredId = 0
      id
    }

    def enqueueAndYield(elem: Any): Unit = {
      val id = idToEnqueueAndYield()
      enqueue(id, elem)
    }

    def enqueueAndPrefer(elem: Any, preferred: Int): Unit = {
      val id = idToEnqueue()
      preferredId = preferred
      enqueue(id, elem)
    }

    def onCancel(output: Int): Unit = ()

    def demandAvailableFor(id: Int) = new TransferState {
      override def isCompleted: Boolean = cancelled(id) || completed(id) || errored(id)
      override def isReady: Boolean = pending(id)
    }

    def demandOrCancelAvailableFor(id: Int) = new TransferState {
      override def isCompleted: Boolean = false
      override def isReady: Boolean = pending(id) || cancelled(id)
    }

    /**
     * Will only transfer an element when all marked outputs
     * have demand, and will complete as soon as any of the marked
     * outputs have canceled.
     */
    val AllOfMarkedOutputs = new TransferState {
      override def isCompleted: Boolean = markedCancelled > 0 || markedCount == 0
      override def isReady: Boolean = markedPending == markedCount
    }

    /**
     * Will transfer an element when any of the  marked outputs
     * have demand, and will complete when all of the marked
     * outputs have canceled.
     */
    val AnyOfMarkedOutputs = new TransferState {
      override def isCompleted: Boolean = markedCancelled == markedCount
      override def isReady: Boolean = markedPending > 0
    }

    // FIXME: Eliminate re-wraps
    def subreceive: SubReceive =
      new SubReceive({
        case ExposedPublishers(publishers) =>
          publishers.zip(outputs).foreach {
            case (pub, output) =>
              output.subreceive(ExposedPublisher(pub))
          }

        case SubstreamRequestMore(id, demand) =>
          if (demand < 1) // According to Reactive Streams Spec 3.9, with non-positive demand must yield onError
            error(id, ReactiveStreamsCompliance.numberOfElementsInRequestMustBePositiveException)
          else {
            if (marked(id) && !pending(id)) markedPending += 1
            pending(id) = true
            outputs(id).subreceive(RequestMore(null, demand))
          }
        case SubstreamCancel(id) =>
          if (unmarkCancelled) {
            unmarkOutput(id)
          }
          if (marked(id) && !cancelled(id)) markedCancelled += 1
          cancelled(id) = true
          onCancel(id)
          outputs(id).subreceive(Cancel(null))
        case SubstreamSubscribePending(id) =>
          outputs(id).subreceive(SubscribePending)
      })

  }

}

/**
 * INTERNAL API
 */
@DoNotInherit private[akka] abstract class FanOut(attributes: Attributes, val outputCount: Int)
    extends Actor
    with ActorLogging
    with Pump {
  import FanOut._

  protected val outputBunch = new OutputBunch(outputCount, self, this)
  protected val primaryInputs: Inputs = {
    val maxInputBufferSize = attributes.mandatoryAttribute[Attributes.InputBuffer].max
    new BatchingInputBuffer(maxInputBufferSize, this) {
      override def onError(e: Throwable): Unit = fail(e)
    }
  }
  private val debugLoggingEnabled = attributes.mandatoryAttribute[ActorAttributes.DebugLogging].enabled

  override def pumpFinished(): Unit = {
    primaryInputs.cancel()
    outputBunch.complete()
    context.stop(self)
  }

  override def pumpFailed(e: Throwable): Unit = fail(e)

  protected def fail(e: Throwable): Unit = {
    if (debugLoggingEnabled)
      log.debug("fail due to: {}", e.getMessage)
    primaryInputs.cancel()
    outputBunch.cancel(e)
    pump()
  }

  override def postStop(): Unit = {
    primaryInputs.cancel()
    outputBunch.cancel(AbruptTerminationException(self))
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    throw new IllegalStateException("This actor cannot be restarted")
  }

  def receive = primaryInputs.subreceive.orElse[Any, Unit](outputBunch.subreceive)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object Unzip {
  def props(attributes: Attributes): Props =
    Props(new Unzip(attributes)).withDeploy(Deploy.local)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class Unzip(attributes: Attributes) extends FanOut(attributes, outputCount = 2) {
  outputBunch.markAllOutputs()

  initialPhase(
    1,
    TransferPhase(primaryInputs.NeedsInput && outputBunch.AllOfMarkedOutputs) { () =>
      primaryInputs.dequeueInputElement() match {
        case (a, b) =>
          outputBunch.enqueue(0, a)
          outputBunch.enqueue(1, b)

        case t: akka.japi.Pair[_, _] =>
          outputBunch.enqueue(0, t.first)
          outputBunch.enqueue(1, t.second)

        case t =>
          throw new IllegalArgumentException(
            s"Unable to unzip elements of type ${t.getClass.getName}, " +
            s"can only handle Tuple2 and akka.japi.Pair!")
      }
    })
}
