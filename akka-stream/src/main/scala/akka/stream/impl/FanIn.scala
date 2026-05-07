/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import org.reactivestreams.{ Subscriber, Subscription }

import akka.actor._
import akka.annotation.{ DoNotInherit, InternalApi }
import akka.stream.AbruptTerminationException
import akka.stream.ActorAttributes
import akka.stream.Attributes

/**
 * INTERNAL API
 */
@InternalApi private[akka] object FanIn {

  final case class OnError(id: Int, cause: Throwable)
      extends DeadLetterSuppression
      with NoSerializationVerificationNeeded
  final case class OnComplete(id: Int) extends DeadLetterSuppression with NoSerializationVerificationNeeded
  final case class OnNext(id: Int, e: Any) extends DeadLetterSuppression with NoSerializationVerificationNeeded
  final case class OnSubscribe(id: Int, subscription: Subscription)
      extends DeadLetterSuppression
      with NoSerializationVerificationNeeded

  final case class SubInput[T](impl: ActorRef, id: Int) extends Subscriber[T] {
    override def onError(cause: Throwable): Unit = {
      ReactiveStreamsCompliance.requireNonNullException(cause)
      impl ! OnError(id, cause)
    }
    override def onComplete(): Unit = impl ! OnComplete(id)
    override def onNext(element: T): Unit = {
      ReactiveStreamsCompliance.requireNonNullElement(element)
      impl ! OnNext(id, element)
    }
    override def onSubscribe(subscription: Subscription): Unit = {
      ReactiveStreamsCompliance.requireNonNullSubscription(subscription)
      impl ! OnSubscribe(id, subscription)
    }
  }

  type State = Byte
  final val Marked = 1
  final val Pending = 2
  final val Depleted = 4
  final val Completed = 8
  final val Cancelled = 16

  abstract class InputBunch(inputCount: Int, bufferSize: Int, pump: Pump) {
    private var allCancelled = false

    private val inputs: Array[BatchingInputBuffer] = Array.tabulate(inputCount) { i =>
      new BatchingInputBuffer(bufferSize, pump) {
        override protected def onError(e: Throwable): Unit = InputBunch.this.onError(i, e)
      }
    }

    private[this] final val states = new Array[State](inputCount)
    private var markCount = 0
    private var markedPending = 0
    private var markedDepleted = 0

    private var receivedInput = false
    private var completedCounter = 0

    private[this] final def hasState(index: Int, flag: Int): Boolean =
      (states(index) & flag) != 0
    private[this] final def setState(index: Int, flag: Int, on: Boolean): Unit =
      states(index) = if (on) (states(index) | flag).toByte else (states(index) & ~flag).toByte

    private[this] final def cancelled(index: Int): Boolean = hasState(index, Cancelled)
    private[this] final def cancelled(index: Int, on: Boolean): Unit = setState(index, Cancelled, on)

    private[this] final def completed(index: Int): Boolean = hasState(index, Completed)
    private[this] final def registerCompleted(index: Int): Unit = {
      completedCounter += 1
      setState(index, Completed, on = true)
    }

    private[this] final def depleted(index: Int): Boolean = hasState(index, Depleted)
    private[this] final def depleted(index: Int, on: Boolean): Unit = setState(index, Depleted, on)

    private[this] final def pending(index: Int): Boolean = hasState(index, Pending)
    private[this] final def pending(index: Int, on: Boolean): Unit = setState(index, Pending, on)

    private[this] final def marked(index: Int): Boolean = hasState(index, Marked)
    private[this] final def marked(index: Int, on: Boolean): Unit = setState(index, Marked, on)

    override def toString: String =
      s"""|InputBunch
          |  marked:    ${states.iterator.map(marked(_)).mkString(", ")}
          |  pending:   ${states.iterator.map(pending(_)).mkString(", ")}
          |  depleted:  ${states.iterator.map(depleted(_)).mkString(", ")}
          |  completed: ${states.iterator.map(completed(_)).mkString(", ")}
          |  cancelled: ${states.iterator.map(cancelled(_)).mkString(", ")}
          |
          |    mark=$markCount pend=$markedPending depl=$markedDepleted pref=$preferredId""".stripMargin

    private var preferredId = 0
    private var _lastDequeuedId = 0
    def lastDequeuedId = _lastDequeuedId

    def cancel(): Unit =
      if (!allCancelled) {
        allCancelled = true
        var i = 0
        while (i < inputs.length) {
          cancel(i)
          i += 1
        }
      }

    def cancel(input: Int) =
      if (!cancelled(input)) {
        inputs(input).cancel()
        cancelled(input, on = true)
        unmarkInput(input)
      }

    def onError(input: Int, e: Throwable): Unit

    def onDepleted(input: Int): Unit = ()

    def onCompleteWhenNoInput(): Unit = ()

    def markInput(input: Int): Unit = {
      if (!marked(input)) {
        if (depleted(input)) markedDepleted += 1
        if (pending(input)) markedPending += 1
        marked(input, on = true)
        markCount += 1
      }
    }

    def unmarkInput(input: Int): Unit = {
      if (marked(input)) {
        if (depleted(input)) markedDepleted -= 1
        if (pending(input)) markedPending -= 1
        marked(input, on = false)
        markCount -= 1
      }
    }

    def markAllInputs(): Unit = {
      var i = 0
      while (i < inputCount) {
        markInput(i)
        i += 1
      }
    }

    def unmarkAllInputs(): Unit = {
      var i = 0
      while (i < inputCount) {
        unmarkInput(i)
        i += 1
      }
    }

    def isPending(input: Int): Boolean = pending(input)

    def isDepleted(input: Int): Boolean = depleted(input)

    def isCancelled(input: Int): Boolean = cancelled(input)

    def isAllCompleted(): Boolean = inputCount == completedCounter

    def idToDequeue(): Int = {
      var id = preferredId
      while (!(marked(id) && pending(id))) {
        id += 1
        if (id == inputCount) id = 0
        require(id != preferredId, "Tried to dequeue without waiting for any input")
      }
      id
    }

    def dequeue(id: Int): Any = {
      require(!isDepleted(id), s"Can't dequeue from depleted $id")
      require(isPending(id), s"No pending input at $id")
      _lastDequeuedId = id
      val input = inputs(id)
      val elem = input.dequeueInputElement()
      if (!input.inputsAvailable) {
        if (marked(id)) markedPending -= 1
        pending(id, on = false)
      }
      if (input.inputsDepleted) {
        if (marked(id)) markedDepleted += 1
        depleted(id, on = true)
        onDepleted(id)
      }
      elem
    }

    def dequeueAndYield(): Any =
      dequeueAndYield(idToDequeue())

    def dequeueAndYield(id: Int): Any = {
      preferredId = id + 1
      if (preferredId == inputCount) preferredId = 0
      dequeue(id)
    }

    def dequeuePreferring(preferred: Int): Any = {
      preferredId = preferred
      val id = idToDequeue()
      dequeue(id)
    }

    val AllOfMarkedInputs = new TransferState {
      override def isCompleted: Boolean = markedDepleted > 0
      override def isReady: Boolean = markedPending == markCount
    }

    val AnyOfMarkedInputs = new TransferState {
      override def isCompleted: Boolean = markedDepleted == markCount && markedPending == 0
      override def isReady: Boolean = markedPending > 0
    }

    def inputsAvailableFor(id: Int) = new TransferState {
      override def isCompleted: Boolean = depleted(id) || cancelled(id) || (!pending(id) && completed(id))
      override def isReady: Boolean = pending(id)
    }

    def inputsOrCompleteAvailableFor(id: Int) = new TransferState {
      override def isCompleted: Boolean = false
      override def isReady: Boolean = pending(id) || depleted(id)
    }

    // FIXME: Eliminate re-wraps
    def subreceive: SubReceive =
      new SubReceive({
        case OnNext(id, elem) =>
          if (marked(id) && !pending(id)) markedPending += 1
          pending(id, on = true)
          receivedInput = true
          inputs(id).subreceive(ActorSubscriberMessage.OnNext(elem))
        case OnSubscribe(id, subscription) =>
          inputs(id).subreceive(ActorSubscriberMessage.OnSubscribe(subscription))
        case OnComplete(id) =>
          if (!pending(id)) {
            if (marked(id) && !depleted(id)) markedDepleted += 1
            depleted(id, on = true)
            onDepleted(id)
          }
          registerCompleted(id)
          inputs(id).subreceive(ActorSubscriberMessage.OnComplete)
          if (!receivedInput && isAllCompleted()) onCompleteWhenNoInput()
        case OnError(id, e) =>
          onError(id, e)
      })

  }

}

/**
 * INTERNAL API
 */
@DoNotInherit private[akka] class FanIn(attributes: Attributes, val inputCount: Int)
    extends Actor
    with ActorLogging
    with Pump {
  import FanIn._

  protected val primaryOutputs: Outputs = new SimpleOutputs(self, this)
  protected val inputBunch = {
    val maxInputBufferSize = attributes.mandatoryAttribute[Attributes.InputBuffer].max
    new InputBunch(inputCount, maxInputBufferSize, this) {
      override def onError(input: Int, e: Throwable): Unit = fail(e)
      override def onCompleteWhenNoInput(): Unit = pumpFinished()
    }
  }
  private val debugLoggingEnabled = attributes.mandatoryAttribute[ActorAttributes.DebugLogging].enabled

  override def pumpFinished(): Unit = {
    inputBunch.cancel()
    primaryOutputs.complete()
    context.stop(self)
  }

  override def pumpFailed(e: Throwable): Unit = fail(e)

  protected def fail(e: Throwable): Unit = {
    if (debugLoggingEnabled)
      log.debug("fail due to: {}", e.getMessage)
    nextPhase(completedPhase)
    primaryOutputs.error(e)
    pump()
  }

  override def postStop(): Unit = {
    inputBunch.cancel()
    primaryOutputs.error(AbruptTerminationException(self))
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    throw new IllegalStateException("This actor cannot be restarted")
  }

  def receive = inputBunch.subreceive.orElse[Any, Unit](primaryOutputs.subreceive)

}
