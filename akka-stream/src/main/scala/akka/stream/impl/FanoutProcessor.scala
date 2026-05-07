/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import org.reactivestreams.Subscriber

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Deploy
import akka.actor.Props
import akka.annotation.InternalApi
import akka.stream.ActorAttributes.StreamSubscriptionTimeout
import akka.stream.Attributes
import akka.stream.StreamSubscriptionTimeoutTerminationMode
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi private[akka] abstract class FanoutOutputs(
    val maxBufferSize: Int,
    val initialBufferSize: Int,
    self: ActorRef,
    val pump: Pump)
    extends DefaultOutputTransferStates
    with SubscriberManagement[Any] {

  private var _subscribed = false
  def subscribed: Boolean = _subscribed

  override type S = ActorSubscriptionWithCursor[_ >: Any]
  override def createSubscription(subscriber: Subscriber[_ >: Any]): S = {
    _subscribed = true
    new ActorSubscriptionWithCursor(self, subscriber)
  }

  protected var exposedPublisher: ActorPublisher[Any] = _

  private var downstreamBufferSpace: Long = 0L
  private var downstreamCompleted = false
  override def demandAvailable = downstreamBufferSpace > 0
  override def demandCount: Long = downstreamBufferSpace

  override val subreceive = new SubReceive(waitingExposedPublisher)

  def enqueueOutputElement(elem: Any): Unit = {
    ReactiveStreamsCompliance.requireNonNullElement(elem)
    downstreamBufferSpace -= 1
    pushToDownstream(elem)
  }

  override def complete(): Unit =
    if (!downstreamCompleted) {
      downstreamCompleted = true
      completeDownstream()
    }

  override def cancel(): Unit = complete()

  override def error(e: Throwable): Unit = {
    if (!downstreamCompleted) {
      downstreamCompleted = true
      abortDownstream(e)
      if (exposedPublisher ne null) exposedPublisher.shutdown(Some(e))
    }
  }

  def isClosed: Boolean = downstreamCompleted

  def afterShutdown(): Unit

  override protected def requestFromUpstream(elements: Long): Unit = downstreamBufferSpace += elements

  private def subscribePending(): Unit =
    exposedPublisher.takePendingSubscribers().foreach(registerSubscriber)

  override protected def shutdown(completed: Boolean): Unit = {
    if (exposedPublisher ne null) {
      if (completed) exposedPublisher.shutdown(None)
      else exposedPublisher.shutdown(ActorPublisher.SomeNormalShutdownReason)
    }
    afterShutdown()
  }

  override protected def cancelUpstream(): Unit = {
    downstreamCompleted = true
  }

  protected def waitingExposedPublisher: Actor.Receive = {
    case ExposedPublisher(publisher) =>
      exposedPublisher = publisher
      subreceive.become(downstreamRunning)
    case other =>
      throw new IllegalStateException(s"The first message must be ExposedPublisher but was [$other]")
  }

  protected def downstreamRunning: Actor.Receive = {
    case SubscribePending =>
      subscribePending()
    case RequestMore(subscription, elements) =>
      moreRequested(subscription.asInstanceOf[ActorSubscriptionWithCursor[Any]], elements)
      pump.pump()
    case Cancel(subscription) =>
      unregisterSubscription(subscription.asInstanceOf[ActorSubscriptionWithCursor[Any]])
      pump.pump()
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object FanoutProcessorImpl {
  def props(attributes: Attributes): Props =
    Props(new FanoutProcessorImpl(attributes)).withDeploy(Deploy.local)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class FanoutProcessorImpl(attributes: Attributes) extends ActorProcessorImpl(attributes) {

  val StreamSubscriptionTimeout(timeout, timeoutMode) = attributes.mandatoryAttribute[StreamSubscriptionTimeout]
  val timeoutTimer = if (timeoutMode != StreamSubscriptionTimeoutTerminationMode.noop) {
    import context.dispatcher
    OptionVal.Some(context.system.scheduler.scheduleOnce(timeout, self, ActorProcessorImpl.SubscriptionTimeout))
  } else OptionVal.None

  override val primaryOutputs: FanoutOutputs = {
    val inputBuffer = attributes.mandatoryAttribute[Attributes.InputBuffer]
    new FanoutOutputs(inputBuffer.max, inputBuffer.initial, self, this) {
      override def afterShutdown(): Unit = afterFlush()
    }
  }

  val running: TransferPhase = TransferPhase(primaryInputs.NeedsInput && primaryOutputs.NeedsDemand) { () =>
    primaryOutputs.enqueueOutputElement(primaryInputs.dequeueInputElement())
  }

  override def pumpFinished(): Unit = {
    primaryInputs.cancel()
    primaryOutputs.complete()
  }

  override def postStop(): Unit = {
    super.postStop()
    timeoutTimer match {
      case OptionVal.Some(timer) => timer.cancel()
      case _                     =>
    }
  }

  def afterFlush(): Unit = context.stop(self)

  initialPhase(1, running)

  def subTimeoutHandling: Receive = {
    case ActorProcessorImpl.SubscriptionTimeout =>
      import StreamSubscriptionTimeoutTerminationMode._
      if (!primaryOutputs.subscribed) {
        timeoutMode match {
          case CancelTermination =>
            primaryInputs.cancel()
            context.stop(self)
          case WarnTermination =>
            log.warning("Subscription timeout for {}", this)
          case NoopTermination => // won't happen
        }
      }
  }
}
