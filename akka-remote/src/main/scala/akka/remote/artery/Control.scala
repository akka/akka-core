/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.ArrayDeque

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Try

import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.remote.UniqueAddress
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage._
import akka.util.OptionVal

/** INTERNAL API: marker trait for protobuf-serializable artery messages */
@InternalApi
private[remote] trait ArteryMessage extends Serializable

/**
 * INTERNAL API: Marker trait for reply messages
 */
@InternalApi
private[remote] trait Reply extends ControlMessage

/**
 * INTERNAL API
 * Marker trait for control messages that can be sent via the system message sub-channel
 * but don't need full reliable delivery. E.g. `HandshakeReq` and `Reply`.
 */
@InternalApi
private[remote] trait ControlMessage extends ArteryMessage

/**
 * INTERNAL API
 */
@InternalApi
private[remote] final case class Quarantined(from: UniqueAddress, to: UniqueAddress) extends ControlMessage

/**
 * INTERNAL API
 */
@InternalApi
private[remote] final case class ActorSystemTerminating(from: UniqueAddress) extends ControlMessage

/**
 * INTERNAL API
 */
@InternalApi
private[remote] final case class ActorSystemTerminatingAck(from: UniqueAddress) extends ArteryMessage

/**
 * INTERNAL API
 */
@InternalApi
private[remote] case object Flush extends ControlMessage

/**
 * INTERNAL API
 */
@InternalApi
private[remote] final case class FlushAck(expectedAcks: Int) extends ArteryMessage

/**
 * INTERNAL API
 */
@InternalApi
private[remote] object InboundControlJunction {

  /**
   * Observer subject for inbound control messages.
   * Interested observers can attach themselves to the
   * subject to get notification of incoming control
   * messages.
   */
  private[remote] trait ControlMessageSubject {
    def attach(observer: ControlMessageObserver): Future[Done]
    def detach(observer: ControlMessageObserver): Unit
  }

  private[remote] trait ControlMessageObserver {

    /**
     * Notification of incoming control message. The message
     * of the envelope is always a `ControlMessage`.
     */
    def notify(inboundEnvelope: InboundEnvelope): Unit

    def controlSubjectCompleted(signal: Try[Done]): Unit
  }

  // messages for the stream callback
  private[InboundControlJunction] sealed trait CallbackMessage
  private[InboundControlJunction] final case class Attach(observer: ControlMessageObserver, done: Promise[Done])
      extends CallbackMessage
  private[InboundControlJunction] final case class Dettach(observer: ControlMessageObserver) extends CallbackMessage
}

/**
 * INTERNAL API
 */
@InternalApi
private[remote] class InboundControlJunction
    extends GraphStageWithMaterializedValue[
      FlowShape[InboundEnvelope, InboundEnvelope],
      InboundControlJunction.ControlMessageSubject] {
  import InboundControlJunction._

  val in: Inlet[InboundEnvelope] = Inlet("InboundControlJunction.in")
  val out: Outlet[InboundEnvelope] = Outlet("InboundControlJunction.out")
  override val shape: FlowShape[InboundEnvelope, InboundEnvelope] = FlowShape(in, out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val logic = new GraphStageLogic(shape) with InHandler with OutHandler with ControlMessageSubject {

      private var observers: Vector[ControlMessageObserver] = Vector.empty

      private val callback = getAsyncCallback[CallbackMessage] {
        case Attach(observer, done) =>
          observers :+= observer
          done.success(Done)
        case Dettach(observer) =>
          observers = observers.filterNot(_ == observer)
      }

      override def postStop(): Unit = {
        observers.foreach(_.controlSubjectCompleted(Try(Done)))
        observers = Vector.empty
      }

      // InHandler
      override def onPush(): Unit = {
        grab(in) match {
          case env: InboundEnvelope if env.message.isInstanceOf[ControlMessage] =>
            observers.foreach(_.notify(env))
            pull(in)
          case env =>
            push(out, env)
        }
      }

      // OutHandler
      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)

      // ControlMessageSubject impl
      override def attach(observer: ControlMessageObserver): Future[Done] = {
        val p = Promise[Done]()
        callback.invoke(Attach(observer, p))
        p.future
      }

      override def detach(observer: ControlMessageObserver): Unit =
        callback.invoke(Dettach(observer))

    }

    (logic, logic)
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[remote] object OutboundControlJunction {
  private[remote] trait OutboundControlIngress {
    def sendControlMessage(message: ControlMessage): Unit
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[remote] class OutboundControlJunction(
    outboundContext: OutboundContext,
    outboundEnvelopePool: ObjectPool[ReusableOutboundEnvelope])
    extends GraphStageWithMaterializedValue[
      FlowShape[OutboundEnvelope, OutboundEnvelope],
      OutboundControlJunction.OutboundControlIngress] {
  import OutboundControlJunction._
  val in: Inlet[OutboundEnvelope] = Inlet("OutboundControlJunction.in")
  val out: Outlet[OutboundEnvelope] = Outlet("OutboundControlJunction.out")
  override val shape: FlowShape[OutboundEnvelope, OutboundEnvelope] = FlowShape(in, out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {

    val logic = new GraphStageLogic(shape)
      with InHandler
      with OutHandler
      with StageLogging
      with OutboundControlIngress {

      val sendControlMessageCallback = getAsyncCallback[ControlMessage](internalSendControlMessage)
      private val maxControlMessageBufferSize: Int = outboundContext.settings.Advanced.OutboundControlQueueSize
      private val buffer = new ArrayDeque[OutboundEnvelope]

      // InHandler
      override def onPush(): Unit = {
        if (buffer.isEmpty && isAvailable(out))
          push(out, grab(in))
        else
          buffer.offer(grab(in))
      }

      // OutHandler
      override def onPull(): Unit = {
        if (buffer.isEmpty && !hasBeenPulled(in))
          pull(in)
        else if (!buffer.isEmpty)
          push(out, buffer.poll())
      }

      private def internalSendControlMessage(message: ControlMessage): Unit = {
        if (buffer.isEmpty && isAvailable(out))
          push(out, wrap(message))
        else if (buffer.size < maxControlMessageBufferSize)
          buffer.offer(wrap(message))
        else {
          // it's alright to drop control messages
          log.debug("Dropping control message [{}] due to full buffer.", Logging.messageClassName(message))
        }
      }

      private def wrap(message: ControlMessage): OutboundEnvelope =
        outboundEnvelopePool.acquire().init(recipient = OptionVal.None, message = message, sender = OptionVal.None)

      override def sendControlMessage(message: ControlMessage): Unit =
        sendControlMessageCallback.invoke(message)

      setHandlers(in, out, this)
    }

    (logic, logic)
  }

}
