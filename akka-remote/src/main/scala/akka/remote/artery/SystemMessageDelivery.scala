/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.ArrayDeque

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NoStackTrace

import akka.Done
import akka.actor.ActorRef
import akka.annotation.InternalApi
import akka.dispatch.sysmsg.SystemMessage
import akka.event.Logging
import akka.remote.UniqueAddress
import akka.remote.artery.InboundControlJunction.ControlMessageObserver
import akka.remote.artery.OutboundHandshake.HandshakeReq
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.StageLogging
import akka.stream.stage.TimerGraphStageLogic
import akka.util.OptionVal
import akka.util.PrettyDuration.PrettyPrintableDuration

/**
 * INTERNAL API
 */
@InternalApi private[remote] object SystemMessageDelivery {
  final case class SystemMessageEnvelope(message: AnyRef, seqNo: Long, ackReplyTo: UniqueAddress) extends ArteryMessage
  final case class Ack(seqNo: Long, from: UniqueAddress) extends Reply
  final case class Nack(seqNo: Long, from: UniqueAddress) extends Reply

  /**
   * Sent when an incarnation of an Association is quarantined. Consumed by the
   * SystemMessageDelivery operator on the sending side, i.e. not sent to remote system.
   * The SystemMessageDelivery operator will clear the sequence number and other state associated
   * operator
   *
   * The incarnation counter is bumped when the handshake is completed, so a new incarnation
   * corresponds to a new UID of the remote system.
   *
   * The SystemMessageDelivery operator also detects that the incarnation has changed when sending or resending
   * system messages.
   */
  final case class ClearSystemMessageDelivery(incarnation: Int)

  final class GaveUpSystemMessageException(msg: String) extends RuntimeException(msg) with NoStackTrace

  private case object ResendTick

  // If other message types than SystemMessage need acked delivery they can extend this trait.
  // Used in tests since real SystemMessage are somewhat cumbersome to create.
  trait AckedDeliveryMessage

}

/**
 * INTERNAL API
 */
@InternalApi private[remote] class SystemMessageDelivery(
    outboundContext: OutboundContext,
    deadLetters: ActorRef,
    resendInterval: FiniteDuration,
    maxBufferSize: Int)
    extends GraphStage[FlowShape[OutboundEnvelope, OutboundEnvelope]] {

  import SystemMessageDelivery._

  val in: Inlet[OutboundEnvelope] = Inlet("SystemMessageDelivery.in")
  val out: Outlet[OutboundEnvelope] = Outlet("SystemMessageDelivery.out")
  override val shape: FlowShape[OutboundEnvelope, OutboundEnvelope] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler with ControlMessageObserver with StageLogging {

      private var replyObserverAttached = false
      private var seqNo = 0L // sequence number for the first message will be 1
      private var incarnation = outboundContext.associationState.incarnation
      private val unacknowledged = new ArrayDeque[OutboundEnvelope]
      private var resending = new ArrayDeque[OutboundEnvelope]
      private var stopping = false

      private val giveUpAfterNanos = outboundContext.settings.Advanced.GiveUpSystemMessageAfter.toNanos
      private var ackTimestamp = System.nanoTime()

      private def localAddress = outboundContext.localAddress
      private def remoteAddress = outboundContext.remoteAddress
      private def remoteAddressLogParam: String =
        outboundContext.associationState.uniqueRemoteAddress().getOrElse(remoteAddress).toString

      override protected def logSource: Class[_] = classOf[SystemMessageDelivery]

      override def preStart(): Unit = {
        val callback = getAsyncCallback[Done] { _ =>
          replyObserverAttached = true
          if (isAvailable(out))
            pull(in) // onPull from downstream already called
        }
        outboundContext.controlSubject.attach(this).foreach(callback.invoke)(ExecutionContext.parasitic)
      }

      override def postStop(): Unit = {
        val pendingCount = unacknowledged.size
        sendUnacknowledgedToDeadLetters()
        unacknowledged.clear()
        if (pendingCount > 0)
          outboundContext.quarantine(s"SystemMessageDelivery stopped with [$pendingCount] pending system messages.")
        outboundContext.controlSubject.detach(this)
      }

      override def onUpstreamFinish(): Unit = {
        if (unacknowledged.isEmpty)
          super.onUpstreamFinish()
        else
          stopping = true
      }

      override protected def onTimer(timerKey: Any): Unit =
        timerKey match {
          case ResendTick =>
            checkGiveUp()
            if (resending.isEmpty && !unacknowledged.isEmpty) {
              resending = unacknowledged.clone()
              tryResend()
            }
            if (!unacknowledged.isEmpty)
              scheduleOnce(ResendTick, resendInterval)

          case other =>
            throw new IllegalArgumentException(s"Unknown timer key: $other")
        }

      // ControlMessageObserver, external call
      override def notify(inboundEnvelope: InboundEnvelope): Unit = {
        inboundEnvelope.message match {
          case ack: Ack   => if (ack.from.address == remoteAddress) ackCallback.invoke(ack)
          case nack: Nack => if (nack.from.address == remoteAddress) nackCallback.invoke(nack)
          case _          => // not interested
        }
      }

      // ControlMessageObserver, external call but on graph logic machinery thread (getAsyncCallback safe)
      override def controlSubjectCompleted(signal: Try[Done]): Unit = {
        getAsyncCallback[Try[Done]] {
          case Success(_)     => completeStage()
          case Failure(cause) => failStage(cause)
        }.invoke(signal)
      }

      private val ackCallback = getAsyncCallback[Ack] { reply =>
        ack(reply.seqNo)
      }

      private val nackCallback = getAsyncCallback[Nack] { reply =>
        if (reply.seqNo <= seqNo) {
          ack(reply.seqNo)
          log.warning(
            "Received negative acknowledgement of system message from [{}], highest acknowledged [{}]",
            outboundContext.remoteAddress,
            reply.seqNo)
          // Nack should be very rare (connection issue) so no urgency of resending, it will be resent
          // by the scheduled tick.
        }
      }

      private def ack(n: Long): Unit = {
        ackTimestamp = System.nanoTime()
        if (n <= seqNo)
          clearUnacknowledged(n)
      }

      @tailrec private def clearUnacknowledged(ackedSeqNo: Long): Unit = {
        if (!unacknowledged.isEmpty &&
            unacknowledged.peek().message.asInstanceOf[SystemMessageEnvelope].seqNo <= ackedSeqNo) {
          unacknowledged.removeFirst()
          if (unacknowledged.isEmpty)
            cancelTimer(resendInterval)

          if (stopping && unacknowledged.isEmpty)
            completeStage()
          else
            clearUnacknowledged(ackedSeqNo)
        }
      }

      private def tryResend(): Unit = {
        if (isAvailable(out) && !resending.isEmpty) {
          val env = resending.poll()

          if (log.isDebugEnabled) {
            env.message match {
              case SystemMessageEnvelope(msg, n, _) =>
                log.debug("Resending system message [{}] [{}]", Logging.simpleName(msg), n)
              case _ =>
                log.debug("Resending control message [{}]", Logging.simpleName(env.message))
            }
          }

          if (incarnation != outboundContext.associationState.incarnation) {
            log.debug("Noticed new incarnation of [{}] from tryResend, clear state", remoteAddressLogParam)
            clear()
          }

          pushCopy(env)
        }
      }

      // important to not send the buffered instance, since it's mutable
      private def pushCopy(outboundEnvelope: OutboundEnvelope): Unit = {
        push(out, outboundEnvelope.copy())
      }

      // InHandler
      override def onPush(): Unit = {
        val outboundEnvelope = grab(in)
        outboundEnvelope.message match {
          case msg @ (_: SystemMessage | _: AckedDeliveryMessage) =>
            if (unacknowledged.size < maxBufferSize) {
              if (seqNo == 0) {
                incarnation = outboundContext.associationState.incarnation
              } else if (incarnation != outboundContext.associationState.incarnation) {
                log.debug("Noticed new incarnation of [{}] from onPush, clear state", remoteAddressLogParam)
                clear()
              }
              seqNo += 1
              if (unacknowledged.isEmpty)
                ackTimestamp = System.nanoTime()
              else
                checkGiveUp()
              val sendEnvelope = outboundEnvelope.withMessage(SystemMessageEnvelope(msg, seqNo, localAddress))
              unacknowledged.offer(sendEnvelope)
              scheduleOnce(ResendTick, resendInterval)
              if (resending.isEmpty && isAvailable(out))
                pushCopy(sendEnvelope)
              else {
                resending.offer(sendEnvelope)
                tryResend()
              }
            } else {
              // buffer overflow
              outboundContext.quarantine(reason = s"System message delivery buffer overflow, size [$maxBufferSize]")
              deadLetters ! outboundEnvelope
              pull(in)
            }
          case _: HandshakeReq =>
            // pass on HandshakeReq
            if (isAvailable(out))
              pushCopy(outboundEnvelope)
          case ClearSystemMessageDelivery(i) =>
            if (i <= incarnation) {
              log.debug("Clear system message delivery of [{}]", remoteAddressLogParam)
              clear()
            }
            pull(in)
          case _ =>
            // e.g. ActorSystemTerminating or ActorSelectionMessage with PriorityMessage, no need for acked delivery
            if (resending.isEmpty && isAvailable(out))
              push(out, outboundEnvelope)
            else {
              resending.offer(outboundEnvelope)
              tryResend()
            }
        }
      }

      private def checkGiveUp(): Unit = {
        if (!unacknowledged.isEmpty && (System.nanoTime() - ackTimestamp > giveUpAfterNanos))
          throw new GaveUpSystemMessageException(
            s"Gave up sending system message to [${outboundContext.remoteAddress}] after " +
            s"${outboundContext.settings.Advanced.GiveUpSystemMessageAfter.pretty}.")
      }

      private def clear(): Unit = {
        sendUnacknowledgedToDeadLetters()
        seqNo = 0L // sequence number for the first message will be 1
        incarnation = outboundContext.associationState.incarnation
        unacknowledged.clear()
        resending.clear()
        cancelTimer(resendInterval)
      }

      private def sendUnacknowledgedToDeadLetters(): Unit = {
        val iter = unacknowledged.iterator
        while (iter.hasNext()) {
          deadLetters ! iter.next()
        }
      }

      // OutHandler
      override def onPull(): Unit = {
        if (replyObserverAttached) { // otherwise it will be pulled after attached
          if (resending.isEmpty && !hasBeenPulled(in) && !stopping)
            pull(in)
          else
            tryResend()
        }
      }

      setHandlers(in, out, this)
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SystemMessageAcker {
  val MaxNegativeAcknowledgementLogging = 1000
}

/**
 * INTERNAL API
 */
@InternalApi private[remote] class SystemMessageAcker(inboundContext: InboundContext)
    extends GraphStage[FlowShape[InboundEnvelope, InboundEnvelope]] {
  import SystemMessageAcker._
  import SystemMessageDelivery._

  val in: Inlet[InboundEnvelope] = Inlet("SystemMessageAcker.in")
  val out: Outlet[InboundEnvelope] = Outlet("SystemMessageAcker.out")
  override val shape: FlowShape[InboundEnvelope, InboundEnvelope] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

      // TODO we might need have to prune old unused entries
      var sequenceNumbers = Map.empty[UniqueAddress, Long]
      var nackCount = 0

      def localAddress = inboundContext.localAddress

      override protected def logSource: Class[_] = classOf[SystemMessageAcker]

      // InHandler
      override def onPush(): Unit = {
        val env = grab(in)

        // for logging
        def fromRemoteAddressStr: String = env.association match {
          case OptionVal.Some(a) => a.remoteAddress.toString
          case _                 => "N/A"
        }

        env.message match {
          case sysEnv @ SystemMessageEnvelope(_, n, ackReplyTo) =>
            val expectedSeqNo = sequenceNumbers.get(ackReplyTo) match {
              case None        => 1L
              case Some(seqNo) => seqNo
            }
            if (n == expectedSeqNo) {
              inboundContext.sendControl(ackReplyTo.address, Ack(n, localAddress))
              sequenceNumbers = sequenceNumbers.updated(ackReplyTo, n + 1)
              val unwrapped = env.withMessage(sysEnv.message)
              push(out, unwrapped)
            } else if (n < expectedSeqNo) {
              if (log.isDebugEnabled)
                log.debug(
                  "Deduplicate system message [{}] from [{}], expected [{}]",
                  n,
                  fromRemoteAddressStr,
                  expectedSeqNo)
              inboundContext.sendControl(ackReplyTo.address, Ack(expectedSeqNo - 1, localAddress))
              pull(in)
            } else {
              if (nackCount < MaxNegativeAcknowledgementLogging) {
                nackCount += 1
                val maxNackReached =
                  if (nackCount == MaxNegativeAcknowledgementLogging)
                    s". This happened [$MaxNegativeAcknowledgementLogging] times and will not be logged more."
                  else ""
                log.warning(
                  "Sending negative acknowledgement of system message [{}] from [{}], highest acknowledged [{}]{}",
                  n,
                  fromRemoteAddressStr,
                  expectedSeqNo - 1,
                  maxNackReached)
              }
              inboundContext.sendControl(ackReplyTo.address, Nack(expectedSeqNo - 1, localAddress))
              pull(in)
            }
          case _ =>
            // messages that don't need acking
            push(out, env)
        }
      }

      // OutHandler
      override def onPull(): Unit = pull(in)

      setHandlers(in, out, this)
    }
}
