/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery.internal

import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.ConsumerController.DeliverThenStop
import akka.actor.typed.delivery.ProducerController
import akka.actor.typed.internal.ActorFlightRecorder
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.StashBuffer
import akka.actor.typed.scaladsl.TimerScheduler
import akka.annotation.InternalApi
import akka.serialization.SerializationExtension
import akka.util.ByteString
import akka.util.ConstantFun.scalaIdentityFunction

/**
 * INTERNAL API
 *
 * ==== Design notes ====
 *
 * The destination consumer will start the flow by sending an initial `Start` message
 * to the `ConsumerController`.
 *
 * The `ProducerController` sends the first message to the `ConsumerController` without waiting for
 * a `Request` from the `ConsumerController`. The main reason for this is that when used with
 * Cluster Sharding the first message will typically create the `ConsumerController`. It's
 * also a way to connect the ProducerController and ConsumerController in a dynamic way, for
 * example when the ProducerController is replaced.
 *
 * The `ConsumerController` sends [[ProducerControllerImpl.Request]] to the `ProducerController`
 * to specify it's ready to receive up to the requested sequence number.
 *
 * The `ConsumerController` sends the first `Request` when it receives the first `SequencedMessage`
 * and has received the `Start` message from the consumer.
 *
 * It sends new `Request` when half of the requested window is remaining, but it also retries
 * the `Request` if no messages are received because that could be caused by lost messages.
 *
 * Apart from the first message the producer will not send more messages than requested.
 *
 * Received messages are wrapped in [[ConsumerController.Delivery]] when sent to the consumer,
 * which is supposed to reply with [[ConsumerController.Confirmed]] when it has processed the message.
 * Next message is not delivered until the previous is confirmed.
 * More messages from the producer that arrive while waiting for the confirmation are stashed by
 * the `ConsumerController` and delivered when previous message was confirmed.
 *
 * In other words, the "request" protocol to the application producer and consumer is one-by-one, but
 * between the `ProducerController` and `ConsumerController` it's window of messages in flight.
 *
 * The consumer and the `ConsumerController` are supposed to be local so that these messages are fast and not lost.
 *
 * If the `ConsumerController` receives a message with unexpected sequence number (not previous + 1)
 * it sends [[ProducerControllerImpl.Resend]] to the `ProducerController` and will ignore all messages until
 * the expected sequence number arrives.
 */
@InternalApi private[akka] object ConsumerControllerImpl {
  import ConsumerController.Command
  import ConsumerController.RegisterToProducerController
  import ConsumerController.SeqNr
  import ConsumerController.SequencedMessage
  import ConsumerController.Start

  sealed trait InternalCommand

  /** For commands defined in public ConsumerController */
  trait UnsealedInternalCommand extends InternalCommand

  private case object Retry extends InternalCommand

  private final case class ConsumerTerminated(consumer: ActorRef[_]) extends InternalCommand

  private final case class State[A](
      producerController: ActorRef[ProducerControllerImpl.InternalCommand],
      producerId: String,
      consumer: ActorRef[ConsumerController.Delivery[A]],
      receivedSeqNr: SeqNr,
      confirmedSeqNr: SeqNr,
      requestedSeqNr: SeqNr,
      collectedChunks: List[SequencedMessage[A]],
      registering: Option[ActorRef[ProducerController.Command[A]]],
      stopping: Boolean) {

    def isNextExpected(seqMsg: SequencedMessage[A]): Boolean =
      seqMsg.seqNr == receivedSeqNr + 1

    def isProducerChanged(seqMsg: SequencedMessage[A]): Boolean =
      seqMsg.producerController != producerController || receivedSeqNr == 0

    def updatedRegistering(seqMsg: SequencedMessage[A]): Option[ActorRef[ProducerController.Command[A]]] = {
      registering match {
        case None          => None
        case s @ Some(reg) => if (seqMsg.producerController == reg) None else s
      }
    }

    def clearCollectedChunks(): State[A] = {
      if (collectedChunks == Nil) this
      else copy(collectedChunks = Nil)
    }
  }

  def apply[A](
      serviceKey: Option[ServiceKey[Command[A]]],
      settings: ConsumerController.Settings): Behavior[Command[A]] = {
    Behaviors
      .withStash[InternalCommand](settings.flowControlWindow) { stashBuffer =>
        Behaviors.setup { context =>
          ActorFlightRecorder.consumerCreated(context.self.path)
          Behaviors.withMdc(msg => mdcForMessage(msg)) {
            context.setLoggerName("akka.actor.typed.delivery.ConsumerController")
            serviceKey.foreach { key =>
              context.system.receptionist ! Receptionist.Register(key, context.self)
            }
            Behaviors.withTimers { timers =>
              // wait for the `Start` message from the consumer, SequencedMessage will be stashed
              def waitForStart(registering: Option[ActorRef[ProducerController.Command[A]]], stopping: Boolean)
                  : Behavior[InternalCommand] = {
                Behaviors.receiveMessagePartial {
                  case reg: RegisterToProducerController[A] @unchecked =>
                    reg.producerController ! ProducerController.RegisterConsumer(context.self)
                    waitForStart(Some(reg.producerController), stopping)

                  case s: Start[A] @unchecked =>
                    ConsumerControllerImpl.enforceLocalConsumer(s.deliverTo)
                    context.watchWith(s.deliverTo, ConsumerTerminated(s.deliverTo))

                    ActorFlightRecorder.consumerStarted(context.self.path)
                    val retryTimer = new RetryTimer(timers, settings.resendIntervalMin, settings.resendIntervalMax)
                    val activeBehavior =
                      new ConsumerControllerImpl[A](context, retryTimer, stashBuffer, settings)
                        .active(initialState(context, s, registering, stopping))
                    context.log.debug("Received Start, unstash [{}] messages.", stashBuffer.size)
                    stashBuffer.unstash(activeBehavior, 1, scalaIdentityFunction)

                  case seqMsg: SequencedMessage[A] @unchecked =>
                    if (stashBuffer.isFull) {
                      ActorFlightRecorder.consumerStashFull(seqMsg.producerId, seqMsg.seqNr)
                      context.log.debug(
                        "Received SequencedMessage seqNr [{}], stashing before Start, discarding message because stash is full.",
                        seqMsg.seqNr)
                    } else {
                      context.log.trace(
                        "Received SequencedMessage seqNr [{}], stashing before Start, stashed size [{}].",
                        seqMsg.seqNr,
                        stashBuffer.size + 1)
                      stashBuffer.stash(seqMsg)
                    }
                    Behaviors.same

                  case _: DeliverThenStop[_] =>
                    if (stashBuffer.isEmpty) {
                      Behaviors.stopped
                    } else {
                      waitForStart(registering, stopping = true)
                    }

                  case Retry =>
                    registering.foreach { reg =>
                      context.log.debug("Retry sending RegisterConsumer to [{}].", reg)
                      reg ! ProducerController.RegisterConsumer(context.self)
                    }
                    Behaviors.same

                  case ConsumerTerminated(c) =>
                    context.log.debug("Consumer [{}] terminated.", c)
                    Behaviors.stopped

                }

              }

              timers.startTimerWithFixedDelay(Retry, Retry, settings.resendIntervalMin)
              waitForStart(None, stopping = false)
            }
          }
        }
      }
      .narrow // expose Command, but not InternalCommand
  }

  private def mdcForMessage(msg: InternalCommand): Map[String, String] = {
    msg match {
      case seqMsg: SequencedMessage[_] => Map("producerId" -> seqMsg.producerId)
      case _                           => Map.empty
    }
  }

  private def initialState[A](
      context: ActorContext[InternalCommand],
      start: Start[A],
      registering: Option[ActorRef[ProducerController.Command[A]]],
      stopping: Boolean): State[A] = {
    State(
      producerController = context.system.deadLetters,
      "n/a",
      start.deliverTo,
      receivedSeqNr = 0,
      confirmedSeqNr = 0,
      requestedSeqNr = 0,
      collectedChunks = Nil,
      registering,
      stopping)
  }

  def enforceLocalConsumer(ref: ActorRef[_]): Unit = {
    if (ref.path.address.hasGlobalScope)
      throw new IllegalArgumentException(s"Consumer [$ref] should be local.")
  }

  private class RetryTimer(
      timers: TimerScheduler[ConsumerControllerImpl.InternalCommand],
      val minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration) {
    private var _interval = minBackoff

    def interval(): FiniteDuration =
      _interval

    def start(): Unit = {
      _interval = minBackoff
      timers.startTimerWithFixedDelay(Retry, _interval)
    }

    def scheduleNext(): Unit = {
      val newInterval =
        if (_interval eq maxBackoff)
          maxBackoff
        else
          maxBackoff.min(_interval * 1.5) match {
            case f: FiniteDuration => f
            case _                 => maxBackoff
          }
      if (newInterval != _interval) {
        _interval = newInterval
        timers.startTimerWithFixedDelay(Retry, _interval)
      }
    }

    def reset(): Unit = {
      if (_interval ne minBackoff)
        start()
    }

  }
}

private class ConsumerControllerImpl[A] private (
    context: ActorContext[ConsumerControllerImpl.InternalCommand],
    retryTimer: ConsumerControllerImpl.RetryTimer,
    stashBuffer: StashBuffer[ConsumerControllerImpl.InternalCommand],
    settings: ConsumerController.Settings) {

  import ConsumerController.Confirmed
  import ConsumerController.Delivery
  import ConsumerController.RegisterToProducerController
  import ConsumerController.SequencedMessage
  import ConsumerController.Start
  import ConsumerControllerImpl._
  import ProducerControllerImpl.Ack
  import ProducerControllerImpl.Request
  import ProducerControllerImpl.Resend
  import settings.flowControlWindow

  private val traceEnabled = context.log.isTraceEnabled

  private lazy val serialization = SerializationExtension(context.system)

  retryTimer.start()

  private def resendLost = !settings.onlyFlowControl

  // Expecting a SequencedMessage from ProducerController, that will be delivered to the consumer if
  // the seqNr is right.
  private def active(s: State[A]): Behavior[InternalCommand] = {
    Behaviors
      .receiveMessage[InternalCommand] {
        case seqMsg: SequencedMessage[A @unchecked] =>
          val pid = seqMsg.producerId
          val seqNr = seqMsg.seqNr
          val expectedSeqNr = s.receivedSeqNr + 1

          ActorFlightRecorder.consumerReceived(pid, seqNr)
          retryTimer.reset()

          if (s.isProducerChanged(seqMsg)) {
            if (seqMsg.first && traceEnabled)
              context.log.trace("Received first SequencedMessage seqNr [{}], delivering to consumer.", seqNr)
            receiveChangedProducer(s, seqMsg)
          } else if (s.registering.isDefined) {
            context.log.debug(
              "Received SequencedMessage seqNr [{}], discarding message because registering to new ProducerController.",
              seqNr)
            stashBuffer.unstash(Behaviors.same, 1, scalaIdentityFunction)
          } else if (s.isNextExpected(seqMsg)) {
            if (traceEnabled)
              context.log.trace("Received SequencedMessage seqNr [{}], delivering to consumer.", seqNr)
            deliver(s.copy(receivedSeqNr = seqNr), seqMsg)
          } else if (seqNr > expectedSeqNr) {
            ActorFlightRecorder.consumerMissing(pid, expectedSeqNr, seqNr)
            context.log.debug(
              "Received SequencedMessage seqNr [{}], but expected [{}], {}.",
              seqNr,
              expectedSeqNr,
              if (resendLost) "requesting resend from expected seqNr" else "delivering to consumer anyway")
            if (resendLost) {
              seqMsg.producerController ! Resend(fromSeqNr = expectedSeqNr)
              stashBuffer.clear()
              retryTimer.start()
              resending(s)
            } else {
              deliver(s.copy(receivedSeqNr = seqNr), seqMsg)
            }
          } else { // seqNr < expectedSeqNr
            ActorFlightRecorder.consumerDuplicate(pid, expectedSeqNr, seqNr)
            context.log.debug("Received duplicate SequencedMessage seqNr [{}], expected [{}].", seqNr, expectedSeqNr)
            if (seqMsg.first)
              stashBuffer.unstash(active(retryRequest(s)), 1, scalaIdentityFunction)
            else
              stashBuffer.unstash(Behaviors.same, 1, scalaIdentityFunction)
          }

        case Retry =>
          receiveRetry(s, () => active(retryRequest(s)))

        case Confirmed =>
          receiveUnexpectedConfirmed()

        case start: Start[A @unchecked] =>
          receiveStart(s, start, newState => active(newState))

        case ConsumerTerminated(c) =>
          receiveConsumerTerminated(c)

        case reg: RegisterToProducerController[A @unchecked] =>
          receiveRegisterToProducerController(s, reg, newState => active(newState))

        case _: DeliverThenStop[_] =>
          receiveDeliverThenStop(s, newState => active(newState))

        case _: UnsealedInternalCommand =>
          Behaviors.unhandled
      }
      .receiveSignal {
        case (_, PostStop) => postStop(s)
      }
  }

  private def receiveChangedProducer(s: State[A], seqMsg: SequencedMessage[A]): Behavior[InternalCommand] = {
    val seqNr = seqMsg.seqNr

    if (seqMsg.first || !resendLost) {
      logChangedProducer(s, seqMsg)

      val newRequestedSeqNr = seqMsg.seqNr - 1 + flowControlWindow
      context.log.debug("Sending Request with requestUpToSeqNr [{}] after first SequencedMessage.", newRequestedSeqNr)
      seqMsg.producerController ! Request(confirmedSeqNr = 0L, newRequestedSeqNr, resendLost, viaTimeout = false)

      deliver(
        s.copy(
          producerController = seqMsg.producerController,
          producerId = seqMsg.producerId,
          receivedSeqNr = seqNr,
          confirmedSeqNr = 0L,
          requestedSeqNr = newRequestedSeqNr,
          registering = s.updatedRegistering(seqMsg)),
        seqMsg)
    } else if (s.receivedSeqNr == 0) {
      // needed for sharding
      context.log.debug(
        "Received SequencedMessage seqNr [{}], from new producer [{}] but it wasn't first. Resending.",
        seqNr,
        seqMsg.producerController)
      // request resend of all unconfirmed, and mark first
      seqMsg.producerController ! Resend(0)
      stashBuffer.clear()
      retryTimer.start()
      resending(s)
    } else {
      context.log.warn(
        "Received SequencedMessage seqNr [{}], discarding message because it was from unexpected " +
        "producer [{}] when expecting [{}].",
        seqNr,
        seqMsg.producerController,
        s.producerController)
      stashBuffer.unstash(Behaviors.same, 1, scalaIdentityFunction)
    }

  }

  private def logChangedProducer(s: State[A], seqMsg: SequencedMessage[A]): Unit = {
    if (s.producerController == context.system.deadLetters) {
      context.log.debug(
        "Associated with new ProducerController [{}], seqNr [{}].",
        seqMsg.producerController,
        seqMsg.seqNr)
    } else {
      ActorFlightRecorder.consumerChangedProducer(seqMsg.producerId)
      context.log.debug(
        "Changing ProducerController from [{}] to [{}], seqNr [{}].",
        s.producerController,
        seqMsg.producerController,
        seqMsg.seqNr)
    }
  }

  // It has detected a missing seqNr and requested a Resend. Expecting a SequencedMessage from the
  // ProducerController with the missing seqNr. Other SequencedMessage with different seqNr will be
  // discarded since they were in flight before the Resend request and will anyway be sent again.
  private def resending(s: State[A]): Behavior[InternalCommand] = {
    if (stashBuffer.nonEmpty)
      throw new IllegalStateException("StashBuffer should be cleared before resending.")
    Behaviors
      .receiveMessage[InternalCommand] {
        case seqMsg: SequencedMessage[A @unchecked] =>
          val seqNr = seqMsg.seqNr

          if (s.isProducerChanged(seqMsg)) {
            if (seqMsg.first && traceEnabled)
              context.log.trace("Received first SequencedMessage seqNr [{}], delivering to consumer.", seqNr)
            receiveChangedProducer(s, seqMsg)
          } else if (s.registering.isDefined) {
            context.log.debug(
              "Received SequencedMessage seqNr [{}], discarding message because registering to new ProducerController.",
              seqNr)
            Behaviors.same
          } else if (s.isNextExpected(seqMsg)) {
            ActorFlightRecorder.consumerReceivedResend(seqNr)
            context.log.debug("Received missing SequencedMessage seqNr [{}].", seqNr)
            deliver(s.copy(receivedSeqNr = seqNr), seqMsg)
          } else {
            context.log.debug(
              "Received SequencedMessage seqNr [{}], discarding message because waiting for [{}].",
              seqNr,
              s.receivedSeqNr + 1)
            if (seqMsg.first)
              retryRequest(s)
            Behaviors.same // ignore until we receive the expected
          }

        case Retry =>
          receiveRetry(
            s,
            () => {
              // in case the Resend message was lost
              context.log.debug("Retry sending Resend [{}].", s.receivedSeqNr + 1)
              s.producerController ! Resend(fromSeqNr = s.receivedSeqNr + 1)
              Behaviors.same
            })

        case Confirmed =>
          receiveUnexpectedConfirmed()

        case start: Start[A @unchecked] =>
          receiveStart(s, start, newState => resending(newState))

        case ConsumerTerminated(c) =>
          receiveConsumerTerminated(c)

        case reg: RegisterToProducerController[A @unchecked] =>
          receiveRegisterToProducerController(s, reg, newState => active(newState))

        case _: DeliverThenStop[_] =>
          receiveDeliverThenStop(s, newState => resending(newState))

        case _: UnsealedInternalCommand =>
          Behaviors.unhandled
      }
      .receiveSignal {
        case (_, PostStop) => postStop(s)
      }
  }

  private def deliver(s: State[A], seqMsg: SequencedMessage[A]): Behavior[InternalCommand] = {
    def previouslyCollectedChunks = if (seqMsg.isFirstChunk) Nil else s.collectedChunks
    if (seqMsg.isLastChunk) {
      val assembledSeqMsg =
        if (seqMsg.message.isInstanceOf[ChunkedMessage]) assembleChunks(seqMsg :: previouslyCollectedChunks)
        else seqMsg
      s.consumer ! Delivery(assembledSeqMsg.message.asInstanceOf[A], context.self, seqMsg.producerId, seqMsg.seqNr)
      waitingForConfirmation(s.clearCollectedChunks(), assembledSeqMsg)
    } else {
      // collecting chunks
      val newRequestedSeqNr =
        if ((s.requestedSeqNr - seqMsg.seqNr) == flowControlWindow / 2) {
          val newRequestedSeqNr = s.requestedSeqNr + flowControlWindow / 2
          ActorFlightRecorder.consumerSentRequest(seqMsg.producerId, newRequestedSeqNr)
          context.log.debug(
            "Sending Request when collecting chunks seqNr [{}], confirmedSeqNr [{}], requestUpToSeqNr [{}].",
            seqMsg.seqNr,
            s.confirmedSeqNr,
            newRequestedSeqNr)
          s.producerController ! Request(
            confirmedSeqNr = s.confirmedSeqNr,
            newRequestedSeqNr,
            resendLost,
            viaTimeout = false)
          retryTimer.start() // reset interval since Request was just sent
          newRequestedSeqNr
        } else {
          s.requestedSeqNr
        }

      stashBuffer.unstash(
        active(s.copy(collectedChunks = seqMsg :: previouslyCollectedChunks, requestedSeqNr = newRequestedSeqNr)),
        1,
        scalaIdentityFunction)
    }
  }

  private def assembleChunks(collectedChunks: List[SequencedMessage[A]]): SequencedMessage[A] = {
    val reverseCollectedChunks = collectedChunks.reverse
    val builder = ByteString.createBuilder
    reverseCollectedChunks.foreach { seqMsg =>
      builder ++= seqMsg.message.asInstanceOf[ChunkedMessage].serialized
    }
    val bytes = builder.result().toArrayUnsafe()
    val head = collectedChunks.head // this is the last chunk
    val headMessage = head.message.asInstanceOf[ChunkedMessage]
    // serialization exceptions are thrown, because it will anyway be stuck with same error if retried and
    // we can't just ignore the message
    val message = serialization.deserialize(bytes, headMessage.serializerId, headMessage.manifest).get
    SequencedMessage(head.producerId, head.seqNr, message, reverseCollectedChunks.head.first, head.ack)(
      head.producerController)
  }

  // The message has been delivered to the consumer and it is now waiting for Confirmed from
  // the consumer. New SequencedMessage from the ProducerController will be stashed.
  private def waitingForConfirmation(s: State[A], seqMsg: SequencedMessage[A]): Behavior[InternalCommand] = {
    Behaviors
      .receiveMessage[InternalCommand] {
        case Confirmed =>
          val seqNr = seqMsg.seqNr
          if (traceEnabled)
            context.log.trace(
              "Received Confirmed seqNr [{}] from consumer, stashed size [{}].",
              seqNr,
              stashBuffer.size)

          val newRequestedSeqNr =
            if (seqMsg.first) {
              // confirm the first message immediately to cancel resending of first
              val newRequestedSeqNr = seqNr - 1 + flowControlWindow
              ActorFlightRecorder.consumerSentRequest(seqMsg.producerId, newRequestedSeqNr)
              context.log.debug(
                "Sending Request after first with confirmedSeqNr [{}], requestUpToSeqNr [{}].",
                seqNr,
                newRequestedSeqNr)
              s.producerController ! Request(confirmedSeqNr = seqNr, newRequestedSeqNr, resendLost, viaTimeout = false)
              newRequestedSeqNr
            } else if ((s.requestedSeqNr - seqNr) == flowControlWindow / 2) {
              val newRequestedSeqNr = s.requestedSeqNr + flowControlWindow / 2
              ActorFlightRecorder.consumerSentRequest(seqMsg.producerId, newRequestedSeqNr)
              context.log.debug(
                "Sending Request with confirmedSeqNr [{}], requestUpToSeqNr [{}].",
                seqNr,
                newRequestedSeqNr)
              s.producerController ! Request(confirmedSeqNr = seqNr, newRequestedSeqNr, resendLost, viaTimeout = false)
              retryTimer.start() // reset interval since Request was just sent
              newRequestedSeqNr
            } else {
              if (seqMsg.ack) {
                if (traceEnabled)
                  context.log.trace("Sending Ack seqNr [{}].", seqNr)
                s.producerController ! Ack(confirmedSeqNr = seqNr)
              }
              s.requestedSeqNr
            }

          if (s.stopping && stashBuffer.isEmpty) {
            context.log.debug("Stopped at seqNr [{}], after delivery of buffered messages.", seqNr)
            Behaviors.stopped { () =>
              // best effort to Ack latest confirmed when stopping
              s.producerController ! Ack(seqNr)
            }
          } else {
            stashBuffer.unstash(
              active(s.copy(confirmedSeqNr = seqNr, requestedSeqNr = newRequestedSeqNr)),
              1,
              scalaIdentityFunction)
          }

        case msg: SequencedMessage[_] =>
          ActorFlightRecorder.consumerReceivedPreviousInProgress(msg.producerId, msg.seqNr, stashBuffer.size + 1)
          val expectedSeqNr = seqMsg.seqNr + stashBuffer.size + 1
          if (msg.seqNr < expectedSeqNr && msg.producerController == seqMsg.producerController) {
            ActorFlightRecorder.consumerDuplicate(msg.producerId, expectedSeqNr, msg.seqNr)
            context.log.debug("Received duplicate SequencedMessage seqNr [{}].", msg.seqNr)
          } else if (stashBuffer.isFull) {
            // possible that the stash is full if ProducerController resends unconfirmed (duplicates)
            // dropping them since they can be resent
            ActorFlightRecorder.consumerStashFull(msg.producerId, msg.seqNr)
            context.log.debug(
              "Received SequencedMessage seqNr [{}], discarding message because stash is full.",
              msg.seqNr)
          } else {
            if (traceEnabled)
              context.log.trace(
                "Received SequencedMessage seqNr [{}], stashing while waiting for consumer to confirm [{}], stashed size [{}].",
                msg.seqNr,
                seqMsg.seqNr,
                stashBuffer.size + 1)
            stashBuffer.stash(msg)
          }
          Behaviors.same

        case Retry =>
          // no retries when waitingForConfirmation, will be performed from (idle) active
          Behaviors.same

        case start: Start[A @unchecked] =>
          start.deliverTo ! Delivery(seqMsg.message.asInstanceOf[A], context.self, seqMsg.producerId, seqMsg.seqNr)
          receiveStart(s, start, newState => waitingForConfirmation(newState, seqMsg))

        case ConsumerTerminated(c) =>
          receiveConsumerTerminated(c)

        case reg: RegisterToProducerController[A @unchecked] =>
          receiveRegisterToProducerController(s, reg, newState => waitingForConfirmation(newState, seqMsg))

        case _: DeliverThenStop[_] =>
          receiveDeliverThenStop(s, newState => waitingForConfirmation(newState, seqMsg))

        case _: UnsealedInternalCommand =>
          Behaviors.unhandled
      }
      .receiveSignal {
        case (_, PostStop) => postStop(s)
      }
  }

  private def receiveRetry(s: State[A], nextBehavior: () => Behavior[InternalCommand]): Behavior[InternalCommand] = {
    retryTimer.scheduleNext()
    if (retryTimer.interval() != retryTimer.minBackoff)
      context.log.debug("Schedule next retry in [{} ms]", retryTimer.interval().toMillis)
    s.registering match {
      case None => nextBehavior()
      case Some(reg) =>
        reg ! ProducerController.RegisterConsumer(context.self)
        Behaviors.same
    }
  }

  private def receiveStart(
      s: State[A],
      start: Start[A],
      nextBehavior: State[A] => Behavior[InternalCommand]): Behavior[InternalCommand] = {
    ConsumerControllerImpl.enforceLocalConsumer(start.deliverTo)
    if (start.deliverTo == s.consumer) {
      nextBehavior(s)
    } else {
      // if consumer is restarted it may send Start again
      context.unwatch(s.consumer)
      context.watchWith(start.deliverTo, ConsumerTerminated(start.deliverTo))
      nextBehavior(s.copy(consumer = start.deliverTo))
    }
  }

  private def receiveRegisterToProducerController(
      s: State[A],
      reg: RegisterToProducerController[A],
      nextBehavior: State[A] => Behavior[InternalCommand]): Behavior[InternalCommand] = {
    if (reg.producerController != s.producerController) {
      context.log.debug(
        "Register to new ProducerController [{}], previous was [{}].",
        reg.producerController,
        s.producerController)
      retryTimer.start()
      reg.producerController ! ProducerController.RegisterConsumer(context.self)
      nextBehavior(s.copy(registering = Some(reg.producerController)))
    } else {
      Behaviors.same
    }
  }

  private def receiveDeliverThenStop(
      s: State[A],
      nextBehavior: State[A] => Behavior[InternalCommand]): Behavior[InternalCommand] = {
    if (stashBuffer.isEmpty && s.receivedSeqNr == s.confirmedSeqNr) {
      context.log.debug("Stopped at seqNr [{}], no buffered messages.", s.confirmedSeqNr)
      Behaviors.stopped
    } else {
      nextBehavior(s.copy(stopping = true))
    }
  }

  private def receiveConsumerTerminated(c: ActorRef[_]): Behavior[InternalCommand] = {
    context.log.debug("Consumer [{}] terminated.", c)
    Behaviors.stopped
  }

  private def receiveUnexpectedConfirmed(): Behavior[InternalCommand] = {
    context.log.warn("Received unexpected Confirmed from consumer.")
    Behaviors.unhandled
  }

  // in case the Request or the SequencedMessage triggering the Request is lost
  private def retryRequest(s: State[A]): State[A] = {
    if (s.producerController == context.system.deadLetters) {
      s
    } else {
      val newRequestedSeqNr = if (resendLost) s.requestedSeqNr else s.receivedSeqNr + flowControlWindow / 2
      ActorFlightRecorder.consumerSentRequest(s.producerId, newRequestedSeqNr)
      context.log.debug(
        "Retry sending Request with confirmedSeqNr [{}], requestUpToSeqNr [{}].",
        s.confirmedSeqNr,
        newRequestedSeqNr)
      s.producerController ! Request(s.confirmedSeqNr, newRequestedSeqNr, resendLost, viaTimeout = true)
      s.copy(requestedSeqNr = newRequestedSeqNr)
    }
  }

  private def postStop(s: State[A]): Behavior[InternalCommand] = {
    // best effort to Ack latest confirmed when stopping
    s.producerController ! Ack(s.confirmedSeqNr)
    Behaviors.same
  }

}
