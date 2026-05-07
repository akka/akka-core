/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.streamref

import scala.annotation.nowarn

import akka.NotUsed
import akka.actor.{ ActorRef, Terminated }
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream._
import akka.stream.impl.FixedSizeBuffer
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.util.{ OptionVal, PrettyDuration }

/** INTERNAL API: Implementation class, not intended to be touched directly by end-users */
@InternalApi
private[stream] final case class SourceRefImpl[T](initialPartnerRef: ActorRef) extends SourceRef[T] {
  def source: Source[T, NotUsed] =
    Source.fromGraph(new SourceRefStageImpl(OptionVal.Some(initialPartnerRef))).mapMaterializedValue(_ => NotUsed)
}

/**
 * INTERNAL API
 */
@InternalApi private[stream] object SourceRefStageImpl {
  private sealed trait ActorRefStage { def ref: ActorRef }

  object WatermarkRequestStrategy {

    /**
     * Create [[WatermarkRequestStrategy]] with `lowWatermark` as half of
     * the specified `highWatermark`.
     */
    def apply(highWatermark: Int): WatermarkRequestStrategy = new WatermarkRequestStrategy(highWatermark)
  }

  /**
   * Requests up to the `highWatermark` when the `remainingRequested` is
   * below the `lowWatermark`. This a good strategy when the actor performs work itself.
   */
  final case class WatermarkRequestStrategy(highWatermark: Int, lowWatermark: Int) {
    require(lowWatermark >= 0, "lowWatermark must be >= 0")
    require(highWatermark >= lowWatermark, "highWatermark must be >= lowWatermark")

    /**
     * Create [[WatermarkRequestStrategy]] with `lowWatermark` as half of
     * the specified `highWatermark`.
     */
    def this(highWatermark: Int) = this(highWatermark, lowWatermark = math.max(1, highWatermark / 2))

    /**
     * Invoked after each incoming message to determine how many more elements to request from the stream.
     *
     * @param remainingRequested current remaining number of elements that
     *   have been requested from upstream but not received yet
     * @return demand of more elements from the stream, returning 0 means that no
     *   more elements will be requested for now
     */
    def requestDemand(remainingRequested: Int): Int =
      if (remainingRequested < lowWatermark)
        highWatermark - remainingRequested
      else 0
  }

  private sealed trait State
  private sealed trait WeKnowPartner extends State {
    def partner: ActorRef
  }

  // we are the "origin", and awaiting the other side to start when we'll receive this ref
  private case object AwaitingPartner extends State
  // we're the "remote" for an already active Source on the other side (the "origin")
  private case class AwaitingSubscription(partner: ActorRef) extends WeKnowPartner
  // subscription aquired and up and running
  private final case class Running(partner: ActorRef) extends WeKnowPartner

  // downstream cancelled or failed, waiting for remote upstream to ack
  private final case class WaitingForCancelAck(partner: ActorRef, cause: Throwable) extends WeKnowPartner
  // upstream completed, we are waiting to allow
  private final case class UpstreamCompleted(partner: ActorRef) extends WeKnowPartner
  private final case class UpstreamTerminated(partner: ActorRef) extends State

  val SubscriptionTimeoutTimerKey = "SubscriptionTimeoutKey"
  val DemandRedeliveryTimerKey = "DemandRedeliveryTimerKey"
  val TerminationDeadlineTimerKey = "TerminationDeadlineTimerKey"
  val CancellationDeadlineTimerKey = "CancellationDeadlineTimerKey"
}

/**
 * INTERNAL API: Actual operator implementation backing [[SourceRef]]s.
 *
 * If initialPartnerRef is set, then the remote side is already set up.
 * If it is none, then we are the side creating the ref.
 */
@InternalApi
private[stream] final class SourceRefStageImpl[Out](val initialPartnerRef: OptionVal[ActorRef])
    extends GraphStageWithMaterializedValue[SourceShape[Out], SinkRef[Out]] { stage =>
  import SourceRefStageImpl._

  val out: Outlet[Out] = Outlet[Out](s"${Logging.simpleName(getClass)}.out")
  override def shape = SourceShape.of(out)

  private def initialRefName =
    initialPartnerRef match {
      case OptionVal.Some(ref) => ref.toString
      case _                   => "<no-initial-ref>"
    }

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, SinkRef[Out]) =
    throw new IllegalStateException("Not supported")

  private[akka] override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes,
      eagerMaterializer: Materializer): (GraphStageLogic, SinkRef[Out]) = {

    val logic = new TimerGraphStageLogic(shape) with StageLogging with ActorRefStage with OutHandler {
      override protected def logSource: Class[_] = classOf[SourceRefStageImpl[_]]

      private[this] val streamRefsMaster = StreamRefsMaster(eagerMaterializer.system)

      // settings ---
      import StreamRefAttributes._
      @nowarn("msg=deprecated") // can't remove this settings access without breaking compat
      private[this] val settings = eagerMaterializer.settings.streamRefSettings

      @nowarn("msg=deprecated") // can't remove this settings access without breaking compat
      private[this] val subscriptionTimeout = inheritedAttributes.get[StreamRefAttributes.SubscriptionTimeout](
        SubscriptionTimeout(settings.subscriptionTimeout))

      @nowarn("msg=deprecated") // can't remove this settings access without breaking compat
      private[this] val bufferCapacity = inheritedAttributes
        .get[StreamRefAttributes.BufferCapacity](StreamRefAttributes.BufferCapacity(settings.bufferCapacity))
        .capacity

      @nowarn("msg=deprecated") // can't remove this settings access without breaking compat
      private[this] val demandRedeliveryInterval = inheritedAttributes
        .get[StreamRefAttributes.DemandRedeliveryInterval](DemandRedeliveryInterval(settings.demandRedeliveryInterval))
        .timeout

      @nowarn("msg=deprecated") // can't remove this settings access without breaking compat
      private[this] val finalTerminationSignalDeadline =
        inheritedAttributes
          .get[StreamRefAttributes.FinalTerminationSignalDeadline](
            FinalTerminationSignalDeadline(settings.finalTerminationSignalDeadline))
          .timeout
      // end of settings ---

      override protected val stageActorName: String = streamRefsMaster.nextSourceRefStageName()
      private[this] val self: GraphStageLogic.StageActor =
        getEagerStageActor(eagerMaterializer)(receiveRemoteMessage)
      override val ref: ActorRef = self.ref
      private[this] implicit def selfSender: ActorRef = ref

      // demand management ---
      private var state: State = initialPartnerRef match {
        case OptionVal.Some(partnerRef) =>
          // this means we're the "remote" for an already active Source on the other side (the "origin")
          self.watch(partnerRef)
          AwaitingSubscription(partnerRef)
        case _ =>
          // we are the "origin", and awaiting the other side to start when we'll receive their partherRef
          AwaitingPartner
      }

      private var expectingSeqNr: Long = 0L
      private var localCumulativeDemand: Long = 0L
      private var localRemainingRequested: Int = 0

      private val receiveBuffer = FixedSizeBuffer[Out](bufferCapacity)

      private val requestStrategy = WatermarkRequestStrategy(highWatermark = receiveBuffer.capacity)
      // end of demand management ---

      override def preStart(): Unit = {
        log.debug(
          "[{}] Starting up with, self ref: {}, state: {}, subscription timeout: {}",
          stageActorName,
          self.ref,
          state,
          PrettyDuration.format(subscriptionTimeout.timeout))

        // This timer will be cancelled if we receive the handshake from the remote SinkRef
        // either created in this method and provided as self.ref as initialPartnerRef
        // or as the response to first CumulativeDemand request sent to remote SinkRef
        scheduleOnce(SubscriptionTimeoutTimerKey, subscriptionTimeout.timeout)
      }

      override def onPull(): Unit = {
        tryPush()
        triggerCumulativeDemand()
      }

      def receiveRemoteMessage: ((ActorRef, Any)) => Unit = {
        case (sender, msg @ StreamRefsProtocol.OnSubscribeHandshake(remoteRef)) =>
          state match {
            case AwaitingPartner =>
              cancelTimer(SubscriptionTimeoutTimerKey)
              log.debug(
                "[{}] Received on subscribe handshake {} while awaiting partner from {}",
                stageActorName,
                msg,
                remoteRef)
              state = Running(remoteRef)
              self.watch(remoteRef)
              triggerCumulativeDemand()
            case AwaitingSubscription(partner) =>
              verifyPartner(sender, partner)
              cancelTimer(SubscriptionTimeoutTimerKey)
              log.debug(
                "[{}] Received on subscribe handshake {} while awaiting subscription from {}",
                stageActorName,
                msg,
                remoteRef)
              state = Running(remoteRef)
              triggerCumulativeDemand()

            case other =>
              throw new IllegalStateException(s"[$stageActorName] Got unexpected $msg in state $other")
          }

        case (sender, msg @ StreamRefsProtocol.SequencedOnNext(seqNr, payload: Out @unchecked)) =>
          observeAndValidateSequenceNr(seqNr, "Illegal sequence nr in SequencedOnNext")
          state match {
            case AwaitingSubscription(partner) =>
              verifyPartner(sender, partner)

              log.debug("[{}] Received seq {} from {}", stageActorName, msg, sender)
              state = Running(partner)
              onReceiveElement(payload)
              triggerCumulativeDemand()

            case Running(partner) =>
              verifyPartner(sender, partner)
              onReceiveElement(payload)
              triggerCumulativeDemand()

            case AwaitingPartner =>
              throw new IllegalStateException(s"[$stageActorName] Got $msg from $sender while AwaitingPartner")

            case WaitingForCancelAck(partner, _) =>
              // awaiting cancellation ack from remote
              verifyPartner(sender, partner)
              log.warning(
                "[{}] Got element from remote but downstream cancelled, dropping element of type {}",
                stageActorName,
                payload.getClass)

            case UpstreamCompleted(partner) =>
              verifyPartner(sender, partner)
              throw new IllegalStateException(
                s"[$stageActorName] Got completion and then received more elements from $sender, this is not supposed to happen.")

            case UpstreamTerminated(partner) =>
              verifyPartner(sender, partner)
              log.debug("[{}] Received element after partner terminated")
              onReceiveElement(payload)

          }

        case (sender, StreamRefsProtocol.RemoteStreamCompleted(seqNr)) =>
          observeAndValidateSequenceNr(seqNr, "Illegal sequence nr in RemoteSinkCompleted")
          state match {
            case Running(partner) =>
              // upstream completed, continue running until we have emitted every element in buffer
              // or downstream cancels
              verifyPartner(sender, partner)
              log.debug(
                "[{}] The remote stream has completed, emitting {} elements left in buffer before completing",
                stageActorName,
                receiveBuffer.used)
              self.unwatch(sender)
              state = UpstreamCompleted(partner)
              tryPush()
            case WaitingForCancelAck(_, _) =>
              // upstream completed while we were waiting for it to receive cancellation and ack
              // upstream may stop without seeing cancellation, but we may not see termination
              // let the cancel timeout hit
              log.debug("[{}] Upstream completed while waiting for cancel ack", stageActorName)
            case other =>
              // UpstreamCompleted, AwaitingPartner or AwaitingSubscription(_) all means a bug here
              throw new IllegalStateException(
                s"[$stageActorName] Saw RemoteStreamCompleted($seqNr) while in state $other, should never happen")
          }

        case (sender, StreamRefsProtocol.RemoteStreamFailure(reason)) =>
          state match {
            case weKnoPartner: WeKnowPartner =>
              val partner = weKnoPartner.partner
              verifyPartner(sender, partner)
              log.debug("[{}] The remote stream has failed, failing (reason: {})", stageActorName, reason)
              failStage(
                RemoteStreamRefActorTerminatedException(
                  s"[$stageActorName] Remote stream (${sender.path}) failed, reason: $reason"))
            case other =>
              throw new IllegalStateException(
                s"[$stageActorName] got RemoteStreamFailure($reason) when in state $other, should never happen")
          }

        case (sender, StreamRefsProtocol.Ack) =>
          state match {
            case WaitingForCancelAck(partner, cause) =>
              verifyPartner(sender, partner)
              log.debug(s"[{}] Got cancellation ack from remote, canceling", stageActorName)
              cancelStage(cause)
            case other =>
              throw new IllegalStateException(s"[$stageActorName] Got an Ack when in state $other")
          }

        case (_, Terminated(p)) =>
          state match {
            case weKnowPartner: WeKnowPartner =>
              if (weKnowPartner.partner != p)
                throw RemoteStreamRefActorTerminatedException(
                  s"[$stageActorName] Received UNEXPECTED Terminated($p) message! " +
                  s"This actor was NOT our trusted remote partner, which was: ${weKnowPartner.partner}. Tearing down.")
              // we need to start a delayed shutdown in case we were network partitioned and the final signal complete/fail
              // will never reach us; so after the given timeout we need to forcefully terminate this side of the stream ref
              // the other (sending) side terminates by default once it gets a Terminated signal so no special handling is needed there.
              scheduleOnce(TerminationDeadlineTimerKey, finalTerminationSignalDeadline)
              log.debug(
                "[{}] Partner terminated, starting delayed shutdown, deadline: [{}]",
                stageActorName,
                finalTerminationSignalDeadline)
              state = UpstreamTerminated(weKnowPartner.partner)
            case weDontKnowPartner =>
              throw new IllegalStateException(
                s"[$stageActorName] Unexpected deathwatch message for $p before we knew partner ref, state $weDontKnowPartner")

          }
        case (sender, msg) =>
          // should never happen but keep the compiler happy (stage actor receive is total)
          throw new IllegalStateException(s"[$stageActorName] Unexpected message in state $state: $msg from $sender")
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case SubscriptionTimeoutTimerKey =>
          state match {
            case AwaitingPartner | AwaitingSubscription(_) =>
              val ex = StreamRefSubscriptionTimeoutException(
                // we know the future has been competed by now, since it is in preStart
                s"[$stageActorName] Remote side did not subscribe (materialize) handed out Sink reference [$ref]," +
                s"within subscription timeout: ${PrettyDuration.format(subscriptionTimeout.timeout)}!")

              throw ex // this will also log the exception, unlike failStage; this should fail rarely, but would be good to have it "loud"
            case other =>
              // this is fine
              log.debug("[{}] Ignoring subscription timeout in state [{}]", stageActorName, other)
          }

        case DemandRedeliveryTimerKey =>
          state match {
            case Running(partner) =>
              log.debug("[{}] Scheduled re-delivery of demand until [{}]", stageActorName, localCumulativeDemand)
              partner ! StreamRefsProtocol.CumulativeDemand(localCumulativeDemand)
              scheduleDemandRedelivery()

            case other =>
              log.debug("[{}] Ignoring demand redelivery timeout in state [{}]", stageActorName, other)
          }

        case TerminationDeadlineTimerKey =>
          state match {
            case UpstreamTerminated(partner) =>
              log.debug(
                "[{}] Remote partner [{}] has terminated unexpectedly and no clean completion/failure message was received",
                stageActorName,
                partner)
              failStage(RemoteStreamRefActorTerminatedException(
                s"[$stageActorName] Remote partner [$partner] has terminated unexpectedly and no clean completion/failure message was received " +
                "(possible reasons: network partition or subscription timeout triggered termination of partner). Tearing down."))

            case AwaitingPartner =>
              log.debug("[{}] Downstream cancelled, but timeout hit before we saw a partner", stageActorName)
              cancelStage(SubscriptionWithCancelException.NoMoreElementsNeeded)

            case other =>
              throw new IllegalStateException(s"TerminationDeadlineTimerKey can't happen in state $other")
          }

        case CancellationDeadlineTimerKey =>
          state match {
            case WaitingForCancelAck(partner, cause) =>
              log.debug(
                "[{}] Waiting for remote ack from [{}] for downstream failure timed out, failing stage with original downstream failure",
                stageActorName,
                partner)
              cancelStage(cause)

            case other =>
              throw new IllegalStateException(
                s"[$stageActorName] CancellationDeadlineTimerKey can't happen in state $other")
          }

        case other => throw new IllegalArgumentException(s"Unknown timer key: ${other}")
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        state match {
          case Running(partner) =>
            triggerCancellationExchange(partner, cause)

          case AwaitingPartner =>
            // we can't do a graceful cancellation dance in this case, wait for partner and then cancel
            // or timeout if we never get a partner
            scheduleOnce(TerminationDeadlineTimerKey, finalTerminationSignalDeadline)

          case AwaitingSubscription(partner) =>
            // we didn't get a first demand yet but have access to the partner - try a cancellation dance
            triggerCancellationExchange(partner, cause)

          case UpstreamCompleted(_) =>
            // we saw upstream complete so let's just complete
            if (receiveBuffer.nonEmpty)
              log.debug(
                "[{}] Downstream cancelled with elements [{}] in buffer, dropping elements",
                stageActorName,
                receiveBuffer.used)
            cause match {
              case _: SubscriptionWithCancelException.NonFailureCancellation => completeStage()
              case failure                                                   => failStage(failure)
            }

          case WaitingForCancelAck(_, _) =>
            // downstream can't finish twice
            throw new UnsupportedOperationException(
              s"[$stageActorName] Didn't expect state $state when downstream finished with $cause")

          case UpstreamTerminated(_) =>
            log.debug("[{}] Downstream cancelled with elements [{}] in buffer", stageActorName, receiveBuffer.used)
            if (receiveBuffer.isEmpty)
              failStage(RemoteStreamRefActorTerminatedException(s"[$stageActorName] unexpectedly terminated"))
            else
              // if there are elements left in the buffer we try to emit those
              tryPush()
        }
      }

      private def triggerCancellationExchange(partner: ActorRef, cause: Throwable): Unit = {
        if (receiveBuffer.nonEmpty)
          log.debug("Downstream cancelled with elements [{}] in buffer, dropping elements", receiveBuffer.used)
        val message = cause match {
          case _: SubscriptionWithCancelException.NonFailureCancellation =>
            log.debug("[{}] Deferred stop on downstream cancel", stageActorName)
            StreamRefsProtocol.RemoteStreamCompleted(expectingSeqNr) // seNr not really used in this case

          case streamFailure =>
            log.debug("[{}] Deferred stop on downstream failure: {}", stageActorName, streamFailure)
            StreamRefsProtocol.RemoteStreamFailure("Downstream failed")
        }
        // sending the cancellation means it is ok for the partner to terminate
        // we either get a response or hit a timeout and shutdown
        self.unwatch(partner)
        partner ! message
        state = WaitingForCancelAck(partner, cause)
        scheduleOnce(CancellationDeadlineTimerKey, subscriptionTimeout.timeout)
        setKeepGoing(true)
      }

      def triggerCumulativeDemand(): Unit = {
        val i = receiveBuffer.remainingCapacity - localRemainingRequested
        if (i > 0) {
          val addDemand = requestStrategy.requestDemand(receiveBuffer.used + localRemainingRequested)
          // only if demand has increased we shoot it right away
          // otherwise it's the same demand level, so it'd be triggered via redelivery anyway
          if (addDemand > 0) {
            def sendDemand(partner: ActorRef): Unit = {
              localCumulativeDemand += addDemand
              localRemainingRequested += addDemand
              val demand = StreamRefsProtocol.CumulativeDemand(localCumulativeDemand)
              partner ! demand
              scheduleDemandRedelivery()
            }
            state match {
              case Running(partner) =>
                log.debug("[{}] Demanding until [{}] (+{})", stageActorName, localCumulativeDemand, addDemand)
                sendDemand(partner)

              case AwaitingSubscription(partner) =>
                log.debug(
                  "[{}] Demanding, before subscription seen, until [{}] (+{})",
                  stageActorName,
                  localCumulativeDemand,
                  addDemand)
                sendDemand(partner)
              case other =>
                log.debug("[{}] Partner ref not set up in state {}, demanding elements deferred", stageActorName, other)
            }
          }
        }
      }

      private def tryPush(): Unit =
        if (receiveBuffer.nonEmpty && isAvailable(out)) {
          val element = receiveBuffer.dequeue()
          push(out, element)
        } else if (receiveBuffer.isEmpty)
          state match {
            case UpstreamCompleted(_) => completeStage()
            case _                    => // all other are ok
          }

      private def onReceiveElement(payload: Out): Unit = {
        localRemainingRequested -= 1
        if (receiveBuffer.isEmpty && isAvailable(out)) {
          push(out, payload)
        } else if (receiveBuffer.isFull) {
          throw new IllegalStateException(
            s"Attempted to overflow buffer! " +
            s"Capacity: ${receiveBuffer.capacity}, incoming element: $payload, " +
            s"localRemainingRequested: $localRemainingRequested, localCumulativeDemand: $localCumulativeDemand")
        } else {
          receiveBuffer.enqueue(payload)
        }
      }

      private def verifyPartner(sender: ActorRef, partner: ActorRef): Unit = {
        if (sender != partner)
          throw InvalidPartnerActorException(
            partner,
            sender,
            s"[$stageActorName] Received message from UNEXPECTED sender [$sender]! " +
            s"This actor is NOT our trusted remote partner, which is [$partner]. Tearing down.")
      }

      /** @throws InvalidSequenceNumberException when sequence number is invalid */
      private def observeAndValidateSequenceNr(seqNr: Long, msg: String): Unit =
        if (isInvalidSequenceNr(seqNr)) {
          log.warning("[{}] {}, expected {} but was {}", stageActorName, msg, expectingSeqNr, seqNr)
          throw InvalidSequenceNumberException(expectingSeqNr, seqNr, msg)
        } else {
          expectingSeqNr += 1
        }

      private def isInvalidSequenceNr(seqNr: Long): Boolean =
        seqNr != expectingSeqNr

      private def scheduleDemandRedelivery(): Unit =
        scheduleOnce(DemandRedeliveryTimerKey, demandRedeliveryInterval)

      setHandler(out, this)
    }
    (logic, SinkRefImpl(logic.ref))
  }

  override def toString: String =
    s"${Logging.simpleName(getClass)}($initialRefName)}"
}
