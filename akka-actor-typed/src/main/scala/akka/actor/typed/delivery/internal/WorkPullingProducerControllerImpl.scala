/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery.internal

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeoutException

import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.DispatcherSelector
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.DurableProducerQueue
import akka.actor.typed.delivery.DurableProducerQueue.ConfirmationQualifier
import akka.actor.typed.delivery.DurableProducerQueue.SeqNr
import akka.actor.typed.delivery.ProducerController
import akka.actor.typed.delivery.WorkPullingProducerController
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.StashBuffer
import akka.annotation.InternalApi
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalApi private[akka] object WorkPullingProducerControllerImpl {

  import WorkPullingProducerController.Command
  import WorkPullingProducerController.RequestNext
  import WorkPullingProducerController.Start

  sealed trait InternalCommand

  /** For commands defined in public WorkPullingProducerController */
  trait UnsealedInternalCommand extends InternalCommand

  private type TotalSeqNr = Long
  private type OutSeqNr = Long
  private type OutKey = String

  private final case class WorkerRequestNext[A](next: ProducerController.RequestNext[A]) extends InternalCommand

  private final case class Ack(outKey: OutKey, confirmedSeqNr: OutSeqNr) extends InternalCommand
  private final case class AskTimeout(outKey: OutKey, outSeqNr: OutSeqNr) extends InternalCommand

  private case object RegisterConsumerDone extends InternalCommand

  private case class LoadStateReply[A](state: DurableProducerQueue.State[A]) extends InternalCommand
  private case class LoadStateFailed(attempt: Int) extends InternalCommand
  private case class StoreMessageSentReply(ack: DurableProducerQueue.StoreMessageSentAck)
  private case class StoreMessageSentFailed[A](messageSent: DurableProducerQueue.MessageSent[A], attempt: Int)
      extends InternalCommand
  private case class StoreMessageSentCompleted[A](messageSent: DurableProducerQueue.MessageSent[A])
      extends InternalCommand
  private case object DurableQueueTerminated extends InternalCommand

  private final case class OutState[A](
      producerController: ActorRef[ProducerController.Command[A]],
      consumerController: ActorRef[ConsumerController.Command[A]],
      seqNr: OutSeqNr,
      unconfirmed: Vector[Unconfirmed[A]],
      askNextTo: Option[ActorRef[ProducerController.MessageWithConfirmation[A]]]) {
    def confirmationQualifier: ConfirmationQualifier = producerController.path.name
  }

  private final case class Unconfirmed[A](
      totalSeqNr: TotalSeqNr,
      outSeqNr: OutSeqNr,
      msg: A,
      replyTo: Option[ActorRef[Done]])

  private final case class State[A](
      currentSeqNr: TotalSeqNr, // only updated when durableQueue is enabled
      workers: Set[ActorRef[ConsumerController.Command[A]]],
      out: Map[OutKey, OutState[A]],
      // when durableQueue is enabled the worker must be selecting before storage
      // to know the confirmationQualifier up-front
      preselectedWorkers: Map[TotalSeqNr, PreselectedWorker],
      // replyAfterStore is used when durableQueue is enabled, otherwise they are tracked in OutState
      replyAfterStore: Map[TotalSeqNr, ActorRef[Done]],
      // when the worker is deregistered but there are still unconfirmed
      handOver: Map[TotalSeqNr, HandOver],
      producer: ActorRef[WorkPullingProducerController.RequestNext[A]],
      requested: Boolean)

  private case class PreselectedWorker(outKey: OutKey, confirmationQualifier: ConfirmationQualifier)

  private case class HandOver(oldConfirmationQualifier: ConfirmationQualifier, oldSeqNr: TotalSeqNr)

  // registration of workers via Receptionist
  private final case class CurrentWorkers[A](workers: Set[ActorRef[ConsumerController.Command[A]]])
      extends InternalCommand

  private final case class Msg[A](msg: A, wasStashed: Boolean, replyTo: Option[ActorRef[Done]]) extends InternalCommand

  private final case class ResendDurableMsg[A](
      msg: A,
      oldConfirmationQualifier: ConfirmationQualifier,
      oldSeqNr: TotalSeqNr)
      extends InternalCommand

  def apply[A: ClassTag](
      producerId: String,
      workerServiceKey: ServiceKey[ConsumerController.Command[A]],
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]],
      settings: WorkPullingProducerController.Settings): Behavior[Command[A]] = {
    Behaviors
      .withStash[InternalCommand](settings.bufferSize) { stashBuffer =>
        Behaviors.setup[InternalCommand] { context =>
          Behaviors.withMdc(staticMdc = Map("producerId" -> producerId)) {
            context.setLoggerName("akka.actor.typed.delivery.WorkPullingProducerController")
            val listingAdapter = context.messageAdapter[Receptionist.Listing](listing =>
              CurrentWorkers[A](listing.allServiceInstances(workerServiceKey)))
            context.system.receptionist ! Receptionist.Subscribe(workerServiceKey, listingAdapter)

            val durableQueue = askLoadState(context, durableQueueBehavior, settings)

            waitingForStart(
              producerId,
              context,
              stashBuffer,
              durableQueue,
              settings,
              None,
              createInitialState(durableQueue.nonEmpty))
          }
        }
      }
      .narrow
  }

  private def createInitialState[A](hasDurableQueue: Boolean) = {
    if (hasDurableQueue) None else Some(DurableProducerQueue.State.empty[A])
  }

  private def waitingForStart[A: ClassTag](
      producerId: String,
      context: ActorContext[InternalCommand],
      stashBuffer: StashBuffer[InternalCommand],
      durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
      settings: WorkPullingProducerController.Settings,
      producer: Option[ActorRef[RequestNext[A]]],
      initialState: Option[DurableProducerQueue.State[A]]): Behavior[InternalCommand] = {

    def becomeActive(p: ActorRef[RequestNext[A]], s: DurableProducerQueue.State[A]): Behavior[InternalCommand] = {
      // resend unconfirmed to self, order doesn't matter for work pulling
      s.unconfirmed.foreach {
        case DurableProducerQueue.MessageSent(oldSeqNr, msg, _, oldConfirmationQualifier, _) =>
          context.self ! ResendDurableMsg(msg, oldConfirmationQualifier, oldSeqNr)
        case _ => // please compiler exhaustiveness check
      }

      val msgAdapter: ActorRef[A] = context.messageAdapter(msg => Msg(msg, wasStashed = false, replyTo = None))
      val requestNext = RequestNext[A](msgAdapter, context.self)
      val b =
        new WorkPullingProducerControllerImpl(context, stashBuffer, producerId, requestNext, durableQueue, settings)
          .active(createInitialState(s.currentSeqNr, p))
      stashBuffer.unstashAll(b)
    }

    Behaviors.receiveMessage {
      case start: Start[A] @unchecked =>
        ProducerControllerImpl.enforceLocalProducer(start.producer)
        initialState match {
          case Some(s) =>
            becomeActive(start.producer, s)
          case None =>
            // waiting for LoadStateReply
            waitingForStart(
              producerId,
              context,
              stashBuffer,
              durableQueue,
              settings,
              Some(start.producer),
              initialState)
        }

      case load: LoadStateReply[A] @unchecked =>
        producer match {
          case Some(p) =>
            becomeActive(p, load.state)
          case None =>
            // waiting for Start
            waitingForStart(producerId, context, stashBuffer, durableQueue, settings, producer, Some(load.state))
        }

      case LoadStateFailed(attempt) =>
        if (attempt >= settings.producerControllerSettings.durableQueueRetryAttempts) {
          val errorMessage = s"LoadState failed after [$attempt] attempts, giving up."
          context.log.error(errorMessage)
          throw new TimeoutException(errorMessage)
        } else {
          context.log.warn(
            "LoadState failed, attempt [{}] of [{}], retrying.",
            attempt,
            settings.producerControllerSettings.durableQueueRetryAttempts)
          // retry
          askLoadState(context, durableQueue, settings, attempt + 1)
          Behaviors.same
        }

      case DurableQueueTerminated =>
        throw new IllegalStateException("DurableQueue was unexpectedly terminated.")

      case other =>
        checkStashFull(stashBuffer)
        stashBuffer.stash(other)
        Behaviors.same
    }
  }

  private def checkStashFull[A](stashBuffer: StashBuffer[InternalCommand]): Unit = {
    if (stashBuffer.isFull)
      throw new IllegalArgumentException(s"Buffer is full, size [${stashBuffer.size}].")
  }

  private def askLoadState[A](
      context: ActorContext[InternalCommand],
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]],
      settings: WorkPullingProducerController.Settings): Option[ActorRef[DurableProducerQueue.Command[A]]] = {

    durableQueueBehavior.map { b =>
      val ref = context.spawn(b, "durable", DispatcherSelector.sameAsParent())
      context.watchWith(ref, DurableQueueTerminated)
      askLoadState(context, Some(ref), settings, attempt = 1)
      ref
    }
  }

  private def askLoadState[A](
      context: ActorContext[InternalCommand],
      durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
      settings: WorkPullingProducerController.Settings,
      attempt: Int): Unit = {
    implicit val loadTimeout: Timeout = settings.producerControllerSettings.durableQueueRequestTimeout
    durableQueue.foreach { ref =>
      context.ask[DurableProducerQueue.LoadState[A], DurableProducerQueue.State[A]](
        ref,
        askReplyTo => DurableProducerQueue.LoadState[A](askReplyTo)) {
        case Success(s) => LoadStateReply(s)
        case Failure(_) => LoadStateFailed(attempt) // timeout
      }
    }
  }

  private def createInitialState[A](
      currentSeqNr: SeqNr,
      producer: ActorRef[WorkPullingProducerController.RequestNext[A]]): State[A] =
    State(currentSeqNr, Set.empty, Map.empty, Map.empty, Map.empty, Map.empty, producer, requested = false)

}

private class WorkPullingProducerControllerImpl[A: ClassTag](
    context: ActorContext[WorkPullingProducerControllerImpl.InternalCommand],
    stashBuffer: StashBuffer[WorkPullingProducerControllerImpl.InternalCommand],
    producerId: String,
    requestNext: WorkPullingProducerController.RequestNext[A],
    durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
    settings: WorkPullingProducerController.Settings) {
  import DurableProducerQueue.MessageSent
  import DurableProducerQueue.StoreMessageConfirmed
  import DurableProducerQueue.StoreMessageSent
  import DurableProducerQueue.StoreMessageSentAck
  import WorkPullingProducerController.GetWorkerStats
  import WorkPullingProducerController.MessageWithConfirmation
  import WorkPullingProducerController.Start
  import WorkPullingProducerController.WorkerStats
  import WorkPullingProducerControllerImpl._

  private val producerControllerSettings = settings.producerControllerSettings
  private val traceEnabled = context.log.isTraceEnabled
  private val durableQueueAskTimeout: Timeout = producerControllerSettings.durableQueueRequestTimeout
  private val workerAskTimeout: Timeout = settings.internalAskTimeout

  private val workerRequestNextAdapter: ActorRef[ProducerController.RequestNext[A]] =
    context.messageAdapter(WorkerRequestNext.apply)

  private def active(s: State[A]): Behavior[InternalCommand] = {

    def onMessage(msg: A, wasStashed: Boolean, replyTo: Option[ActorRef[Done]], totalSeqNr: TotalSeqNr): State[A] = {
      val consumersWithDemand = s.out.iterator.filter { case (_, out) => out.askNextTo.isDefined }.toVector
      if (traceEnabled)
        context.log.trace(
          "Received message seqNr [{}], wasStashed [{}], consumersWithDemand [{}], hasRequested [{}].",
          totalSeqNr,
          wasStashed,
          consumersWithDemand.map(_._1).mkString(", "),
          s.requested)
      if (!s.requested && !wasStashed && durableQueue.isEmpty)
        throw new IllegalStateException(s"Unexpected message [$msg], wasn't requested nor unstashed.")

      val selectedWorker =
        if (durableQueue.isDefined) {
          s.preselectedWorkers.get(totalSeqNr) match {
            case Some(PreselectedWorker(outKey, confirmationQualifier)) =>
              s.out.get(outKey) match {
                case Some(out) => Right(outKey -> out)
                case None      =>
                  // the preselected was deregistered in the meantime
                  context.self ! ResendDurableMsg(msg, confirmationQualifier, totalSeqNr)
                  Left(s)
              }
            case None =>
              throw new IllegalStateException(s"Expected preselected worker for seqNr [$totalSeqNr].")
          }
        } else {
          selectWorker() match {
            case Some(w) => Right(w)
            case None =>
              checkStashFull(stashBuffer)
              context.log.debug("Stashing message, seqNr [{}]", totalSeqNr)
              stashBuffer.stash(Msg(msg, wasStashed = true, replyTo))
              val newRequested = if (wasStashed) s.requested else false
              Left(s.copy(requested = newRequested))
          }
        }

      selectedWorker match {
        case Right((outKey, out)) =>
          val newUnconfirmed = out.unconfirmed :+ Unconfirmed(totalSeqNr, out.seqNr, msg, replyTo)
          val newOut = s.out.updated(outKey, out.copy(unconfirmed = newUnconfirmed, askNextTo = None))
          implicit val askTimeout: Timeout = workerAskTimeout
          context.ask[ProducerController.MessageWithConfirmation[A], OutSeqNr](
            out.askNextTo.get,
            ProducerController.MessageWithConfirmation(msg, _)) {
            case Success(seqNr) => Ack(outKey, seqNr)
            case Failure(_)     => AskTimeout(outKey, out.seqNr)
          }

          def tellRequestNext(): Unit = {
            if (traceEnabled)
              context.log.trace("Sending RequestNext to producer, seqNr [{}].", totalSeqNr)
            s.producer ! requestNext
          }

          val hasMoreDemand = consumersWithDemand.size >= 2
          // decision table based on s.hasRequested, wasStashed, hasMoreDemand, stashBuffer.isEmpty
          val newRequested =
            if (s.requested && !wasStashed && hasMoreDemand) {
              // request immediately since more demand
              tellRequestNext()
              true
            } else if (s.requested && !wasStashed && !hasMoreDemand) {
              // wait until more demand
              false
            } else if (!s.requested && wasStashed && hasMoreDemand && stashBuffer.isEmpty) {
              // msg was unstashed, the last from stash
              tellRequestNext()
              true
            } else if (!s.requested && wasStashed && hasMoreDemand && stashBuffer.nonEmpty) {
              // more in stash
              false
            } else if (!s.requested && wasStashed && !hasMoreDemand) {
              // wait until more demand
              false
            } else if (s.requested && wasStashed) {
              // msg was unstashed, but pending request already in progress
              true
            } else if (durableQueue.isDefined && !s.requested && !wasStashed) {
              // msg ResendDurableMsg, and stashed before storage
              false
            } else {
              throw new IllegalStateException(s"Invalid combination of hasRequested [${s.requested}], " +
              s"wasStashed [$wasStashed], hasMoreDemand [$hasMoreDemand], stashBuffer.isEmpty [${stashBuffer.isEmpty}]")
            }

          s.copy(out = newOut, requested = newRequested, preselectedWorkers = s.preselectedWorkers - totalSeqNr)

        case Left(newState) =>
          newState
      }
    }

    def workersWithDemand: Vector[(OutKey, OutState[A])] =
      s.out.iterator.filter { case (_, out) => out.askNextTo.isDefined }.toVector

    def selectWorker(): Option[(OutKey, OutState[A])] = {
      val preselected = s.preselectedWorkers.valuesIterator.map(_.outKey).toSet
      val workers = workersWithDemand.filterNot {
        case (outKey, _) => preselected(outKey)
      }
      if (workers.isEmpty) {
        None
      } else {
        val i = ThreadLocalRandom.current().nextInt(workers.size)
        Some(workers(i))
      }
    }

    def onMessageBeforeDurableQueue(msg: A, replyTo: Option[ActorRef[Done]]): Behavior[InternalCommand] = {
      selectWorker() match {
        case Some((outKey, out)) =>
          storeMessageSent(
            MessageSent(
              s.currentSeqNr,
              msg,
              ack = replyTo.isDefined,
              out.confirmationQualifier,
              System.currentTimeMillis()),
            attempt = 1)
          val newReplyAfterStore = replyTo match {
            case None    => s.replyAfterStore
            case Some(r) => s.replyAfterStore.updated(s.currentSeqNr, r)
          }
          active(
            s.copy(
              currentSeqNr = s.currentSeqNr + 1,
              preselectedWorkers =
                s.preselectedWorkers.updated(s.currentSeqNr, PreselectedWorker(outKey, out.confirmationQualifier)),
              replyAfterStore = newReplyAfterStore))
        case None =>
          checkStashFull(stashBuffer)
          // no demand from any workers, or all already preselected
          context.log.debug("Stash before storage, seqNr [{}]", s.currentSeqNr)
          // not stored yet, so don't treat it as stashed
          stashBuffer.stash(Msg(msg, wasStashed = false, replyTo))
          active(s)
      }
    }

    def onResendDurableMsg(resend: ResendDurableMsg[A]): Behavior[InternalCommand] = {
      require(durableQueue.isDefined, "Unexpected ResendDurableMsg when DurableQueue not defined.")
      selectWorker() match {
        case Some((outKey, out)) =>
          storeMessageSent(
            MessageSent(s.currentSeqNr, resend.msg, ack = false, out.confirmationQualifier, System.currentTimeMillis()),
            attempt = 1)
          // When StoreMessageSentCompleted (oldConfirmationQualifier, oldSeqNr) confirmation will be stored
          active(
            s.copy(
              currentSeqNr = s.currentSeqNr + 1,
              preselectedWorkers =
                s.preselectedWorkers.updated(s.currentSeqNr, PreselectedWorker(outKey, out.confirmationQualifier)),
              handOver = s.handOver.updated(s.currentSeqNr, HandOver(resend.oldConfirmationQualifier, resend.oldSeqNr))))
        case None =>
          checkStashFull(stashBuffer)
          // no demand from any workers, or all already preselected
          context.log.debug("Stash before storage of resent durable message, seqNr [{}].", s.currentSeqNr)
          // not stored yet, so don't treat it as stashed
          stashBuffer.stash(resend)
          active(s)
      }
    }

    def receiveStoreMessageSentCompleted(seqNr: SeqNr, m: A) = {
      s.replyAfterStore.get(seqNr).foreach { replyTo =>
        if (traceEnabled)
          context.log.trace("Sending reply for seqNr [{}] after storage.", seqNr)
        replyTo ! Done
      }

      val wasHandOver =
        s.handOver.get(seqNr) match {
          case Some(HandOver(oldConfirmationQualifier, oldSeqNr)) =>
            durableQueue.foreach { d =>
              d ! StoreMessageConfirmed(oldSeqNr, oldConfirmationQualifier, System.currentTimeMillis())
            }
            true
          case None =>
            false
        }

      val newState = onMessage(m, wasStashed = wasHandOver, replyTo = None, seqNr)
      active(newState.copy(replyAfterStore = newState.replyAfterStore - seqNr, handOver = newState.handOver - seqNr))
    }

    def receiveAck(ack: Ack): Behavior[InternalCommand] = {
      s.out.get(ack.outKey) match {
        case Some(outState) =>
          val newUnconfirmed = onAck(outState, ack.confirmedSeqNr)
          active(s.copy(out = s.out.updated(ack.outKey, outState.copy(unconfirmed = newUnconfirmed))))
        case None =>
          // obsolete Next, ConsumerController already deregistered
          Behaviors.unhandled
      }
    }

    def onAck(outState: OutState[A], confirmedSeqNr: OutSeqNr): Vector[Unconfirmed[A]] = {
      val (confirmed, newUnconfirmed) = outState.unconfirmed.partition {
        case Unconfirmed(_, seqNr, _, _) => seqNr <= confirmedSeqNr
      }

      if (confirmed.nonEmpty) {
        if (traceEnabled)
          context.log.trace("Received Ack seqNr [{}] from worker [{}].", confirmedSeqNr, outState.confirmationQualifier)
        confirmed.foreach {
          case Unconfirmed(_, _, _, None) => // no reply
          case Unconfirmed(_, _, _, Some(replyTo)) =>
            replyTo ! Done
        }

        durableQueue.foreach { d =>
          // Storing the confirmedSeqNr can be "write behind", at-least-once delivery
          d ! StoreMessageConfirmed(
            confirmed.last.totalSeqNr,
            outState.confirmationQualifier,
            System.currentTimeMillis())
        }
      }

      newUnconfirmed
    }

    def receiveWorkerRequestNext(w: WorkerRequestNext[A]): Behavior[InternalCommand] = {
      val next = w.next
      val outKey = next.producerId
      s.out.get(outKey) match {
        case Some(outState) =>
          val confirmedSeqNr = w.next.confirmedSeqNr
          if (traceEnabled)
            context.log.trace(
              "Received RequestNext from worker [{}], confirmedSeqNr [{}].",
              w.next.producerId,
              confirmedSeqNr)

          val newUnconfirmed = onAck(outState, confirmedSeqNr)

          val newOut =
            s.out.updated(
              outKey,
              outState
                .copy(seqNr = w.next.currentSeqNr, unconfirmed = newUnconfirmed, askNextTo = Some(next.askNextTo)))

          if (stashBuffer.nonEmpty) {
            context.log.debug("Unstash [{}] after RequestNext from worker [{}]", stashBuffer.size, w.next.producerId)
            stashBuffer.unstashAll(active(s.copy(out = newOut)))
          } else if (s.requested) {
            active(s.copy(out = newOut))
          } else {
            if (traceEnabled)
              context.log
                .trace("Sending RequestNext to producer after RequestNext from worker [{}].", w.next.producerId)
            s.producer ! requestNext
            active(s.copy(out = newOut, requested = true))
          }

        case None =>
          // obsolete Next, ConsumerController already deregistered
          Behaviors.unhandled
      }
    }

    def receiveCurrentWorkers(curr: CurrentWorkers[A]): Behavior[InternalCommand] = {
      // TODO #28722 we could also track unreachable workers and avoid them when selecting worker
      val addedWorkers = curr.workers.diff(s.workers)
      val removedWorkers = s.workers.diff(curr.workers)

      val newState = addedWorkers.foldLeft(s) { (acc, c) =>
        val uuid = UUID.randomUUID().toString
        val outKey = s"$producerId-$uuid"
        context.log.debug("Registered worker [{}], with producerId [{}].", c, outKey)
        val p = context.spawn(
          ProducerController[A](outKey, durableQueueBehavior = None, producerControllerSettings),
          uuid,
          DispatcherSelector.sameAsParent())
        p ! ProducerController.Start(workerRequestNextAdapter)
        p ! ProducerController.RegisterConsumer(c)
        acc.copy(out = acc.out.updated(outKey, OutState(p, c, 0L, Vector.empty, None)))
      }

      val newState2 = removedWorkers.foldLeft(newState) { (acc, c) =>
        acc.out.find { case (_, outState) => outState.consumerController == c } match {
          case Some((key, outState)) =>
            context.log.debug("Deregistered worker [{}], with producerId [{}].", c, key)
            context.stop(outState.producerController)
            // resend the unconfirmed, sending to self since order of messages for WorkPulling doesn't matter anyway
            if (outState.unconfirmed.nonEmpty)
              context.log.debug(
                "Resending unconfirmed from deregistered worker with producerId [{}], from seqNr [{}] to [{}].",
                key,
                outState.unconfirmed.head.outSeqNr,
                outState.unconfirmed.last.outSeqNr)
            outState.unconfirmed.foreach {
              case Unconfirmed(totalSeqNr, _, msg, replyTo) =>
                if (durableQueue.isEmpty)
                  context.self ! Msg(msg, wasStashed = true, replyTo)
                else
                  context.self ! ResendDurableMsg(msg, outState.confirmationQualifier, totalSeqNr)
            }
            acc.copy(out = acc.out - key)

          case None =>
            context.log.debug("Deregistered non-existing worker [{}]", c)
            acc
        }
      }

      active(newState2.copy(workers = curr.workers))
    }

    def receiveStart(start: Start[A]): Behavior[InternalCommand] = {
      ProducerControllerImpl.enforceLocalProducer(start.producer)
      context.log.debug("Register new Producer [{}], currentSeqNr [{}].", start.producer, s.currentSeqNr)
      if (s.requested)
        start.producer ! requestNext
      active(s.copy(producer = start.producer))
    }

    Behaviors.receiveMessage {
      case Msg(msg: A, wasStashed, replyTo) =>
        if (durableQueue.isEmpty || wasStashed)
          active(onMessage(msg, wasStashed, replyTo, s.currentSeqNr))
        else
          onMessageBeforeDurableQueue(msg, replyTo)

      case MessageWithConfirmation(msg: A, replyTo) =>
        if (durableQueue.isEmpty)
          active(onMessage(msg, wasStashed = false, Some(replyTo), s.currentSeqNr))
        else
          onMessageBeforeDurableQueue(msg, Some(replyTo))

      case m: ResendDurableMsg[A @unchecked] =>
        onResendDurableMsg(m)

      case StoreMessageSentCompleted(MessageSent(seqNr, m: A, _, _, _)) =>
        receiveStoreMessageSentCompleted(seqNr, m)

      case f: StoreMessageSentFailed[A @unchecked] =>
        receiveStoreMessageSentFailed(f)

      case ack: Ack =>
        receiveAck(ack)

      case w: WorkerRequestNext[A @unchecked] =>
        receiveWorkerRequestNext(w)

      case curr: CurrentWorkers[A @unchecked] =>
        receiveCurrentWorkers(curr)

      case GetWorkerStats(replyTo) =>
        replyTo ! WorkerStats(s.workers.size)
        Behaviors.same

      case RegisterConsumerDone =>
        Behaviors.same

      case start: Start[A @unchecked] =>
        receiveStart(start)

      case AskTimeout(outKey, outSeqNr) =>
        context.log.debug(
          "Message seqNr [{}] sent to worker [{}] timed out. It will be be redelivered.",
          outSeqNr,
          outKey)
        Behaviors.same

      case DurableQueueTerminated =>
        throw new IllegalStateException("DurableQueue was unexpectedly terminated.")

      case unexpected =>
        throw new RuntimeException(s"Unexpected message: $unexpected")
    }
  }

  private def receiveStoreMessageSentFailed(f: StoreMessageSentFailed[A]): Behavior[InternalCommand] = {
    if (f.attempt >= producerControllerSettings.durableQueueRetryAttempts) {
      val errorMessage =
        s"StoreMessageSentFailed seqNr [${f.messageSent.seqNr}] failed after [${f.attempt}] attempts, giving up."
      context.log.error(errorMessage)
      throw new TimeoutException(errorMessage)
    } else {
      context.log.warn("StoreMessageSent seqNr [{}] failed, attempt [{}], retrying.", f.messageSent.seqNr, f.attempt)
      // retry
      storeMessageSent(f.messageSent, attempt = f.attempt + 1)
      Behaviors.same
    }
  }

  private def storeMessageSent(messageSent: MessageSent[A], attempt: Int): Unit = {
    implicit val askTimout: Timeout = durableQueueAskTimeout
    context.ask[StoreMessageSent[A], StoreMessageSentAck](
      durableQueue.get,
      askReplyTo => StoreMessageSent(messageSent, askReplyTo)) {
      case Success(_) => StoreMessageSentCompleted(messageSent)
      case Failure(_) => StoreMessageSentFailed(messageSent, attempt) // timeout
    }
  }

}
