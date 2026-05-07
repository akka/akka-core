/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.delivery.internal

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
import akka.actor.typed.delivery.internal.ProducerControllerImpl
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.StashBuffer
import akka.annotation.InternalApi
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.delivery.ShardingProducerController
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ShardingProducerControllerImpl {

  import ShardingProducerController.Command
  import ShardingProducerController.EntityId
  import ShardingProducerController.RequestNext
  import ShardingProducerController.Start

  sealed trait InternalCommand

  /** For commands defined in public ShardingProducerController */
  trait UnsealedInternalCommand extends InternalCommand

  private type TotalSeqNr = Long
  private type OutSeqNr = Long
  private type OutKey = String

  private final case class Ack(outKey: OutKey, confirmedSeqNr: OutSeqNr) extends InternalCommand
  private final case class AskTimeout(outKey: OutKey, outSeqNr: OutSeqNr) extends InternalCommand

  private final case class WrappedRequestNext[A](next: ProducerController.RequestNext[A]) extends InternalCommand

  private final case class Msg[A](envelope: ShardingEnvelope[A], alreadyStored: TotalSeqNr) extends InternalCommand {
    def isAlreadyStored: Boolean = alreadyStored > 0
  }

  private case class LoadStateReply[A](state: DurableProducerQueue.State[A]) extends InternalCommand
  private case class LoadStateFailed(attempt: Int) extends InternalCommand
  private case class StoreMessageSentReply(ack: DurableProducerQueue.StoreMessageSentAck)
  private case class StoreMessageSentFailed[A](messageSent: DurableProducerQueue.MessageSent[A], attempt: Int)
      extends InternalCommand
  private case class StoreMessageSentCompleted[A](messageSent: DurableProducerQueue.MessageSent[A])
      extends InternalCommand
  private case object DurableQueueTerminated extends InternalCommand

  private case object ResendFirstUnconfirmed extends InternalCommand
  private case object CleanupUnused extends InternalCommand

  private final case class OutState[A](
      entityId: EntityId,
      producerController: ActorRef[ProducerController.Command[A]],
      nextTo: Option[ProducerController.RequestNext[A]],
      buffered: Vector[Buffered[A]],
      seqNr: OutSeqNr,
      unconfirmed: Vector[Unconfirmed[A]],
      usedNanoTime: Long) {
    if (nextTo.nonEmpty && buffered.nonEmpty)
      throw new IllegalStateException("nextTo and buffered shouldn't both be nonEmpty.")
  }

  private final case class Buffered[A](totalSeqNr: TotalSeqNr, msg: A, replyTo: Option[ActorRef[Done]])

  private final case class Unconfirmed[A](totalSeqNr: TotalSeqNr, outSeqNr: OutSeqNr, replyTo: Option[ActorRef[Done]])

  private final case class State[A](
      currentSeqNr: TotalSeqNr,
      producer: ActorRef[ShardingProducerController.RequestNext[A]],
      out: Map[OutKey, OutState[A]],
      // replyAfterStore is used when durableQueue is enabled, otherwise they are tracked in OutState
      replyAfterStore: Map[TotalSeqNr, ActorRef[Done]]) {

    def bufferSize: Long = {
      out.valuesIterator.foldLeft(0L) { case (acc, outState) => acc + outState.buffered.size }
    }
  }

  def apply[A: ClassTag](
      producerId: String,
      region: ActorRef[ShardingEnvelope[ConsumerController.SequencedMessage[A]]],
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]],
      settings: ShardingProducerController.Settings): Behavior[Command[A]] = {
    Behaviors
      .withStash[InternalCommand](settings.bufferSize) { stashBuffer =>
        Behaviors.setup[InternalCommand] { context =>
          Behaviors.withMdc(staticMdc = Map("producerId" -> producerId)) {
            context.setLoggerName("akka.cluster.sharding.typed.delivery.ShardingProducerController")

            val durableQueue = askLoadState(context, durableQueueBehavior, settings)

            waitingForStart(
              producerId,
              context,
              stashBuffer,
              region,
              durableQueue,
              None,
              createInitialState(durableQueue.nonEmpty),
              settings)
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
      region: ActorRef[ShardingEnvelope[ConsumerController.SequencedMessage[A]]],
      durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
      producer: Option[ActorRef[RequestNext[A]]],
      initialState: Option[DurableProducerQueue.State[A]],
      settings: ShardingProducerController.Settings): Behavior[InternalCommand] = {

    def becomeActive(p: ActorRef[RequestNext[A]], s: DurableProducerQueue.State[A]): Behavior[InternalCommand] = {
      Behaviors.withTimers { timers =>
        timers.startTimerWithFixedDelay(CleanupUnused, settings.cleanupUnusedAfter / 2)
        timers.startTimerWithFixedDelay(ResendFirstUnconfirmed, settings.resendFirstUnconfirmedIdleTimeout / 2)

        // resend unconfirmed before other stashed messages
        Behaviors.withStash[InternalCommand](settings.bufferSize) { newStashBuffer =>
          Behaviors.setup { _ =>
            s.unconfirmed.foreach { m =>
              newStashBuffer.stash(Msg(ShardingEnvelope(m.confirmationQualifier, m.message), alreadyStored = m.seqNr))
            }
            // append other stashed messages after the unconfirmed
            stashBuffer.foreach(newStashBuffer.stash)

            val msgAdapter: ActorRef[ShardingEnvelope[A]] = context.messageAdapter(msg => Msg(msg, alreadyStored = 0))
            if (s.unconfirmed.isEmpty)
              p ! RequestNext(msgAdapter, context.self, Set.empty, Map.empty)
            val b = new ShardingProducerControllerImpl(context, producerId, msgAdapter, region, durableQueue, settings)
              .active(State(s.currentSeqNr, p, Map.empty, Map.empty))

            newStashBuffer.unstashAll(b)
          }
        }
      }
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
              region,
              durableQueue,
              Some(start.producer),
              initialState,
              settings)
        }

      case load: LoadStateReply[A] @unchecked =>
        producer match {
          case Some(p) =>
            becomeActive(p, load.state)
          case None =>
            // waiting for LoadStateReply
            waitingForStart(
              producerId,
              context,
              stashBuffer,
              region,
              durableQueue,
              producer,
              Some(load.state),
              settings)
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
      settings: ShardingProducerController.Settings): Option[ActorRef[DurableProducerQueue.Command[A]]] = {

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
      settings: ShardingProducerController.Settings,
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

}

private class ShardingProducerControllerImpl[A: ClassTag](
    context: ActorContext[ShardingProducerControllerImpl.InternalCommand],
    producerId: String,
    msgAdapter: ActorRef[ShardingEnvelope[A]],
    region: ActorRef[ShardingEnvelope[ConsumerController.SequencedMessage[A]]],
    durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
    settings: ShardingProducerController.Settings) {
  import DurableProducerQueue.MessageSent
  import DurableProducerQueue.StoreMessageConfirmed
  import DurableProducerQueue.StoreMessageSent
  import DurableProducerQueue.StoreMessageSentAck
  import ShardingProducerController.EntityId
  import ShardingProducerController.MessageWithConfirmation
  import ShardingProducerController.RequestNext
  import ShardingProducerController.Start
  import ShardingProducerControllerImpl._

  private val producerControllerSettings = settings.producerControllerSettings
  private val durableQueueAskTimeout: Timeout = producerControllerSettings.durableQueueRequestTimeout
  private val entityAskTimeout: Timeout = settings.internalAskTimeout
  private val traceEnabled = context.log.isTraceEnabled

  private val requestNextAdapter: ActorRef[ProducerController.RequestNext[A]] =
    context.messageAdapter(WrappedRequestNext.apply)

  private def active(s: State[A]): Behavior[InternalCommand] = {

    def onMessage(
        entityId: EntityId,
        msg: A,
        replyTo: Option[ActorRef[Done]],
        totalSeqNr: TotalSeqNr,
        newReplyAfterStore: Map[TotalSeqNr, ActorRef[Done]]): Behavior[InternalCommand] = {

      val outKey = s"$producerId-$entityId"
      val newState =
        s.out.get(outKey) match {
          case Some(out @ OutState(_, _, Some(nextTo), _, _, _, _)) =>
            // there is demand, send immediately
            send(msg, outKey, out.seqNr, nextTo)
            val newUnconfirmed = out.unconfirmed :+ Unconfirmed(totalSeqNr, out.seqNr, replyTo)
            s.copy(
              out = s.out.updated(
                outKey,
                out.copy(
                  seqNr = out.seqNr + 1,
                  nextTo = None,
                  unconfirmed = newUnconfirmed,
                  usedNanoTime = System.nanoTime())),
              replyAfterStore = newReplyAfterStore)
          case Some(out @ OutState(_, _, None, buffered, _, _, _)) =>
            // no demand, buffer
            if (s.bufferSize >= settings.bufferSize)
              throw new IllegalArgumentException(s"Buffer is full, size [${settings.bufferSize}].")
            context.log.debug(
              "Buffering message to entityId [{}], buffer size for entity [{}]",
              entityId,
              buffered.size + 1)
            val newBuffered = buffered :+ Buffered(totalSeqNr, msg, replyTo)
            val newS =
              s.copy(
                out = s.out.updated(outKey, out.copy(buffered = newBuffered)),
                replyAfterStore = newReplyAfterStore)
            // send an updated RequestNext to indicate buffer usage
            s.producer ! createRequestNext(newS)
            newS
          case None =>
            context.log.debug("Creating ProducerController for entity [{}]", entityId)
            val send: ConsumerController.SequencedMessage[A] => Unit = { seqMsg =>
              region ! ShardingEnvelope(entityId, seqMsg)
            }
            val p = context.spawn(
              ProducerController[A](outKey, durableQueueBehavior = None, producerControllerSettings, send),
              entityId,
              DispatcherSelector.sameAsParent())
            p ! ProducerController.Start(requestNextAdapter)
            s.copy(
              out = s.out.updated(
                outKey,
                OutState(
                  entityId,
                  p,
                  None,
                  Vector(Buffered(totalSeqNr, msg, replyTo)),
                  1L,
                  Vector.empty,
                  System.nanoTime())),
              replyAfterStore = newReplyAfterStore)
        }

      active(newState)
    }

    def onAck(outState: OutState[A], confirmedSeqNr: OutSeqNr): Vector[Unconfirmed[A]] = {
      val (confirmed, newUnconfirmed) = outState.unconfirmed.partition {
        case Unconfirmed(_, seqNr, _) => seqNr <= confirmedSeqNr
      }

      if (confirmed.nonEmpty) {
        confirmed.foreach {
          case Unconfirmed(_, _, None) => // no reply
          case Unconfirmed(_, _, Some(replyTo)) =>
            replyTo ! Done
        }

        durableQueue.foreach { d =>
          // Storing the confirmedSeqNr can be "write behind", at-least-once delivery
          d ! StoreMessageConfirmed(confirmed.last.totalSeqNr, outState.entityId, System.currentTimeMillis())
        }
      }

      newUnconfirmed
    }

    def receiveStoreMessageSentCompleted(
        seqNr: SeqNr,
        msg: A,
        entityId: ConfirmationQualifier): Behavior[InternalCommand] = {
      s.replyAfterStore.get(seqNr).foreach { replyTo =>
        context.log.info("Confirmation reply to [{}] after storage", seqNr)
        replyTo ! Done
      }
      val newReplyAfterStore = s.replyAfterStore - seqNr

      onMessage(entityId, msg, replyTo = None, seqNr, newReplyAfterStore)
    }

    def receiveStoreMessageSentFailed(f: StoreMessageSentFailed[A]): Behavior[InternalCommand] = {
      if (f.attempt >= producerControllerSettings.durableQueueRetryAttempts) {
        val errorMessage =
          s"StoreMessageSentFailed seqNr [${f.messageSent.seqNr}] failed after [${f.attempt}] attempts, giving up."
        context.log.error(errorMessage)
        throw new TimeoutException(errorMessage)
      } else {
        context.log.info(s"StoreMessageSent seqNr [{}] failed, attempt [{}], retrying.", f.messageSent.seqNr, f.attempt)
        // retry
        storeMessageSent(f.messageSent, attempt = f.attempt + 1)
        Behaviors.same
      }
    }

    def receiveAck(ack: Ack): Behavior[InternalCommand] = {
      s.out.get(ack.outKey) match {
        case Some(outState) =>
          if (traceEnabled)
            context.log.trace("Received Ack, confirmed [{}], current [{}].", ack.confirmedSeqNr, s.currentSeqNr)
          val newUnconfirmed = onAck(outState, ack.confirmedSeqNr)
          val newUsedNanoTime =
            if (newUnconfirmed.size != outState.unconfirmed.size) System.nanoTime() else outState.usedNanoTime
          active(
            s.copy(out =
              s.out.updated(ack.outKey, outState.copy(unconfirmed = newUnconfirmed, usedNanoTime = newUsedNanoTime))))
        case None =>
          // obsolete Ack, ConsumerController already deregistered
          Behaviors.unhandled
      }
    }

    def receiveWrappedRequestNext(w: WrappedRequestNext[A]): Behavior[InternalCommand] = {
      val next = w.next
      val outKey = next.producerId
      s.out.get(outKey) match {
        case Some(out) =>
          if (out.nextTo.nonEmpty)
            throw new IllegalStateException(s"Received RequestNext but already has demand for [$outKey]")

          val confirmedSeqNr = w.next.confirmedSeqNr
          if (traceEnabled)
            context.log.trace("Received RequestNext from [{}], confirmed seqNr [{}]", out.entityId, confirmedSeqNr)
          val newUnconfirmed = onAck(out, confirmedSeqNr)

          if (out.buffered.nonEmpty) {
            val buf = out.buffered.head
            send(buf.msg, outKey, out.seqNr, next)
            val newUnconfirmed2 = newUnconfirmed :+ Unconfirmed(buf.totalSeqNr, out.seqNr, buf.replyTo)
            val newProducers = s.out.updated(
              outKey,
              out.copy(
                seqNr = out.seqNr + 1,
                nextTo = None,
                unconfirmed = newUnconfirmed2,
                buffered = out.buffered.tail,
                usedNanoTime = System.nanoTime()))
            active(s.copy(out = newProducers))
          } else {
            val newProducers =
              s.out.updated(
                outKey,
                out.copy(nextTo = Some(next), unconfirmed = newUnconfirmed, usedNanoTime = System.nanoTime()))
            val newState = s.copy(out = newProducers)
            // send an updated RequestNext
            s.producer ! createRequestNext(newState)
            active(newState)
          }

        case None =>
          // if ProducerController was stopped and there was a RequestNext in flight, but will not happen in practise
          context.log.warn("Received RequestNext for unknown [{}]", outKey)
          Behaviors.same
      }
    }

    def receiveStart(start: Start[A]): Behavior[InternalCommand] = {
      ProducerControllerImpl.enforceLocalProducer(start.producer)
      context.log.debug("Register new Producer [{}], currentSeqNr [{}].", start.producer, s.currentSeqNr)
      start.producer ! createRequestNext(s)
      active(s.copy(producer = start.producer))
    }

    def receiveResendFirstUnconfirmed(): Behavior[InternalCommand] = {
      val now = System.nanoTime()
      s.out.foreach {
        case (outKey: OutKey, outState) =>
          val idleDurationMillis = (now - outState.usedNanoTime) / 1000 / 1000
          if (outState.unconfirmed.nonEmpty && idleDurationMillis >= settings.resendFirstUnconfirmedIdleTimeout.toMillis) {
            context.log.debug(
              "Resend first unconfirmed for [{}], because it was idle for [{} ms]",
              outKey,
              idleDurationMillis)
            outState.producerController
              .unsafeUpcast[ProducerControllerImpl.InternalCommand] ! ProducerControllerImpl.ResendFirstUnconfirmed
          }
      }
      Behaviors.same
    }

    def receiveCleanupUnused(): Behavior[InternalCommand] = {
      val now = System.nanoTime()
      val removeOutKeys =
        s.out.flatMap {
          case (outKey: OutKey, outState) =>
            val idleDurationMillis = (now - outState.usedNanoTime) / 1000 / 1000
            if (outState.unconfirmed.isEmpty && outState.buffered.isEmpty && idleDurationMillis >= settings.cleanupUnusedAfter.toMillis) {
              context.log.debug("Cleanup unused [{}], because it was idle for [{} ms]", outKey, idleDurationMillis)
              context.stop(outState.producerController)
              Some(outKey)
            } else
              None
        }
      if (removeOutKeys.isEmpty)
        Behaviors.same
      else
        active(s.copy(out = s.out -- removeOutKeys))
    }

    Behaviors.receiveMessage {

      case msg: Msg[A @unchecked] =>
        if (durableQueue.isEmpty) {
          // currentSeqNr is only updated when durableQueue is enabled
          onMessage(msg.envelope.entityId, msg.envelope.message, None, s.currentSeqNr, s.replyAfterStore)
        } else if (msg.isAlreadyStored) {
          // loaded from durable queue, currentSeqNr has already b
          onMessage(msg.envelope.entityId, msg.envelope.message, None, msg.alreadyStored, s.replyAfterStore)
        } else {
          storeMessageSent(
            MessageSent(
              s.currentSeqNr,
              msg.envelope.message,
              ack = false,
              msg.envelope.entityId,
              System.currentTimeMillis()),
            attempt = 1)
          active(s.copy(currentSeqNr = s.currentSeqNr + 1))
        }

      case MessageWithConfirmation(entityId, message: A, replyTo) =>
        if (durableQueue.isEmpty) {
          onMessage(entityId, message, Some(replyTo), s.currentSeqNr, s.replyAfterStore)
        } else {
          storeMessageSent(
            MessageSent(s.currentSeqNr, message, ack = true, entityId, System.currentTimeMillis()),
            attempt = 1)
          val newReplyAfterStore = s.replyAfterStore.updated(s.currentSeqNr, replyTo)
          active(s.copy(currentSeqNr = s.currentSeqNr + 1, replyAfterStore = newReplyAfterStore))
        }

      case StoreMessageSentCompleted(MessageSent(seqNr, msg: A, _, entityId, _)) =>
        receiveStoreMessageSentCompleted(seqNr, msg, entityId)

      case f: StoreMessageSentFailed[A @unchecked] =>
        receiveStoreMessageSentFailed(f)

      case ack: Ack =>
        receiveAck(ack)

      case w: WrappedRequestNext[A @unchecked] =>
        receiveWrappedRequestNext(w)

      case ResendFirstUnconfirmed =>
        receiveResendFirstUnconfirmed()

      case CleanupUnused =>
        receiveCleanupUnused()

      case start: Start[A @unchecked] =>
        receiveStart(start)

      case AskTimeout(outKey, outSeqNr) =>
        context.log.debug(
          "Message seqNr [{}] sent to entity [{}] timed out. It will be be redelivered.",
          outSeqNr,
          outKey)
        Behaviors.same

      case DurableQueueTerminated =>
        throw new IllegalStateException("DurableQueue was unexpectedly terminated.")

      case unexpected =>
        throw new RuntimeException(s"Unexpected message: $unexpected")
    }
  }

  private def createRequestNext(s: State[A]): RequestNext[A] = {
    val entitiesWithDemand = s.out.valuesIterator.collect { case out if out.nextTo.nonEmpty => out.entityId }.toSet
    val bufferedForEntitesWithoutDemand = s.out.valuesIterator.collect {
      case out if out.nextTo.isEmpty => out.entityId -> out.buffered.size
    }.toMap
    RequestNext(msgAdapter, context.self, entitiesWithDemand, bufferedForEntitesWithoutDemand)
  }

  private def send(msg: A, outKey: OutKey, outSeqNr: OutSeqNr, nextTo: ProducerController.RequestNext[A]): Unit = {
    if (traceEnabled)
      context.log.trace("Sending [{}] to [{}] with outSeqNr [{}].", msg.getClass.getName, outKey, outSeqNr)
    implicit val askTimeout: Timeout = entityAskTimeout
    context.ask[ProducerController.MessageWithConfirmation[A], OutSeqNr](
      nextTo.askNextTo,
      ProducerController.MessageWithConfirmation(msg, _)) {
      case Success(seqNr) =>
        if (seqNr != outSeqNr)
          context.log.error("Inconsistent Ack seqNr [{}] != [{}]", seqNr, outSeqNr)
        Ack(outKey, seqNr)
      case Failure(_) =>
        AskTimeout(outKey, outSeqNr)
    }
  }

  private def storeMessageSent(messageSent: MessageSent[A], attempt: Int): Unit = {
    implicit val askTimeout: Timeout = durableQueueAskTimeout
    context.ask[StoreMessageSent[A], StoreMessageSentAck](
      durableQueue.get,
      askReplyTo => StoreMessageSent(messageSent, askReplyTo)) {
      case Success(_) => StoreMessageSentCompleted(messageSent)
      case Failure(_) => StoreMessageSentFailed(messageSent, attempt) // timeout
    }
  }
}
