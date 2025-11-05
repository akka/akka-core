/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import scala.collection.immutable

import akka.actor.typed.ActorRef
import akka.actor.typed.delivery.internal.ChunkedMessage
import akka.actor.typed.delivery.internal.DeliverySerializable
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi

/**
 * Actor message protocol for storing and confirming reliable delivery of messages. A [[akka.actor.typed.Behavior]]
 * implementation of this protocol can optionally be used with [[ProducerController]] when messages shall survive
 * a crash of the producer side.
 *
 * An implementation of this exists in `akka.persistence.typed.delivery.EventSourcedProducerQueue`.
 */
@ApiMayChange // TODO #28719 when removing ApiMayChange consider removing `case class` for some of the messages
object DurableProducerQueue {

  type SeqNr = Long
  // Timestamp in millis since epoch, System.currentTimeMillis
  type TimestampMillis = Long

  type ConfirmationQualifier = String

  val NoQualifier: ConfirmationQualifier = ""

  trait Command[A]

  /**
   * Request that is used at startup to retrieve the unconfirmed messages and current sequence number.
   */
  final case class LoadState[A](replyTo: ActorRef[State[A]]) extends Command[A]

  /**
   * Store the fact that a message is to be sent. Replies with [[StoreMessageSentAck]] when
   * the message has been successfully been stored.
   *
   * This command may be retied and the implementation should be idempotent, i.e. deduplicate
   * already processed sequence numbers.
   */
  final case class StoreMessageSent[A](sent: MessageSent[A], replyTo: ActorRef[StoreMessageSentAck]) extends Command[A]

  final case class StoreMessageSentAck(storedSeqNr: SeqNr)

  /**
   * Store the fact that a message has been confirmed to be delivered and processed.
   *
   * This command may be retied and the implementation should be idempotent, i.e. deduplicate
   * already processed sequence numbers.
   */
  final case class StoreMessageConfirmed[A](
      seqNr: SeqNr,
      confirmationQualifier: ConfirmationQualifier,
      timestampMillis: TimestampMillis)
      extends Command[A]

  object State {
    def empty[A]: State[A] = State(1L, 0L, Map.empty, Vector.empty)
  }
  final case class State[A](
      currentSeqNr: SeqNr,
      highestConfirmedSeqNr: SeqNr,
      confirmedSeqNr: Map[ConfirmationQualifier, (SeqNr, TimestampMillis)],
      unconfirmed: immutable.IndexedSeq[MessageSent[A]])
      extends DeliverySerializable {

    def addMessageSent(sent: MessageSent[A]): State[A] = {
      copy(currentSeqNr = sent.seqNr + 1, unconfirmed = unconfirmed :+ sent)
    }

    def confirmed(
        seqNr: SeqNr,
        confirmationQualifier: ConfirmationQualifier,
        timestampMillis: TimestampMillis): State[A] = {
      val newUnconfirmed = unconfirmed.filterNot { u =>
        u.confirmationQualifier == confirmationQualifier && u.seqNr <= seqNr
      }
      copy(
        highestConfirmedSeqNr = math.max(highestConfirmedSeqNr, seqNr),
        confirmedSeqNr = confirmedSeqNr.updated(confirmationQualifier, (seqNr, timestampMillis)),
        unconfirmed = newUnconfirmed)
    }

    def cleanup(confirmationQualifiers: Set[String]): State[A] = {
      copy(confirmedSeqNr = confirmedSeqNr -- confirmationQualifiers)
    }

    /**
     * If not all chunked messages were stored before crash those partial chunked messages should not be resent.
     */
    def cleanupPartialChunkedMessages(): State[A] = {
      if (unconfirmed.isEmpty || unconfirmed.forall(u => u.isFirstChunk && u.isLastChunk)) {
        this
      } else {
        val tmp = Vector.newBuilder[MessageSent[A]]
        val newUnconfirmed = Vector.newBuilder[MessageSent[A]]
        var newCurrentSeqNr = highestConfirmedSeqNr + 1
        unconfirmed.foreach { u =>
          if (u.isFirstChunk && u.isLastChunk) {
            tmp.clear()
            newUnconfirmed += u
            newCurrentSeqNr = u.seqNr + 1
          } else if (u.isFirstChunk && !u.isLastChunk) {
            tmp.clear()
            tmp += u
          } else if (!u.isLastChunk) {
            tmp += u
          } else if (u.isLastChunk) {
            newUnconfirmed ++= tmp.result()
            newUnconfirmed += u
            newCurrentSeqNr = u.seqNr + 1
            tmp.clear()
          }
        }
        copy(currentSeqNr = newCurrentSeqNr, unconfirmed = newUnconfirmed.result())
      }
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] sealed trait Event extends DeliverySerializable

  /**
   * The fact (event) that a message has been sent.
   */
  final class MessageSent[A](
      val seqNr: SeqNr,
      val message: MessageSent.MessageOrChunk,
      val ack: Boolean,
      val confirmationQualifier: ConfirmationQualifier,
      val timestampMillis: TimestampMillis)
      extends Event {

    /** INTERNAL API */
    @InternalApi private[akka] def isFirstChunk: Boolean = {
      message match {
        case c: ChunkedMessage => c.firstChunk
        case _                 => true
      }
    }

    /** INTERNAL API */
    @InternalApi private[akka] def isLastChunk: Boolean = {
      message match {
        case c: ChunkedMessage => c.lastChunk
        case _                 => true
      }
    }

    def withConfirmationQualifier(qualifier: ConfirmationQualifier): MessageSent[A] =
      new MessageSent(seqNr, message, ack, qualifier, timestampMillis)

    def withTimestampMillis(timestamp: TimestampMillis): MessageSent[A] =
      new MessageSent(seqNr, message, ack, confirmationQualifier, timestamp)

    override def equals(obj: Any): Boolean = {
      obj match {
        case other: MessageSent[_] =>
          seqNr == other.seqNr && message == other.message && ack == other.ack && confirmationQualifier == other.confirmationQualifier && timestampMillis == other.timestampMillis
        case _ => false
      }
    }

    override def hashCode(): Int = seqNr.hashCode()

    override def toString: ConfirmationQualifier =
      s"MessageSent($seqNr,$message,$ack,$confirmationQualifier,$timestampMillis)"

  }

  object MessageSent {

    /**
     * SequencedMessage.message can be `A` or `ChunkedMessage`.
     */
    type MessageOrChunk = Any

    def apply[A](
        seqNr: SeqNr,
        message: A,
        ack: Boolean,
        confirmationQualifier: ConfirmationQualifier,
        timestampMillis: TimestampMillis): MessageSent[A] =
      new MessageSent(seqNr, message, ack, confirmationQualifier, timestampMillis)

    /**
     * INTERNAL API
     */
    @InternalApi private[akka] def fromChunked[A](
        seqNr: SeqNr,
        chunkedMessage: ChunkedMessage,
        ack: Boolean,
        confirmationQualifier: ConfirmationQualifier,
        timestampMillis: TimestampMillis): MessageSent[A] =
      new MessageSent(seqNr, chunkedMessage, ack, confirmationQualifier, timestampMillis)

    /**
     * INTERNAL API
     */
    @InternalApi private[akka] def fromMessageOrChunked[A](
        seqNr: SeqNr,
        message: MessageOrChunk,
        ack: Boolean,
        confirmationQualifier: ConfirmationQualifier,
        timestampMillis: TimestampMillis): MessageSent[A] =
      new MessageSent(seqNr, message, ack, confirmationQualifier, timestampMillis)

    def unapply(
        sent: MessageSent[_]): Option[(SeqNr, MessageOrChunk, Boolean, ConfirmationQualifier, TimestampMillis)] =
      Some((sent.seqNr, sent.message, sent.ack, sent.confirmationQualifier, sent.timestampMillis))
  }

  /**
   * INTERNAL API: The fact (event) that a message has been confirmed to be delivered and processed.
   */
  @InternalApi private[akka] final case class Confirmed(
      seqNr: SeqNr,
      confirmationQualifier: ConfirmationQualifier,
      timestampMillis: TimestampMillis)
      extends Event

  /**
   * INTERNAL API: Remove entries related to the confirmationQualifiers that haven't been used for a while.
   */
  @InternalApi private[akka] final case class Cleanup(confirmationQualifiers: Set[String]) extends Event

}
