/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.collection.immutable

import akka.actor._

/**
 * INTERNAL API.
 *
 * Messages exchanged between persistent actors and a journal.
 */
private[persistence] object JournalProtocol {

  /** Marker trait shared by internal journal messages. */
  sealed trait Message extends Protocol.Message

  /** Internal journal command. */
  sealed trait Request extends Message

  /** Internal journal acknowledgement. */
  sealed trait Response extends Message

  /**
   * Request to delete all persistent messages with sequence numbers up to `toSequenceNr`
   * (inclusive). `Long.MaxValue` may be used as `toSequenceNr` to delete all persistent messages.
   */
  final case class DeleteMessagesTo(persistenceId: String, toSequenceNr: Long, persistentActor: ActorRef)
      extends Request

  object WriteMessages {
    def apply(
        messages: immutable.Seq[PersistentEnvelope],
        persistentActor: ActorRef,
        actorInstanceId: Int): WriteMessages =
      WriteMessages(messages, persistentActor, actorInstanceId, bypassCircuitBreaker = false)
  }

  /**
   * Request to write messages.
   *
   * @param messages messages to be written.
   * @param persistentActor write requestor.
   */
  final case class WriteMessages(
      messages: immutable.Seq[PersistentEnvelope],
      persistentActor: ActorRef,
      actorInstanceId: Int,
      bypassCircuitBreaker: Boolean)
      extends Request
      with NoSerializationVerificationNeeded

  /**
   * Reply message to a successful [[WriteMessages]] request. This reply is sent to the requestor
   * before all subsequent [[WriteMessageSuccess]] replies.
   */
  case object WriteMessagesSuccessful extends Response

  /**
   * Reply message to a failed [[WriteMessages]] request. This reply is sent to the requestor
   * before all subsequent [[WriteMessageFailure]] replies.
   *
   * @param cause failure cause.
   * @param writeCount the number of atomic writes that failed.
   */
  final case class WriteMessagesFailed(cause: Throwable, writeCount: Int) extends Response

  /**
   * Reply message to a successful [[WriteMessages]] request. For each contained [[PersistentRepr]] message
   * in the request, a separate reply is sent to the requestor.
   *
   * @param persistent successfully written message.
   */
  final case class WriteMessageSuccess(persistent: PersistentRepr, actorInstanceId: Int) extends Response

  /**
   * Reply message to a rejected [[WriteMessages]] request. The write of this message was rejected before
   * it was stored, e.g. because it could not be serialized. For each contained [[PersistentRepr]] message
   * in the request, a separate reply is sent to the requestor.
   *
   * @param message message rejected to be written.
   * @param cause failure cause.
   */
  final case class WriteMessageRejected(message: PersistentRepr, cause: Throwable, actorInstanceId: Int)
      extends Response
      with NoSerializationVerificationNeeded

  /**
   * Reply message to a failed [[WriteMessages]] request. For each contained [[PersistentRepr]] message
   * in the request, a separate reply is sent to the requestor.
   *
   * @param message message failed to be written.
   * @param cause failure cause.
   */
  final case class WriteMessageFailure(message: PersistentRepr, cause: Throwable, actorInstanceId: Int)
      extends Response
      with NoSerializationVerificationNeeded

  /**
   * Reply message to a [[WriteMessages]] with a non-persistent message.
   *
   * @param message looped message.
   */
  final case class LoopMessageSuccess(message: Any, actorInstanceId: Int)
      extends Response
      with NoSerializationVerificationNeeded

  /**
   * Request to replay messages to `persistentActor`.
   *
   * @param fromSequenceNr sequence number where replay should start (inclusive).
   * @param toSequenceNr sequence number where replay should end (inclusive).
   * @param max maximum number of messages to be replayed.
   * @param persistenceId requesting persistent actor id.
   * @param persistentActor requesting persistent actor.
   */
  final case class ReplayMessages(
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long,
      persistenceId: String,
      persistentActor: ActorRef)
      extends Request

  /**
   * Reply message to a [[ReplayMessages]] request. A separate reply is sent to the requestor for each
   * replayed message.
   *
   * @param persistent replayed message.
   */
  final case class ReplayedMessage(persistent: PersistentRepr)
      extends Response
      with DeadLetterSuppression
      with NoSerializationVerificationNeeded

  /**
   * Reply message to a successful [[ReplayMessages]] request. This reply is sent to the requestor
   * after all [[ReplayedMessage]] have been sent (if any).
   *
   * It includes the highest stored sequence number of a given persistent actor. Note that the
   * replay might have been limited to a lower sequence number.
   *
   * @param highestSequenceNr highest stored sequence number.
   */
  case class RecoverySuccess(highestSequenceNr: Long) extends Response with DeadLetterSuppression

  /**
   * Reply message to a failed [[ReplayMessages]] request. This reply is sent to the requestor
   * if a replay could not be successfully completed.
   */
  final case class ReplayMessagesFailure(cause: Throwable) extends Response with DeadLetterSuppression

}

/**
 * Reply message to a successful [[JournalProtocol.DeleteMessagesTo]] request.
 */
final case class DeleteMessagesSuccess(toSequenceNr: Long) extends JournalProtocol.Response

/**
 * Reply message to a failed [[JournalProtocol.DeleteMessagesTo]] request.
 */
final case class DeleteMessagesFailure(cause: Throwable, toSequenceNr: Long) extends JournalProtocol.Response
