/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import akka.actor.ActorPath
import akka.actor.typed.internal.jfr.DeliveryConsumerChangedProducer
import akka.actor.typed.internal.jfr.DeliveryConsumerCreated
import akka.actor.typed.internal.jfr.DeliveryConsumerDuplicate
import akka.actor.typed.internal.jfr.DeliveryConsumerMissing
import akka.actor.typed.internal.jfr.DeliveryConsumerReceived
import akka.actor.typed.internal.jfr.DeliveryConsumerReceivedPreviousInProgress
import akka.actor.typed.internal.jfr.DeliveryConsumerReceivedResend
import akka.actor.typed.internal.jfr.DeliveryConsumerSentRequest
import akka.actor.typed.internal.jfr.DeliveryConsumerStarted
import akka.actor.typed.internal.jfr.DeliveryConsumerStashFull
import akka.actor.typed.internal.jfr.DeliveryProducerCreated
import akka.actor.typed.internal.jfr.DeliveryProducerReceived
import akka.actor.typed.internal.jfr.DeliveryProducerReceivedRequest
import akka.actor.typed.internal.jfr.DeliveryProducerReceivedResend
import akka.actor.typed.internal.jfr.DeliveryProducerRequestNext
import akka.actor.typed.internal.jfr.DeliveryProducerResentFirst
import akka.actor.typed.internal.jfr.DeliveryProducerResentFirstUnconfirmed
import akka.actor.typed.internal.jfr.DeliveryProducerResentUnconfirmed
import akka.actor.typed.internal.jfr.DeliveryProducerSent
import akka.actor.typed.internal.jfr.DeliveryProducerStarted
import akka.actor.typed.internal.jfr.DeliveryProducerWaitingForRequest
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
object ActorFlightRecorder {

  def producerCreated(producerId: String, path: ActorPath): Unit =
    new DeliveryProducerCreated(producerId, path.toString).commit()
  def producerStarted(producerId: String, path: ActorPath): Unit =
    new DeliveryProducerStarted(producerId, path.toString).commit()
  def producerRequestNext(producerId: String, currentSeqNr: Long, confirmedSeqNr: Long): Unit =
    new DeliveryProducerRequestNext(producerId, currentSeqNr, confirmedSeqNr).commit()
  def producerSent(producerId: String, seqNr: Long): Unit =
    new DeliveryProducerSent(producerId, seqNr).commit()
  def producerWaitingForRequest(producerId: String, currentSeqNr: Long): Unit =
    new DeliveryProducerWaitingForRequest(producerId, currentSeqNr).commit()
  def producerResentUnconfirmed(producerId: String, fromSeqNr: Long, toSeqNr: Long): Unit =
    new DeliveryProducerResentUnconfirmed(producerId, fromSeqNr, toSeqNr).commit()
  def producerResentFirst(producerId: String, firstSeqNr: Long): Unit =
    new DeliveryProducerResentFirst(producerId, firstSeqNr).commit()
  def producerResentFirstUnconfirmed(producerId: String, seqNr: Long): Unit =
    new DeliveryProducerResentFirstUnconfirmed(producerId, seqNr).commit()
  def producerReceived(producerId: String, currentSeqNr: Long): Unit =
    new DeliveryProducerReceived(producerId, currentSeqNr).commit()
  def producerReceivedRequest(producerId: String, requestedSeqNr: Long, confirmedSeqNr: Long): Unit =
    new DeliveryProducerReceivedRequest(producerId, requestedSeqNr, confirmedSeqNr).commit()
  def producerReceivedResend(producerId: String, fromSeqNr: Long): Unit =
    new DeliveryProducerReceivedResend(producerId, fromSeqNr).commit()

  def consumerCreated(path: ActorPath): Unit =
    new DeliveryConsumerCreated(path.toString).commit()
  def consumerStarted(path: ActorPath): Unit =
    new DeliveryConsumerStarted(path.toString).commit()
  def consumerReceived(producerId: String, seqNr: Long): Unit =
    new DeliveryConsumerReceived(producerId, seqNr).commit()
  def consumerReceivedPreviousInProgress(producerId: String, seqNr: Long, stashed: Int): Unit =
    new DeliveryConsumerReceivedPreviousInProgress(producerId, seqNr: Long, stashed).commit()
  def consumerDuplicate(producerId: String, expectedSeqNr: Long, seqNr: Long): Unit =
    new DeliveryConsumerDuplicate(producerId, expectedSeqNr, seqNr).commit()
  def consumerMissing(producerId: String, expectedSeqNr: Long, seqNr: Long): Unit =
    new DeliveryConsumerMissing(producerId, expectedSeqNr, seqNr).commit()
  def consumerReceivedResend(seqNr: Long): Unit =
    new DeliveryConsumerReceivedResend(seqNr).commit()
  def consumerSentRequest(producerId: String, requestedSeqNr: Long): Unit =
    new DeliveryConsumerSentRequest(producerId, requestedSeqNr).commit()
  def consumerChangedProducer(producerId: String): Unit =
    new DeliveryConsumerChangedProducer(producerId).commit()
  def consumerStashFull(producerId: String, seqNr: Long): Unit =
    new DeliveryConsumerStashFull(producerId, seqNr).commit()

}
