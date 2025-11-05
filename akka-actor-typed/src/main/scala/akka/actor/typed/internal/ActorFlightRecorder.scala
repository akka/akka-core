/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import akka.actor.ActorPath
import akka.actor.typed.{ ActorSystem, Extension, ExtensionId }
import akka.annotation.InternalApi
import akka.util.FlightRecorderLoader

/**
 * INTERNAL API
 */
@InternalApi
object ActorFlightRecorder extends ExtensionId[ActorFlightRecorder] {

  override def createExtension(system: ActorSystem[_]): ActorFlightRecorder =
    FlightRecorderLoader.load[ActorFlightRecorder](
      system,
      "akka.actor.typed.internal.jfr.JFRActorFlightRecorder",
      NoOpActorFlightRecorder)
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] trait ActorFlightRecorder extends Extension {
  val delivery: DeliveryFlightRecorder
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait DeliveryFlightRecorder {

  def producerCreated(producerId: String, path: ActorPath): Unit
  def producerStarted(producerId: String, path: ActorPath): Unit
  def producerRequestNext(producerId: String, currentSeqNr: Long, confirmedSeqNr: Long): Unit
  def producerSent(producerId: String, seqNr: Long): Unit
  def producerWaitingForRequest(producerId: String, currentSeqNr: Long): Unit
  def producerResentUnconfirmed(producerId: String, fromSeqNr: Long, toSeqNr: Long): Unit
  def producerResentFirst(producerId: String, firstSeqNr: Long): Unit
  def producerResentFirstUnconfirmed(producerId: String, seqNr: Long): Unit
  def producerReceived(producerId: String, currentSeqNr: Long): Unit
  def producerReceivedRequest(producerId: String, requestedSeqNr: Long, confirmedSeqNr: Long): Unit
  def producerReceivedResend(producerId: String, fromSeqNr: Long): Unit

  def consumerCreated(path: ActorPath): Unit
  def consumerStarted(path: ActorPath): Unit
  def consumerReceived(producerId: String, seqNr: Long): Unit
  def consumerReceivedPreviousInProgress(producerId: String, seqNr: Long, stashed: Int): Unit
  def consumerDuplicate(producerId: String, expectedSeqNr: Long, seqNr: Long): Unit
  def consumerMissing(producerId: String, expectedSeqNr: Long, seqNr: Long): Unit
  def consumerReceivedResend(seqNr: Long): Unit
  def consumerSentRequest(producerId: String, requestedSeqNr: Long): Unit
  def consumerChangedProducer(producerId: String): Unit
  def consumerStashFull(producerId: String, seqNr: Long): Unit
}

/**
 * JFR is only available under certain circumstances (JDK11 for now, possible OpenJDK 8 in the future) so therefore
 * the default on JDK 8 needs to be a no-op flight recorder.
 *
 * INTERNAL
 */
@InternalApi
private[akka] case object NoOpActorFlightRecorder extends ActorFlightRecorder {
  override val delivery: DeliveryFlightRecorder = NoOpDeliveryFlightRecorder
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object NoOpDeliveryFlightRecorder extends DeliveryFlightRecorder {

  override def producerCreated(producerId: String, path: ActorPath): Unit = ()
  override def producerStarted(producerId: String, path: ActorPath): Unit = ()
  override def producerRequestNext(producerId: String, currentSeqNr: Long, confirmedSeqNr: Long): Unit = ()
  override def producerSent(producerId: String, seqNr: Long): Unit = ()
  override def producerWaitingForRequest(producerId: String, currentSeqNr: Long): Unit = ()
  override def producerResentUnconfirmed(producerId: String, fromSeqNr: Long, toSeqNr: Long): Unit = ()
  override def producerResentFirst(producerId: String, firstSeqNr: Long): Unit = ()
  override def producerResentFirstUnconfirmed(producerId: String, seqNr: Long): Unit = ()
  override def producerReceived(producerId: String, currentSeqNr: Long): Unit = ()
  override def producerReceivedRequest(producerId: String, requestedSeqNr: Long, confirmedSeqNr: Long): Unit = ()
  override def producerReceivedResend(producerId: String, fromSeqNr: Long): Unit = ()

  override def consumerCreated(path: ActorPath): Unit = ()
  override def consumerStarted(path: ActorPath): Unit = ()
  override def consumerReceived(producerId: String, seqNr: Long): Unit = ()
  override def consumerReceivedPreviousInProgress(producerId: String, seqNr: Long, stashed: Int): Unit = ()
  override def consumerDuplicate(producerId: String, expectedSeqNr: Long, seqNr: Long): Unit = ()
  override def consumerMissing(producerId: String, expectedSeqNr: Long, seqNr: Long): Unit = ()
  override def consumerReceivedResend(seqNr: Long): Unit = ()
  override def consumerSentRequest(producerId: String, requestedSeqNr: Long): Unit = ()
  override def consumerChangedProducer(producerId: String): Unit = ()
  override def consumerStashFull(producerId: String, seqNr: Long): Unit = ()

}
