/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed.internal.delivery

import java.io.NotSerializableException

import akka.actor.typed.ActorRefResolver
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.DurableProducerQueue
import akka.actor.typed.delivery.ProducerController
import akka.actor.typed.delivery.internal.ChunkedMessage
import akka.actor.typed.delivery.internal.ProducerControllerImpl
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.cluster.typed.internal.protobuf.ReliableDelivery
import akka.cluster.typed.internal.protobuf.ReliableDelivery.Confirmed
import akka.protobufv3.internal.ByteString
import akka.remote.ByteStringUtils
import akka.remote.ContainerFormats
import akka.remote.ContainerFormats.Payload
import akka.remote.serialization.WrappedPayloadSupport
import akka.serialization.BaseSerializer
import akka.serialization.SerializerWithStringManifest
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ReliableDeliverySerializer(val system: akka.actor.ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  private val payloadSupport = new WrappedPayloadSupport(system)
  // lazy because Serializers are initialized early on. `toTyped` might then try to
  // initialize the classic ActorSystemAdapter extension.
  private lazy val resolver = ActorRefResolver(system.toTyped)

  private val SequencedMessageManifest = "a"
  private val AckManifest = "b"
  private val RequestManifest = "c"
  private val ResendManifest = "d"
  private val RegisterConsumerManifest = "e"

  private val DurableQueueMessageSentManifest = "f"
  private val DurableQueueConfirmedManifest = "g"
  private val DurableQueueStateManifest = "h"
  private val DurableQueueCleanupManifest = "i"

  override def manifest(o: AnyRef): String = o match {
    case _: ConsumerController.SequencedMessage[_] => SequencedMessageManifest
    case _: ProducerControllerImpl.Ack             => AckManifest
    case _: ProducerControllerImpl.Request         => RequestManifest
    case _: ProducerControllerImpl.Resend          => ResendManifest
    case _: ProducerController.RegisterConsumer[_] => RegisterConsumerManifest
    case _: DurableProducerQueue.MessageSent[_]    => DurableQueueMessageSentManifest
    case _: DurableProducerQueue.Confirmed         => DurableQueueConfirmedManifest
    case _: DurableProducerQueue.State[_]          => DurableQueueStateManifest
    case _: DurableProducerQueue.Cleanup           => DurableQueueCleanupManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case m: ConsumerController.SequencedMessage[_] => sequencedMessageToBinary(m)
    case m: ProducerControllerImpl.Ack             => ackToBinary(m)
    case m: ProducerControllerImpl.Request         => requestToBinary(m)
    case m: ProducerControllerImpl.Resend          => resendToBinary(m)
    case m: ProducerController.RegisterConsumer[_] => registerConsumerToBinary(m)
    case m: DurableProducerQueue.MessageSent[_]    => durableQueueMessageSentToBinary(m)
    case m: DurableProducerQueue.Confirmed         => durableQueueConfirmedToBinary(m)
    case m: DurableProducerQueue.State[_]          => durableQueueStateToBinary(m)
    case m: DurableProducerQueue.Cleanup           => durableQueueCleanupToBinary(m)
    case _ =>
      throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
  }

  private def sequencedMessageToBinary(m: ConsumerController.SequencedMessage[_]): Array[Byte] = {
    val b = ReliableDelivery.SequencedMessage.newBuilder()
    b.setProducerId(m.producerId)
    b.setSeqNr(m.seqNr)
    b.setFirst(m.first)
    b.setAck(m.ack)
    b.setProducerControllerRef(resolver.toSerializationFormat(m.producerController))

    m.message match {
      case chunk: ChunkedMessage =>
        b.setMessage(chunkedMessageToProto(chunk))
        b.setFirstChunk(chunk.firstChunk)
        b.setLastChunk(chunk.lastChunk)
      case _ =>
        b.setMessage(payloadSupport.payloadBuilder(m.message))
    }

    b.build().toByteArray()
  }

  private def chunkedMessageToProto(chunk: ChunkedMessage): Payload.Builder = {
    val payloadBuilder = ContainerFormats.Payload.newBuilder()
    payloadBuilder.setEnclosedMessage(ByteStringUtils.toProtoByteStringUnsafe(chunk.serialized.toArrayUnsafe()))
    payloadBuilder.setMessageManifest(ByteString.copyFromUtf8(chunk.manifest))
    payloadBuilder.setSerializerId(chunk.serializerId)
    payloadBuilder
  }

  private def ackToBinary(m: ProducerControllerImpl.Ack): Array[Byte] = {
    val b = ReliableDelivery.Ack.newBuilder()
    b.setConfirmedSeqNr(m.confirmedSeqNr)
    b.build().toByteArray()
  }

  private def requestToBinary(m: ProducerControllerImpl.Request): Array[Byte] = {
    val b = ReliableDelivery.Request.newBuilder()
    b.setConfirmedSeqNr(m.confirmedSeqNr)
    b.setRequestUpToSeqNr(m.requestUpToSeqNr)
    b.setSupportResend(m.supportResend)
    b.setViaTimeout(m.viaTimeout)
    b.build().toByteArray()
  }

  private def resendToBinary(m: ProducerControllerImpl.Resend): Array[Byte] = {
    val b = ReliableDelivery.Resend.newBuilder()
    b.setFromSeqNr(m.fromSeqNr)
    b.build().toByteArray()
  }

  private def registerConsumerToBinary(m: ProducerController.RegisterConsumer[_]): Array[Byte] = {
    val b = ReliableDelivery.RegisterConsumer.newBuilder()
    b.setConsumerControllerRef(resolver.toSerializationFormat(m.consumerController))
    b.build().toByteArray()
  }

  private def durableQueueMessageSentToBinary(m: DurableProducerQueue.MessageSent[_]): Array[Byte] = {
    durableQueueMessageSentToProto(m).toByteArray()
  }

  private def durableQueueMessageSentToProto(m: DurableProducerQueue.MessageSent[_]): ReliableDelivery.MessageSent = {
    val b = ReliableDelivery.MessageSent.newBuilder()
    b.setSeqNr(m.seqNr)
    b.setQualifier(m.confirmationQualifier)
    b.setAck(m.ack)
    b.setTimestamp(m.timestampMillis)

    m.message match {
      case chunk: ChunkedMessage =>
        b.setMessage(chunkedMessageToProto(chunk))
        b.setFirstChunk(chunk.firstChunk)
        b.setLastChunk(chunk.lastChunk)
      case _ =>
        b.setMessage(payloadSupport.payloadBuilder(m.message))
    }

    b.build()
  }

  private def durableQueueConfirmedToBinary(m: DurableProducerQueue.Confirmed): _root_.scala.Array[Byte] = {
    durableQueueConfirmedToProto(m.confirmationQualifier, m.seqNr, m.timestampMillis).toByteArray()
  }

  private def durableQueueConfirmedToProto(
      qualifier: String,
      seqNr: DurableProducerQueue.SeqNr,
      timestampMillis: DurableProducerQueue.TimestampMillis): Confirmed = {
    val b = ReliableDelivery.Confirmed.newBuilder()
    b.setSeqNr(seqNr)
    b.setQualifier(qualifier)
    b.setTimestamp(timestampMillis)
    b.build()
  }

  private def durableQueueStateToBinary(m: DurableProducerQueue.State[_]): Array[Byte] = {
    val b = ReliableDelivery.State.newBuilder()
    b.setCurrentSeqNr(m.currentSeqNr)
    b.setHighestConfirmedSeqNr(m.highestConfirmedSeqNr)
    b.addAllConfirmed(m.confirmedSeqNr.map {
      case (qualifier, (seqNr, timestamp)) => durableQueueConfirmedToProto(qualifier, seqNr, timestamp)
    }.asJava)
    b.addAllUnconfirmed(m.unconfirmed.map(durableQueueMessageSentToProto).asJava)
    b.build().toByteArray()
  }

  private def durableQueueCleanupToBinary(m: DurableProducerQueue.Cleanup): Array[Byte] = {
    val b = ReliableDelivery.Cleanup.newBuilder()
    b.addAllQualifiers(m.confirmationQualifiers.asJava)
    b.build().toByteArray()
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case SequencedMessageManifest        => sequencedMessageFromBinary(bytes)
    case AckManifest                     => ackFromBinary(bytes)
    case RequestManifest                 => requestFromBinary(bytes)
    case ResendManifest                  => resendFromBinary(bytes)
    case RegisterConsumerManifest        => registerConsumerFromBinary(bytes)
    case DurableQueueMessageSentManifest => durableQueueMessageSentFromBinary(bytes)
    case DurableQueueConfirmedManifest   => durableQueueConfirmedFromBinary(bytes)
    case DurableQueueStateManifest       => durableQueueStateFromBinary(bytes)
    case DurableQueueCleanupManifest     => durableQueueCleanupFromBinary(bytes)
    case _ =>
      throw new NotSerializableException(
        s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]")
  }

  private def sequencedMessageFromBinary(bytes: Array[Byte]): AnyRef = {
    val seqMsg = ReliableDelivery.SequencedMessage.parseFrom(bytes)
    val wrappedMsg =
      if (seqMsg.hasFirstChunk) {
        val manifest =
          if (seqMsg.getMessage.hasMessageManifest) seqMsg.getMessage.getMessageManifest.toStringUtf8 else ""
        ChunkedMessage(
          akka.util.ByteString.fromArrayUnsafe(seqMsg.getMessage.getEnclosedMessage.toByteArray),
          seqMsg.getFirstChunk,
          seqMsg.getLastChunk,
          seqMsg.getMessage.getSerializerId,
          manifest)
      } else {
        payloadSupport.deserializePayload(seqMsg.getMessage)
      }
    ConsumerController.SequencedMessage(
      seqMsg.getProducerId,
      seqMsg.getSeqNr,
      wrappedMsg,
      seqMsg.getFirst,
      seqMsg.getAck)(resolver.resolveActorRef(seqMsg.getProducerControllerRef))
  }

  private def ackFromBinary(bytes: Array[Byte]): AnyRef = {
    val ack = ReliableDelivery.Ack.parseFrom(bytes)
    ProducerControllerImpl.Ack(ack.getConfirmedSeqNr)
  }

  private def requestFromBinary(bytes: Array[Byte]): AnyRef = {
    val req = ReliableDelivery.Request.parseFrom(bytes)
    ProducerControllerImpl.Request(
      req.getConfirmedSeqNr,
      req.getRequestUpToSeqNr,
      req.getSupportResend,
      req.getViaTimeout)
  }

  private def resendFromBinary(bytes: Array[Byte]): AnyRef = {
    val resend = ReliableDelivery.Resend.parseFrom(bytes)
    ProducerControllerImpl.Resend(resend.getFromSeqNr)
  }

  private def registerConsumerFromBinary(bytes: Array[Byte]): AnyRef = {
    val reg = ReliableDelivery.RegisterConsumer.parseFrom(bytes)
    ProducerController.RegisterConsumer(
      resolver.resolveActorRef[ConsumerController.Command[Any]](reg.getConsumerControllerRef))
  }

  private def durableQueueMessageSentFromBinary(bytes: Array[Byte]): AnyRef = {
    val sent = ReliableDelivery.MessageSent.parseFrom(bytes)
    durableQueueMessageSentFromProto(sent)
  }

  private def durableQueueMessageSentFromProto(
      sent: ReliableDelivery.MessageSent): DurableProducerQueue.MessageSent[Any] = {
    val wrappedMsg =
      if (sent.hasFirstChunk) {
        val manifest =
          if (sent.getMessage.hasMessageManifest) sent.getMessage.getMessageManifest.toStringUtf8 else ""
        ChunkedMessage(
          akka.util.ByteString.fromArrayUnsafe(sent.getMessage.getEnclosedMessage.toByteArray),
          sent.getFirstChunk,
          sent.getLastChunk,
          sent.getMessage.getSerializerId,
          manifest)
      } else {
        payloadSupport.deserializePayload(sent.getMessage)
      }
    DurableProducerQueue.MessageSent(sent.getSeqNr, wrappedMsg, sent.getAck, sent.getQualifier, sent.getTimestamp)
  }

  private def durableQueueConfirmedFromBinary(bytes: Array[Byte]): AnyRef = {
    val confirmed = ReliableDelivery.Confirmed.parseFrom(bytes)
    DurableProducerQueue.Confirmed(confirmed.getSeqNr, confirmed.getQualifier, confirmed.getTimestamp)
  }

  private def durableQueueStateFromBinary(bytes: Array[Byte]): AnyRef = {
    val state = ReliableDelivery.State.parseFrom(bytes)
    DurableProducerQueue.State(
      state.getCurrentSeqNr,
      state.getHighestConfirmedSeqNr,
      state.getConfirmedList.asScala
        .map(confirmed => confirmed.getQualifier -> (confirmed.getSeqNr -> confirmed.getTimestamp))
        .toMap,
      state.getUnconfirmedList.asScala.toVector.map(durableQueueMessageSentFromProto))
  }

  private def durableQueueCleanupFromBinary(bytes: Array[Byte]): AnyRef = {
    val cleanup = ReliableDelivery.Cleanup.parseFrom(bytes)
    DurableProducerQueue.Cleanup(cleanup.getQualifiersList.iterator.asScala.toSet)
  }

}
