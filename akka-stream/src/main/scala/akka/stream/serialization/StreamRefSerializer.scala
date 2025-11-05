/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.serialization

import java.nio.charset.StandardCharsets

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.protobufv3.internal.ByteString
import akka.protobufv3.internal.UnsafeByteOperations
import akka.serialization._
import akka.stream.StreamRefMessages
import akka.stream.impl.streamref._

/** INTERNAL API */
@InternalApi
private[akka] final class StreamRefSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  private[this] lazy val serialization = SerializationExtension(system)

  private[this] val SequencedOnNextManifest = "A"
  private[this] val CumulativeDemandManifest = "B"
  private[this] val RemoteSinkFailureManifest = "C"
  private[this] val RemoteSinkCompletedManifest = "D"
  private[this] val SourceRefManifest = "E"
  private[this] val SinkRefManifest = "F"
  private[this] val OnSubscribeHandshakeManifest = "G"
  private[this] val AckManifest = "H"

  override def manifest(o: AnyRef): String = o match {
    // protocol
    case _: StreamRefsProtocol.SequencedOnNext[_] => SequencedOnNextManifest
    case _: StreamRefsProtocol.CumulativeDemand   => CumulativeDemandManifest
    // handshake
    case _: StreamRefsProtocol.OnSubscribeHandshake => OnSubscribeHandshakeManifest
    // completion
    case _: StreamRefsProtocol.RemoteStreamFailure   => RemoteSinkFailureManifest
    case _: StreamRefsProtocol.RemoteStreamCompleted => RemoteSinkCompletedManifest
    // refs
    case _: SourceRefImpl[_] => SourceRefManifest
    //    case _: MaterializedSourceRef[_]                 => SourceRefManifest
    case _: SinkRefImpl[_] => SinkRefManifest
    //    case _: MaterializedSinkRef[_]                   => SinkRefManifest
    case StreamRefsProtocol.Ack => AckManifest
    case unknown                => throw new IllegalArgumentException(s"Unsupported object ${unknown.getClass}")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    // protocol
    case o: StreamRefsProtocol.SequencedOnNext[_] => serializeSequencedOnNext(o).toByteArray
    case d: StreamRefsProtocol.CumulativeDemand   => serializeCumulativeDemand(d).toByteArray
    // handshake
    case h: StreamRefsProtocol.OnSubscribeHandshake => serializeOnSubscribeHandshake(h).toByteArray
    // termination
    case d: StreamRefsProtocol.RemoteStreamFailure   => serializeRemoteSinkFailure(d).toByteArray
    case d: StreamRefsProtocol.RemoteStreamCompleted => serializeRemoteSinkCompleted(d).toByteArray
    // refs
    case ref: SinkRefImpl[_] => serializeSinkRef(ref).toByteArray
    //    case ref: MaterializedSinkRef[_]                 => ??? // serializeSinkRef(ref).toByteArray
    case ref: SourceRefImpl[_] => serializeSourceRef(ref).toByteArray
    //    case ref: MaterializedSourceRef[_]               => serializeSourceRef(ref.).toByteArray
    case StreamRefsProtocol.Ack => Array.emptyByteArray
    case unknown                => throw new IllegalArgumentException(s"Unsupported object ${unknown.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    // protocol
    case OnSubscribeHandshakeManifest => deserializeOnSubscribeHandshake(bytes)
    case SequencedOnNextManifest      => deserializeSequencedOnNext(bytes)
    case CumulativeDemandManifest     => deserializeCumulativeDemand(bytes)
    case RemoteSinkCompletedManifest  => deserializeRemoteStreamCompleted(bytes)
    case RemoteSinkFailureManifest    => deserializeRemoteStreamFailure(bytes)
    // refs
    case SinkRefManifest   => deserializeSinkRef(bytes)
    case SourceRefManifest => deserializeSourceRef(bytes)
    case AckManifest       => StreamRefsProtocol.Ack
    case unknown           => throw new IllegalArgumentException(s"Unsupported manifest '$unknown''")
  }

  // -----

  private def serializeCumulativeDemand(d: StreamRefsProtocol.CumulativeDemand): StreamRefMessages.CumulativeDemand = {
    StreamRefMessages.CumulativeDemand.newBuilder().setSeqNr(d.seqNr).build()
  }

  private def serializeRemoteSinkFailure(
      d: StreamRefsProtocol.RemoteStreamFailure): StreamRefMessages.RemoteStreamFailure = {
    StreamRefMessages.RemoteStreamFailure
      .newBuilder()
      .setCause {
        val msg = Option(d.msg).getOrElse(d.getClass.getName)
        UnsafeByteOperations.unsafeWrap(msg.getBytes(StandardCharsets.UTF_8))
      }
      .build()
  }

  private def serializeRemoteSinkCompleted(
      d: StreamRefsProtocol.RemoteStreamCompleted): StreamRefMessages.RemoteStreamCompleted = {
    StreamRefMessages.RemoteStreamCompleted.newBuilder().setSeqNr(d.seqNr).build()
  }

  private def serializeOnSubscribeHandshake(
      o: StreamRefsProtocol.OnSubscribeHandshake): StreamRefMessages.OnSubscribeHandshake = {
    StreamRefMessages.OnSubscribeHandshake
      .newBuilder()
      .setTargetRef(StreamRefMessages.ActorRef.newBuilder().setPath(Serialization.serializedActorPath(o.targetRef)))
      .build()
  }

  private def serializeSequencedOnNext(o: StreamRefsProtocol.SequencedOnNext[_]) = {
    val p = o.payload.asInstanceOf[AnyRef]
    val msgSerializer = serialization.findSerializerFor(p)

    val payloadBuilder = StreamRefMessages.Payload
      .newBuilder()
      .setEnclosedMessage(UnsafeByteOperations.unsafeWrap(msgSerializer.toBinary(p)))
      .setSerializerId(msgSerializer.identifier)

    val ms = Serializers.manifestFor(msgSerializer, p)
    if (ms.nonEmpty) payloadBuilder.setMessageManifest(ByteString.copyFromUtf8(ms))

    StreamRefMessages.SequencedOnNext.newBuilder().setSeqNr(o.seqNr).setPayload(payloadBuilder.build()).build()
  }

  private def serializeSinkRef(sink: SinkRefImpl[_]): StreamRefMessages.SinkRef = {
    StreamRefMessages.SinkRef
      .newBuilder()
      .setTargetRef(
        StreamRefMessages.ActorRef.newBuilder().setPath(Serialization.serializedActorPath(sink.initialPartnerRef)))
      .build()
  }

  private def serializeSourceRef(source: SourceRefImpl[_]): StreamRefMessages.SourceRef = {
    StreamRefMessages.SourceRef
      .newBuilder()
      .setOriginRef(
        StreamRefMessages.ActorRef.newBuilder().setPath(Serialization.serializedActorPath(source.initialPartnerRef)))
      .build()
  }

  // ----------

  private def deserializeOnSubscribeHandshake(bytes: Array[Byte]): StreamRefsProtocol.OnSubscribeHandshake = {
    val handshake = StreamRefMessages.OnSubscribeHandshake.parseFrom(bytes)
    val targetRef = serialization.system.provider.resolveActorRef(handshake.getTargetRef.getPath)
    StreamRefsProtocol.OnSubscribeHandshake(targetRef)
  }

  private def deserializeSinkRef(bytes: Array[Byte]): SinkRefImpl[Any] = {
    val ref = StreamRefMessages.SinkRef.parseFrom(bytes)
    val initialTargetRef = serialization.system.provider.resolveActorRef(ref.getTargetRef.getPath)

    SinkRefImpl[Any](initialTargetRef)
  }

  private def deserializeSourceRef(bytes: Array[Byte]): SourceRefImpl[Any] = {
    val ref = StreamRefMessages.SourceRef.parseFrom(bytes)
    val initialPartnerRef = serialization.system.provider.resolveActorRef(ref.getOriginRef.getPath)

    SourceRefImpl[Any](initialPartnerRef)
  }

  private def deserializeSequencedOnNext(bytes: Array[Byte]): StreamRefsProtocol.SequencedOnNext[AnyRef] = {
    val o = StreamRefMessages.SequencedOnNext.parseFrom(bytes)
    val p = o.getPayload
    val payload =
      serialization.deserialize(p.getEnclosedMessage.toByteArray, p.getSerializerId, p.getMessageManifest.toStringUtf8)
    StreamRefsProtocol.SequencedOnNext(o.getSeqNr, payload.get)
  }

  private def deserializeCumulativeDemand(bytes: Array[Byte]): StreamRefsProtocol.CumulativeDemand = {
    val d = StreamRefMessages.CumulativeDemand.parseFrom(bytes)
    StreamRefsProtocol.CumulativeDemand(d.getSeqNr)
  }

  private def deserializeRemoteStreamCompleted(bytes: Array[Byte]): StreamRefsProtocol.RemoteStreamCompleted = {
    val d = StreamRefMessages.RemoteStreamCompleted.parseFrom(bytes)
    StreamRefsProtocol.RemoteStreamCompleted(d.getSeqNr)
  }

  private def deserializeRemoteStreamFailure(bytes: Array[Byte]): AnyRef = {
    val d = StreamRefMessages.RemoteStreamFailure.parseFrom(bytes)
    StreamRefsProtocol.RemoteStreamFailure(d.getCause.toStringUtf8)
  }

}
