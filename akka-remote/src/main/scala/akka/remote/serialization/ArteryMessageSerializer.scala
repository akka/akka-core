/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import java.io.NotSerializableException

import akka.actor.{ ActorRef, Address, ExtendedActorSystem }
import akka.protobufv3.internal.MessageLite
import akka.remote._
import akka.remote.RemoteWatcher.ArteryHeartbeatRsp
import akka.remote.artery.{ ActorSystemTerminating, ActorSystemTerminatingAck, Quarantined, SystemMessageDelivery }
import akka.remote.artery.Flush
import akka.remote.artery.FlushAck
import akka.remote.artery.OutboundHandshake.{ HandshakeReq, HandshakeRsp }
import akka.remote.artery.compress.{ CompressionProtocol, CompressionTable }
import akka.remote.artery.compress.CompressionProtocol._
import akka.serialization.{ BaseSerializer, Serialization, SerializationExtension, SerializerWithStringManifest }

/** INTERNAL API */
private[akka] object ArteryMessageSerializer {
  private val QuarantinedManifest = "a"
  private val ActorSystemTerminatingManifest = "b"
  private val ActorSystemTerminatingAckManifest = "c"
  private val HandshakeReqManifest = "d"
  private val HandshakeRspManifest = "e"
  private val ActorRefCompressionAdvertisementManifest = "f"
  private val ActorRefCompressionAdvertisementAckManifest = "g"
  private val ClassManifestCompressionAdvertisementManifest = "h"
  private val ClassManifestCompressionAdvertisementAckManifest = "i"
  private val SystemMessageEnvelopeManifest = "j"
  private val SystemMessageDeliveryAckManifest = "k"
  private val SystemMessageDeliveryNackManifest = "l"

  private val ArteryHeartbeatManifest = "m"
  private val ArteryHeartbeatRspManifest = "n"

  private val FlushManifest = "o"
  private val FlushAckManifest = "p"

  private final val DeadLettersRepresentation = ""
}

/** INTERNAL API */
private[akka] final class ArteryMessageSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {
  import ArteryMessageSerializer._

  private lazy val serialization = SerializationExtension(system)

  override def manifest(o: AnyRef): String = o match { // most frequent ones first
    case _: SystemMessageDelivery.SystemMessageEnvelope               => SystemMessageEnvelopeManifest
    case _: SystemMessageDelivery.Ack                                 => SystemMessageDeliveryAckManifest
    case _: HandshakeReq                                              => HandshakeReqManifest
    case _: HandshakeRsp                                              => HandshakeRspManifest
    case _: RemoteWatcher.ArteryHeartbeat.type                        => ArteryHeartbeatManifest
    case _: RemoteWatcher.ArteryHeartbeatRsp                          => ArteryHeartbeatRspManifest
    case _: SystemMessageDelivery.Nack                                => SystemMessageDeliveryNackManifest
    case _: Quarantined                                               => QuarantinedManifest
    case Flush                                                        => FlushManifest
    case _: FlushAck                                                  => FlushAckManifest
    case _: ActorSystemTerminating                                    => ActorSystemTerminatingManifest
    case _: ActorSystemTerminatingAck                                 => ActorSystemTerminatingAckManifest
    case _: CompressionProtocol.ActorRefCompressionAdvertisement      => ActorRefCompressionAdvertisementManifest
    case _: CompressionProtocol.ActorRefCompressionAdvertisementAck   => ActorRefCompressionAdvertisementAckManifest
    case _: CompressionProtocol.ClassManifestCompressionAdvertisement => ClassManifestCompressionAdvertisementManifest
    case _: CompressionProtocol.ClassManifestCompressionAdvertisementAck =>
      ClassManifestCompressionAdvertisementAckManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match { // most frequent ones first
    case env: SystemMessageDelivery.SystemMessageEnvelope => serializeSystemMessageEnvelope(env).toByteArray
    case SystemMessageDelivery.Ack(seqNo, from)           => serializeSystemMessageDeliveryAck(seqNo, from).toByteArray
    case HandshakeReq(from, to)                           => serializeHandshakeReq(from, to).toByteArray
    case HandshakeRsp(from)                               => serializeWithAddress(from).toByteArray
    case RemoteWatcher.ArteryHeartbeat                    => Array.emptyByteArray
    case RemoteWatcher.ArteryHeartbeatRsp(from)           => serializeArteryHeartbeatRsp(from).toByteArray
    case SystemMessageDelivery.Nack(seqNo, from)          => serializeSystemMessageDeliveryAck(seqNo, from).toByteArray
    case q: Quarantined                                   => serializeQuarantined(q).toByteArray
    case Flush                                            => Array.emptyByteArray
    case FlushAck(expectedAcks)                           => serializeFlushAck(expectedAcks).toByteArray
    case ActorSystemTerminating(from)                     => serializeWithAddress(from).toByteArray
    case ActorSystemTerminatingAck(from)                  => serializeWithAddress(from).toByteArray
    case adv: ActorRefCompressionAdvertisement            => serializeActorRefCompressionAdvertisement(adv).toByteArray
    case ActorRefCompressionAdvertisementAck(from, id) =>
      serializeCompressionTableAdvertisementAck(from, id).toByteArray
    case adv: ClassManifestCompressionAdvertisement => serializeCompressionAdvertisement(adv)(identity).toByteArray
    case ClassManifestCompressionAdvertisementAck(from, id) =>
      serializeCompressionTableAdvertisementAck(from, id).toByteArray
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match { // most frequent ones first (could be made a HashMap in the future)
      case SystemMessageEnvelopeManifest => deserializeSystemMessageEnvelope(bytes)
      case SystemMessageDeliveryAckManifest =>
        deserializeSystemMessageDeliveryAck(bytes, SystemMessageDelivery.Ack.apply)
      case HandshakeReqManifest => deserializeHandshakeReq(bytes, HandshakeReq.apply)
      case HandshakeRspManifest => deserializeWithFromAddress(bytes, HandshakeRsp.apply)
      case SystemMessageDeliveryNackManifest =>
        deserializeSystemMessageDeliveryAck(bytes, SystemMessageDelivery.Nack.apply)
      case QuarantinedManifest                      => deserializeQuarantined(ArteryControlFormats.Quarantined.parseFrom(bytes))
      case FlushManifest                            => Flush
      case FlushAckManifest                         => deserializeFlushAck(bytes)
      case ActorSystemTerminatingManifest           => deserializeWithFromAddress(bytes, ActorSystemTerminating.apply)
      case ActorSystemTerminatingAckManifest        => deserializeWithFromAddress(bytes, ActorSystemTerminatingAck.apply)
      case ActorRefCompressionAdvertisementManifest => deserializeActorRefCompressionAdvertisement(bytes)
      case ActorRefCompressionAdvertisementAckManifest =>
        deserializeCompressionTableAdvertisementAck(bytes, ActorRefCompressionAdvertisementAck.apply)
      case ClassManifestCompressionAdvertisementManifest =>
        deserializeCompressionAdvertisement(bytes, identity, ClassManifestCompressionAdvertisement.apply)
      case ClassManifestCompressionAdvertisementAckManifest =>
        deserializeCompressionTableAdvertisementAck(bytes, ClassManifestCompressionAdvertisementAck.apply)
      case ArteryHeartbeatManifest    => RemoteWatcher.ArteryHeartbeat
      case ArteryHeartbeatRspManifest => deserializeArteryHeartbeatRsp(bytes, ArteryHeartbeatRsp.apply)
      case _ =>
        throw new NotSerializableException(
          s"Manifest '$manifest' not defined for ArteryControlMessageSerializer (serializer id $identifier)")
    }

  import scala.jdk.CollectionConverters._

  def serializeQuarantined(quarantined: Quarantined): ArteryControlFormats.Quarantined =
    ArteryControlFormats.Quarantined
      .newBuilder()
      .setFrom(serializeUniqueAddress(quarantined.from))
      .setTo(serializeUniqueAddress(quarantined.to))
      .build

  def deserializeQuarantined(quarantined: ArteryControlFormats.Quarantined): Quarantined =
    Quarantined(deserializeUniqueAddress(quarantined.getFrom), deserializeUniqueAddress(quarantined.getTo))

  def serializeActorRef(ref: ActorRef): String =
    if ((ref eq ActorRef.noSender) || (ref eq system.deadLetters)) DeadLettersRepresentation
    else Serialization.serializedActorPath(ref)

  def deserializeActorRef(str: String): ActorRef =
    if (str == DeadLettersRepresentation) system.deadLetters
    else system.provider.resolveActorRef(str)

  def serializeActorRefCompressionAdvertisement(
      adv: ActorRefCompressionAdvertisement): ArteryControlFormats.CompressionTableAdvertisement =
    serializeCompressionAdvertisement(adv)(serializeActorRef)

  def deserializeActorRefCompressionAdvertisement(bytes: Array[Byte]): ActorRefCompressionAdvertisement =
    deserializeCompressionAdvertisement(bytes, deserializeActorRef, ActorRefCompressionAdvertisement.apply)

  def serializeCompressionAdvertisement[T](adv: CompressionAdvertisement[T])(
      keySerializer: T => String): ArteryControlFormats.CompressionTableAdvertisement = {
    val builder =
      ArteryControlFormats.CompressionTableAdvertisement.newBuilder
        .setFrom(serializeUniqueAddress(adv.from))
        .setOriginUid(adv.table.originUid)
        .setTableVersion(adv.table.version)

    adv.table.dictionary.foreach {
      case (key, value) =>
        builder.addKeys(keySerializer(key)).addValues(value)
    }

    builder.build
  }

  def deserializeCompressionAdvertisement[T, U](
      bytes: Array[Byte],
      keyDeserializer: String => T,
      create: (UniqueAddress, CompressionTable[T]) => U): U = {
    val protoAdv = ArteryControlFormats.CompressionTableAdvertisement.parseFrom(bytes)

    val kvs =
      protoAdv.getKeysList.asScala
        .map(keyDeserializer)
        .zip(protoAdv.getValuesList.asScala.asInstanceOf[Iterable[Int]] /* to avoid having to call toInt explicitly */ )

    val table = CompressionTable[T](protoAdv.getOriginUid, protoAdv.getTableVersion.byteValue, kvs.toMap)
    create(deserializeUniqueAddress(protoAdv.getFrom), table)
  }

  def serializeCompressionTableAdvertisementAck(from: UniqueAddress, version: Int): MessageLite =
    ArteryControlFormats.CompressionTableAdvertisementAck.newBuilder
      .setFrom(serializeUniqueAddress(from))
      .setVersion(version)
      .build()

  def deserializeCompressionTableAdvertisementAck(
      bytes: Array[Byte],
      create: (UniqueAddress, Byte) => AnyRef): AnyRef = {
    val msg = ArteryControlFormats.CompressionTableAdvertisementAck.parseFrom(bytes)
    create(deserializeUniqueAddress(msg.getFrom), msg.getVersion.toByte)
  }

  def serializeSystemMessageEnvelope(
      env: SystemMessageDelivery.SystemMessageEnvelope): ArteryControlFormats.SystemMessageEnvelope = {
    val msg = MessageSerializer.serialize(system, env.message)

    val builder =
      ArteryControlFormats.SystemMessageEnvelope.newBuilder
        .setMessage(msg.getMessage)
        .setSerializerId(msg.getSerializerId)
        .setSeqNo(env.seqNo)
        .setAckReplyTo(serializeUniqueAddress(env.ackReplyTo))

    if (msg.hasMessageManifest) builder.setMessageManifest(msg.getMessageManifest)

    builder.build
  }
  def deserializeSystemMessageEnvelope(bytes: Array[Byte]): SystemMessageDelivery.SystemMessageEnvelope = {
    val protoEnv = ArteryControlFormats.SystemMessageEnvelope.parseFrom(bytes)

    SystemMessageDelivery.SystemMessageEnvelope(
      serialization
        .deserialize(
          protoEnv.getMessage.toByteArray,
          protoEnv.getSerializerId,
          if (protoEnv.hasMessageManifest) protoEnv.getMessageManifest.toStringUtf8 else "")
        .get,
      protoEnv.getSeqNo,
      deserializeUniqueAddress(protoEnv.getAckReplyTo))
  }

  def serializeSystemMessageDeliveryAck(
      seqNo: Long,
      from: UniqueAddress): ArteryControlFormats.SystemMessageDeliveryAck =
    ArteryControlFormats.SystemMessageDeliveryAck.newBuilder.setSeqNo(seqNo).setFrom(serializeUniqueAddress(from)).build

  def deserializeSystemMessageDeliveryAck(bytes: Array[Byte], create: (Long, UniqueAddress) => AnyRef): AnyRef = {
    val protoAck = ArteryControlFormats.SystemMessageDeliveryAck.parseFrom(bytes)

    create(protoAck.getSeqNo, deserializeUniqueAddress(protoAck.getFrom))
  }

  def serializeWithAddress(from: UniqueAddress): MessageLite =
    ArteryControlFormats.MessageWithAddress.newBuilder.setAddress(serializeUniqueAddress(from)).build()

  def deserializeWithFromAddress(bytes: Array[Byte], create: UniqueAddress => AnyRef): AnyRef =
    create(deserializeUniqueAddress(ArteryControlFormats.MessageWithAddress.parseFrom(bytes).getAddress))

  def serializeHandshakeReq(from: UniqueAddress, to: Address): MessageLite =
    ArteryControlFormats.HandshakeReq.newBuilder
      .setFrom(serializeUniqueAddress(from))
      .setTo(serializeAddress(to))
      .build()

  def deserializeHandshakeReq(bytes: Array[Byte], create: (UniqueAddress, Address) => HandshakeReq): HandshakeReq = {
    val protoEnv = ArteryControlFormats.HandshakeReq.parseFrom(bytes)
    create(deserializeUniqueAddress(protoEnv.getFrom), deserializeAddress(protoEnv.getTo))
  }

  def serializeUniqueAddress(address: UniqueAddress): ArteryControlFormats.UniqueAddress =
    ArteryControlFormats.UniqueAddress
      .newBuilder()
      .setAddress(serializeAddress(address.address))
      .setUid(address.uid)
      .build()

  def deserializeUniqueAddress(address: ArteryControlFormats.UniqueAddress): UniqueAddress =
    UniqueAddress(deserializeAddress(address.getAddress), address.getUid)

  def serializeAddress(address: Address): ArteryControlFormats.Address =
    address match {
      case Address(protocol, system, Some(host), Some(port)) =>
        ArteryControlFormats.Address
          .newBuilder()
          .setProtocol(protocol)
          .setSystem(system)
          .setHostname(host)
          .setPort(port)
          .build()
      case _ => throw new IllegalArgumentException(s"Address [$address] could not be serialized: host or port missing.")
    }

  def deserializeAddress(address: ArteryControlFormats.Address): Address =
    Address(address.getProtocol, address.getSystem, address.getHostname, address.getPort)

  def serializeArteryHeartbeatRsp(uid: Long): ArteryControlFormats.ArteryHeartbeatRsp =
    ArteryControlFormats.ArteryHeartbeatRsp.newBuilder().setUid(uid).build()

  def deserializeArteryHeartbeatRsp(bytes: Array[Byte], create: Long => ArteryHeartbeatRsp): ArteryHeartbeatRsp = {
    val msg = ArteryControlFormats.ArteryHeartbeatRsp.parseFrom(bytes)
    create(msg.getUid)
  }

  def serializeFlushAck(expectedAcks: Int): ArteryControlFormats.FlushAck =
    ArteryControlFormats.FlushAck.newBuilder().setExpectedAcks(expectedAcks).build()

  def deserializeFlushAck(bytes: Array[Byte]): FlushAck = {
    if (bytes.isEmpty)
      FlushAck(1) // for wire compatibility with Akka 2.6.16 or earlier, which didn't include the expectedAcks
    else {
      val msg = ArteryControlFormats.FlushAck.parseFrom(bytes)
      val expectedAcks = if (msg.hasExpectedAcks) msg.getExpectedAcks else 1
      FlushAck(expectedAcks)
    }
  }
}
