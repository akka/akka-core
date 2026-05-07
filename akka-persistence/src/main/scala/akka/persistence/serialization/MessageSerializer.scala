/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.serialization

import java.io.NotSerializableException
import scala.collection.immutable
import scala.collection.immutable.VectorBuilder
import akka.actor.{ ActorPath, ExtendedActorSystem }
import akka.actor.Actor
import akka.persistence._
import akka.persistence.AtLeastOnceDelivery._
import akka.persistence.fsm.PersistentFSM.{ PersistentFSMSnapshot, StateChangeEvent }
import akka.persistence.serialization.{ MessageFormats => mf }
import akka.protobufv3.internal.ByteString
import akka.protobufv3.internal.UnsafeByteOperations
import akka.serialization._

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

/**
 * Marker trait for all protobuf-serializable messages in `akka.persistence`.
 */
trait Message extends Serializable

/**
 * Protobuf serializer for [[akka.persistence.PersistentRepr]], [[akka.persistence.AtLeastOnceDelivery]] and [[akka.persistence.fsm.PersistentFSM.StateChangeEvent]] messages.
 */
class MessageSerializer(val system: ExtendedActorSystem) extends BaseSerializer {
  import PersistentRepr.Undefined

  val AtomicWriteClass = classOf[AtomicWrite]
  val PersistentReprClass = classOf[PersistentRepr]
  val PersistentImplClass = classOf[PersistentImpl]
  val AtLeastOnceDeliverySnapshotClass = classOf[AtLeastOnceDeliverySnapshot]
  val PersistentStateChangeEventClass = classOf[StateChangeEvent]
  val PersistentFSMSnapshotClass = classOf[PersistentFSMSnapshot[Any]]

  private lazy val serialization = SerializationExtension(system)

  override val includeManifest: Boolean = true

  /**
   * Serializes persistent messages. Delegates serialization of a persistent
   * message's payload to a matching `akka.serialization.Serializer`.
   */
  def toBinary(o: AnyRef): Array[Byte] = o match {
    case p: PersistentRepr                        => persistentMessageBuilder(p).build().toByteArray
    case a: AtomicWrite                           => atomicWriteBuilder(a).build().toByteArray
    case a: AtLeastOnceDeliverySnapshot           => atLeastOnceDeliverySnapshotBuilder(a).build.toByteArray
    case s: StateChangeEvent                      => stateChangeBuilder(s).build.toByteArray
    case p: PersistentFSMSnapshot[Any @unchecked] => persistentFSMSnapshotBuilder(p).build.toByteArray
    case _                                        => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  /**
   * Deserializes persistent messages. Delegates deserialization of a persistent
   * message's payload to a matching `akka.serialization.Serializer`.
   */
  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): Message = manifest match {
    case None => persistent(mf.PersistentMessage.parseFrom(bytes))
    case Some(c) =>
      c match {
        case PersistentImplClass => persistent(mf.PersistentMessage.parseFrom(bytes))
        case PersistentReprClass => persistent(mf.PersistentMessage.parseFrom(bytes))
        case AtomicWriteClass    => atomicWrite(mf.AtomicWrite.parseFrom(bytes))
        case AtLeastOnceDeliverySnapshotClass =>
          atLeastOnceDeliverySnapshot(mf.AtLeastOnceDeliverySnapshot.parseFrom(bytes))
        case PersistentStateChangeEventClass => stateChange(mf.PersistentStateChangeEvent.parseFrom(bytes))
        case PersistentFSMSnapshotClass      => persistentFSMSnapshot(mf.PersistentFSMSnapshot.parseFrom(bytes))
        case _                               => throw new NotSerializableException(s"Can't deserialize object of type ${c}")
      }
  }

  //
  // toBinary helpers
  //

  def atLeastOnceDeliverySnapshotBuilder(snap: AtLeastOnceDeliverySnapshot): mf.AtLeastOnceDeliverySnapshot.Builder = {
    val builder = mf.AtLeastOnceDeliverySnapshot.newBuilder
    builder.setCurrentDeliveryId(snap.currentDeliveryId)
    snap.unconfirmedDeliveries.foreach { unconfirmed =>
      val unconfirmedBuilder =
        mf.AtLeastOnceDeliverySnapshot.UnconfirmedDelivery.newBuilder
          .setDeliveryId(unconfirmed.deliveryId)
          .setDestination(unconfirmed.destination.toString)
          .setPayload(persistentPayloadBuilder(unconfirmed.message.asInstanceOf[AnyRef]))
      builder.addUnconfirmedDeliveries(unconfirmedBuilder)
    }
    builder
  }

  private[persistence] def stateChangeBuilder(stateChange: StateChangeEvent): mf.PersistentStateChangeEvent.Builder = {
    val builder = mf.PersistentStateChangeEvent.newBuilder.setStateIdentifier(stateChange.stateIdentifier)
    stateChange.timeout match {
      case None          => builder
      case Some(timeout) => builder.setTimeoutNanos(timeout.toNanos)
    }
  }

  private[persistence] def persistentFSMSnapshotBuilder(
      persistentFSMSnapshot: PersistentFSMSnapshot[Any]): mf.PersistentFSMSnapshot.Builder = {
    val builder = mf.PersistentFSMSnapshot.newBuilder
      .setStateIdentifier(persistentFSMSnapshot.stateIdentifier)
      .setData(persistentPayloadBuilder(persistentFSMSnapshot.data.asInstanceOf[AnyRef]))
    persistentFSMSnapshot.timeout match {
      case None          => builder
      case Some(timeout) => builder.setTimeoutNanos(timeout.toNanos)
    }
  }

  def atLeastOnceDeliverySnapshot(
      atLeastOnceDeliverySnapshot: mf.AtLeastOnceDeliverySnapshot): AtLeastOnceDeliverySnapshot = {
    import scala.jdk.CollectionConverters._
    val unconfirmedDeliveries = new VectorBuilder[UnconfirmedDelivery]()
    atLeastOnceDeliverySnapshot.getUnconfirmedDeliveriesList().iterator().asScala.foreach { next =>
      unconfirmedDeliveries += UnconfirmedDelivery(
        next.getDeliveryId,
        ActorPath.fromString(next.getDestination),
        payload(next.getPayload))
    }

    AtLeastOnceDeliverySnapshot(atLeastOnceDeliverySnapshot.getCurrentDeliveryId, unconfirmedDeliveries.result())
  }

  def stateChange(persistentStateChange: mf.PersistentStateChangeEvent): StateChangeEvent = {
    StateChangeEvent(
      persistentStateChange.getStateIdentifier,
      // timeout field is deprecated, left for backward compatibility. timeoutNanos is used instead.
      if (persistentStateChange.hasTimeoutNanos)
        Some(Duration.fromNanos(persistentStateChange.getTimeoutNanos))
      else if (persistentStateChange.hasTimeout)
        Some(Duration(persistentStateChange.getTimeout).asInstanceOf[FiniteDuration])
      else None)
  }

  def persistentFSMSnapshot(persistentFSMSnapshot: mf.PersistentFSMSnapshot): PersistentFSMSnapshot[Any] = {
    PersistentFSMSnapshot(
      persistentFSMSnapshot.getStateIdentifier,
      payload(persistentFSMSnapshot.getData),
      if (persistentFSMSnapshot.hasTimeoutNanos)
        Some(Duration.fromNanos(persistentFSMSnapshot.getTimeoutNanos))
      else None)
  }

  private def atomicWriteBuilder(a: AtomicWrite) = {
    val builder = mf.AtomicWrite.newBuilder
    a.payload.foreach { p =>
      builder.addPayload(persistentMessageBuilder(p))
    }
    builder
  }

  private def persistentMessageBuilder(persistent: PersistentRepr) = {
    val builder = mf.PersistentMessage.newBuilder

    if (persistent.persistenceId != Undefined) builder.setPersistenceId(persistent.persistenceId)
    if (persistent.sender != Actor.noSender) builder.setSender(Serialization.serializedActorPath(persistent.sender))
    if (persistent.manifest != PersistentRepr.Undefined) builder.setManifest(persistent.manifest)

    builder.setPayload(persistentPayloadBuilder(persistent.payload.asInstanceOf[AnyRef]))
    persistent.metadata match {
      case Some(meta) =>
        builder.setMetadata(persistentPayloadBuilder(meta.asInstanceOf[AnyRef]))
      case _ =>
    }

    builder.setSequenceNr(persistent.sequenceNr)
    // deleted is not used in new records from 2.4
    if (persistent.writerUuid != Undefined) builder.setWriterUuid(persistent.writerUuid)
    if (persistent.timestamp > 0L) builder.setTimestamp(persistent.timestamp)
    builder
  }

  private def persistentPayloadBuilder(payload: AnyRef) = {
    def payloadBuilder() = {
      val serializer = serialization.findSerializerFor(payload)
      val builder = mf.PersistentPayload.newBuilder()

      val ms = Serializers.manifestFor(serializer, payload)
      if (ms.nonEmpty) builder.setPayloadManifest(ByteString.copyFromUtf8(ms))

      builder.setPayload(UnsafeByteOperations.unsafeWrap(serializer.toBinary(payload)))
      builder.setSerializerId(serializer.identifier)
      builder
    }

    val oldInfo = Serialization.currentTransportInformation.value
    try {
      if (oldInfo eq null)
        Serialization.currentTransportInformation.value = system.provider.serializationInformation
      payloadBuilder()
    } finally Serialization.currentTransportInformation.value = oldInfo
  }

  //
  // fromBinary helpers
  //

  private def persistent(persistentMessage: mf.PersistentMessage): PersistentRepr = {
    var repr = PersistentRepr(
      payload(persistentMessage.getPayload),
      persistentMessage.getSequenceNr,
      if (persistentMessage.hasPersistenceId) persistentMessage.getPersistenceId else Undefined,
      if (persistentMessage.hasManifest) persistentMessage.getManifest else Undefined,
      if (persistentMessage.hasDeleted) persistentMessage.getDeleted else false,
      if (persistentMessage.hasSender) system.provider.resolveActorRef(persistentMessage.getSender)
      else Actor.noSender,
      if (persistentMessage.hasWriterUuid) persistentMessage.getWriterUuid else Undefined)

    repr = if (persistentMessage.hasTimestamp) repr.withTimestamp(persistentMessage.getTimestamp) else repr
    if (persistentMessage.hasMetadata) repr.withMetadata(payload(persistentMessage.getMetadata)) else repr
  }

  private def atomicWrite(atomicWrite: mf.AtomicWrite): AtomicWrite = {
    import scala.jdk.CollectionConverters._
    AtomicWrite(atomicWrite.getPayloadList.asScala.iterator.map(persistent).to(immutable.IndexedSeq))
  }

  private def payload(persistentPayload: mf.PersistentPayload): Any = {
    val manifest =
      if (persistentPayload.hasPayloadManifest)
        persistentPayload.getPayloadManifest.toStringUtf8
      else ""

    serialization.deserialize(persistentPayload.getPayload.toByteArray, persistentPayload.getSerializerId, manifest).get
  }

}
