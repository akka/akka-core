/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.serialization

import java.{ lang => jl }
import java.io.NotSerializableException
import java.util.{ ArrayList, Collections, Comparator }

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.annotation.InternalApi
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.crdt.{ Counter, ORSet }
import akka.persistence.typed.internal.PublishedEventImpl
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.persistence.typed.internal.ReplicatedPublishedEventMetaData
import akka.persistence.typed.internal.ReplicatedSnapshotMetadata
import akka.persistence.typed.internal.VersionVector
import akka.remote.ByteStringUtils
import akka.remote.ContainerFormats.Payload
import akka.remote.serialization.WrappedPayloadSupport
import akka.serialization.{ BaseSerializer, SerializerWithStringManifest }
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ReplicatedEventSourcingSerializer {
  object Comparator extends Comparator[Payload] {
    override def compare(a: Payload, b: Payload): Int = {
      val aByteString = a.getEnclosedMessage
      val bByteString = b.getEnclosedMessage
      val aSize = aByteString.size
      val bSize = bByteString.size
      if (aSize == bSize) {
        val aIter = aByteString.iterator
        val bIter = bByteString.iterator
        @tailrec def findDiff(): Int = {
          if (aIter.hasNext) {
            val aByte = aIter.nextByte()
            val bByte = bIter.nextByte()
            if (aByte < bByte) -1
            else if (aByte > bByte) 1
            else findDiff()
          } else 0
        }
        findDiff()
      } else if (aSize < bSize) -1
      else 1
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class ReplicatedEventSourcingSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  private val wrappedSupport = new WrappedPayloadSupport(system)
  // lazy because Serializers are initialized early on. `toTyped` might then try to
  // initialize the classic ActorSystemAdapter extension.
  private lazy val resolver = ActorRefResolver(system.toTyped)

  private val CrdtCounterManifest = "AA"
  private val CrdtCounterUpdatedManifest = "AB"

  private val ORSetManifest = "CA"
  private val ORSetAddManifest = "CB"
  private val ORSetRemoveManifest = "CC"
  private val ORSetFullManifest = "CD"
  private val ORSetDeltaGroupManifest = "CE"

  private val VersionVectorManifest = "DA"

  private val ReplicatedEventMetadataManifest = "RE"
  private val ReplicatedSnapshotMetadataManifest = "RS"

  private val PublishedEventManifest = "PA"

  def manifest(o: AnyRef) = o match {
    case _: ORSet[_]                  => ORSetManifest
    case _: ORSet.AddDeltaOp[_]       => ORSetAddManifest
    case _: ORSet.RemoveDeltaOp[_]    => ORSetRemoveManifest
    case _: ORSet.DeltaGroup[_]       => ORSetDeltaGroupManifest
    case _: ORSet.FullStateDeltaOp[_] => ORSetFullManifest

    case _: Counter         => CrdtCounterManifest
    case _: Counter.Updated => CrdtCounterUpdatedManifest

    case _: VersionVector => VersionVectorManifest

    case _: ReplicatedEventMetadata    => ReplicatedEventMetadataManifest
    case _: ReplicatedSnapshotMetadata => ReplicatedSnapshotMetadataManifest

    case _: PublishedEventImpl => PublishedEventManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  def toBinary(o: AnyRef) = o match {
    case m: ReplicatedEventMetadata    => replicatedEventMetadataToProto(m).toByteArray
    case m: ReplicatedSnapshotMetadata => replicatedSnapshotMetadataToByteArray(m)

    case m: VersionVector => versionVectorToProto(m).toByteArray

    case m: ORSet[_]                  => orsetToProto(m).toByteArray
    case m: ORSet.AddDeltaOp[_]       => orsetToProto(m.underlying).toByteArray
    case m: ORSet.RemoveDeltaOp[_]    => orsetToProto(m.underlying).toByteArray
    case m: ORSet.DeltaGroup[_]       => orsetDeltaGroupToProto(m).toByteArray
    case m: ORSet.FullStateDeltaOp[_] => orsetToProto(m.underlying).toByteArray

    case m: Counter         => counterToProtoByteArray(m)
    case m: Counter.Updated => counterUpdatedToProtoBufByteArray(m)

    case m: PublishedEventImpl => publishedEventToProtoByteArray(m)
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  def fromBinary(bytes: Array[Byte], manifest: String) = manifest match {

    case ReplicatedEventMetadataManifest    => replicatedEventMetadataFromBinary(bytes)
    case ReplicatedSnapshotMetadataManifest => replicatedSnapshotMetadataFromBinary(bytes)

    case VersionVectorManifest => versionVectorFromBinary(bytes)

    case ORSetManifest           => orsetFromBinary(bytes)
    case ORSetAddManifest        => orsetAddFromBinary(bytes)
    case ORSetRemoveManifest     => orsetRemoveFromBinary(bytes)
    case ORSetFullManifest       => orsetFullFromBinary(bytes)
    case ORSetDeltaGroupManifest => orsetDeltaGroupFromBinary(bytes)

    case CrdtCounterManifest        => counterFromBinary(bytes)
    case CrdtCounterUpdatedManifest => counterUpdatedFromBinary(bytes)

    case PublishedEventManifest => publishedEventFromBinary(bytes)

    case _ =>
      throw new NotSerializableException(
        s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]")
  }

  def publishedEventToProtoByteArray(impl: PublishedEventImpl): Array[Byte] = {
    val builder = ReplicatedEventSourcing.PublishedEvent
      .newBuilder()
      .setPersistenceId(impl.persistenceId.id)
      .setSequenceNr(impl.sequenceNumber)
      .setPayload(wrappedSupport.payloadBuilder(impl.payload))
      .setTimestamp(impl.timestamp)

    impl.replicatedMetaData match {
      case Some(m) =>
        val replicatedPublishedEventMetaDataBuilder =
          ReplicatedEventSourcing.ReplicatedPublishedEventMetaData
            .newBuilder()
            .setReplicaId(m.replicaId.id)
            .setVersionVector(versionVectorToProto(m.version))
        m.metadata.foreach { metadataPayload =>
          replicatedPublishedEventMetaDataBuilder.setMetadata(wrappedSupport.payloadBuilder(metadataPayload))
        }
        builder.setMetadata(replicatedPublishedEventMetaDataBuilder)
      case None =>
    }

    impl.replyTo match {
      case Some(ref) => builder.setReplyTo(resolver.toSerializationFormat(ref))
      case None      =>
    }

    builder.build().toByteArray
  }

  def publishedEventFromBinary(bytes: Array[Byte]): PublishedEventImpl = {
    val p = ReplicatedEventSourcing.PublishedEvent.parseFrom(bytes)
    PublishedEventImpl(
      PersistenceId.ofUniqueId(p.getPersistenceId),
      p.getSequenceNr,
      wrappedSupport.deserializePayload(p.getPayload),
      p.getTimestamp,
      if (p.hasMetadata) {
        val protoReplicatedMeta = p.getMetadata
        val meta =
          if (protoReplicatedMeta.hasMetadata)
            Some(wrappedSupport.deserializePayload(protoReplicatedMeta.getMetadata))
          else
            None

        Some(
          new ReplicatedPublishedEventMetaData(
            ReplicaId(protoReplicatedMeta.getReplicaId),
            versionVectorFromProto(protoReplicatedMeta.getVersionVector),
            meta))
      } else None,
      if (!p.hasReplyTo) None
      else Some(resolver.resolveActorRef(p.getReplyTo)))
  }

  def counterFromBinary(bytes: Array[Byte]): Counter =
    Counter(BigInt(ReplicatedEventSourcing.Counter.parseFrom(bytes).getValue.toByteArray))

  def counterUpdatedFromBinary(bytes: Array[Byte]): Counter.Updated =
    Counter.Updated(BigInt(ReplicatedEventSourcing.CounterUpdate.parseFrom(bytes).getDelta.toByteArray))

  def counterToProtoByteArray(counter: Counter): Array[Byte] =
    ReplicatedEventSourcing.Counter
      .newBuilder()
      .setValue(ByteStringUtils.toProtoByteStringUnsafe(counter.value.toByteArray))
      .build()
      .toByteArray

  def counterUpdatedToProtoBufByteArray(updated: Counter.Updated): Array[Byte] =
    ReplicatedEventSourcing.CounterUpdate
      .newBuilder()
      .setDelta(ByteStringUtils.toProtoByteStringUnsafe(updated.delta.toByteArray))
      .build()
      .toByteArray

  def orsetToProto(orset: ORSet[_]): ReplicatedEventSourcing.ORSet =
    orsetToProtoImpl(orset.asInstanceOf[ORSet[Any]])

  private def orsetToProtoImpl(orset: ORSet[Any]): ReplicatedEventSourcing.ORSet = {
    val b =
      ReplicatedEventSourcing.ORSet
        .newBuilder()
        .setOriginDc(orset.originReplica)
        .setVvector(versionVectorToProto(orset.vvector))
    // using java collections and sorting for performance (avoid conversions)
    val stringElements = new ArrayList[String]
    val intElements = new ArrayList[Integer]
    val longElements = new ArrayList[jl.Long]
    val otherElements = new ArrayList[Payload]
    var otherElementsMap = Map.empty[Payload, Any]
    orset.elementsMap.keysIterator.foreach {
      case s: String => stringElements.add(s)
      case i: Int    => intElements.add(i)
      case l: Long   => longElements.add(l)
      case other =>
        val enclosedMsg = wrappedSupport.payloadBuilder(other).build()
        otherElements.add(enclosedMsg)
        // need the mapping back to the `other` when adding dots
        otherElementsMap = otherElementsMap.updated(enclosedMsg, other)
    }

    def addDots(elements: ArrayList[_]): Unit = {
      // add corresponding dots in same order
      val iter = elements.iterator
      while (iter.hasNext) {
        val element = iter.next() match {
          case enclosedMsg: Payload => otherElementsMap(enclosedMsg)
          case e                    => e
        }
        b.addDots(versionVectorToProto(orset.elementsMap(element)))
      }
    }

    if (!stringElements.isEmpty) {
      Collections.sort(stringElements)
      b.addAllStringElements(stringElements)
      addDots(stringElements)
    }
    if (!intElements.isEmpty) {
      Collections.sort(intElements)
      b.addAllIntElements(intElements)
      addDots(intElements)
    }
    if (!longElements.isEmpty) {
      Collections.sort(longElements)
      b.addAllLongElements(longElements)
      addDots(longElements)
    }
    if (!otherElements.isEmpty) {
      Collections.sort(otherElements, ReplicatedEventSourcingSerializer.Comparator)
      b.addAllOtherElements(otherElements)
      addDots(otherElements)
    }

    b.build()
  }

  def replicatedEventMetadataToProto(rem: ReplicatedEventMetadata): ReplicatedEventSourcing.ReplicatedEventMetadata = {
    ReplicatedEventSourcing.ReplicatedEventMetadata
      .newBuilder()
      .setOriginSequenceNr(rem.originSequenceNr)
      .setConcurrent(rem.concurrent)
      .setOriginReplica(rem.originReplica.id)
      .setVersionVector(versionVectorToProto(rem.version))
      .build()
  }

  def replicatedSnapshotMetadataToByteArray(rsm: ReplicatedSnapshotMetadata): Array[Byte] = {
    ReplicatedEventSourcing.ReplicatedSnapshotMetadata
      .newBuilder()
      .setVersion(versionVectorToProto(rsm.version))
      .addAllSeenPerReplica(rsm.seenPerReplica.map(seenToProto).asJava)
      .build()
      .toByteArray
  }

  def seenToProto(t: (ReplicaId, Long)): ReplicatedEventSourcing.ReplicatedSnapshotMetadata.Seen = {
    ReplicatedEventSourcing.ReplicatedSnapshotMetadata.Seen
      .newBuilder()
      .setReplicaId(t._1.id)
      .setSequenceNr(t._2)
      .build()
  }

  def orsetFromBinary(bytes: Array[Byte]): ORSet[Any] =
    orsetFromProto(ReplicatedEventSourcing.ORSet.parseFrom(bytes))

  private def orsetAddFromBinary(bytes: Array[Byte]): ORSet.AddDeltaOp[Any] =
    new ORSet.AddDeltaOp(orsetFromProto(ReplicatedEventSourcing.ORSet.parseFrom(bytes)))

  private def orsetRemoveFromBinary(bytes: Array[Byte]): ORSet.RemoveDeltaOp[Any] =
    new ORSet.RemoveDeltaOp(orsetFromProto(ReplicatedEventSourcing.ORSet.parseFrom(bytes)))

  private def orsetFullFromBinary(bytes: Array[Byte]): ORSet.FullStateDeltaOp[Any] =
    new ORSet.FullStateDeltaOp(orsetFromProto(ReplicatedEventSourcing.ORSet.parseFrom(bytes)))

  private def orsetDeltaGroupToProto(deltaGroup: ORSet.DeltaGroup[_]): ReplicatedEventSourcing.ORSetDeltaGroup = {
    def createEntry(opType: ReplicatedEventSourcing.ORSetDeltaOp, u: ORSet[_]) = {
      ReplicatedEventSourcing.ORSetDeltaGroup.Entry.newBuilder().setOperation(opType).setUnderlying(orsetToProto(u))
    }

    val b = ReplicatedEventSourcing.ORSetDeltaGroup.newBuilder()
    deltaGroup.ops.foreach {
      case add: ORSet.AddDeltaOp[_] =>
        b.addEntries(createEntry(ReplicatedEventSourcing.ORSetDeltaOp.Add, add.underlying))
      case remove: ORSet.RemoveDeltaOp[_] =>
        b.addEntries(createEntry(ReplicatedEventSourcing.ORSetDeltaOp.Remove, remove.underlying))
      case full: ORSet.FullStateDeltaOp[_] =>
        b.addEntries(createEntry(ReplicatedEventSourcing.ORSetDeltaOp.Full, full.underlying))
      case _: ORSet.DeltaGroup[_] =>
        throw new IllegalArgumentException("ORSet.DeltaGroup should not be nested")
    }
    b.build()
  }

  private def orsetDeltaGroupFromBinary(bytes: Array[Byte]): ORSet.DeltaGroup[Any] = {
    val deltaGroup = ReplicatedEventSourcing.ORSetDeltaGroup.parseFrom(bytes)
    val ops: Vector[ORSet.DeltaOp] =
      deltaGroup.getEntriesList.asScala.map { entry =>
        if (entry.getOperation == ReplicatedEventSourcing.ORSetDeltaOp.Add)
          ORSet.AddDeltaOp(orsetFromProto(entry.getUnderlying))
        else if (entry.getOperation == ReplicatedEventSourcing.ORSetDeltaOp.Remove)
          ORSet.RemoveDeltaOp(orsetFromProto(entry.getUnderlying))
        else if (entry.getOperation == ReplicatedEventSourcing.ORSetDeltaOp.Full)
          ORSet.FullStateDeltaOp(orsetFromProto(entry.getUnderlying))
        else
          throw new NotSerializableException(s"Unknow ORSet delta operation ${entry.getOperation}")
      }.toVector
    ORSet.DeltaGroup(ops)
  }

  def orsetFromProto(orset: ReplicatedEventSourcing.ORSet): ORSet[Any] = {
    val elements: Iterator[Any] =
      (orset.getStringElementsList.iterator.asScala ++
      orset.getIntElementsList.iterator.asScala ++
      orset.getLongElementsList.iterator.asScala ++
      orset.getOtherElementsList.iterator.asScala.map(wrappedSupport.deserializePayload))

    val dots = orset.getDotsList.asScala.map(versionVectorFromProto).iterator
    val elementsMap = elements.zip(dots).toMap

    new ORSet(orset.getOriginDc, elementsMap, vvector = versionVectorFromProto(orset.getVvector))
  }

  def versionVectorToProto(versionVector: VersionVector): ReplicatedEventSourcing.VersionVector = {
    val b = ReplicatedEventSourcing.VersionVector.newBuilder()
    versionVector.versionsIterator.foreach {
      case (key, value) =>
        b.addEntries(ReplicatedEventSourcing.VersionVector.Entry.newBuilder().setKey(key).setVersion(value))
    }
    b.build()
  }

  def versionVectorFromBinary(bytes: Array[Byte]): VersionVector =
    versionVectorFromProto(ReplicatedEventSourcing.VersionVector.parseFrom(bytes))

  def versionVectorFromProto(versionVector: ReplicatedEventSourcing.VersionVector): VersionVector = {
    val entries = versionVector.getEntriesList
    if (entries.isEmpty)
      VersionVector.empty
    else if (entries.size == 1)
      VersionVector(entries.get(0).getKey, entries.get(0).getVersion)
    else {
      val versions = TreeMap.empty[String, Long] ++ versionVector.getEntriesList.asScala.map(entry =>
          entry.getKey -> entry.getVersion)
      VersionVector(versions)
    }
  }

  def replicatedEventMetadataFromBinary(bytes: Array[Byte]): ReplicatedEventMetadata = {
    val parsed = ReplicatedEventSourcing.ReplicatedEventMetadata.parseFrom(bytes)
    metadataFromProto(parsed)
  }

  private def metadataFromProto(parsed: ReplicatedEventSourcing.ReplicatedEventMetadata): ReplicatedEventMetadata = {
    ReplicatedEventMetadata(
      ReplicaId(parsed.getOriginReplica),
      parsed.getOriginSequenceNr,
      versionVectorFromProto(parsed.getVersionVector),
      parsed.getConcurrent)
  }

  def replicatedSnapshotMetadataFromBinary(bytes: Array[Byte]): ReplicatedSnapshotMetadata = {
    val parsed: ReplicatedEventSourcing.ReplicatedSnapshotMetadata =
      ReplicatedEventSourcing.ReplicatedSnapshotMetadata.parseFrom(bytes)
    ReplicatedSnapshotMetadata(
      versionVectorFromProto(parsed.getVersion),
      parsed.getSeenPerReplicaList.asScala.map(seen => ReplicaId(seen.getReplicaId) -> seen.getSequenceNr).toMap)
  }

}
