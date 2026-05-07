/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata.protobuf

import java.{ lang => jl }
import java.io.NotSerializableException
import java.util
import java.util.ArrayList
import java.util.Collections
import java.util.Comparator

import scala.annotation.tailrec
import scala.collection.immutable

import akka.actor.ActorRef
import akka.actor.ExtendedActorSystem
import akka.cluster.ddata._
import akka.cluster.ddata.Replicator.Internal._
import akka.cluster.ddata.protobuf.msg.{ ReplicatedDataMessages => rd }
import akka.cluster.ddata.protobuf.msg.{ ReplicatorMessages => dm }
import akka.cluster.ddata.protobuf.msg.ReplicatorMessages.OtherMessage
import akka.protobufv3.internal.GeneratedMessageV3
import akka.remote.ByteStringUtils
import akka.serialization.BaseSerializer
import akka.serialization.Serialization
import akka.serialization.SerializerWithStringManifest
import akka.util.ByteString.UTF_8
import scala.jdk.CollectionConverters._

private object ReplicatedDataSerializer {
  /*
   * Generic superclass to allow to compare Entry types used in protobuf.
   */
  abstract class KeyComparator[A <: GeneratedMessageV3] extends Comparator[A] {

    /**
     * Get the key from the entry. The key may be a String, Int, Long, or OtherMessage
     * @param entry The protobuf entry used with Map types
     * @return The Key
     */
    def getKey(entry: A): Any
    final def compare(x: A, y: A): Int = compareKeys(getKey(x), getKey(y))

    private final def compareKeys(t1: Any, t2: Any): Int = (t1, t2) match {
      case (k1: String, k2: String)             => k1.compareTo(k2)
      case (_: String, _)                       => -1
      case (_, _: String)                       => 1
      case (k1: Int, k2: Int)                   => k1.compareTo(k2)
      case (_: Int, _)                          => -1
      case (_, _: Int)                          => 1
      case (k1: Long, k2: Long)                 => k1.compareTo(k2)
      case (_: Long, _)                         => -1
      case (_, _: Long)                         => 1
      case (k1: OtherMessage, k2: OtherMessage) => OtherMessageComparator.compare(k1, k2)
      case (k1, k2) =>
        throw new IllegalStateException(
          s"Invalid keys (${k1.getClass}, ${k2.getClass}): must be of type String, Int, Long or OtherMessage")
    }
  }

  implicit object ORMapEntryComparator extends KeyComparator[rd.ORMap.Entry] {
    override def getKey(e: rd.ORMap.Entry): Any =
      if (e.hasStringKey) e.getStringKey
      else if (e.hasIntKey) e.getIntKey
      else if (e.hasLongKey) e.getLongKey
      else e.getOtherKey
  }
  implicit object LWWMapEntryComparator extends KeyComparator[rd.LWWMap.Entry] {
    override def getKey(e: rd.LWWMap.Entry): Any =
      if (e.hasStringKey) e.getStringKey
      else if (e.hasIntKey) e.getIntKey
      else if (e.hasLongKey) e.getLongKey
      else e.getOtherKey
  }
  implicit object PNCounterMapEntryComparator extends KeyComparator[rd.PNCounterMap.Entry] {
    override def getKey(e: rd.PNCounterMap.Entry): Any =
      if (e.hasStringKey) e.getStringKey
      else if (e.hasIntKey) e.getIntKey
      else if (e.hasLongKey) e.getLongKey
      else e.getOtherKey
  }
  implicit object ORMultiMapEntryComparator extends KeyComparator[rd.ORMultiMap.Entry] {
    override def getKey(e: rd.ORMultiMap.Entry): Any =
      if (e.hasStringKey) e.getStringKey
      else if (e.hasIntKey) e.getIntKey
      else if (e.hasLongKey) e.getLongKey
      else e.getOtherKey
  }

  sealed trait ProtoMapEntryWriter[
      Entry <: GeneratedMessageV3,
      EntryBuilder <: GeneratedMessageV3.Builder[EntryBuilder],
      Value <: GeneratedMessageV3] {
    def setStringKey(builder: EntryBuilder, key: String, value: Value): Entry
    def setLongKey(builder: EntryBuilder, key: Long, value: Value): Entry
    def setIntKey(builder: EntryBuilder, key: Int, value: Value): Entry
    def setOtherKey(builder: EntryBuilder, key: dm.OtherMessage, value: Value): Entry
  }

  sealed trait ProtoMapEntryReader[Entry <: GeneratedMessageV3, A <: GeneratedMessageV3] {
    def hasStringKey(entry: Entry): Boolean
    def getStringKey(entry: Entry): String
    def hasIntKey(entry: Entry): Boolean
    def getIntKey(entry: Entry): Int
    def hasLongKey(entry: Entry): Boolean
    def getLongKey(entry: Entry): Long
    def hasOtherKey(entry: Entry): Boolean
    def getOtherKey(entry: Entry): dm.OtherMessage
    def getValue(entry: Entry): A
  }

  implicit object ORMapEntry
      extends ProtoMapEntryWriter[rd.ORMap.Entry, rd.ORMap.Entry.Builder, dm.OtherMessage]
      with ProtoMapEntryReader[rd.ORMap.Entry, dm.OtherMessage] {
    override def setStringKey(builder: rd.ORMap.Entry.Builder, key: String, value: dm.OtherMessage): rd.ORMap.Entry =
      builder.setStringKey(key).setValue(value).build()
    override def setLongKey(builder: rd.ORMap.Entry.Builder, key: Long, value: dm.OtherMessage): rd.ORMap.Entry =
      builder.setLongKey(key).setValue(value).build()
    override def setIntKey(builder: rd.ORMap.Entry.Builder, key: Int, value: dm.OtherMessage): rd.ORMap.Entry =
      builder.setIntKey(key).setValue(value).build()
    override def setOtherKey(
        builder: rd.ORMap.Entry.Builder,
        key: dm.OtherMessage,
        value: dm.OtherMessage): rd.ORMap.Entry = builder.setOtherKey(key).setValue(value).build()
    override def hasStringKey(entry: rd.ORMap.Entry): Boolean = entry.hasStringKey
    override def getStringKey(entry: rd.ORMap.Entry): String = entry.getStringKey
    override def hasIntKey(entry: rd.ORMap.Entry): Boolean = entry.hasIntKey
    override def getIntKey(entry: rd.ORMap.Entry): Int = entry.getIntKey
    override def hasLongKey(entry: rd.ORMap.Entry): Boolean = entry.hasLongKey
    override def getLongKey(entry: rd.ORMap.Entry): Long = entry.getLongKey
    override def hasOtherKey(entry: rd.ORMap.Entry): Boolean = entry.hasOtherKey
    override def getOtherKey(entry: rd.ORMap.Entry): OtherMessage = entry.getOtherKey
    override def getValue(entry: rd.ORMap.Entry): dm.OtherMessage = entry.getValue
  }

  implicit object LWWMapEntry
      extends ProtoMapEntryWriter[rd.LWWMap.Entry, rd.LWWMap.Entry.Builder, rd.LWWRegister]
      with ProtoMapEntryReader[rd.LWWMap.Entry, rd.LWWRegister] {
    override def setStringKey(builder: rd.LWWMap.Entry.Builder, key: String, value: rd.LWWRegister): rd.LWWMap.Entry =
      builder.setStringKey(key).setValue(value).build()
    override def setLongKey(builder: rd.LWWMap.Entry.Builder, key: Long, value: rd.LWWRegister): rd.LWWMap.Entry =
      builder.setLongKey(key).setValue(value).build()
    override def setIntKey(builder: rd.LWWMap.Entry.Builder, key: Int, value: rd.LWWRegister): rd.LWWMap.Entry =
      builder.setIntKey(key).setValue(value).build()
    override def setOtherKey(
        builder: rd.LWWMap.Entry.Builder,
        key: OtherMessage,
        value: rd.LWWRegister): rd.LWWMap.Entry = builder.setOtherKey(key).setValue(value).build()
    override def hasStringKey(entry: rd.LWWMap.Entry): Boolean = entry.hasStringKey
    override def getStringKey(entry: rd.LWWMap.Entry): String = entry.getStringKey
    override def hasIntKey(entry: rd.LWWMap.Entry): Boolean = entry.hasIntKey
    override def getIntKey(entry: rd.LWWMap.Entry): Int = entry.getIntKey
    override def hasLongKey(entry: rd.LWWMap.Entry): Boolean = entry.hasLongKey
    override def getLongKey(entry: rd.LWWMap.Entry): Long = entry.getLongKey
    override def hasOtherKey(entry: rd.LWWMap.Entry): Boolean = entry.hasOtherKey
    override def getOtherKey(entry: rd.LWWMap.Entry): OtherMessage = entry.getOtherKey
    override def getValue(entry: rd.LWWMap.Entry): rd.LWWRegister = entry.getValue
  }

  implicit object PNCounterMapEntry
      extends ProtoMapEntryWriter[rd.PNCounterMap.Entry, rd.PNCounterMap.Entry.Builder, rd.PNCounter]
      with ProtoMapEntryReader[rd.PNCounterMap.Entry, rd.PNCounter] {
    override def setStringKey(
        builder: rd.PNCounterMap.Entry.Builder,
        key: String,
        value: rd.PNCounter): rd.PNCounterMap.Entry =
      builder.setStringKey(key).setValue(value).build()
    override def setLongKey(
        builder: rd.PNCounterMap.Entry.Builder,
        key: Long,
        value: rd.PNCounter): rd.PNCounterMap.Entry =
      builder.setLongKey(key).setValue(value).build()
    override def setIntKey(
        builder: rd.PNCounterMap.Entry.Builder,
        key: Int,
        value: rd.PNCounter): rd.PNCounterMap.Entry = builder.setIntKey(key).setValue(value).build()
    override def setOtherKey(
        builder: rd.PNCounterMap.Entry.Builder,
        key: OtherMessage,
        value: rd.PNCounter): rd.PNCounterMap.Entry =
      builder.setOtherKey(key).setValue(value).build()
    override def hasStringKey(entry: rd.PNCounterMap.Entry): Boolean = entry.hasStringKey
    override def getStringKey(entry: rd.PNCounterMap.Entry): String = entry.getStringKey
    override def hasIntKey(entry: rd.PNCounterMap.Entry): Boolean = entry.hasIntKey
    override def getIntKey(entry: rd.PNCounterMap.Entry): Int = entry.getIntKey
    override def hasLongKey(entry: rd.PNCounterMap.Entry): Boolean = entry.hasLongKey
    override def getLongKey(entry: rd.PNCounterMap.Entry): Long = entry.getLongKey
    override def hasOtherKey(entry: rd.PNCounterMap.Entry): Boolean = entry.hasOtherKey
    override def getOtherKey(entry: rd.PNCounterMap.Entry): OtherMessage = entry.getOtherKey
    override def getValue(entry: rd.PNCounterMap.Entry): rd.PNCounter = entry.getValue
  }

  implicit object ORMultiMapEntry
      extends ProtoMapEntryWriter[rd.ORMultiMap.Entry, rd.ORMultiMap.Entry.Builder, rd.ORSet]
      with ProtoMapEntryReader[rd.ORMultiMap.Entry, rd.ORSet] {
    override def setStringKey(builder: rd.ORMultiMap.Entry.Builder, key: String, value: rd.ORSet): rd.ORMultiMap.Entry =
      builder.setStringKey(key).setValue(value).build()
    override def setLongKey(builder: rd.ORMultiMap.Entry.Builder, key: Long, value: rd.ORSet): rd.ORMultiMap.Entry =
      builder.setLongKey(key).setValue(value).build()
    override def setIntKey(builder: rd.ORMultiMap.Entry.Builder, key: Int, value: rd.ORSet): rd.ORMultiMap.Entry =
      builder.setIntKey(key).setValue(value).build()
    override def setOtherKey(
        builder: rd.ORMultiMap.Entry.Builder,
        key: dm.OtherMessage,
        value: rd.ORSet): rd.ORMultiMap.Entry = builder.setOtherKey(key).setValue(value).build()
    override def hasStringKey(entry: rd.ORMultiMap.Entry): Boolean = entry.hasStringKey
    override def getStringKey(entry: rd.ORMultiMap.Entry): String = entry.getStringKey
    override def hasIntKey(entry: rd.ORMultiMap.Entry): Boolean = entry.hasIntKey
    override def getIntKey(entry: rd.ORMultiMap.Entry): Int = entry.getIntKey
    override def hasLongKey(entry: rd.ORMultiMap.Entry): Boolean = entry.hasLongKey
    override def getLongKey(entry: rd.ORMultiMap.Entry): Long = entry.getLongKey
    override def hasOtherKey(entry: rd.ORMultiMap.Entry): Boolean = entry.hasOtherKey
    override def getOtherKey(entry: rd.ORMultiMap.Entry): OtherMessage = entry.getOtherKey
    override def getValue(entry: rd.ORMultiMap.Entry): rd.ORSet = entry.getValue
  }

  implicit object ORMapDeltaGroupEntry
      extends ProtoMapEntryWriter[rd.ORMapDeltaGroup.MapEntry, rd.ORMapDeltaGroup.MapEntry.Builder, dm.OtherMessage]
      with ProtoMapEntryReader[rd.ORMapDeltaGroup.MapEntry, dm.OtherMessage] {
    override def setStringKey(
        builder: rd.ORMapDeltaGroup.MapEntry.Builder,
        key: String,
        value: dm.OtherMessage): rd.ORMapDeltaGroup.MapEntry =
      builder.setStringKey(key).setValue(value).build()
    override def setLongKey(
        builder: rd.ORMapDeltaGroup.MapEntry.Builder,
        key: Long,
        value: dm.OtherMessage): rd.ORMapDeltaGroup.MapEntry =
      builder.setLongKey(key).setValue(value).build()
    override def setIntKey(
        builder: rd.ORMapDeltaGroup.MapEntry.Builder,
        key: Int,
        value: dm.OtherMessage): rd.ORMapDeltaGroup.MapEntry =
      builder.setIntKey(key).setValue(value).build()
    override def setOtherKey(
        builder: rd.ORMapDeltaGroup.MapEntry.Builder,
        key: dm.OtherMessage,
        value: dm.OtherMessage): rd.ORMapDeltaGroup.MapEntry =
      builder.setOtherKey(key).setValue(value).build()
    override def hasStringKey(entry: rd.ORMapDeltaGroup.MapEntry): Boolean = entry.hasStringKey
    override def getStringKey(entry: rd.ORMapDeltaGroup.MapEntry): String = entry.getStringKey
    override def hasIntKey(entry: rd.ORMapDeltaGroup.MapEntry): Boolean = entry.hasIntKey
    override def getIntKey(entry: rd.ORMapDeltaGroup.MapEntry): Int = entry.getIntKey
    override def hasLongKey(entry: rd.ORMapDeltaGroup.MapEntry): Boolean = entry.hasLongKey
    override def getLongKey(entry: rd.ORMapDeltaGroup.MapEntry): Long = entry.getLongKey
    override def hasOtherKey(entry: rd.ORMapDeltaGroup.MapEntry): Boolean = entry.hasOtherKey
    override def getOtherKey(entry: rd.ORMapDeltaGroup.MapEntry): OtherMessage = entry.getOtherKey
    override def getValue(entry: rd.ORMapDeltaGroup.MapEntry): dm.OtherMessage = entry.getValue
  }

}

/**
 * Protobuf serializer of ReplicatedData.
 */
class ReplicatedDataSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with SerializationSupport
    with BaseSerializer {

  import ReplicatedDataSerializer._

  private val DeletedDataManifest = "A"
  private val GSetManifest = "B"
  private val GSetKeyManifest = "b"
  private val ORSetManifest = "C"
  private val ORSetKeyManifest = "c"
  private val ORSetAddManifest = "Ca"
  private val ORSetRemoveManifest = "Cr"
  private val ORSetFullManifest = "Cf"
  private val ORSetDeltaGroupManifest = "Cg"
  private val FlagManifest = "D"
  private val FlagKeyManifest = "d"
  private val LWWRegisterManifest = "E"
  private val LWWRegisterKeyManifest = "e"
  private val GCounterManifest = "F"
  private val GCounterKeyManifest = "f"
  private val PNCounterManifest = "G"
  private val PNCounterKeyManifest = "g"
  private val ORMapManifest = "H"
  private val ORMapKeyManifest = "h"
  private val ORMapPutManifest = "Ha"
  private val ORMapRemoveManifest = "Hr"
  private val ORMapRemoveKeyManifest = "Hk"
  private val ORMapUpdateManifest = "Hu"
  private val ORMapDeltaGroupManifest = "Hg"
  private val LWWMapManifest = "I"
  private val LWWMapKeyManifest = "i"
  private val PNCounterMapManifest = "J"
  private val PNCounterMapKeyManifest = "j"
  private val ORMultiMapManifest = "K"
  private val ORMultiMapKeyManifest = "k"
  private val VersionVectorManifest = "L"
  private val UnspecificKeyManifest = "m"

  private val fromBinaryMap = collection.immutable.HashMap[String, Array[Byte] => AnyRef](
    GSetManifest -> gsetFromBinary,
    ORSetManifest -> orsetFromBinary,
    ORSetAddManifest -> orsetAddFromBinary,
    ORSetRemoveManifest -> orsetRemoveFromBinary,
    ORSetFullManifest -> orsetFullFromBinary,
    ORSetDeltaGroupManifest -> orsetDeltaGroupFromBinary,
    FlagManifest -> flagFromBinary,
    LWWRegisterManifest -> lwwRegisterFromBinary,
    GCounterManifest -> gcounterFromBinary,
    PNCounterManifest -> pncounterFromBinary,
    ORMapManifest -> ormapFromBinary,
    ORMapPutManifest -> ormapPutFromBinary,
    ORMapRemoveManifest -> ormapRemoveFromBinary,
    ORMapRemoveKeyManifest -> ormapRemoveKeyFromBinary,
    ORMapUpdateManifest -> ormapUpdateFromBinary,
    ORMapDeltaGroupManifest -> ormapDeltaGroupFromBinary,
    LWWMapManifest -> lwwmapFromBinary,
    PNCounterMapManifest -> pncountermapFromBinary,
    ORMultiMapManifest -> multimapFromBinary,
    DeletedDataManifest -> (_ => DeletedData),
    VersionVectorManifest -> versionVectorFromBinary,
    GSetKeyManifest -> (bytes => GSetKey(keyIdFromBinary(bytes))),
    ORSetKeyManifest -> (bytes => ORSetKey(keyIdFromBinary(bytes))),
    FlagKeyManifest -> (bytes => FlagKey(keyIdFromBinary(bytes))),
    LWWRegisterKeyManifest -> (bytes => LWWRegisterKey(keyIdFromBinary(bytes))),
    GCounterKeyManifest -> (bytes => GCounterKey(keyIdFromBinary(bytes))),
    PNCounterKeyManifest -> (bytes => PNCounterKey(keyIdFromBinary(bytes))),
    ORMapKeyManifest -> (bytes => ORMapKey(keyIdFromBinary(bytes))),
    LWWMapKeyManifest -> (bytes => LWWMapKey(keyIdFromBinary(bytes))),
    PNCounterMapKeyManifest -> (bytes => PNCounterMapKey(keyIdFromBinary(bytes))),
    ORMultiMapKeyManifest -> (bytes => ORMultiMapKey(keyIdFromBinary(bytes))),
    UnspecificKeyManifest -> (bytes => Key.UnspecificKey(keyIdFromBinary(bytes))))

  override def manifest(obj: AnyRef): String = obj match {
    case _: ORSet[_]                     => ORSetManifest
    case _: ORSet.AddDeltaOp[_]          => ORSetAddManifest
    case _: ORSet.RemoveDeltaOp[_]       => ORSetRemoveManifest
    case _: GSet[_]                      => GSetManifest
    case _: GCounter                     => GCounterManifest
    case _: PNCounter                    => PNCounterManifest
    case _: Flag                         => FlagManifest
    case _: LWWRegister[_]               => LWWRegisterManifest
    case _: ORMap[_, _]                  => ORMapManifest
    case _: ORMap.PutDeltaOp[_, _]       => ORMapPutManifest
    case _: ORMap.RemoveDeltaOp[_, _]    => ORMapRemoveManifest
    case _: ORMap.RemoveKeyDeltaOp[_, _] => ORMapRemoveKeyManifest
    case _: ORMap.UpdateDeltaOp[_, _]    => ORMapUpdateManifest
    case _: LWWMap[_, _]                 => LWWMapManifest
    case _: PNCounterMap[_]              => PNCounterMapManifest
    case _: ORMultiMap[_, _]             => ORMultiMapManifest
    case DeletedData                     => DeletedDataManifest
    case _: VersionVector                => VersionVectorManifest

    case _: ORSetKey[_]         => ORSetKeyManifest
    case _: GSetKey[_]          => GSetKeyManifest
    case _: GCounterKey         => GCounterKeyManifest
    case _: PNCounterKey        => PNCounterKeyManifest
    case _: FlagKey             => FlagKeyManifest
    case _: LWWRegisterKey[_]   => LWWRegisterKeyManifest
    case _: ORMapKey[_, _]      => ORMapKeyManifest
    case _: LWWMapKey[_, _]     => LWWMapKeyManifest
    case _: PNCounterMapKey[_]  => PNCounterMapKeyManifest
    case _: ORMultiMapKey[_, _] => ORMultiMapKeyManifest

    case _: ORSet.DeltaGroup[_]       => ORSetDeltaGroupManifest
    case _: ORMap.DeltaGroup[_, _]    => ORMapDeltaGroupManifest
    case _: ORSet.FullStateDeltaOp[_] => ORSetFullManifest

    case _: Key.UnspecificKey => UnspecificKeyManifest

    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${obj.getClass} in [${getClass.getName}]")
  }

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case m: ORSet[_]                     => compress(orsetToProto(m))
    case m: ORSet.AddDeltaOp[_]          => orsetToProto(m.underlying).toByteArray
    case m: ORSet.RemoveDeltaOp[_]       => orsetToProto(m.underlying).toByteArray
    case m: GSet[_]                      => gsetToProto(m).toByteArray
    case m: GCounter                     => gcounterToProto(m).toByteArray
    case m: PNCounter                    => pncounterToProto(m).toByteArray
    case m: Flag                         => flagToProto(m).toByteArray
    case m: LWWRegister[_]               => lwwRegisterToProto(m).toByteArray
    case m: ORMap[_, _]                  => compress(ormapToProto(m))
    case m: ORMap.PutDeltaOp[_, _]       => ormapPutToProto(m).toByteArray
    case m: ORMap.RemoveDeltaOp[_, _]    => ormapRemoveToProto(m).toByteArray
    case m: ORMap.RemoveKeyDeltaOp[_, _] => ormapRemoveKeyToProto(m).toByteArray
    case m: ORMap.UpdateDeltaOp[_, _]    => ormapUpdateToProto(m).toByteArray
    case m: LWWMap[_, _]                 => compress(lwwmapToProto(m))
    case m: PNCounterMap[_]              => compress(pncountermapToProto(m))
    case m: ORMultiMap[_, _]             => compress(multimapToProto(m))
    case DeletedData                     => dm.Empty.getDefaultInstance.toByteArray
    case m: VersionVector                => versionVectorToProto(m).toByteArray
    case Key(id)                         => keyIdToBinary(id)
    case m: ORSet.DeltaGroup[_]          => orsetDeltaGroupToProto(m).toByteArray
    case m: ORMap.DeltaGroup[_, _]       => ormapDeltaGroupToProto(m).toByteArray
    case m: ORSet.FullStateDeltaOp[_]    => orsetToProto(m.underlying).toByteArray
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${obj.getClass} in [${getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    fromBinaryMap.get(manifest) match {
      case Some(f) => f(bytes)
      case None =>
        throw new NotSerializableException(
          s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]")
    }

  def gsetToProto(gset: GSet[_]): rd.GSet = {
    val b = rd.GSet.newBuilder()
    // using java collections and sorting for performance (avoid conversions)
    val stringElements = new ArrayList[String]
    val intElements = new ArrayList[Integer]
    val longElements = new ArrayList[jl.Long]
    val otherElements = new ArrayList[dm.OtherMessage]
    val actorRefElements = new ArrayList[String]
    gset.elements.foreach {
      case s: String     => stringElements.add(s)
      case i: Int        => intElements.add(i)
      case l: Long       => longElements.add(l)
      case ref: ActorRef => actorRefElements.add(Serialization.serializedActorPath(ref))
      case other         => otherElements.add(otherMessageToProto(other))
    }
    if (!stringElements.isEmpty) {
      Collections.sort(stringElements)
      b.addAllStringElements(stringElements)
    }
    if (!intElements.isEmpty) {
      Collections.sort(intElements)
      b.addAllIntElements(intElements)
    }
    if (!longElements.isEmpty) {
      Collections.sort(longElements)
      b.addAllLongElements(longElements)
    }
    if (!otherElements.isEmpty) {
      Collections.sort(otherElements, OtherMessageComparator)
      b.addAllOtherElements(otherElements)
    }
    if (!actorRefElements.isEmpty) {
      Collections.sort(actorRefElements)
      b.addAllActorRefElements(actorRefElements)
    }
    b.build()
  }

  def gsetFromBinary(bytes: Array[Byte]): GSet[_] =
    gsetFromProto(rd.GSet.parseFrom(bytes))

  def gsetFromProto(gset: rd.GSet): GSet[Any] = {
    val elements: Iterator[Any] = {
      gset.getStringElementsList.iterator.asScala ++
      gset.getIntElementsList.iterator.asScala ++
      gset.getLongElementsList.iterator.asScala ++
      gset.getOtherElementsList.iterator.asScala.map(otherMessageFromProto) ++
      gset.getActorRefElementsList.iterator.asScala.map(resolveActorRef)
    }
    GSet(elements.toSet)
  }

  def orsetToProto(orset: ORSet[_]): rd.ORSet =
    orsetToProtoImpl(orset.asInstanceOf[ORSet[Any]])

  private def orsetToProtoImpl(orset: ORSet[Any]): rd.ORSet = {
    val b = rd.ORSet.newBuilder().setVvector(versionVectorToProto(orset.vvector))
    // using java collections and sorting for performance (avoid conversions)
    val stringElements = new ArrayList[String]
    val intElements = new ArrayList[Integer]
    val longElements = new ArrayList[jl.Long]
    val otherElements = new ArrayList[dm.OtherMessage]
    var otherElementsMap = Map.empty[dm.OtherMessage, Any]
    val actorRefElements = new ArrayList[ActorRef]
    orset.elementsMap.keysIterator.foreach {
      case s: String     => stringElements.add(s)
      case i: Int        => intElements.add(i)
      case l: Long       => longElements.add(l)
      case ref: ActorRef => actorRefElements.add(ref)
      case other =>
        val enclosedMsg = otherMessageToProto(other)
        otherElements.add(enclosedMsg)
        // need the mapping back to the `other` when adding dots
        otherElementsMap = otherElementsMap.updated(enclosedMsg, other)
    }

    def addDots(elements: ArrayList[_]): Unit = {
      // add corresponding dots in same order
      val iter = elements.iterator
      while (iter.hasNext) {
        val element = iter.next() match {
          case enclosedMsg: dm.OtherMessage => otherElementsMap(enclosedMsg)
          case e                            => e
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
      Collections.sort(otherElements, OtherMessageComparator)
      b.addAllOtherElements(otherElements)
      addDots(otherElements)
    }
    if (!actorRefElements.isEmpty) {
      Collections.sort(actorRefElements)
      val iter = actorRefElements.iterator
      while (iter.hasNext) {
        b.addActorRefElements(Serialization.serializedActorPath(iter.next()))
      }
      addDots(actorRefElements)
    }

    b.build()
  }

  def orsetFromBinary(bytes: Array[Byte]): ORSet[Any] =
    orsetFromProto(rd.ORSet.parseFrom(decompress(bytes)))

  private def orsetAddFromBinary(bytes: Array[Byte]): ORSet.AddDeltaOp[Any] =
    new ORSet.AddDeltaOp(orsetFromProto(rd.ORSet.parseFrom(bytes)))

  private def orsetRemoveFromBinary(bytes: Array[Byte]): ORSet.RemoveDeltaOp[Any] =
    new ORSet.RemoveDeltaOp(orsetFromProto(rd.ORSet.parseFrom(bytes)))

  private def orsetFullFromBinary(bytes: Array[Byte]): ORSet.FullStateDeltaOp[Any] =
    new ORSet.FullStateDeltaOp(orsetFromProto(rd.ORSet.parseFrom(bytes)))

  private def orsetDeltaGroupToProto(deltaGroup: ORSet.DeltaGroup[_]): rd.ORSetDeltaGroup = {
    def createEntry(opType: rd.ORSetDeltaOp, u: ORSet[_]) = {
      rd.ORSetDeltaGroup.Entry.newBuilder().setOperation(opType).setUnderlying(orsetToProto(u))
    }

    val b = rd.ORSetDeltaGroup.newBuilder()
    deltaGroup.ops.foreach {
      case ORSet.AddDeltaOp(u) =>
        b.addEntries(createEntry(rd.ORSetDeltaOp.Add, u))
      case ORSet.RemoveDeltaOp(u) =>
        b.addEntries(createEntry(rd.ORSetDeltaOp.Remove, u))
      case ORSet.FullStateDeltaOp(u) =>
        b.addEntries(createEntry(rd.ORSetDeltaOp.Full, u))
      case ORSet.DeltaGroup(_) =>
        throw new IllegalArgumentException("ORSet.DeltaGroup should not be nested")
    }
    b.build()
  }

  private def orsetDeltaGroupFromBinary(bytes: Array[Byte]): ORSet.DeltaGroup[Any] = {
    val deltaGroup = rd.ORSetDeltaGroup.parseFrom(bytes)
    val ops: Vector[ORSet.DeltaOp] =
      deltaGroup.getEntriesList.asScala.iterator
        .map { entry =>
          if (entry.getOperation == rd.ORSetDeltaOp.Add)
            ORSet.AddDeltaOp(orsetFromProto(entry.getUnderlying))
          else if (entry.getOperation == rd.ORSetDeltaOp.Remove)
            ORSet.RemoveDeltaOp(orsetFromProto(entry.getUnderlying))
          else if (entry.getOperation == rd.ORSetDeltaOp.Full)
            ORSet.FullStateDeltaOp(orsetFromProto(entry.getUnderlying))
          else
            throw new NotSerializableException(s"Unknown ORSet delta operation ${entry.getOperation}")
        }
        .to(immutable.Vector)
    ORSet.DeltaGroup(ops)
  }

  def orsetFromProto(orset: rd.ORSet): ORSet[Any] = {
    val elements: Iterator[Any] = {
      orset.getStringElementsList.iterator.asScala ++
      orset.getIntElementsList.iterator.asScala ++
      orset.getLongElementsList.iterator.asScala ++
      orset.getOtherElementsList.iterator.asScala.map(otherMessageFromProto) ++
      orset.getActorRefElementsList.iterator.asScala.map(resolveActorRef)
    }

    val dots = orset.getDotsList.asScala.map(versionVectorFromProto).iterator
    val elementsMap = elements.zip(dots).toMap

    new ORSet(elementsMap, vvector = versionVectorFromProto(orset.getVvector))
  }

  def flagToProto(flag: Flag): rd.Flag =
    rd.Flag.newBuilder().setEnabled(flag.enabled).build()

  def flagFromBinary(bytes: Array[Byte]): Flag =
    flagFromProto(rd.Flag.parseFrom(bytes))

  def flagFromProto(flag: rd.Flag): Flag =
    if (flag.getEnabled) Flag.Enabled else Flag.Disabled

  def lwwRegisterToProto(lwwRegister: LWWRegister[_]): rd.LWWRegister =
    rd.LWWRegister
      .newBuilder()
      .setTimestamp(lwwRegister.timestamp)
      .setNode(uniqueAddressToProto(lwwRegister.node))
      .setState(otherMessageToProto(lwwRegister.value))
      .build()

  def lwwRegisterFromBinary(bytes: Array[Byte]): LWWRegister[Any] =
    lwwRegisterFromProto(rd.LWWRegister.parseFrom(bytes))

  def lwwRegisterFromProto(lwwRegister: rd.LWWRegister): LWWRegister[Any] =
    new LWWRegister(
      uniqueAddressFromProto(lwwRegister.getNode),
      otherMessageFromProto(lwwRegister.getState),
      lwwRegister.getTimestamp)

  def gcounterToProto(gcounter: GCounter): rd.GCounter = {
    val b = rd.GCounter.newBuilder()
    gcounter.state.toVector.sortBy { case (address, _) => address }.foreach {
      case (address, value) =>
        b.addEntries(
          rd.GCounter.Entry
            .newBuilder()
            .setNode(uniqueAddressToProto(address))
            .setValue(ByteStringUtils.toProtoByteStringUnsafe(value.toByteArray)))
    }
    b.build()
  }

  def gcounterFromBinary(bytes: Array[Byte]): GCounter =
    gcounterFromProto(rd.GCounter.parseFrom(bytes))

  def gcounterFromProto(gcounter: rd.GCounter): GCounter = {
    new GCounter(
      state = gcounter.getEntriesList.asScala.iterator
        .map(entry => uniqueAddressFromProto(entry.getNode) -> BigInt(entry.getValue.toByteArray))
        .toMap)
  }

  def pncounterToProto(pncounter: PNCounter): rd.PNCounter =
    rd.PNCounter
      .newBuilder()
      .setIncrements(gcounterToProto(pncounter.increments))
      .setDecrements(gcounterToProto(pncounter.decrements))
      .build()

  def pncounterFromBinary(bytes: Array[Byte]): PNCounter =
    pncounterFromProto(rd.PNCounter.parseFrom(bytes))

  def pncounterFromProto(pncounter: rd.PNCounter): PNCounter = {
    new PNCounter(
      increments = gcounterFromProto(pncounter.getIncrements),
      decrements = gcounterFromProto(pncounter.getDecrements))
  }

  /*
   * Convert a Map[A, B] to an Iterable[Entry] where Entry is the protobuf map entry.
   */
  private def getEntries[
      IKey,
      IValue,
      EntryBuilder <: GeneratedMessageV3.Builder[EntryBuilder],
      PEntry <: GeneratedMessageV3,
      PValue <: GeneratedMessageV3](
      input: Map[IKey, IValue],
      createBuilder: () => EntryBuilder,
      valueConverter: IValue => PValue)(
      implicit comparator: Comparator[PEntry],
      eh: ProtoMapEntryWriter[PEntry, EntryBuilder, PValue]): java.lang.Iterable[PEntry] = {
    // The resulting Iterable needs to be ordered deterministically in order to create same signature upon serializing same data
    val protoEntries = new util.TreeSet[PEntry](comparator)
    input.foreach {
      case (key: String, value) => protoEntries.add(eh.setStringKey(createBuilder(), key, valueConverter(value)))
      case (key: Int, value)    => protoEntries.add(eh.setIntKey(createBuilder(), key, valueConverter(value)))
      case (key: Long, value)   => protoEntries.add(eh.setLongKey(createBuilder(), key, valueConverter(value)))
      case (key, value) =>
        protoEntries.add(eh.setOtherKey(createBuilder(), otherMessageToProto(key), valueConverter(value)))
    }
    protoEntries
  }

  def ormapToProto(ormap: ORMap[_, _]): rd.ORMap = {
    val ormapBuilder = rd.ORMap.newBuilder()
    val entries: jl.Iterable[rd.ORMap.Entry] =
      getEntries(ormap.values, rd.ORMap.Entry.newBuilder _, otherMessageToProto)
    ormapBuilder.setKeys(orsetToProto(ormap.keys)).addAllEntries(entries).build()
  }

  def ormapFromBinary(bytes: Array[Byte]): ORMap[Any, ReplicatedData] =
    ormapFromProto(rd.ORMap.parseFrom(decompress(bytes)))

  def mapTypeFromProto[PEntry <: GeneratedMessageV3, A <: GeneratedMessageV3, B <: ReplicatedData](
      input: util.List[PEntry],
      valueCreator: A => B)(implicit eh: ProtoMapEntryReader[PEntry, A]): Map[Any, B] = {
    input.asScala.map { entry =>
      if (eh.hasStringKey(entry)) eh.getStringKey(entry) -> valueCreator(eh.getValue(entry))
      else if (eh.hasIntKey(entry)) eh.getIntKey(entry) -> valueCreator(eh.getValue(entry))
      else if (eh.hasLongKey(entry)) eh.getLongKey(entry) -> valueCreator(eh.getValue(entry))
      else if (eh.hasOtherKey(entry)) otherMessageFromProto(eh.getOtherKey(entry)) -> valueCreator(eh.getValue(entry))
      else
        throw new IllegalArgumentException(
          s"Can't deserialize ${entry.getClass} because it does not have any key in the serialized message.")
    }.toMap
  }

  def ormapFromProto(ormap: rd.ORMap): ORMap[Any, ReplicatedData] = {
    val entries = mapTypeFromProto(
      ormap.getEntriesList,
      (v: dm.OtherMessage) => otherMessageFromProto(v).asInstanceOf[ReplicatedData])
    new ORMap(keys = orsetFromProto(ormap.getKeys), entries, ORMap.VanillaORMapTag)
  }

  def singleMapEntryFromProto[PEntry <: GeneratedMessageV3, A <: GeneratedMessageV3, B <: ReplicatedData](
      input: util.List[PEntry],
      valueCreator: A => B)(implicit eh: ProtoMapEntryReader[PEntry, A]): Map[Any, B] = {
    val map = mapTypeFromProto(input, valueCreator)
    if (map.size > 1)
      throw new IllegalArgumentException(
        s"Can't deserialize the key/value pair in the ORMap delta - too many pairs on the wire")
    else
      map
  }

  def singleKeyEntryFromProto[PEntry <: GeneratedMessageV3, A <: GeneratedMessageV3](entryOption: Option[PEntry])(
      implicit eh: ProtoMapEntryReader[PEntry, A]): Any =
    entryOption match {
      case Some(entry) =>
        if (eh.hasStringKey(entry)) eh.getStringKey(entry)
        else if (eh.hasIntKey(entry)) eh.getIntKey(entry)
        else if (eh.hasLongKey(entry)) eh.getLongKey(entry)
        else if (eh.hasOtherKey(entry)) otherMessageFromProto(eh.getOtherKey(entry))
        else throw new IllegalArgumentException(s"Can't deserialize the key in the ORMap delta")
      case _ => throw new IllegalArgumentException(s"Can't deserialize the key in the ORMap delta")
    }

  // wire protocol is always DeltaGroup
  private def ormapPutFromBinary(bytes: Array[Byte]): ORMap.PutDeltaOp[Any, ReplicatedData] = {
    val ops = ormapDeltaGroupOpsFromBinary(bytes)
    if (ops.size == 1 && ops.head.isInstanceOf[ORMap.PutDeltaOp[_, _]])
      ops.head.asInstanceOf[ORMap.PutDeltaOp[Any, ReplicatedData]]
    else
      throw new NotSerializableException("Improper ORMap delta put operation size or kind")
  }

  // wire protocol is always delta group
  private def ormapRemoveFromBinary(bytes: Array[Byte]): ORMap.RemoveDeltaOp[Any, ReplicatedData] = {
    val ops = ormapDeltaGroupOpsFromBinary(bytes)
    if (ops.size == 1 && ops.head.isInstanceOf[ORMap.RemoveDeltaOp[_, _]])
      ops.head.asInstanceOf[ORMap.RemoveDeltaOp[Any, ReplicatedData]]
    else
      throw new NotSerializableException("Improper ORMap delta remove operation size or kind")
  }

  // wire protocol is always delta group
  private def ormapRemoveKeyFromBinary(bytes: Array[Byte]): ORMap.RemoveKeyDeltaOp[Any, ReplicatedData] = {
    val ops = ormapDeltaGroupOpsFromBinary(bytes)
    if (ops.size == 1 && ops.head.isInstanceOf[ORMap.RemoveKeyDeltaOp[_, _]])
      ops.head.asInstanceOf[ORMap.RemoveKeyDeltaOp[Any, ReplicatedData]]
    else
      throw new NotSerializableException("Improper ORMap delta remove key operation size or kind")
  }

  // wire protocol is always delta group
  private def ormapUpdateFromBinary(bytes: Array[Byte]): ORMap.UpdateDeltaOp[Any, ReplicatedDelta] = {
    val ops = ormapDeltaGroupOpsFromBinary(bytes)
    if (ops.size == 1 && ops.head.isInstanceOf[ORMap.UpdateDeltaOp[_, _]])
      ops.head.asInstanceOf[ORMap.UpdateDeltaOp[Any, ReplicatedDelta]]
    else
      throw new NotSerializableException("Improper ORMap delta update operation size or kind")
  }

  // this can be made client-extendable in the same way as Http codes in Spray are
  private def zeroTagFromCode(code: Int) = code match {
    case ORMap.VanillaORMapTag.value                   => ORMap.VanillaORMapTag
    case PNCounterMap.PNCounterMapTag.value            => PNCounterMap.PNCounterMapTag
    case ORMultiMap.ORMultiMapTag.value                => ORMultiMap.ORMultiMapTag
    case ORMultiMap.ORMultiMapWithValueDeltasTag.value => ORMultiMap.ORMultiMapWithValueDeltasTag
    case LWWMap.LWWMapTag.value                        => LWWMap.LWWMapTag
    case _                                             => throw new IllegalArgumentException("Invalid ZeroTag code")
  }

  private def ormapDeltaGroupFromBinary(bytes: Array[Byte]): ORMap.DeltaGroup[Any, ReplicatedData] = {
    ORMap.DeltaGroup(ormapDeltaGroupOpsFromBinary(bytes))
  }

  private def ormapDeltaGroupOpsFromBinary(bytes: Array[Byte]): scala.collection.immutable.IndexedSeq[ORMap.DeltaOp] = {
    val deltaGroup = rd.ORMapDeltaGroup.parseFrom(bytes)
    val ops: Vector[ORMap.DeltaOp] =
      deltaGroup.getEntriesList.asScala.iterator
        .map { entry =>
          if (entry.getOperation == rd.ORMapDeltaOp.ORMapPut) {
            val map =
              singleMapEntryFromProto(
                entry.getEntryDataList,
                (v: dm.OtherMessage) => otherMessageFromProto(v).asInstanceOf[ReplicatedData])
            ORMap.PutDeltaOp(
              ORSet.AddDeltaOp(orsetFromProto(entry.getUnderlying)),
              map.head,
              zeroTagFromCode(entry.getZeroTag))
          } else if (entry.getOperation == rd.ORMapDeltaOp.ORMapRemove) {
            ORMap.RemoveDeltaOp(
              ORSet.RemoveDeltaOp(orsetFromProto(entry.getUnderlying)),
              zeroTagFromCode(entry.getZeroTag))
          } else if (entry.getOperation == rd.ORMapDeltaOp.ORMapRemoveKey) {
            val elem = singleKeyEntryFromProto(entry.getEntryDataList.asScala.headOption)
            ORMap.RemoveKeyDeltaOp(
              ORSet.RemoveDeltaOp(orsetFromProto(entry.getUnderlying)),
              elem,
              zeroTagFromCode(entry.getZeroTag))
          } else if (entry.getOperation == rd.ORMapDeltaOp.ORMapUpdate) {
            val map = mapTypeFromProto(
              entry.getEntryDataList,
              (v: dm.OtherMessage) => otherMessageFromProto(v).asInstanceOf[ReplicatedDelta])
            ORMap.UpdateDeltaOp(
              ORSet.AddDeltaOp(orsetFromProto(entry.getUnderlying)),
              map,
              zeroTagFromCode(entry.getZeroTag))
          } else
            throw new NotSerializableException(s"Unknown ORMap delta operation ${entry.getOperation}")
        }
        .to(immutable.Vector)
    ops
  }

  private def ormapPutToProto(deltaOp: ORMap.PutDeltaOp[_, _]): rd.ORMapDeltaGroup = {
    ormapDeltaGroupOpsToProto(immutable.IndexedSeq(deltaOp.asInstanceOf[ORMap.DeltaOp]))
  }

  private def ormapRemoveToProto(deltaOp: ORMap.RemoveDeltaOp[_, _]): rd.ORMapDeltaGroup = {
    ormapDeltaGroupOpsToProto(immutable.IndexedSeq(deltaOp.asInstanceOf[ORMap.DeltaOp]))
  }

  private def ormapRemoveKeyToProto(deltaOp: ORMap.RemoveKeyDeltaOp[_, _]): rd.ORMapDeltaGroup = {
    ormapDeltaGroupOpsToProto(immutable.IndexedSeq(deltaOp.asInstanceOf[ORMap.DeltaOp]))
  }

  private def ormapUpdateToProto(deltaOp: ORMap.UpdateDeltaOp[_, _]): rd.ORMapDeltaGroup = {
    ormapDeltaGroupOpsToProto(immutable.IndexedSeq(deltaOp.asInstanceOf[ORMap.DeltaOp]))
  }

  private def ormapDeltaGroupToProto(deltaGroup: ORMap.DeltaGroup[_, _]): rd.ORMapDeltaGroup = {
    ormapDeltaGroupOpsToProto(deltaGroup.ops)
  }

  private def ormapDeltaGroupOpsToProto(deltaGroupOps: immutable.IndexedSeq[ORMap.DeltaOp]): rd.ORMapDeltaGroup = {
    def createEntry(opType: rd.ORMapDeltaOp, u: ORSet[_], m: Map[_, _], zt: Int) = {
      if (m.size > 1 && opType != rd.ORMapDeltaOp.ORMapUpdate)
        throw new IllegalArgumentException("Invalid size of ORMap delta map")
      else {
        val builder =
          rd.ORMapDeltaGroup.Entry.newBuilder().setOperation(opType).setUnderlying(orsetToProto(u)).setZeroTag(zt)
        m.foreach {
          case (key: String, value) =>
            builder.addEntryData(
              rd.ORMapDeltaGroup.MapEntry.newBuilder().setStringKey(key).setValue(otherMessageToProto(value)).build())
          case (key: Int, value) =>
            builder.addEntryData(
              rd.ORMapDeltaGroup.MapEntry.newBuilder().setIntKey(key).setValue(otherMessageToProto(value)).build())
          case (key: Long, value) =>
            builder.addEntryData(
              rd.ORMapDeltaGroup.MapEntry.newBuilder().setLongKey(key).setValue(otherMessageToProto(value)).build())
          case (key, value) =>
            builder.addEntryData(
              rd.ORMapDeltaGroup.MapEntry
                .newBuilder()
                .setOtherKey(otherMessageToProto(key))
                .setValue(otherMessageToProto(value))
                .build())
        }
        builder
      }
    }

    def createEntryWithKey(opType: rd.ORMapDeltaOp, u: ORSet[_], k: Any, zt: Int) = {
      val entryDataBuilder = rd.ORMapDeltaGroup.MapEntry.newBuilder()
      k match {
        case key: String => entryDataBuilder.setStringKey(key)
        case key: Int    => entryDataBuilder.setIntKey(key)
        case key: Long   => entryDataBuilder.setLongKey(key)
        case key         => entryDataBuilder.setOtherKey(otherMessageToProto(key))
      }
      val builder =
        rd.ORMapDeltaGroup.Entry.newBuilder().setOperation(opType).setUnderlying(orsetToProto(u)).setZeroTag(zt)
      builder.addEntryData(entryDataBuilder.build())
      builder
    }

    val b = rd.ORMapDeltaGroup.newBuilder()
    deltaGroupOps.foreach {
      case ORMap.PutDeltaOp(op, pair, zt) =>
        b.addEntries(
          createEntry(rd.ORMapDeltaOp.ORMapPut, op.asInstanceOf[ORSet.AddDeltaOp[_]].underlying, Map(pair), zt.value))
      case ORMap.RemoveDeltaOp(op, zt) =>
        b.addEntries(
          createEntry(
            rd.ORMapDeltaOp.ORMapRemove,
            op.asInstanceOf[ORSet.RemoveDeltaOp[_]].underlying,
            Map.empty,
            zt.value))
      case ORMap.RemoveKeyDeltaOp(op, k, zt) =>
        b.addEntries(
          createEntryWithKey(
            rd.ORMapDeltaOp.ORMapRemoveKey,
            op.asInstanceOf[ORSet.RemoveDeltaOp[_]].underlying,
            k,
            zt.value))
      case ORMap.UpdateDeltaOp(op, m, zt) =>
        b.addEntries(
          createEntry(rd.ORMapDeltaOp.ORMapUpdate, op.asInstanceOf[ORSet.AddDeltaOp[_]].underlying, m, zt.value))
      case ORMap.DeltaGroup(_) =>
        throw new IllegalArgumentException("ORMap.DeltaGroup should not be nested")
    }
    b.build()
  }

  def lwwmapToProto(lwwmap: LWWMap[_, _]): rd.LWWMap = {
    val lwwmapBuilder = rd.LWWMap.newBuilder()
    val entries: jl.Iterable[rd.LWWMap.Entry] =
      getEntries(lwwmap.underlying.entries, rd.LWWMap.Entry.newBuilder _, lwwRegisterToProto)
    lwwmapBuilder.setKeys(orsetToProto(lwwmap.underlying.keys)).addAllEntries(entries).build()
  }

  def lwwmapFromBinary(bytes: Array[Byte]): LWWMap[Any, Any] =
    lwwmapFromProto(rd.LWWMap.parseFrom(decompress(bytes)))

  def lwwmapFromProto(lwwmap: rd.LWWMap): LWWMap[Any, Any] = {
    val entries = mapTypeFromProto(lwwmap.getEntriesList, lwwRegisterFromProto)
    new LWWMap(new ORMap(keys = orsetFromProto(lwwmap.getKeys), entries, LWWMap.LWWMapTag))
  }

  def pncountermapToProto(pncountermap: PNCounterMap[_]): rd.PNCounterMap = {
    val pncountermapBuilder = rd.PNCounterMap.newBuilder()
    val entries: jl.Iterable[rd.PNCounterMap.Entry] =
      getEntries(pncountermap.underlying.entries, rd.PNCounterMap.Entry.newBuilder _, pncounterToProto)
    pncountermapBuilder.setKeys(orsetToProto(pncountermap.underlying.keys)).addAllEntries(entries).build()
  }

  def pncountermapFromBinary(bytes: Array[Byte]): PNCounterMap[_] =
    pncountermapFromProto(rd.PNCounterMap.parseFrom(decompress(bytes)))

  def pncountermapFromProto(pncountermap: rd.PNCounterMap): PNCounterMap[_] = {
    val entries = mapTypeFromProto(pncountermap.getEntriesList, pncounterFromProto)
    new PNCounterMap(new ORMap(keys = orsetFromProto(pncountermap.getKeys), entries, PNCounterMap.PNCounterMapTag))
  }

  def multimapToProto(multimap: ORMultiMap[_, _]): rd.ORMultiMap = {
    val ormultimapBuilder = rd.ORMultiMap.newBuilder()
    val entries: jl.Iterable[rd.ORMultiMap.Entry] =
      getEntries(multimap.underlying.entries, rd.ORMultiMap.Entry.newBuilder _, orsetToProto)
    ormultimapBuilder.setKeys(orsetToProto(multimap.underlying.keys)).addAllEntries(entries)
    if (multimap.withValueDeltas)
      ormultimapBuilder.setWithValueDeltas(true)
    ormultimapBuilder.build()
  }

  def multimapFromBinary(bytes: Array[Byte]): ORMultiMap[Any, Any] =
    multimapFromProto(rd.ORMultiMap.parseFrom(decompress(bytes)))

  def multimapFromProto(multimap: rd.ORMultiMap): ORMultiMap[Any, Any] = {
    val entries = mapTypeFromProto(multimap.getEntriesList, orsetFromProto)
    val withValueDeltas =
      if (multimap.hasWithValueDeltas)
        multimap.getWithValueDeltas
      else false
    new ORMultiMap(
      new ORMap(
        keys = orsetFromProto(multimap.getKeys),
        entries,
        if (withValueDeltas)
          ORMultiMap.ORMultiMapWithValueDeltasTag
        else
          ORMultiMap.ORMultiMapTag),
      withValueDeltas)
  }

  def keyIdToBinary(id: String): Array[Byte] =
    id.getBytes(UTF_8)

  def keyIdFromBinary(bytes: Array[Byte]): String =
    new String(bytes, UTF_8)

}

object OtherMessageComparator extends Comparator[dm.OtherMessage] {
  override def compare(a: dm.OtherMessage, b: dm.OtherMessage): Int = {
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
