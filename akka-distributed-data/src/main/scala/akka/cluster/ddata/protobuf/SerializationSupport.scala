/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata.protobuf

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.cluster.UniqueAddress
import akka.cluster.ddata.VersionVector
import akka.cluster.ddata.protobuf.msg.{ ReplicatorMessages => dm }
import akka.protobufv3.internal.ByteString
import akka.protobufv3.internal.MessageLite
import akka.remote.ByteStringUtils
import akka.serialization._
import scala.jdk.CollectionConverters._

/**
 * Some useful serialization helper methods.
 */
trait SerializationSupport {

  private final val BufferSize = 1024 * 4

  def system: ExtendedActorSystem

  @volatile
  private var ser: Serialization = _
  def serialization: Serialization = {
    if (ser == null) ser = SerializationExtension(system)
    ser
  }

  @volatile
  private var protocol: String = _
  def addressProtocol: String = {
    if (protocol == null) protocol = system.provider.getDefaultAddress.protocol
    protocol
  }

  @volatile
  private var transportInfo: Serialization.Information = _
  def transportInformation: Serialization.Information = {
    if (transportInfo == null) {
      val address = system.provider.getDefaultAddress
      transportInfo = Serialization.Information(address, system)
    }
    transportInfo
  }

  def compress(msg: MessageLite): Array[Byte] = {
    val bos = new ByteArrayOutputStream(BufferSize)
    val zip = new GZIPOutputStream(bos)
    try msg.writeTo(zip)
    finally zip.close()
    bos.toByteArray
  }

  def decompress(bytes: Array[Byte]): Array[Byte] = {
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val out = new ByteArrayOutputStream()
    val buffer = new Array[Byte](BufferSize)

    @tailrec def readChunk(): Unit = in.read(buffer) match {
      case -1 => ()
      case n =>
        out.write(buffer, 0, n)
        readChunk()
    }

    try readChunk()
    finally in.close()
    out.toByteArray
  }

  def addressToProto(address: Address): dm.Address.Builder = address match {
    case Address(_, _, Some(host), Some(port)) =>
      dm.Address.newBuilder().setHostname(host).setPort(port)
    case _ => throw new IllegalArgumentException(s"Address [${address}] could not be serialized: host or port missing.")
  }

  def addressFromProto(address: dm.Address): Address =
    Address(addressProtocol, system.name, address.getHostname, address.getPort)

  def uniqueAddressToProto(uniqueAddress: UniqueAddress): dm.UniqueAddress.Builder =
    dm.UniqueAddress
      .newBuilder()
      .setAddress(addressToProto(uniqueAddress.address))
      .setUid(uniqueAddress.longUid.toInt)
      .setUid2((uniqueAddress.longUid >> 32).toInt)

  def uniqueAddressFromProto(uniqueAddress: dm.UniqueAddress): UniqueAddress =
    UniqueAddress(addressFromProto(uniqueAddress.getAddress), if (uniqueAddress.hasUid2) {
      // new remote node join the two parts of the long uid back
      (uniqueAddress.getUid2.toLong << 32) | (uniqueAddress.getUid & 0XFFFFFFFFL)
    } else {
      // old remote node
      uniqueAddress.getUid.toLong
    })

  def versionVectorToProto(versionVector: VersionVector): dm.VersionVector = {
    val b = dm.VersionVector.newBuilder()
    versionVector.versionsIterator.foreach {
      case (node, value) =>
        b.addEntries(dm.VersionVector.Entry.newBuilder().setNode(uniqueAddressToProto(node)).setVersion(value))
    }
    b.build()
  }

  def versionVectorFromBinary(bytes: Array[Byte]): VersionVector =
    versionVectorFromProto(dm.VersionVector.parseFrom(bytes))

  def versionVectorFromProto(versionVector: dm.VersionVector): VersionVector = {
    val entries = versionVector.getEntriesList
    if (entries.isEmpty)
      VersionVector.empty
    else if (entries.size == 1)
      VersionVector(uniqueAddressFromProto(entries.get(0).getNode), entries.get(0).getVersion)
    else {
      val versions: TreeMap[UniqueAddress, Long] =
        scala.collection.immutable.TreeMap.from(versionVector.getEntriesList.asScala.iterator.map(entry =>
          uniqueAddressFromProto(entry.getNode) -> entry.getVersion))
      VersionVector(versions)
    }
  }

  def resolveActorRef(path: String): ActorRef =
    system.provider.resolveActorRef(path)

  def otherMessageToProto(msg: Any): dm.OtherMessage = {
    def buildOther(): dm.OtherMessage = {
      val m = msg.asInstanceOf[AnyRef]
      val msgSerializer = serialization.findSerializerFor(m)
      val builder = dm.OtherMessage
        .newBuilder()
        .setEnclosedMessage(ByteStringUtils.toProtoByteStringUnsafe(msgSerializer.toBinary(m)))
        .setSerializerId(msgSerializer.identifier)

      val ms = Serializers.manifestFor(msgSerializer, m)
      if (ms.nonEmpty) builder.setMessageManifest(ByteString.copyFromUtf8(ms))

      builder.build()
    }

    // Serialize actor references with full address information (defaultAddress).
    // When sending remote messages currentTransportInformation is already set,
    // but when serializing for digests or DurableStore it must be set here.
    val oldInfo = Serialization.currentTransportInformation.value
    try {
      if (oldInfo eq null)
        Serialization.currentTransportInformation.value = system.provider.serializationInformation
      buildOther()
    } finally Serialization.currentTransportInformation.value = oldInfo

  }

  def otherMessageFromBinary(bytes: Array[Byte]): AnyRef =
    otherMessageFromProto(dm.OtherMessage.parseFrom(bytes))

  def otherMessageFromProto(other: dm.OtherMessage): AnyRef = {
    val manifest = if (other.hasMessageManifest) other.getMessageManifest.toStringUtf8 else ""
    serialization.deserialize(other.getEnclosedMessage.toByteArray, other.getSerializerId, manifest).get
  }

}

/**
 * Java API
 */
abstract class AbstractSerializationSupport extends JSerializer with SerializationSupport
