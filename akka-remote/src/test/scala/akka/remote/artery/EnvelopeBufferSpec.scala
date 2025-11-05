/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.nio.{ ByteBuffer, ByteOrder }

import akka.actor._
import akka.remote.artery.compress.{ CompressionTable, CompressionTestUtils, InboundCompressions }
import akka.serialization.Serialization
import akka.testkit.AkkaSpec
import akka.util.{ ByteString, OptionVal }

class EnvelopeBufferSpec extends AkkaSpec {
  import CompressionTestUtils._

  object TestCompressor extends InboundCompressions {
    val refToIdx: Map[ActorRef, Int] = Map(
      minimalRef("compressable0") -> 0,
      minimalRef("compressable1") -> 1,
      minimalRef("reallylongcompressablestring") -> 2)
    val idxToRef: Map[Int, ActorRef] = refToIdx.map(_.swap)

    val serializerToIdx = Map("serializer0" -> 0, "serializer1" -> 1)
    val idxToSer = serializerToIdx.map(_.swap)

    val manifestToIdx = Map("manifest0" -> 0, "manifest1" -> 1)
    val idxToManifest = manifestToIdx.map(_.swap)

    val outboundActorRefTable: CompressionTable[ActorRef] =
      CompressionTable(17L, version = 28.toByte, refToIdx)

    val outboundClassManifestTable: CompressionTable[String] =
      CompressionTable(17L, version = 35.toByte, manifestToIdx)

    override def hitActorRef(originUid: Long, remote: Address, ref: ActorRef, n: Int): Unit = ()
    override def decompressActorRef(originUid: Long, tableVersion: Byte, idx: Int): OptionVal[ActorRef] =
      OptionVal(idxToRef(idx))
    override def confirmActorRefCompressionAdvertisement(originUid: Long, tableVersion: Byte): Unit = ()

    override def hitClassManifest(originUid: Long, remote: Address, manifest: String, n: Int): Unit = ()
    override def decompressClassManifest(originUid: Long, tableVersion: Byte, idx: Int): OptionVal[String] =
      OptionVal(idxToManifest(idx))
    override def confirmClassManifestCompressionAdvertisement(originUid: Long, tableVersion: Byte): Unit = ()
    override def close(originUid: Long): Unit = ()

    override def runNextActorRefAdvertisement(): Unit = ???
    override def runNextClassManifestAdvertisement(): Unit = ???
    override def currentOriginUids: Set[Long] = ???
  }

  val version = ArteryTransport.HighestVersion

  "EnvelopeBuffer" must {
    val headerOut = HeaderBuilder.in(TestCompressor)
    val headerIn = HeaderBuilder.out()

    headerIn.setOutboundActorRefCompression(TestCompressor.outboundActorRefTable)
    headerIn.setOutboundClassManifestCompression(TestCompressor.outboundClassManifestTable)

    val byteBuffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)
    val envelope = new EnvelopeBuffer(byteBuffer)

    val originUid = 1L

    "be able to encode and decode headers with compressed literals" in {
      headerIn.setVersion(version)
      headerIn.setUid(42)
      headerIn.setSerializer(4)
      headerIn.setRecipientActorRef(minimalRef("compressable1"))
      headerIn.setSenderActorRef(minimalRef("compressable0"))

      headerIn.setManifest("manifest1")

      envelope.writeHeader(headerIn)
      envelope.byteBuffer
        .position() should ===(EnvelopeBuffer.MetadataContainerAndLiteralSectionOffset) // Fully compressed header

      envelope.byteBuffer.flip()
      envelope.parseHeader(headerOut)

      headerOut.version should ===(version)
      headerOut.uid should ===(42L)
      headerOut.inboundActorRefCompressionTableVersion should ===(28.toByte)
      headerOut.inboundClassManifestCompressionTableVersion should ===(35.toByte)
      headerOut.serializer should ===(4)
      headerOut.senderActorRef(originUid).get.path.toSerializationFormat should ===(
        "akka://EnvelopeBufferSpec/compressable0")
      headerOut.senderActorRefPath should ===(OptionVal.None)
      headerOut.recipientActorRef(originUid).get.path.toSerializationFormat should ===(
        "akka://EnvelopeBufferSpec/compressable1")
      headerOut.recipientActorRefPath should ===(OptionVal.None)
      headerOut.manifest(originUid).get should ===("manifest1")
    }

    "be able to encode and decode headers with uncompressed literals" in {
      val senderRef = minimalRef("uncompressable0")
      val recipientRef = minimalRef("uncompressable11")

      headerIn.setVersion(version)
      headerIn.setUid(42)
      headerIn.setSerializer(4)
      headerIn.setSenderActorRef(senderRef)
      headerIn.setRecipientActorRef(recipientRef)
      headerIn.setManifest("uncompressable3333")

      val expectedHeaderLength =
        EnvelopeBuffer.MetadataContainerAndLiteralSectionOffset + // Constant header part
        2 + lengthOfSerializedActorRefPath(senderRef) + // Length field + literal
        2 + lengthOfSerializedActorRefPath(recipientRef) + // Length field + literal
        2 + "uncompressable3333".length // Length field + literal

      envelope.writeHeader(headerIn)
      envelope.byteBuffer.position() should ===(expectedHeaderLength)

      envelope.byteBuffer.flip()
      envelope.parseHeader(headerOut)

      headerOut.version should ===(version)
      headerOut.uid should ===(42L)
      headerOut.serializer should ===(4)
      headerOut.senderActorRefPath should ===(OptionVal.Some("akka://EnvelopeBufferSpec/uncompressable0"))
      headerOut.senderActorRef(originUid) should ===(OptionVal.None)
      headerOut.recipientActorRefPath should ===(OptionVal.Some("akka://EnvelopeBufferSpec/uncompressable11"))
      headerOut.recipientActorRef(originUid) should ===(OptionVal.None)
      headerOut.manifest(originUid).get should ===("uncompressable3333")
    }

    "be able to encode and decode headers with mixed literals" in {
      val recipientRef = minimalRef("uncompressable1")

      headerIn.setVersion(version)
      headerIn.setUid(42)
      headerIn.setSerializer(4)
      headerIn.setSenderActorRef(minimalRef("reallylongcompressablestring"))
      headerIn.setRecipientActorRef(recipientRef)
      headerIn.setManifest("manifest1")

      envelope.writeHeader(headerIn)
      envelope.byteBuffer.position() should ===(
        EnvelopeBuffer.MetadataContainerAndLiteralSectionOffset +
        2 + lengthOfSerializedActorRefPath(recipientRef))

      envelope.byteBuffer.flip()
      envelope.parseHeader(headerOut)

      headerOut.version should ===(version)
      headerOut.uid should ===(42L)
      headerOut.serializer should ===(4)
      headerOut.senderActorRef(originUid).get.path.toSerializationFormat should ===(
        "akka://EnvelopeBufferSpec/reallylongcompressablestring")
      headerOut.senderActorRefPath should ===(OptionVal.None)
      headerOut.recipientActorRefPath should ===(OptionVal.Some("akka://EnvelopeBufferSpec/uncompressable1"))
      headerOut.recipientActorRef(originUid) should ===(OptionVal.None)
      headerOut.manifest(originUid).get should ===("manifest1")

      val senderRef = minimalRef("uncompressable0")

      headerIn.setVersion(version)
      headerIn.setUid(Long.MinValue)
      headerIn.setSerializer(-1)
      headerIn.setSenderActorRef(senderRef)
      headerIn.setRecipientActorRef(minimalRef("reallylongcompressablestring"))
      headerIn.setManifest("longlonglongliteralmanifest")

      envelope.writeHeader(headerIn)
      envelope.byteBuffer.position() should ===(
        EnvelopeBuffer.MetadataContainerAndLiteralSectionOffset +
        2 + lengthOfSerializedActorRefPath(senderRef) +
        2 + "longlonglongliteralmanifest".length)

      envelope.byteBuffer.flip()
      envelope.parseHeader(headerOut)

      headerOut.version should ===(version)
      headerOut.uid should ===(Long.MinValue)
      headerOut.serializer should ===(-1)
      headerOut.senderActorRefPath should ===(OptionVal.Some("akka://EnvelopeBufferSpec/uncompressable0"))
      headerOut.senderActorRef(originUid) should ===(OptionVal.None)
      headerOut.recipientActorRef(originUid).get.path.toSerializationFormat should ===(
        "akka://EnvelopeBufferSpec/reallylongcompressablestring")
      headerOut.recipientActorRefPath should ===(OptionVal.None)
      headerOut.manifest(originUid).get should ===("longlonglongliteralmanifest")
    }

    "be able to encode and decode headers with mixed literals and payload" in {
      val payload = ByteString("Hello Artery!")

      headerIn.setVersion(version)
      headerIn.setUid(42)
      headerIn.setSerializer(4)
      headerIn.setSenderActorRef(minimalRef("reallylongcompressablestring"))
      headerIn.setRecipientActorRef(minimalRef("uncompressable1"))
      headerIn.setManifest("manifest1")

      envelope.writeHeader(headerIn)
      envelope.byteBuffer.put(payload.toByteBuffer)
      envelope.byteBuffer.flip()

      envelope.parseHeader(headerOut)

      headerOut.version should ===(version)
      headerOut.uid should ===(42L)
      headerOut.serializer should ===(4)
      headerOut.senderActorRef(originUid).get.path.toSerializationFormat should ===(
        "akka://EnvelopeBufferSpec/reallylongcompressablestring")
      headerOut.senderActorRefPath should ===(OptionVal.None)
      headerOut.recipientActorRefPath should ===(OptionVal.Some("akka://EnvelopeBufferSpec/uncompressable1"))
      headerOut.recipientActorRef(originUid) should ===(OptionVal.None)
      headerOut.manifest(originUid).get should ===("manifest1")

      ByteString.fromByteBuffer(envelope.byteBuffer) should ===(payload)
    }

    "produce an identical copy" in {
      val senderRef = minimalRef("uncompressable0")
      val recipientRef = minimalRef("uncompressable11")

      headerIn.setVersion(version)
      headerIn.setUid(42)
      headerIn.setSerializer(4)
      headerIn.setSenderActorRef(senderRef)
      headerIn.setRecipientActorRef(recipientRef)
      headerIn.setManifest("uncompressable3333")

      envelope.writeHeader(headerIn)

      val copy = envelope.copy()
      val position = copy.byteBuffer.position()
      position should ===(envelope.byteBuffer.position())
      copy.byteBuffer.order() should ===(envelope.byteBuffer.order())
      copy.byteBuffer should ===(envelope.byteBuffer)
      (copy.byteBuffer shouldNot be).theSameInstanceAs(envelope.byteBuffer)
    }
  }

  def lengthOfSerializedActorRefPath(ref: ActorRef): Int =
    Serialization.serializedActorPath(ref).length
}
