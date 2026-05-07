/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.internal

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.Base64
import java.util.UUID

import scala.util.control.NonFatal

import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.Sequence
import akka.persistence.query.TimeBasedUUID
import akka.persistence.query.TimestampOffset
import akka.persistence.query.TimestampOffsetBySlice
import akka.persistence.query.internal.protobuf.QueryMessages
import akka.persistence.query.typed.EventEnvelope
import akka.remote.serialization.WrappedPayloadSupport.{ deserializePayload, payloadBuilder }
import akka.serialization.BaseSerializer
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.serialization.Serializers
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 *
 * Serializer for [[EventEnvelope]] and [[Offset]].
 */
@InternalApi private[akka] final class QuerySerializer(val system: akka.actor.ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  private val log = Logging(system, classOf[QuerySerializer])

  private lazy val serialization = SerializationExtension(system)

  private final val EventEnvelopeManifest = "a"

  private final val SequenceOffsetManifest = "SEQ"
  private final val TimeBasedUUIDOffsetManifest = "TBU"
  private final val TimestampOffsetManifest = "TSO"
  private final val TimestampOffsetBySliceManifest = "TBS"
  private final val NoOffsetManifest = "NO"

  private val manifestSeparator = ':'
  // persistenceId and timestamp must not contain this separator char
  private val timestampOffsetSeparator = ';'
  // doubling the `timestampOffsetSeparator` for separator between entries
  private val timestampOffsetBySliceSeparator = ";;"

  override def manifest(o: AnyRef): String = o match {
    case _: EventEnvelope[_] => EventEnvelopeManifest
    case offset: Offset      => toStorageRepresentation(offset)._2
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case env: EventEnvelope[_] =>
      val builder = QueryMessages.EventEnvelope.newBuilder()

      val (offset, offsetManifest) = toStorageRepresentation(env.offset)

      builder
        .setPersistenceId(env.persistenceId)
        .setEntityType(env.entityType)
        .setSlice(env.slice)
        .setSequenceNr(env.sequenceNr)
        .setTimestamp(env.timestamp)
        .setOffset(offset)
        .setOffsetManifest(offsetManifest)
        .setFiltered(env.filtered)
        .setSource(env.source)

      if (env.tags.nonEmpty) {
        builder.addAllTags(env.tags.asJava)
      }

      env.eventOption.foreach(event => builder.setEvent(payloadBuilder(event, serialization, log)))
      env.internalEventMetadata.foreach(meta => builder.setMetadata(payloadBuilder(meta, serialization, log)))

      builder.build().toByteArray()

    case offset: Offset =>
      toStorageRepresentation(offset)._1.getBytes(UTF_8)

    case _ =>
      throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case EventEnvelopeManifest =>
      val env = QueryMessages.EventEnvelope.parseFrom(bytes)

      val offset = fromStorageRepresentation(env.getOffset, env.getOffsetManifest)

      val eventOption =
        if (env.hasEvent) Option(deserializePayload(env.getEvent, serialization))
        else None
      val metaOption =
        if (env.hasMetadata) Option(deserializePayload(env.getMetadata, serialization))
        else None

      val filtered = env.hasFiltered && env.getFiltered
      val source = if (env.hasSource) env.getSource else ""
      val tags =
        if (env.getTagsList.isEmpty) Set.empty[String]
        else env.getTagsList.iterator.asScala.toSet

      new EventEnvelope(
        offset,
        env.getPersistenceId,
        env.getSequenceNr,
        eventOption,
        env.getTimestamp,
        metaOption,
        env.getEntityType,
        env.getSlice,
        filtered,
        source,
        tags)

    case _ =>
      fromStorageRepresentation(new String(bytes, UTF_8), manifest)
  }

  /**
   * Deserialize an offset from a stored string representation and manifest.
   * The offset is converted from its string representation to its real type.
   */
  private def fromStorageRepresentation(offsetStr: String, manifest: String): Offset = {
    manifest match {
      case TimestampOffsetManifest        => timestampOffsetFromStorageRepresentation(offsetStr)
      case TimestampOffsetBySliceManifest => timestampOffsetBySliceFromStorageRepresentation(offsetStr)
      case SequenceOffsetManifest         => Offset.sequence(offsetStr.toLong)
      case TimeBasedUUIDOffsetManifest    => Offset.timeBasedUUID(UUID.fromString(offsetStr))
      case NoOffsetManifest               => NoOffset
      case _ =>
        manifest.split(manifestSeparator) match {
          case Array(serializerIdStr, serializerManifest) =>
            val serializerId = serializerIdStr.toInt
            val bytes = Base64.getDecoder.decode(offsetStr)
            serialization.deserialize(bytes, serializerId, serializerManifest).get match {
              case offset: Offset => offset
              case other =>
                throw new NotSerializableException(
                  s"Unimplemented deserialization of offset with serializerId [$serializerId] and manifest [$manifest] " +
                  s"in [${getClass.getName}]. [${other.getClass.getName}] is not an Offset.")
            }
          case _ =>
            throw new NotSerializableException(
              s"Unimplemented deserialization of offset with manifest [$manifest] " +
              s"in [${getClass.getName}]. [$manifest] doesn't contain two parts.")
        }
    }
  }

  /**
   * Convert the offset to a tuple (String, String) where the first element is
   * the String representation of the offset and the second element is its manifest.
   */
  private def toStorageRepresentation(offset: Offset): (String, String) = {
    offset match {
      case t: TimestampOffset => (timestampOffsetToStorageRepresentation(t), TimestampOffsetManifest)
      case tbs: TimestampOffsetBySlice =>
        (timestampOffsetBySliceToStorageRepresentation(tbs), TimestampOffsetBySliceManifest)
      case seq: Sequence      => (seq.value.toString, SequenceOffsetManifest)
      case tbu: TimeBasedUUID => (tbu.value.toString, TimeBasedUUIDOffsetManifest)
      case NoOffset           => ("", NoOffsetManifest)
      case _ =>
        val obj = offset.asInstanceOf[AnyRef]
        val serializer = serialization.findSerializerFor(obj)
        val serializerId = serializer.identifier
        val serializerManifest = Serializers.manifestFor(serializer, obj)
        val bytes = serializer.toBinary(obj)
        val offsetStr = Base64.getEncoder.encodeToString(bytes)
        if (serializerManifest.contains(manifestSeparator))
          throw new IllegalArgumentException(
            s"Serializer manifest [$serializerManifest] for " +
            s"offset [${offset.getClass.getName}] must not contain [$manifestSeparator] character.")
        (offsetStr, s"$serializerId$manifestSeparator$serializerManifest")
    }
  }

  private def timestampOffsetFromStorageRepresentation(str: String): TimestampOffset = {
    try {
      str.split(timestampOffsetSeparator) match {
        case Array(timestamp, readTimestamp, pid, seqNr) =>
          // optimized for the normal case
          TimestampOffset(Instant.parse(timestamp), Instant.parse(readTimestamp), Map(pid -> seqNr.toLong))
        case Array(timestamp) =>
          TimestampOffset(Instant.parse(timestamp), Map.empty)
        case Array(timestamp, readTimestamp) =>
          TimestampOffset(Instant.parse(timestamp), Instant.parse(readTimestamp), Map.empty)
        case parts =>
          val seen = parts.toList
            .drop(2)
            .grouped(2)
            .map {
              case pid :: seqNr :: Nil => pid -> seqNr.toLong
              case _ =>
                throw new IllegalArgumentException(
                  s"Invalid representation of Map(pid -> seqNr) [${parts.toList.drop(1).mkString(",")}]")
            }
            .toMap
          TimestampOffset(Instant.parse(parts(0)), Instant.parse(parts(1)), seen)
      }
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Unexpected serialized TimestampOffset format [$str].", e)
    }
  }

  private def timestampOffsetToStorageRepresentation(offset: TimestampOffset): String = {
    def checkSeparator(pid: String): Unit =
      if (pid.contains(timestampOffsetSeparator))
        throw new IllegalArgumentException(
          s"persistenceId [$pid] in offset [$offset] " +
          s"must not contain [$timestampOffsetSeparator] character")

    val str = new java.lang.StringBuilder
    str.append(offset.timestamp).append(timestampOffsetSeparator).append(offset.readTimestamp)
    if (offset.seen.size == 1) {
      // optimized for the normal case
      val pid = offset.seen.head._1
      checkSeparator(pid)
      val seqNr = offset.seen.head._2
      str.append(timestampOffsetSeparator).append(pid).append(timestampOffsetSeparator).append(seqNr)
    } else if (offset.seen.nonEmpty) {
      offset.seen.toList.sortBy(_._1).foreach {
        case (pid, seqNr) =>
          checkSeparator(pid)
          str.append(timestampOffsetSeparator).append(pid).append(timestampOffsetSeparator).append(seqNr)
      }
    }
    str.toString
  }

  private def timestampOffsetBySliceFromStorageRepresentation(str: String): TimestampOffsetBySlice = {
    try {
      val offsets =
        if (str.isEmpty) Map.empty[Int, TimestampOffset]
        else
          str
            .split(timestampOffsetBySliceSeparator)
            .map { entry =>
              val sliceSeparator = entry.indexOf(timestampOffsetSeparator)
              val slice = entry.substring(0, sliceSeparator).toInt
              val offset = timestampOffsetFromStorageRepresentation(entry.substring(sliceSeparator + 1))
              slice -> offset
            }
            .toMap
      TimestampOffsetBySlice(offsets)
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Unexpected serialized TimestampOffsetBySlice format [$str].", e)
    }
  }

  private def timestampOffsetBySliceToStorageRepresentation(offsetBySlice: TimestampOffsetBySlice): String = {
    val str = new java.lang.StringBuilder
    if (offsetBySlice.offsets.nonEmpty) {
      var first = true
      offsetBySlice.offsets.foreach {
        case (slice, offset) =>
          if (!first) str.append(timestampOffsetBySliceSeparator)
          str.append(slice).append(timestampOffsetSeparator).append(timestampOffsetToStorageRepresentation(offset))
          first = false
      }
    }
    str.toString
  }

}
