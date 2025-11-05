/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.query

import java.time.Instant
import java.util.UUID

import akka.annotation.ApiMayChange
import akka.util.UUIDComparator

object Offset {

  // factories to aid discoverability
  def noOffset: Offset = NoOffset
  def sequence(value: Long): Offset = Sequence(value)
  def timeBasedUUID(uuid: UUID): Offset = TimeBasedUUID(uuid)
  def timestamp(instant: Instant): TimestampOffset = TimestampOffset(instant, instant, Map.empty)

}

abstract class Offset

/**
 * Corresponds to an ordered sequence number for the events. Note that the corresponding
 * offset of each event is provided in the [[akka.persistence.query.EventEnvelope]],
 * which makes it possible to resume the stream at a later point from a given offset.
 *
 * The `offset` is exclusive, i.e. the event with the exact same sequence number will not be included
 * in the returned stream. This means that you can use the offset that is returned in `EventEnvelope`
 * as the `offset` parameter in a subsequent query.
 */
final case class Sequence(value: Long) extends Offset with Ordered[Sequence] {
  override def compare(that: Sequence): Int = value.compare(that.value)
}

/**
 * Corresponds to an ordered unique identifier of the events. Note that the corresponding
 * offset of each event is provided in the [[akka.persistence.query.EventEnvelope]],
 * which makes it possible to resume the stream at a later point from a given offset.
 *
 * The `offset` is exclusive, i.e. the event with the exact same sequence number will not be included
 * in the returned stream. This means that you can use the offset that is returned in `EventEnvelope`
 * as the `offset` parameter in a subsequent query.
 */
final case class TimeBasedUUID(value: UUID) extends Offset with Ordered[TimeBasedUUID] {
  if (value == null || value.version != 1) {
    throw new IllegalArgumentException("UUID " + value + " is not a time-based UUID")
  }

  override def compare(other: TimeBasedUUID): Int = UUIDComparator.comparator.compare(value, other.value)
}

object TimestampOffset {
  val Zero: TimestampOffset = TimestampOffset(Instant.EPOCH, Instant.EPOCH, Map.empty)

  def apply(timestamp: Instant, seen: Map[String, Long]): TimestampOffset =
    TimestampOffset(timestamp, Instant.EPOCH, seen)

  /**
   * Try to convert the Offset to a TimestampOffset. Epoch timestamp is used for `NoOffset`.
   */
  def toTimestampOffset(offset: Offset): TimestampOffset = {
    offset match {
      case t: TimestampOffset => t
      case NoOffset           => TimestampOffset.Zero
      case null               => throw new IllegalArgumentException("Offset must not be null")
      case other =>
        throw new IllegalArgumentException(
          s"Supported offset types are TimestampOffset and NoOffset, " +
          s"received ${other.getClass.getName}")
    }
  }
}

/**
 * Timestamp based offset. Since there can be several events for the same timestamp it keeps
 * track of what sequence nrs for every persistence id that have been seen at this specific timestamp.
 *
 * The `offset` is exclusive, i.e. the event with the exact same sequence number will not be included
 * in the returned stream. This means that you can use the offset that is returned in `EventEnvelope`
 * as the `offset` parameter in a subsequent query.
 *
 * API May Change
 *
 * @param timestamp
 *   time when the event was stored, microsecond granularity database timestamp
 * @param readTimestamp
 *   time when the event was read, microsecond granularity database timestamp
 * @param seen
 *   List of sequence nrs for every persistence id seen at this timestamp
 */
@ApiMayChange
final case class TimestampOffset(timestamp: Instant, readTimestamp: Instant, seen: Map[String, Long]) extends Offset {

  /** Java API */
  def getSeen(): java.util.Map[String, java.lang.Long] = {
    import akka.util.ccompat.JavaConverters._
    seen.map { case (pid, seqNr) => pid -> java.lang.Long.valueOf(seqNr) }.asJava
  }

  override def hashCode(): Int = timestamp.hashCode()

  override def equals(obj: Any): Boolean =
    obj match {
      case other: TimestampOffset => timestamp == other.timestamp && seen == other.seen
      case _                      => false
    }
}

/**
 * Used when retrieving all events.
 */
case object NoOffset extends Offset {

  /**
   * Java API:
   */
  def getInstance: Offset = this
}
