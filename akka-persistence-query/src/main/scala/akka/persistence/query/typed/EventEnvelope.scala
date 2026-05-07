/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed

import java.util.{ Set => JSet }
import java.util.Optional

import akka.persistence.query.Offset
import akka.util.HashCode
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import akka.annotation.InternalStableApi
import akka.persistence.CompositeMetadata

object EventEnvelope {

  def apply[Event](
      offset: Offset,
      persistenceId: String,
      sequenceNr: Long,
      event: Event,
      timestamp: Long,
      entityType: String,
      slice: Int,
      filtered: Boolean,
      source: String,
      tags: Set[String]): EventEnvelope[Event] =
    new EventEnvelope(
      offset,
      persistenceId,
      sequenceNr,
      Option(event),
      timestamp,
      None,
      entityType,
      slice,
      filtered,
      source,
      tags)

  def apply[Event](
      offset: Offset,
      persistenceId: String,
      sequenceNr: Long,
      event: Event,
      timestamp: Long,
      entityType: String,
      slice: Int,
      filtered: Boolean,
      source: String): EventEnvelope[Event] =
    new EventEnvelope(
      offset,
      persistenceId,
      sequenceNr,
      Option(event),
      timestamp,
      None,
      entityType,
      slice,
      filtered,
      source)

  def apply[Event](
      offset: Offset,
      persistenceId: String,
      sequenceNr: Long,
      event: Event,
      timestamp: Long,
      entityType: String,
      slice: Int): EventEnvelope[Event] =
    apply(offset, persistenceId, sequenceNr, event, timestamp, entityType, slice, filtered = false, source = "")

  def create[Event](
      offset: Offset,
      persistenceId: String,
      sequenceNr: Long,
      event: Event,
      timestamp: Long,
      entityType: String,
      slice: Int,
      filtered: Boolean,
      source: String,
      tags: JSet[String]): EventEnvelope[Event] =
    apply(offset, persistenceId, sequenceNr, event, timestamp, entityType, slice, filtered, source, tags.asScala.toSet)

  def create[Event](
      offset: Offset,
      persistenceId: String,
      sequenceNr: Long,
      event: Event,
      timestamp: Long,
      entityType: String,
      slice: Int,
      filtered: Boolean,
      source: String): EventEnvelope[Event] =
    apply(offset, persistenceId, sequenceNr, event, timestamp, entityType, slice, filtered, source)

  def create[Event](
      offset: Offset,
      persistenceId: String,
      sequenceNr: Long,
      event: Event,
      timestamp: Long,
      entityType: String,
      slice: Int): EventEnvelope[Event] =
    create(offset, persistenceId, sequenceNr, event, timestamp, entityType, slice, filtered = false, source = "")

  def unapply[Event](arg: EventEnvelope[Event]): Option[(Offset, String, Long, Option[Event], Long)] =
    Some((arg.offset, arg.persistenceId, arg.sequenceNr, arg.eventOption, arg.timestamp))
}

/**
 * Event wrapper adding meta data for the events in the result stream of
 * [[akka.persistence.query.typed.scaladsl.EventsBySliceQuery]] query, or similar queries.
 *
 * If the `event` is not defined it has not been loaded yet. It can be loaded with
 * [[akka.persistence.query.typed.scaladsl.LoadEventQuery]].
 *
 * The `timestamp` is the time the event was stored, in milliseconds since midnight, January 1, 1970 UTC (same as
 * `System.currentTimeMillis`).
 *
 * It is an improved `EventEnvelope` compared to [[akka.persistence.query.EventEnvelope]].
 */
final class EventEnvelope[Event](
    val offset: Offset,
    val persistenceId: String,
    val sequenceNr: Long,
    val eventOption: Option[Event],
    val timestamp: Long,
    @deprecatedName("eventMetadata")
    _eventMetadata: Option[Any],
    val entityType: String,
    val slice: Int,
    val filtered: Boolean,
    val source: String,
    val tags: Set[String]) {

  def this(
      offset: Offset,
      persistenceId: String,
      sequenceNr: Long,
      eventOption: Option[Event],
      timestamp: Long,
      eventMetadata: Option[Any],
      entityType: String,
      slice: Int,
      filtered: Boolean,
      source: String) =
    this(
      offset,
      persistenceId,
      sequenceNr,
      eventOption,
      timestamp,
      eventMetadata,
      entityType,
      slice,
      filtered,
      source,
      tags = Set.empty)

  def this(
      offset: Offset,
      persistenceId: String,
      sequenceNr: Long,
      eventOption: Option[Event],
      timestamp: Long,
      eventMetadata: Option[Any],
      entityType: String,
      slice: Int) =
    this(
      offset,
      persistenceId,
      sequenceNr,
      eventOption,
      timestamp,
      eventMetadata,
      entityType,
      slice,
      filtered = false,
      source = "")

  def event: Event =
    eventOption match {
      case Some(evt) => evt
      case None =>
        if (filtered) {
          throw new IllegalStateException(
            "Event was filtered so payload is not present. Use eventOption to handle more gracefully.")
        } else {
          throw new IllegalStateException(
            "Event was not loaded. Use eventOption and load the event on demand with LoadEventQuery.")
        }
    }

  /**
   * Java API
   */
  def getEvent(): Event =
    eventOption match {
      case Some(evt) => evt
      case None =>
        if (filtered) {
          throw new IllegalStateException(
            "Event was filtered so payload is not present. Use getOptionalEvent to handle more gracefully.")
        } else {
          throw new IllegalStateException(
            "Event was not loaded. Use getOptionalEvent and load the event on demand with LoadEventQuery.")
        }
    }

  /**
   * Java API
   */
  def getOptionalEvent(): Optional[Event] = {
    import scala.jdk.OptionConverters._
    eventOption.toJava
  }

  /**
   * Scala API
   */
  @deprecated("Use metadata with metadataType parameter")
  def eventMetadata: Option[Any] = {
    // For backwards compatibility this will use the metadata that was added last (ReplicatedEventMetaData)
    _eventMetadata.collect {
      case CompositeMetadata(entries) => entries.head
      case other                      => other
    }
  }

  /**
   * Java API
   */
  @deprecated("Use getMetadata with metadataType parameter")
  def getEventMetaData(): Optional[AnyRef] = {
    import scala.jdk.OptionConverters._
    eventMetadata.map(_.asInstanceOf[AnyRef]).toJava
  }

  /**
   * Scala API: The metadata of a given type that is associated with the event.
   */
  def metadata[M: ClassTag]: Option[M] =
    CompositeMetadata.extract[M](_eventMetadata)

  /**
   * Java API: The metadata of a given type that is associated with the event.
   */
  def getMetadata[M](metadataType: Class[M]): Optional[M] = {
    import scala.jdk.OptionConverters._
    implicit val ct: ClassTag[M] = ClassTag(metadataType)
    metadata.toJava
  }

  /**
   * Java API: Drop metadata of the given type if present
   */
  def removeMetadata(metadataType: Class[_]): EventEnvelope[Event] =
    removeMetadata(ClassTag(metadataType))

  /**
   * Scala API: Drop metadata of the given type if present
   */
  def removeMetadata[M](implicit classTag: ClassTag[M]): EventEnvelope[Event] = {
    internalEventMetadata match {
      case Some(c: CompositeMetadata) =>
        val filtered = c.entries.filterNot(_.getClass == classTag.runtimeClass)
        if (filtered eq c.entries) this
        else copy(eventMetadata = Some(CompositeMetadata(filtered)))
      case Some(m) if m.getClass == classTag.runtimeClass => copy(eventMetadata = None)
      case _                                              => this
    }
  }

  /**
   * INTERNAL API
   */
  @InternalStableApi private[akka] def internalEventMetadata: Option[Any] =
    _eventMetadata

  /**
   * Java API:
   */
  def getTags(): JSet[String] = tags.asJava

  /**
   * `entityType` and `slice` should be derived from the `persistenceId`, but must be explicitly defined
   * when changing the `persistenceId` of the envelope.
   * The `slice` should be calculated with [[akka.persistence.Persistence.sliceForPersistenceId]] for
   * the given `persistenceId`.
   * The `entityType` should be extracted from the `persistenceId` with
   * `akka.persistence.typed.PersistenceId.extractEntityType`.
   */
  def withPersistenceId(persistenceId: String, entityType: String, slice: Int): EventEnvelope[Event] =
    copy(persistenceId = persistenceId, entityType = entityType, slice = slice)

  def withEvent(event: Event): EventEnvelope[Event] =
    copy(eventOption = Option(event))

  def withEventOption(eventOption: Option[Event]): EventEnvelope[Event] =
    copy(eventOption = eventOption)

  def withTags(tags: Set[String]): EventEnvelope[Event] =
    copy(tags = tags)

  def withMetadata(metadata: Any): EventEnvelope[Event] = {
    _eventMetadata match {
      case Some(c: CompositeMetadata) =>
        copy(eventMetadata = Some(CompositeMetadata(metadata +: c.entries)))
      case Some(other) =>
        copy(eventMetadata = Some(CompositeMetadata(metadata :: other :: Nil)))
      case None =>
        copy(eventMetadata = Option(metadata))
    }

  }

  private def copy(
      offset: Offset = offset,
      persistenceId: String = persistenceId,
      sequenceNr: Long = sequenceNr,
      eventOption: Option[Event] = eventOption,
      timestamp: Long = timestamp,
      eventMetadata: Option[Any] = _eventMetadata,
      entityType: String = entityType,
      slice: Int = slice,
      filtered: Boolean = filtered,
      source: String = source,
      tags: Set[String] = tags): EventEnvelope[Event] = {
    new EventEnvelope(
      offset,
      persistenceId,
      sequenceNr,
      eventOption,
      timestamp,
      eventMetadata,
      entityType,
      slice,
      filtered,
      source,
      tags)
  }

  override def hashCode(): Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, offset)
    result = HashCode.hash(result, persistenceId)
    result = HashCode.hash(result, sequenceNr)
    result
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: EventEnvelope[_] =>
      offset == other.offset && persistenceId == other.persistenceId && sequenceNr == other.sequenceNr &&
      eventOption == other.eventOption && timestamp == other.timestamp && _eventMetadata == other.internalEventMetadata &&
      entityType == other.entityType && slice == other.slice && filtered == other.filtered && source == other.source &&
      tags == other.tags
    case _ => false
  }

  override def toString: String = {
    val eventStr = eventOption match {
      case Some(evt) => evt.getClass.getName
      case None      => ""
    }
    val metaStr = _eventMetadata match {
      case Some(CompositeMetadata(entries)) => entries.map(_.getClass.getName).mkString("[", ",", "]")
      case Some(other)                      => other.getClass.getName
      case None                             => ""
    }
    s"EventEnvelope($offset,$persistenceId,$sequenceNr,$eventStr,$timestamp,$metaStr,$entityType,$slice,$filtered,$source,${tags
      .mkString("[", ", ", "]")})"
  }
}
