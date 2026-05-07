/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

import java.util.Optional

import scala.reflect.ClassTag
import scala.runtime.AbstractFunction4

import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.persistence.CompositeMetadata
import akka.util.HashCode

// for binary compatibility (used to be a case class)
object EventEnvelope extends AbstractFunction4[Offset, String, Long, Any, EventEnvelope] {
  def apply(offset: Offset, persistenceId: String, sequenceNr: Long, event: Any, timestamp: Long): EventEnvelope =
    new EventEnvelope(offset, persistenceId, sequenceNr, event, timestamp, None)

  def apply(
      offset: Offset,
      persistenceId: String,
      sequenceNr: Long,
      event: Any,
      timestamp: Long,
      meta: Option[Any]): EventEnvelope =
    new EventEnvelope(offset, persistenceId, sequenceNr, event, timestamp, meta)

  @deprecated("for binary compatibility", "2.6.2")
  override def apply(offset: Offset, persistenceId: String, sequenceNr: Long, event: Any): EventEnvelope =
    new EventEnvelope(offset, persistenceId, sequenceNr, event)

  def unapply(arg: EventEnvelope): Option[(Offset, String, Long, Any)] =
    Some((arg.offset, arg.persistenceId, arg.sequenceNr, arg.event))

}

/**
 * Event wrapper adding meta data for the events in the result stream of
 * [[akka.persistence.query.scaladsl.EventsByTagQuery]] query, or similar queries.
 *
 * The `timestamp` is the time the event was stored, in milliseconds since midnight, January 1, 1970 UTC
 * (same as `System.currentTimeMillis`).
 */
final class EventEnvelope(
    val offset: Offset,
    val persistenceId: String,
    val sequenceNr: Long,
    val event: Any,
    val timestamp: Long,
    @deprecatedName("eventMetadata")
    _eventMetadata: Option[Any])
    extends Product4[Offset, String, Long, Any]
    with Serializable {

  @deprecated("for binary compatibility", "2.6.2")
  def this(offset: Offset, persistenceId: String, sequenceNr: Long, event: Any) =
    this(offset, persistenceId, sequenceNr, event, 0L, None)

  // bin compat 2.6.7
  def this(offset: Offset, persistenceId: String, sequenceNr: Long, event: Any, timestamp: Long) =
    this(offset, persistenceId, sequenceNr, event, timestamp, None)

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
  def removeMetadata(metadataType: Class[_]): EventEnvelope =
    removeMetadata(ClassTag(metadataType))

  /**
   * Scala API: Drop metadata of the given type if present
   */
  def removeMetadata[M](implicit classTag: ClassTag[M]): EventEnvelope = {
    internalEventMetadata match {
      case Some(c: CompositeMetadata) =>
        val filtered = c.entries.filterNot(_.getClass == classTag.runtimeClass)
        if (filtered eq c.entries) this
        else internalCopy(internalEventMetadata = Some(CompositeMetadata(filtered)))
      case Some(m) if m.getClass == classTag.runtimeClass => internalCopy(internalEventMetadata = None)
      case _                                              => this
    }
  }

  /**
   * INTERNAL API
   */
  @InternalStableApi private[akka] def internalEventMetadata: Option[Any] =
    _eventMetadata

  override def hashCode(): Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, offset)
    result = HashCode.hash(result, persistenceId)
    result = HashCode.hash(result, sequenceNr)
    result = HashCode.hash(result, event)
    result
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: EventEnvelope =>
      offset == other.offset && persistenceId == other.persistenceId && sequenceNr == other.sequenceNr &&
      event == other.event // timestamp && metadata not included in equals for backwards compatibility
    case _ => false
  }

  override def toString: String = {
    val eventStr = event.getClass.getName
    val metaStr = _eventMetadata match {
      case Some(CompositeMetadata(entries)) => entries.map(_.getClass.getName).mkString("[", ",", "]")
      case Some(other)                      => other.getClass.getName
      case None                             => ""
    }
    s"EventEnvelope($offset,$persistenceId,$sequenceNr,$eventStr,$timestamp,$metaStr)"
  }

  // for binary compatibility (used to be a case class)
  def copy(
      offset: Offset = this.offset,
      persistenceId: String = this.persistenceId,
      sequenceNr: Long = this.sequenceNr,
      event: Any = this.event): EventEnvelope =
    new EventEnvelope(offset, persistenceId, sequenceNr, event, timestamp, this._eventMetadata)

  private def internalCopy(
      offset: Offset = this.offset,
      persistenceId: String = this.persistenceId,
      sequenceNr: Long = this.sequenceNr,
      event: Any = this.event,
      internalEventMetadata: Option[Any]): EventEnvelope =
    new EventEnvelope(offset, persistenceId, sequenceNr, event, timestamp, internalEventMetadata)

  @InternalApi
  private[akka] def withMetadata(metadata: Any): EventEnvelope =
    _eventMetadata match {
      case Some(c: CompositeMetadata) =>
        internalCopy(internalEventMetadata = Some(CompositeMetadata(metadata +: c.entries)))
      case Some(other) =>
        internalCopy(internalEventMetadata = Some(CompositeMetadata(metadata :: other :: Nil)))
      case None =>
        internalCopy(internalEventMetadata = Option(metadata))
    }

  // Product4, for binary compatibility (used to be a case class)
  override def productPrefix = "EventEnvelope"
  override def _1: Offset = offset
  override def _2: String = persistenceId
  override def _3: Long = sequenceNr
  override def _4: Any = event
  override def canEqual(that: Any): Boolean = that.isInstanceOf[EventEnvelope]

}
