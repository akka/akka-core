/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import scala.annotation.varargs
import scala.collection.immutable

/**
 * An [[EventAdapter]] is both a [[WriteEventAdapter]] and a [[ReadEventAdapter]].
 * Facility to convert from and to specialised data models, as may be required by specialized persistence Journals.
 *
 * Typical use cases include (but are not limited to):
 * <ul>
 *   <li>adding metadata, a.k.a. "tagging" - by wrapping objects into tagged counterparts</li>
 *   <li>manually converting to the Journals storage format, such as JSON, BSON or any specialised binary format</li>
 *   <li>adapting incoming events in any way before persisting them by the journal</li>
 * </ul>
 */
trait EventAdapter extends WriteEventAdapter with ReadEventAdapter

/**
 * Facility to convert to specialised data models, as may be required by specialized persistence Journals.
 *
 * Typical use cases include (but are not limited to):
 * <ul>
 *   <li>adding metadata, a.k.a. "tagging" - by wrapping objects into tagged counterparts</li>
 *   <li>manually converting to the Journals storage format, such as JSON, BSON or any specialised binary format</li>
 *   <li>splitting up large events into sequences of smaller ones</li>
 * </ul>
 */
trait WriteEventAdapter {
  //#event-adapter-api
  /**
   * Return the manifest (type hint) that will be provided in the `fromJournal` method.
   * Use `""` if manifest is not needed.
   */
  def manifest(event: Any): String

  /**
   * Convert domain event to journal event type.
   *
   * Some journal may require a specific type to be returned to them,
   * for example if a primary key has to be associated with each event then a journal
   * may require adapters to return `com.example.myjournal.EventWithPrimaryKey(event, key)`.
   *
   * The `toJournal` adaptation must be an 1-to-1 transformation.
   * It is not allowed to drop incoming events during the `toJournal` adaptation.
   *
   * @param event the application-side domain event to be adapted to the journal model
   * @return the adapted event object, possibly the same object if no adaptation was performed
   */
  def toJournal(event: Any): Any
  //#event-adapter-api
}

/**
 * Facility to convert from and to specialised data models, as may be required by specialized persistence Journals.
 *
 * Typical use cases include (but are not limited to):
 * <ul>
 *   <li>extracting events from "envelopes"</li>
 *   <li>manually converting to the Journals storage format, such as JSON, BSON or any specialised binary format</li>
 *   <li>adapting incoming events from a "data model" to the "domain model"</li>
 * </ul>
 */
trait ReadEventAdapter {
  //#event-adapter-api
  /**
   * Convert a event from its journal model to the applications domain model.
   *
   * One event may be adapter into multiple (or none) events which should be delivered to the [[akka.persistence.PersistentActor]].
   * Use the specialised [[akka.persistence.journal.EventSeq#single]] method to emit exactly one event,
   * or [[akka.persistence.journal.EventSeq#empty]] in case the adapter is not handling this event. Multiple [[EventAdapter]] instances are
   * applied in order as defined in configuration and their emitted event seqs are concatenated and delivered in order
   * to the PersistentActor.
   *
   * @param event event to be adapted before delivering to the PersistentActor
   * @param manifest optionally provided manifest (type hint) in case the Adapter has stored one for this event, `""` if none
   * @return sequence containing the adapted events (possibly zero) which will be delivered to the PersistentActor
   */
  def fromJournal(event: Any, manifest: String): EventSeq
  //#event-adapter-api
}

sealed abstract class EventSeq {
  def events: immutable.Seq[Any]
}
object EventSeq {

  /** Java API */
  final def empty: EventSeq = EmptyEventSeq

  /** Java API */
  final def single(event: Any): EventSeq = new SingleEventSeq(event)

  /** Java API */
  @varargs final def create(events: Any*): EventSeq = EventsSeq(events.toList)
  final def apply(events: Any*): EventSeq = EventsSeq(events.toList)
}
final case class SingleEventSeq(event: Any) extends EventSeq { // TODO try to make it a value class, would save allocations
  override val events: immutable.Seq[Any] = List(event)
  override def toString = s"SingleEventSeq($event)"
}

sealed trait EmptyEventSeq extends EventSeq
object EmptyEventSeq extends EmptyEventSeq {
  override def events = Nil
}

final case class EventsSeq[E](events: immutable.Seq[E]) extends EventSeq

/** No-op model adapter which passes through the incoming events as-is. */
case object IdentityEventAdapter extends EventAdapter {
  override def toJournal(event: Any): Any = event
  override def fromJournal(event: Any, manifest: String): EventSeq = EventSeq.single(event)
  override def manifest(event: Any): String = ""
}

/** INTERNAL API */
private[akka] case class NoopWriteEventAdapter(private val readEventAdapter: ReadEventAdapter) extends EventAdapter {
  // pass-through read
  override def fromJournal(event: Any, manifest: String): EventSeq =
    readEventAdapter.fromJournal(event, manifest)

  // no-op write
  override def manifest(event: Any): String = ""
  override def toJournal(event: Any): Any = event
}

/** INTERNAL API */
private[akka] case class NoopReadEventAdapter(private val writeEventAdapter: WriteEventAdapter) extends EventAdapter {
  // pass-through write
  override def manifest(event: Any): String = writeEventAdapter.manifest(event)
  override def toJournal(event: Any): Any = writeEventAdapter.toJournal(event)

  // no-op read
  override def fromJournal(event: Any, manifest: String): EventSeq =
    EventSeq(event)
}
