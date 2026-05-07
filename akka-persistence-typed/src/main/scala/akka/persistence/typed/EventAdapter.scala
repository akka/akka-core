/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed

import scala.annotation.varargs
import scala.collection.immutable

import akka.annotation.InternalApi

/**
 * Facility to convert from and to specialised data models, as may be required by specialized persistence Journals.
 * Typical use cases include (but are not limited to):
 * <ul>
 *   <li>extracting events from "envelopes"</li>
 *   <li>adapting events from a "domain model" to the "data model", e.g. converting to the Journals storage format,
 *       such as JSON, BSON or any specialised binary format</li>
 *   <li>adapting events from a "data model" to the "domain model"</li>
 *   <li>adding metadata that is understood by the journal</li>
 *   <li>migration by splitting up events into sequences of other events</li>
 *   <li>migration filtering out unused events, or replacing an event with another</li>
 * </ul>
 */
abstract class EventAdapter[E, P] {

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
   * @param e the application-side domain event to be adapted to the journal model
   * @return the adapted event object, possibly the same object if no adaptation was performed
   */
  def toJournal(e: E): P

  /**
   * Return the manifest (type hint) that will be provided in the `fromJournal` method.
   * Use `""` if manifest is not needed.
   */
  def manifest(event: E): String

  /**
   * Transform the event on recovery from the journal.
   * Note that this is not called in any read side so will need to be applied
   * manually when using Query.
   *
   * Convert a event from its journal model to the applications domain model.
   *
   * One event may be adapter into multiple (or none) events which should be delivered to the `EventSourcedBehavior`.
   * Use the specialised [[EventSeq.single]] method to emit exactly one event,
   * or [[EventSeq.empty]] in case the adapter is not handling this event.
   *
   * @param p event to be adapted before delivering to the `EventSourcedBehavior`
   * @param manifest optionally provided manifest (type hint) in case the Adapter has stored one
   *                 for this event, `""` if none
   * @return sequence containing the adapted events (possibly zero) which will be delivered to
   *         the `EventSourcedBehavior`
   */
  def fromJournal(p: P, manifest: String): EventSeq[E]
}

sealed trait EventSeq[+A] {
  def events: immutable.Seq[A]
  def isEmpty: Boolean = events.isEmpty
  def nonEmpty: Boolean = events.nonEmpty
  def size: Int
}
object EventSeq {

  final def empty[A]: EventSeq[A] = EmptyEventSeq.asInstanceOf[EventSeq[A]]

  final def single[A](event: A): EventSeq[A] = SingleEventSeq(event)

  @varargs final def many[A](events: A*): EventSeq[A] = EventsSeq(events.toList)

  /** Java API */
  final def create[A](events: java.util.List[A]): EventSeq[A] = {
    import scala.jdk.CollectionConverters._
    EventsSeq(events.asScala.toList)
  }

  /** Scala API */
  final def apply[A](events: immutable.Seq[A]): EventSeq[A] = EventsSeq(events)

}

/** INTERNAL API */
@InternalApi private[akka] final case class SingleEventSeq[A](event: A) extends EventSeq[A] {
  override def events: immutable.Seq[A] = List(event)
  override def size: Int = 1
}

/** INTERNAL API */
@InternalApi private[akka] case object EmptyEventSeq extends EventSeq[Nothing] {
  override def events: immutable.Seq[Nothing] = Nil
  override def size: Int = 0
}

/** INTERNAL API */
@InternalApi private[akka] final case class EventsSeq[A](override val events: immutable.Seq[A]) extends EventSeq[A] {
  override def size: Int = events.size
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object NoOpEventAdapter {
  private val i = new NoOpEventAdapter[Nothing]
  def instance[E]: NoOpEventAdapter[E] = i.asInstanceOf[NoOpEventAdapter[E]]
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class NoOpEventAdapter[E] extends EventAdapter[E, Any] {
  override def toJournal(e: E): Any = e
  override def fromJournal(p: Any, manifest: String): EventSeq[E] = EventSeq.single(p.asInstanceOf[E])
  override def manifest(event: E): String = ""
}
