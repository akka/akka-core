/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.journal.leveldb.javadsl

import akka.NotUsed
import akka.persistence.query.{ EventEnvelope, Offset }
import akka.persistence.query.javadsl._
import akka.stream.javadsl.Source

/**
 * Java API: [[akka.persistence.query.javadsl.ReadJournal]] implementation for LevelDB.
 *
 * It is retrieved with:
 * {{{
 * LeveldbReadJournal queries =
 *   PersistenceQuery.get(system).getReadJournalFor(LeveldbReadJournal.class, LeveldbReadJournal.Identifier());
 * }}}
 *
 * Corresponding Scala API is in [[akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"akka.persistence.query.journal.leveldb"`
 * for the default [[LeveldbReadJournal#Identifier]]. See `reference.conf`.
 *
 */
@deprecated("Use another journal implementation", "2.6.15")
class LeveldbReadJournal(scaladslReadJournal: akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal)
    extends ReadJournal
    with PersistenceIdsQuery
    with CurrentPersistenceIdsQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByTagQuery
    with CurrentEventsByTagQuery {

  /**
   * `persistenceIds` is used for retrieving all `persistenceIds` of all
   * persistent actors.
   *
   * The returned event stream is unordered and you can expect different order for multiple
   * executions of the query.
   *
   * The stream is not completed when it reaches the end of the currently used `persistenceIds`,
   * but it continues to push new `persistenceIds` when new persistent actors are created.
   * Corresponding query that is completed when it reaches the end of the currently
   * currently used `persistenceIds` is provided by [[#currentPersistenceIds]].
   *
   * The LevelDB write journal is notifying the query side as soon as new `persistenceIds` are
   * created and there is no periodic polling or batching involved in this query.
   *
   * The stream is completed with failure if there is a failure in executing the query in the
   * backend journal.
   */
  override def persistenceIds(): Source[String, NotUsed] =
    scaladslReadJournal.persistenceIds().asJava

  /**
   * Same type of query as [[#persistenceIds]] but the stream
   * is completed immediately when it reaches the end of the "result set". Persistent
   * actors that are created after the query is completed are not included in the stream.
   */
  override def currentPersistenceIds(): Source[String, NotUsed] =
    scaladslReadJournal.currentPersistenceIds().asJava

  /**
   * `eventsByPersistenceId` is used for retrieving events for a specific
   * `PersistentActor` identified by `persistenceId`.
   *
   * You can retrieve a subset of all events by specifying `fromSequenceNr` and `toSequenceNr`
   * or use `0L` and `Long.MaxValue` respectively to retrieve all events. Note that
   * the corresponding sequence number of each event is provided in the
   * [[akka.persistence.query.EventEnvelope]], which makes it possible to resume the
   * stream at a later point from a given sequence number.
   *
   * The returned event stream is ordered by sequence number, i.e. the same order as the
   * `PersistentActor` persisted the events. The same prefix of stream elements (in same order)
   *  are returned for multiple executions of the query, except for when events have been deleted.
   *
   * The stream is not completed when it reaches the end of the currently stored events,
   * but it continues to push new events when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * stored events is provided by [[#currentEventsByPersistenceId]].
   *
   * The LevelDB write journal is notifying the query side as soon as events are persisted, but for
   * efficiency reasons the query side retrieves the events in batches that sometimes can
   * be delayed up to the configured `refresh-interval`.
   *
   * The stream is completed with failure if there is a failure in executing the query in the
   * backend journal.
   */
  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    scaladslReadJournal.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  /**
   * Same type of query as [[#eventsByPersistenceId]] but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    scaladslReadJournal.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  /**
   * `eventsByTag` is used for retrieving events that were marked with
   * a given tag, e.g. all events of an Aggregate Root type.
   *
   * To tag events you create an [[akka.persistence.journal.EventAdapter]] that wraps the events
   * in a [[akka.persistence.journal.Tagged]] with the given `tags`.
   *
   * You can retrieve a subset of all events by specifying `offset`, or use `0L` to retrieve all
   * events with a given tag. The `offset` corresponds to an ordered sequence number for
   * the specific tag. Note that the corresponding offset of each event is provided in the
   * [[akka.persistence.query.EventEnvelope]], which makes it possible to resume the
   * stream at a later point from a given offset.
   *
   * In addition to the `offset` the `EventEnvelope` also provides `persistenceId` and `sequenceNr`
   * for each event. The `sequenceNr` is the sequence number for the persistent actor with the
   * `persistenceId` that persisted the event. The `persistenceId` + `sequenceNr` is an unique
   * identifier for the event.
   *
   * The `offset` is exclusive, i.e. the event with the exact same sequence number will not be included
   * in the returned stream. This means that you can use the offset that is returned in `EventEnvelope`
   * as the `offset` parameter in a subsequent query.
   *
   * The returned event stream is ordered by the offset (tag sequence number), which corresponds
   * to the same order as the write journal stored the events. The same stream elements (in same order)
   * are returned for multiple executions of the query. Deleted events are not deleted from the
   * tagged event stream.
   *
   * The stream is not completed when it reaches the end of the currently stored events,
   * but it continues to push new events when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * stored events is provided by [[#currentEventsByTag]].
   *
   * The LevelDB write journal is notifying the query side as soon as tagged events are persisted, but for
   * efficiency reasons the query side retrieves the events in batches that sometimes can
   * be delayed up to the configured `refresh-interval`.
   *
   * The stream is completed with failure if there is a failure in executing the query in the
   * backend journal.
   */
  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    scaladslReadJournal.eventsByTag(tag, offset).asJava

  /**
   * Same type of query as [[#eventsByTag]] but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    scaladslReadJournal.currentEventsByTag(tag, offset).asJava

}

object LeveldbReadJournal {

  /**
   * The default identifier for [[LeveldbReadJournal]] to be used with
   * [[akka.persistence.query.PersistenceQuery#getReadJournalFor]].
   *
   * The value is `"akka.persistence.query.journal.leveldb"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  final val Identifier = akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal.Identifier
}
