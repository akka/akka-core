/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.javadsl

/**
 * API for reading persistent events and information derived
 * from stored persistent events.
 *
 * The purpose of the API is not to enforce compatibility between different
 * journal implementations, because the technical capabilities may be very different.
 * The interface is very open so that different journals may implement specific queries.
 *
 * There are a few pre-defined queries that a query implementation may implement,
 * such as [[EventsByPersistenceIdQuery]], [[PersistenceIdsQuery]] and [[EventsByTagQuery]]
 * Implementation of these queries are optional and query (journal) plugins may define
 * their own specialized queries by implementing other methods.
 *
 * Usage:
 * {{{
 * SomeCoolReadJournal journal =
 *   PersistenceQuery.get(system).getReadJournalFor(SomeCoolReadJournal.class, queryPluginConfigPath);
 * Source<EventEnvolope, Unit> events = journal.eventsByTag("mytag", 0L);
 * }}}
 *
 * For Scala API see [[akka.persistence.query.scaladsl.ReadJournal]].
 */
trait ReadJournal
