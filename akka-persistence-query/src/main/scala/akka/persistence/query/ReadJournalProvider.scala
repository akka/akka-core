/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

/**
 * A query plugin must implement a class that implements this trait.
 * It provides the concrete implementations for the Java and Scala APIs.
 *
 * A read journal plugin must provide implementations for both
 * `akka.persistence.query.scaladsl.ReadJournal` and `akka.persistence.query.javadsl.ReadJournal`.
 * The plugin must implement both the `scaladsl` and the `javadsl` traits because the
 * `akka.stream.scaladsl.Source` and `akka.stream.javadsl.Source` are different types
 * and even though those types can easily be converted to each other it is most convenient
 * for the end user to get access to the Java or Scala `Source` directly.
 * One of the implementations can delegate to the other.
 *
 */
trait ReadJournalProvider {

  /**
   * The `ReadJournal` implementation for the Scala API.
   * This corresponds to the instance that is returned by [[PersistenceQuery#readJournalFor]].
   */
  def scaladslReadJournal(): scaladsl.ReadJournal

  /**
   * The `ReadJournal` implementation for the Java API.
   * This corresponds to the instance that is returned by [[PersistenceQuery#getReadJournalFor]].
   */
  def javadslReadJournal(): javadsl.ReadJournal
}
