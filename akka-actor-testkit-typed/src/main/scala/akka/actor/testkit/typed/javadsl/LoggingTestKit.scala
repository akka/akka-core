/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.javadsl

import java.util.function.Supplier

import org.slf4j.event.Level

import akka.actor.testkit.typed.LoggingEvent
import akka.actor.testkit.typed.internal.LoggingTestKitImpl
import akka.actor.typed.ActorSystem
import akka.annotation.DoNotInherit

/**
 * Facilities for verifying logs.
 *
 * Requires Logback.
 *
 * See the static factory methods as starting point for creating `LoggingTestKit`.
 *
 * Not for user extension.
 */
@DoNotInherit abstract class LoggingTestKit {

  /**
   * Number of events the testkit is supposed to match. By default 1.
   *
   * When occurrences > 0 it will not look for excess messages that are logged asynchronously
   * outside (after) the `intercept` thunk and it has already found expected number.
   *
   * When occurrences is 0 it will look for unexpected matching events, and then it will
   * also look for excess messages during the configured `akka.actor.testkit.typed.expect-no-message-default`
   * duration.
   */
  def withOccurrences(newOccurrences: Int): LoggingTestKit

  /**
   * Matching events with the given log level.
   */
  def withLogLevel(newLogLevel: Level): LoggingTestKit

  /**
   * Matching events with the given logger name or sub-names in the same way
   * as configuration loggers are configured in logback.xml.
   * By default the root logger is used.
   */
  def withLoggerName(newLoggerName: String): LoggingTestKit

  /**
   * Matching events that have "akkaSource" MDC value equal to the given value.
   * "akkaSource" is typically the actor path.
   */
  def withSource(newSource: String): LoggingTestKit

  /**
   * Matching events with a message that contains the given value.
   */
  def withMessageContains(newMessageContains: String): LoggingTestKit

  /**
   * Matching events with a message that matches the given regular expression.
   */
  def withMessageRegex(newMessageRegex: String): LoggingTestKit

  /**
   * Matching events with an included `throwable` that is a class or subclass of the given
   * `Throwable` class.
   */
  def withCause(newCause: Class[_ <: Throwable]): LoggingTestKit

  /**
   * Matching events with MDC containing all entries of the given `Map`.
   * The event MDC may have more entries than the given `Map`.
   */
  def withMdc(newMdc: java.util.Map[String, String]): LoggingTestKit

  /**
   * After matching the expected number of hits, check for excess messages
   */
  def withCheckExcess(check: Boolean): LoggingTestKit

  /**
   * Matching events for which the supplied function returns `true`.
   */
  def withCustom(newCustom: Function[LoggingEvent, Boolean]): LoggingTestKit
  // this is a Scala Function, ^ but that can be used with lambda from Java

  /**
   * @return `true` if the event matches the conditions of the filter.
   */
  def matches(event: LoggingEvent): Boolean

  /**
   * Run the given code block and assert that the criteria of this `LoggingTestKit` has
   * matched within the configured `akka.actor.testkit.typed.filter-leeway`
   * as often as requested by its `occurrences` parameter specifies.
   *
   * Care is taken to remove the testkit when the block is finished or aborted.
   */
  def expect[T](system: ActorSystem[_], code: Supplier[T]): T

}

/**
 * Facilities for selectively matching log messages.
 *
 * Requires Logback.
 */
object LoggingTestKit {

  /**
   * An empty filter that doesn't match any events.
   *
   * More conditions can be added to the returned [LoggingEventFilter].
   */
  def empty: LoggingTestKit = LoggingTestKitImpl.empty

  /**
   * Create a filter for events with a log message
   * that contains the given `messageIncludes`.
   *
   * More conditions can be added to the returned [LoggingEventFilter].
   */
  def messageContains(str: String): LoggingTestKit =
    empty.withMessageContains(str)

  /**
   * Create a filter for TRACE level events with a log message
   * that contains the given `messageIncludes`.
   *
   * More conditions can be added to the returned [LoggingEventFilter].
   */
  def trace(messageIncludes: String): LoggingTestKit =
    messageContains(messageIncludes).withLogLevel(Level.TRACE)

  /**
   * Create a filter for DEBUG level events with a log message
   * that contains the given `messageIncludes`.
   *
   * More conditions can be added to the returned [LoggingEventFilter].
   */
  def debug(messageIncludes: String): LoggingTestKit =
    messageContains(messageIncludes).withLogLevel(Level.DEBUG)

  /**
   * Create a filter for INFO level events with a log message
   * that contains the given `messageIncludes`.
   *
   * More conditions can be added to the returned [LoggingEventFilter].
   */
  def info(messageIncludes: String): LoggingTestKit =
    messageContains(messageIncludes).withLogLevel(Level.INFO)

  /**
   * Create a filter for WARN level events with a log message
   * that contains the given `messageIncludes`.
   *
   * More conditions can be added to the returned [LoggingEventFilter].
   */
  def warn(messageIncludes: String): LoggingTestKit =
    messageContains(messageIncludes).withLogLevel(Level.WARN)

  /**
   * Create a filter for WARN level events with a an included
   * `throwable` that is a class or subclass of the given
   * * `Throwable` class.
   *
   * More conditions can be added to the returned [LoggingEventFilter].
   */
  def warn(causeClass: Class[Throwable]): LoggingTestKit =
    empty.withLogLevel(Level.WARN).withCause(causeClass)

  /**
   * Create a filter for ERROR level events with a log message
   * that contains the given `messageIncludes`.
   *
   * More conditions can be added to the returned [LoggingEventFilter].
   */
  def error(messageIncludes: String): LoggingTestKit =
    messageContains(messageIncludes).withLogLevel(Level.ERROR)

  /**
   * Create a filter for ERROR level events with a an included
   * `throwable` that is a class or subclass of the given
   * * `Throwable` class.
   *
   * More conditions can be added to the returned [LoggingEventFilter].
   */
  def error(causeClass: Class[_ <: Throwable]): LoggingTestKit =
    empty.withLogLevel(Level.ERROR).withCause(causeClass)

  /**
   * Create a custom event filter. The filter will match those events for
   * which for which the supplied function returns `true`.
   */
  def custom(test: Function[LoggingEvent, Boolean]): LoggingTestKit =
    empty.withCustom(test) // this is a Scala Function, but that can be used with lambda from Java

  /**
   * Filter for the logging of dead letters.
   */
  def deadLetters(): LoggingTestKit =
    empty.withLogLevel(Level.INFO).withMessageRegex(".*was not delivered.*dead letters encountered.*")
}
