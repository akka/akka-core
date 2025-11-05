/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import java.util.Optional

import org.slf4j.Logger
import org.slf4j.event.Level

import akka.annotation.{ DoNotInherit, InternalApi }

/**
 * Logging options when using `Behaviors.logMessages`.
 */
@DoNotInherit
abstract sealed class LogOptions {

  /**
   * User control whether messages are logged or not.  This is useful when you want to have an application configuration
   * to control when to log messages.
   */
  def withEnabled(enabled: Boolean): LogOptions

  /**
   * The [[org.slf4j.event.Level]] to use when logging messages.
   */
  def withLevel(level: Level): LogOptions

  /**
   * A [[org.slf4j.Logger]] to use when logging messages.
   */
  def withLogger(logger: Logger): LogOptions

  def enabled: Boolean
  def level: Level
  def logger: Option[Logger]

  /** Java API */
  def getLogger: Optional[Logger]
}

/**
 * Factories for log options
 */
object LogOptions {

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] final case class LogOptionsImpl(enabled: Boolean, level: Level, logger: Option[Logger])
      extends LogOptions {

    /**
     * User control whether messages are logged or not.  This is useful when you want to have an application configuration
     * to control when to log messages.
     */
    override def withEnabled(enabled: Boolean): LogOptions = this.copy(enabled = enabled)

    /**
     * The [[org.slf4j.event.Level]] to use when logging messages.
     */
    override def withLevel(level: Level): LogOptions = this.copy(level = level)

    /**
     * A [[org.slf4j.Logger]] to use when logging messages.
     */
    override def withLogger(logger: Logger): LogOptions = this.copy(logger = Option(logger))

    /** Java API */
    override def getLogger: Optional[Logger] = Optional.ofNullable(logger.orNull)
  }

  /**
   * Scala API: Create a new log options with defaults.
   */
  def apply(): LogOptions = LogOptionsImpl(enabled = true, Level.DEBUG, None)

  /**
   * Java API: Create a new log options.
   */
  def create(): LogOptions = apply()
}
