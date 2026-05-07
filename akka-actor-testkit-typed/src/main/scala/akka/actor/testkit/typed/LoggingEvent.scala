/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed

import java.util.Optional

import scala.jdk.OptionConverters._

import org.slf4j.Marker
import org.slf4j.event.Level

import scala.jdk.CollectionConverters._

object LoggingEvent {

  /**
   * Scala API
   */
  def apply(level: Level, loggerName: String, threadName: String, message: String, timeStamp: Long): LoggingEvent =
    new LoggingEvent(level, loggerName, threadName, message, timeStamp, None, None, Map.empty)

  /**
   * Java API
   */
  def create(level: Level, loggerName: String, threadName: String, message: String, timeStamp: Long): LoggingEvent =
    apply(level, loggerName, threadName, message, timeStamp)

  /**
   * Java API
   */
  def create(
      level: Level,
      loggerName: String,
      threadName: String,
      message: String,
      timeStamp: Long,
      marker: Optional[Marker],
      throwable: Optional[Throwable],
      mdc: java.util.Map[String, String]) =
    apply(level, loggerName, threadName, message, timeStamp, marker.toScala, throwable.toScala, mdc.asScala.toMap)
}

/**
 * Representation of logging event when testing with [[akka.actor.testkit.typed.scaladsl.LoggingTestKit]]
 * or [[akka.actor.testkit.typed.javadsl.LoggingTestKit]].
 */
final case class LoggingEvent(
    level: Level,
    loggerName: String,
    threadName: String,
    message: String,
    timeStamp: Long,
    marker: Option[Marker],
    throwable: Option[Throwable],
    mdc: Map[String, String]) {

  /**
   * Java API
   */
  def getMarker: Optional[Marker] =
    marker.toJava

  /**
   * Java API
   */
  def getThrowable: Optional[Throwable] =
    throwable.toJava

  /**
   * Java API
   */
  def getMdc: java.util.Map[String, String] = {
    import scala.jdk.CollectionConverters._
    mdc.asJava
  }

}
