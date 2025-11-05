/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.internal

import scala.annotation.tailrec

import org.slf4j.LoggerFactory
import org.slf4j.event.Level

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object LogbackUtil {
  def loggerNameOrRoot(loggerName: String): String =
    if (loggerName == "") org.slf4j.Logger.ROOT_LOGGER_NAME else loggerName

  def getLogbackLogger(loggerName: String): ch.qos.logback.classic.Logger =
    getLogbackLoggerInternal(loggerName, 50)

  @tailrec
  private def getLogbackLoggerInternal(loggerName: String, count: Int): ch.qos.logback.classic.Logger = {
    LoggerFactory.getLogger(loggerNameOrRoot(loggerName)) match {
      case logger: ch.qos.logback.classic.Logger              => logger
      case _: org.slf4j.helpers.SubstituteLogger if count > 0 =>
        // Wait for logging initialisation https://www.slf4j.org/codes.html#substituteLogger
        Thread.sleep(50)
        getLogbackLoggerInternal(loggerName, count - 1)
      case null =>
        throw new IllegalArgumentException(s"Couldn't find logger for [$loggerName].")
      case other =>
        throw new IllegalArgumentException(
          s"Requires Logback logger for [$loggerName], it was a [${other.getClass.getName}]")
    }
  }

  def convertLevel(level: ch.qos.logback.classic.Level): Level = {
    level.levelInt match {
      case ch.qos.logback.classic.Level.TRACE_INT => Level.TRACE
      case ch.qos.logback.classic.Level.DEBUG_INT => Level.DEBUG
      case ch.qos.logback.classic.Level.INFO_INT  => Level.INFO
      case ch.qos.logback.classic.Level.WARN_INT  => Level.WARN
      case ch.qos.logback.classic.Level.ERROR_INT => Level.ERROR
      case _ =>
        throw new IllegalArgumentException("Level " + level.levelStr + ", " + level.levelInt + " is unknown.")
    }
  }
}
