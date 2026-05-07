/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit.javadsl

import java.util.function.Supplier

import akka.actor.ActorSystem
import akka.event.Logging
import akka.testkit.{ DebugFilter, ErrorFilter, InfoFilter, WarningFilter }

class EventFilter(clazz: Class[_], system: ActorSystem) {

  require(
    classOf[Throwable].isAssignableFrom(clazz) || classOf[Logging.LogEvent].isAssignableFrom(clazz),
    "supplied class must either be LogEvent or Throwable")

  private val _clazz: Class[_ <: Logging.LogEvent] =
    if (classOf[Throwable].isAssignableFrom(clazz))
      classOf[Logging.Error]
    else
      clazz.asInstanceOf[Class[_ <: Logging.LogEvent]]

  private var exceptionType: Class[_ <: Throwable] =
    if (classOf[Throwable].isAssignableFrom(clazz))
      clazz.asInstanceOf[Class[_ <: Throwable]]
    else
      null

  private var source: String = _
  private var message: String = _
  private var pattern: Boolean = false
  private var complete: Boolean = false
  private var occurrences: Int = Integer.MAX_VALUE

  def intercept[T](code: Supplier[T]): T = {
    val filter: akka.testkit.EventFilter =
      if (_clazz eq classOf[Logging.Error]) {
        if (exceptionType == null) exceptionType = Logging.noCause.getClass
        new ErrorFilter(exceptionType, source, message, pattern, complete, occurrences)
      } else if (_clazz eq classOf[Logging.Warning]) {
        new WarningFilter(source, message, pattern, complete, occurrences)
      } else if (_clazz eq classOf[Logging.Info]) {
        new InfoFilter(source, message, pattern, complete, occurrences)
      } else if (_clazz eq classOf[Logging.Debug]) {
        new DebugFilter(source, message, pattern, complete, occurrences)
      } else throw new IllegalArgumentException("unknown LogLevel " + clazz)

    filter.intercept(code.get)(system)
  }

  def message(msg: String): EventFilter = {
    message = msg
    pattern = false
    complete = true
    this
  }

  def startsWith(msg: String): EventFilter = {
    message = msg
    pattern = false
    complete = false
    this
  }

  def matches(regex: String): EventFilter = {
    message = regex
    pattern = true
    this
  }

  def from(source: String): EventFilter = {
    this.source = source
    this
  }

  def occurrences(number: Int): EventFilter = {
    occurrences = number
    this
  }
}
