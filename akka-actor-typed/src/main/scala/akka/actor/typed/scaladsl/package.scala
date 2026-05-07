/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import org.slf4j.Logger
import org.slf4j.Marker

package object scaladsl {

  /**
   * Extension methods to [[org.slf4j.Logger]] that are useful because the Scala
   * compiler can't select the right overloaded methods for some cases when using
   * 2 template arguments and varargs (>= 3 arguments) with primitive types.
   *
   * Enable these extension methods with:
   *
   * {{{
   * import akka.actor.typed.scaladsl.LoggerOps
   * }}}
   * or
   * {{{
   *import akka.actor.typed.scaladsl._
   * }}}
   *
   * @param log the underlying [[org.slf4j.Logger]]
   */
  @deprecated("Not needed in 2.13 and later", "2.10.0")
  implicit class LoggerOps(val log: Logger) extends AnyVal {

    /**
     * Log a message at the TRACE level according to the specified format
     * and 2 arguments.
     *
     * This form avoids superfluous object creation when the logger
     * is disabled for the TRACE level.
     *
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    def trace2(format: String, arg1: Any, arg2: Any): Unit =
      log.trace(format, arg1, arg2)

    /**
     * Log marker data and message at the TRACE level according to the specified format
     * and 2 arguments.
     *
     * This form avoids superfluous object creation when the logger
     * is disabled for the TRACE level.
     *
     * @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    def trace2(marker: Marker, format: String, arg1: Any, arg2: Any): Unit =
      log.trace(marker, format, arg1, arg2)

    /**
     * Log a message at the TRACE level according to the specified format
     * and arguments.
     *
     * This form avoids superfluous string concatenation when the logger
     * is disabled for the TRACE level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an `Array[Object]` before invoking the method,
     * even if this logger is disabled for TRACE. The `trace` variants taking
     * one and `trace2` taking two arguments exist solely in order to avoid this hidden cost.
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    def traceN(format: String, arguments: Any*): Unit = {
      val arr = arguments.toArray
      if (arr.isInstanceOf[Array[Object]])
        log.trace(format, arr.asInstanceOf[Array[Object]]: _*) // this seems to always be the case
      else
        log.trace(format, arr.map(_.asInstanceOf[AnyRef]): _*)
    }

    /**
     * Log marker data and message at the TRACE level according to the specified format
     * and arguments.
     *
     * This form avoids superfluous string concatenation when the logger
     * is disabled for the TRACE level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an `Array[Object]` before invoking the method,
     * even if this logger is disabled for TRACE. The `trace` variants taking
     * one and `trace2` taking two arguments exist solely in order to avoid this hidden cost.
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    def traceN(marker: Marker, format: String, arguments: Any*): Unit = {
      val arr = arguments.toArray
      if (arr.isInstanceOf[Array[Object]])
        log.trace(marker, format, arr.asInstanceOf[Array[Object]]: _*) // this seems to always be the case
      else
        log.trace(marker, format, arr.map(_.asInstanceOf[AnyRef]): _*)
    }

    /**
     * Log a message at the DEBUG level according to the specified format
     * and 2 arguments.
     *
     * This form avoids superfluous object creation when the logger
     * is disabled for the DEBUG level.
     *
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    def debug(format: String, arg1: Any, arg2: Any): Unit =
      log.debug(format, arg1, arg2)

    /**
     * Log marker data and message at the DEBUG level according to the specified format
     * and 2 arguments.
     *
     * This form avoids superfluous object creation when the logger
     * is disabled for the DEBUG level.
     *
     * @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    def debug(marker: Marker, format: String, arg1: Any, arg2: Any): Unit =
      log.debug(marker, format, arg1, arg2)

    /**
     * Log a message at the DEBUG level according to the specified format
     * and arguments.
     *
     * This form avoids superfluous string concatenation when the logger
     * is disabled for the DEBUG level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an `Array[Object]` before invoking the method,
     * even if this logger is disabled for DEBUG. The `debug` variants taking
     * one and `debug2` taking two arguments exist solely in order to avoid this hidden cost.
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    def debug(format: String, arguments: Any*): Unit = {
      val arr = arguments.toArray
      if (arr.isInstanceOf[Array[Object]])
        log.debug(format, arr.asInstanceOf[Array[Object]]: _*) // this seems to always be the case
      else
        log.debug(format, arr.map(_.asInstanceOf[AnyRef]): _*)
    }

    /**
     * Log marker data and message at the DEBUG level according to the specified format
     * and arguments.
     *
     * This form avoids superfluous string concatenation when the logger
     * is disabled for the DEBUG level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an `Array[Object]` before invoking the method,
     * even if this logger is disabled for DEBUG. The `debug` variants taking
     * one and `debug2` taking two arguments exist solely in order to avoid this hidden cost.
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    def debug(marker: Marker, format: String, arguments: Any*): Unit = {
      val arr = arguments.toArray
      if (arr.isInstanceOf[Array[Object]])
        log.debug(marker, format, arr.asInstanceOf[Array[Object]]: _*) // this seems to always be the case
      else
        log.debug(marker, format, arr.map(_.asInstanceOf[AnyRef]): _*)
    }

    /**
     * Log a message at the INFO level according to the specified format
     * and 2 arguments.
     *
     * This form avoids superfluous object creation when the logger
     * is disabled for the INFO level.
     *
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    def info(format: String, arg1: Any, arg2: Any): Unit =
      log.info(format, arg1, arg2)

    /**
     * Log marker data and message at the INFO level according to the specified format
     * and 2 arguments.
     *
     * This form avoids superfluous object creation when the logger
     * is disabled for the INFO level.
     *
     * @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    def info(marker: Marker, format: String, arg1: Any, arg2: Any): Unit =
      log.info(marker, format, arg1, arg2)

    /**
     * Log a message at the INFO level according to the specified format
     * and arguments.
     *
     * This form avoids superfluous string concatenation when the logger
     * is disabled for the INFO level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an `Array[Object]` before invoking the method,
     * even if this logger is disabled for INFO. The `info` variants taking
     * one and `info2` taking two arguments exist solely in order to avoid this hidden cost.
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    def info(format: String, arguments: Any*): Unit = {
      val arr = arguments.toArray
      if (arr.isInstanceOf[Array[Object]])
        log.info(format, arr.asInstanceOf[Array[Object]]: _*) // this seems to always be the case
      else
        log.info(format, arr.map(_.asInstanceOf[AnyRef]): _*)
    }

    /**
     * Log marker data and message at the INFO level according to the specified format
     * and arguments.
     *
     * This form avoids superfluous string concatenation when the logger
     * is disabled for the INFO level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an `Array[Object]` before invoking the method,
     * even if this logger is disabled for INFO. The `info` variants taking
     * one and `info2` taking two arguments exist solely in order to avoid this hidden cost.
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    def info(marker: Marker, format: String, arguments: Any*): Unit = {
      val arr = arguments.toArray
      if (arr.isInstanceOf[Array[Object]])
        log.info(marker, format, arr.asInstanceOf[Array[Object]]: _*) // this seems to always be the case
      else
        log.info(marker, format, arr.map(_.asInstanceOf[AnyRef]): _*)
    }

    /**
     * Log a message at the WARN level according to the specified format
     * and 2 arguments.
     *
     * This form avoids superfluous object creation when the logger
     * is disabled for the WARN level.
     *
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    def warn(format: String, arg1: Any, arg2: Any): Unit =
      log.warn(format, arg1, arg2)

    /**
     * Log marker data and message at the WARN level according to the specified format
     * and 2 arguments.
     *
     * This form avoids superfluous object creation when the logger
     * is disabled for the WARN level.
     *
     * @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    def warn(marker: Marker, format: String, arg1: Any, arg2: Any): Unit =
      log.warn(marker, format, arg1, arg2)

    /**
     * Log a message at the WARN level according to the specified format
     * and arguments.
     *
     * This form avoids superfluous string concatenation when the logger
     * is disabled for the WARN level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an `Array[Object]` before invoking the method,
     * even if this logger is disabled for WARN. The `warn` variants taking
     * one and `warn2` taking two arguments exist solely in order to avoid this hidden cost.
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    def warnN(format: String, arguments: Any*): Unit = {
      val arr = arguments.toArray
      if (arr.isInstanceOf[Array[Object]])
        log.warn(format, arr.asInstanceOf[Array[Object]]: _*) // this seems to always be the case
      else
        log.warn(format, arr.map(_.asInstanceOf[AnyRef]): _*)
    }

    /**
     * Log marker data and message at the WARN level according to the specified format
     * and arguments.
     *
     * This form avoids superfluous string concatenation when the logger
     * is disabled for the WARN level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an `Array[Object]` before invoking the method,
     * even if this logger is disabled for WARN. The `warn` variants taking
     * one and `warn2` taking two arguments exist solely in order to avoid this hidden cost.
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    def warnN(marker: Marker, format: String, arguments: Any*): Unit = {
      val arr = arguments.toArray
      if (arr.isInstanceOf[Array[Object]])
        log.warn(marker, format, arr.asInstanceOf[Array[Object]]: _*) // this seems to always be the case
      else
        log.warn(marker, format, arr.map(_.asInstanceOf[AnyRef]): _*)
    }

    /**
     * Log a message at the ERROR level according to the specified format
     * and 2 arguments.
     *
     * This form avoids superfluous object creation when the logger
     * is disabled for the ERROR level.
     *
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    def error2(format: String, arg1: Any, arg2: Any): Unit =
      log.error(format, arg1, arg2)

    /**
     * Log marker data and message at the ERROR level according to the specified format
     * and 2 arguments.
     *
     * This form avoids superfluous object creation when the logger
     * is disabled for the ERROR level.
     *
     * @param marker the marker data specific to this log statement
     * @param format the format string
     * @param arg1   the first argument
     * @param arg2   the second argument
     */
    def error2(marker: Marker, format: String, arg1: Any, arg2: Any): Unit =
      log.error(marker, format, arg1, arg2)

    /**
     * Log a message at the ERROR level according to the specified format
     * and arguments.
     *
     * This form avoids superfluous string concatenation when the logger
     * is disabled for the ERROR level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an `Array[Object]` before invoking the method,
     * even if this logger is disabled for ERROR. The `error` variants taking
     * one and `error2` taking two arguments exist solely in order to avoid this hidden cost.
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    def errorN(format: String, arguments: Any*): Unit = {
      val arr = arguments.toArray
      if (arr.isInstanceOf[Array[Object]])
        log.error(format, arr.asInstanceOf[Array[Object]]: _*) // this seems to always be the case
      else
        log.error(format, arr.map(_.asInstanceOf[AnyRef]): _*)
    }

    /**
     * Log marker data and message at the ERROR level according to the specified format
     * and arguments.
     *
     * This form avoids superfluous string concatenation when the logger
     * is disabled for the ERROR level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an `Array[Object]` before invoking the method,
     * even if this logger is disabled for ERROR. The `error` variants taking
     * one and `error2` taking two arguments exist solely in order to avoid this hidden cost.
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    def errorN(marker: Marker, format: String, arguments: Any*): Unit = {
      val arr = arguments.toArray
      if (arr.isInstanceOf[Array[Object]])
        log.error(marker, format, arr.asInstanceOf[Array[Object]]: _*) // this seems to always be the case
      else
        log.error(marker, format, arr.map(_.asInstanceOf[AnyRef]): _*)
    }

  }

}
