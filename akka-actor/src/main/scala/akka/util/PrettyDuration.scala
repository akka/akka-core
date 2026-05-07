/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.Locale

import scala.concurrent.duration._

/** INTERNAL API */
private[akka] object PrettyDuration {

  /**
   * JAVA API
   * Selects most appropriate TimeUnit for given duration and formats it accordingly, with 4 digits precision
   */
  def format(duration: Duration): String = duration.pretty

  /**
   * JAVA API
   * Selects most appropriate TimeUnit for given duration and formats it accordingly
   */
  def format(duration: Duration, includeNanos: Boolean, precision: Int): String =
    duration.pretty(includeNanos, precision)

  implicit class PrettyPrintableDuration(val duration: Duration) extends AnyVal {

    /** Selects most appropriate TimeUnit for given duration and formats it accordingly, with 4 digits precision **/
    def pretty: String = pretty(includeNanos = false)

    /** Selects most appropriate TimeUnit for given duration and formats it accordingly */
    def pretty(includeNanos: Boolean, precision: Int = 4): String = {
      require(precision > 0, "precision must be > 0")

      duration match {
        case d: FiniteDuration =>
          val nanos = d.toNanos
          val unit = chooseUnit(nanos)
          val value = nanos.toDouble / NANOSECONDS.convert(1, unit)

          s"%.${precision}g %s%s".formatLocal(
            Locale.ROOT,
            value,
            abbreviate(unit),
            if (includeNanos) s" ($nanos ns)" else "")

        case Duration.MinusInf => s"-∞ (minus infinity)"
        case Duration.Inf      => s"∞ (infinity)"
        case _                 => "undefined"
      }
    }

    def chooseUnit(nanos: Long): TimeUnit = {
      val d = nanos.nanos

      if (d.toDays > 0) DAYS
      else if (d.toHours > 0) HOURS
      else if (d.toMinutes > 0) MINUTES
      else if (d.toSeconds > 0) SECONDS
      else if (d.toMillis > 0) MILLISECONDS
      else if (d.toMicros > 0) MICROSECONDS
      else NANOSECONDS
    }

    def abbreviate(unit: TimeUnit): String = unit match {
      case NANOSECONDS  => "ns"
      case MICROSECONDS => "μs"
      case MILLISECONDS => "ms"
      case SECONDS      => "s"
      case MINUTES      => "min"
      case HOURS        => "h"
      case DAYS         => "d"
    }
  }

}
