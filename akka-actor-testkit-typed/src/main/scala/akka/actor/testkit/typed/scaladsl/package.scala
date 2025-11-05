/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed

import scala.concurrent.duration.FiniteDuration

package object scaladsl {

  /**
   * Scala API. Scale timeouts (durations) during tests with the configured
   * 'akka.actor.testkit.typed.timefactor'.
   * Implicit class providing `dilated` method.
   *
   * {{{
   * import scala.concurrent.duration._
   * import akka.actor.testkit.typed.scaladsl._
   * 10.milliseconds.dilated
   * }}}
   *
   * Uses the scaling factor from the `TestTimeFactor` in the [[TestKitSettings]]
   * (in implicit scope).
   *
   */
  implicit class TestDuration(val duration: FiniteDuration) extends AnyVal {
    def dilated(implicit settings: TestKitSettings): FiniteDuration = settings.dilated(duration)
  }

}
