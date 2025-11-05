/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.util
import java.time.{ Duration => JDuration }

import scala.concurrent.duration.{ Duration, FiniteDuration }

import akka.annotation.InternalStableApi

/**
 * INTERNAL API
 */
@InternalStableApi
private[akka] object JavaDurationConverters {
  def asFiniteDuration(duration: JDuration): FiniteDuration = duration.asScala

  final implicit class JavaDurationOps(val self: JDuration) extends AnyVal {
    def asScala: FiniteDuration = Duration.fromNanos(self.toNanos)
  }

  final implicit class ScalaDurationOps(val self: Duration) extends AnyVal {
    def asJava: JDuration = JDuration.ofNanos(self.toNanos)
  }
}
