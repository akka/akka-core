/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.collection.immutable
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.reflect.ClassTag

import akka.actor.ActorSystem

package object testkit {
  def filterEvents[T](eventFilters: Iterable[EventFilter])(block: => T)(implicit system: ActorSystem): T = {
    def now = System.currentTimeMillis

    system.eventStream.publish(TestEvent.Mute(eventFilters.to(immutable.Seq)))

    try {
      val result = block

      val testKitSettings = TestKitExtension(system)
      val stop = now + testKitSettings.TestEventFilterLeeway.dilated.toMillis
      val failed = eventFilters
        .filterNot(_.awaitDone(Duration(stop - now, MILLISECONDS)))
        .map("Timeout (" + testKitSettings.TestEventFilterLeeway.dilated + ") waiting for " + _)
      if (failed.nonEmpty)
        throw new AssertionError("Filter completion error:\n" + failed.mkString("\n"))

      result
    } finally {
      system.eventStream.publish(TestEvent.UnMute(eventFilters.to(immutable.Seq)))
    }
  }

  def filterEvents[T](eventFilters: EventFilter*)(block: => T)(implicit system: ActorSystem): T =
    filterEvents(eventFilters.toSeq)(block)

  def filterException[T <: Throwable](block: => Unit)(implicit system: ActorSystem, t: ClassTag[T]): Unit =
    EventFilter[T]().intercept(block)

  /**
   * Scala API. Scale timeouts (durations) during tests with the configured
   * 'akka.test.timefactor'.
   * Implicit class providing `dilated` method.
   * {{{
   * import scala.concurrent.duration._
   * import akka.testkit._
   * 10.milliseconds.dilated
   * }}}
   * Corresponding Java API is available in JavaTestKit.dilated()
   */
  implicit class TestDuration(val duration: FiniteDuration) extends AnyVal {
    def dilated(implicit system: ActorSystem): FiniteDuration =
      Duration.fromNanos((duration.toNanos * TestKitExtension(system).TestTimeFactor + 0.5).toLong)
  }

}
