/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.testkit.scaladsl

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.stream._
import akka.stream.Attributes.none
import akka.stream.scaladsl._
import akka.stream.testkit._
import akka.stream.testkit.StreamTestKit.ProbeSource

/**
 * Factory methods for test sources.
 */
object TestSource {

  /**
   * A Source that materializes to a [[akka.stream.testkit.TestPublisher.Probe]].
   */
  @deprecated("Use `TestSource()` with implicit ClassicActorSystemProvider instead.", "2.7.0")
  def probe[T](implicit system: ActorSystem): Source[T, TestPublisher.Probe[T]] =
    apply()

  /**
   * A Source that materializes to a [[akka.stream.testkit.TestPublisher.Probe]].
   */
  def apply[T]()(implicit system: ClassicActorSystemProvider): Source[T, TestPublisher.Probe[T]] = {
    implicit val sys: ActorSystem = system.classicSystem
    Source.fromGraph[T, TestPublisher.Probe[T]](new ProbeSource(none, SourceShape(Outlet("ProbeSource.out"))))
  }

}
