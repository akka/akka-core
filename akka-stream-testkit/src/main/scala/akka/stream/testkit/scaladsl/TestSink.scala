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
import akka.stream.testkit.StreamTestKit.ProbeSink
import akka.stream.testkit.TestSubscriber.Probe

/**
 * Factory methods for test sinks.
 */
object TestSink {

  /**
   * A Sink that materialized to a [[akka.stream.testkit.TestSubscriber.Probe]].
   */
  @deprecated("Use `TestSink()` with implicit ClassicActorSystemProvider instead.", "2.7.0")
  def probe[T](implicit system: ActorSystem): Sink[T, Probe[T]] =
    apply()

  /**
   * A Sink that materialized to a [[akka.stream.testkit.TestSubscriber.Probe]].
   */
  def apply[T]()(implicit system: ClassicActorSystemProvider): Sink[T, Probe[T]] = {
    implicit val sys: ActorSystem = system.classicSystem
    Sink.fromGraph[T, TestSubscriber.Probe[T]](new ProbeSink(none, SinkShape(Inlet("ProbeSink.in"))))
  }

}
