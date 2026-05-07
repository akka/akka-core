/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.testkit.javadsl

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.stream.javadsl.Sink
import akka.stream.testkit._

/** Java API */
object TestSink {

  /**
   * A Sink that materialized to a [[akka.stream.testkit.TestSubscriber.Probe]].
   */
  @deprecated("Use `TestSink.create` with ClassicActorSystemProvider instead.", "2.7.0")
  def probe[T](system: ActorSystem): Sink[T, TestSubscriber.Probe[T]] =
    create(system)

  /**
   * A Sink that materialized to a [[akka.stream.testkit.TestSubscriber.Probe]].
   */
  def create[T](system: ClassicActorSystemProvider): Sink[T, TestSubscriber.Probe[T]] =
    new Sink(scaladsl.TestSink[T]()(system))

}
