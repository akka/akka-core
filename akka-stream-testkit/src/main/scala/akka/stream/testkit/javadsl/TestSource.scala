/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.testkit.javadsl

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.stream.javadsl.Source
import akka.stream.testkit._

/** Java API */
object TestSource {

  /**
   * A Source that materializes to a [[akka.stream.testkit.TestPublisher.Probe]].
   */
  @deprecated("Use `TestSource.create` with ClassicActorSystemProvider instead.", "2.7.0")
  def probe[T](system: ActorSystem): Source[T, TestPublisher.Probe[T]] =
    create(system)

  /**
   * A Source that materializes to a [[akka.stream.testkit.TestPublisher.Probe]].
   */
  def create[T](system: ClassicActorSystemProvider): Source[T, TestPublisher.Probe[T]] =
    new Source(scaladsl.TestSource[T]()(system))

}
