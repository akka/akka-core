/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.typed.scaladsl

import akka.actor.typed._
import akka.stream.{ CompletionStrategy, OverflowStrategy }
import akka.stream.scaladsl._

/**
 * Collection of Sources aimed at integrating with typed Actors.
 */
object ActorSource {

  import akka.actor.typed.scaladsl.adapter._

  /**
   * Creates a `Source` that is materialized as an [[akka.actor.typed.ActorRef]].
   * Messages sent to this actor will be emitted to the stream if there is demand from downstream,
   * otherwise they will be buffered until request for demand is received.
   *
   * Depending on the defined [[akka.stream.OverflowStrategy]] it might drop elements if
   * there is no space available in the buffer.
   *
   * The strategy [[akka.stream.OverflowStrategy.backpressure]] is not supported, and an
   * IllegalArgument("Backpressure overflowStrategy not supported") will be thrown if it is passed as argument.
   *
   * The buffer can be disabled by using `bufferSize` of 0 and then received messages are dropped if there is no demand
   * from downstream. When `bufferSize` is 0 the `overflowStrategy` does not matter.
   *
   * The stream can be completed successfully by sending the actor reference a message that is matched by
   * `completionMatcher` in which case already buffered elements will be signaled before signaling
   * completion.
   *
   * The stream can be completed with failure by sending a message that is matched by `failureMatcher`. The extracted
   * [[Throwable]] will be used to fail the stream. In case the Actor is still draining its internal buffer (after having received
   * a message matched by `completionMatcher`) before signaling completion and it receives a message matched by `failureMatcher`,
   * the failure will be signaled downstream immediately (instead of the completion signal).
   *
   * The actor will be stopped when the stream is completed, failed or canceled from downstream,
   * i.e. you can watch it to get notified when that happens.
   *
   * See also [[akka.stream.scaladsl.Source.queue]].
   *
   * @param bufferSize The size of the buffer in element count
   * @param overflowStrategy Strategy that is used when incoming elements cannot fit inside the buffer
   */
  def actorRef[T](
      completionMatcher: PartialFunction[T, Unit],
      failureMatcher: PartialFunction[T, Throwable],
      bufferSize: Int,
      overflowStrategy: OverflowStrategy): Source[T, ActorRef[T]] =
    Source
      .actorRef[T](
        completionMatcher.asInstanceOf[PartialFunction[Any, Unit]].andThen(_ => CompletionStrategy.Draining),
        failureMatcher.asInstanceOf[PartialFunction[Any, Throwable]],
        bufferSize,
        overflowStrategy)
      .mapMaterializedValue(actorRefAdapter)

  /**
   * Creates a `Source` that is materialized as an [[akka.actor.typed.ActorRef]].
   * Messages sent to this actor will be emitted to the stream if there is demand from downstream,
   * and a new message will only be accepted after the previous messages has been consumed and acknowledged back.
   * The stream will complete with failure if a message is sent before the acknowledgement has been replied back.
   *
   * The stream can be completed by sending a message that is matched by `completionMatcher` which decides
   * if the stream is to drained before completion or should complete immediately.
   *
   * A message that is matched by `failureMatcher` fails the stream. The extracted
   * [[Throwable]] will be used to fail the stream. In case the Actor is still draining its internal buffer (after having received
   * a message matched by `completionMatcher`) before signaling completion and it receives a message matched by `failureMatcher`,
   * the failure will be signaled downstream immediately (instead of the completion signal).
   *
   * The actor will be stopped when the stream is completed, failed or canceled from downstream,
   * i.e. you can watch it to get notified when that happens.
   *
   * @param ackTo actor to be signalled when an element has been emitted to the stream
   * @param ackMessage a fixed message to be sent to `ackTo` to signal demand
   * @param completionMatcher a partial function applied to the messages received materialized actor,
   *                          a matching message will complete the stream with the return [[CompletionStrategy]]
   * @param failureMatcher a partial function applied to the messages received materialized actor,
   *                       a matching message will fail the stream with the returned [[Throwable]]
   */
  def actorRefWithBackpressure[T, Ack](
      ackTo: ActorRef[Ack],
      ackMessage: Ack,
      completionMatcher: PartialFunction[T, CompletionStrategy],
      failureMatcher: PartialFunction[T, Throwable]): Source[T, ActorRef[T]] =
    Source
      .actorRefWithAck[T](
        Some(ackTo.toClassic),
        ackMessage,
        completionMatcher.asInstanceOf[PartialFunction[Any, CompletionStrategy]],
        failureMatcher.asInstanceOf[PartialFunction[Any, Throwable]])
      .mapMaterializedValue(actorRefAdapter)

  /**
   * Creates a `Source` that is materialized as an [[akka.actor.typed.ActorRef]].
   * Messages sent to this actor will be emitted to the stream if there is demand from downstream,
   * and a new message will only be accepted after the previous messages has been consumed and acknowledged back.
   * The stream will complete with failure if a message is sent before the acknowledgement has been replied back.
   *
   * The stream can be completed with failure by sending a message that is matched by `failureMatcher`. The extracted
   * [[Throwable]] will be used to fail the stream. In case the Actor is still draining its internal buffer (after having received
   * a message matched by `completionMatcher`) before signaling completion and it receives a message matched by `failureMatcher`,
   * the failure will be signaled downstream immediately (instead of the completion signal).
   *
   * The actor will be stopped when the stream is completed, failed or canceled from downstream,
   * i.e. you can watch it to get notified when that happens.
   */
  @deprecated("Use actorRefWithBackpressure instead", "2.6.0")
  def actorRefWithAck[T, Ack](
      ackTo: ActorRef[Ack],
      ackMessage: Ack,
      completionMatcher: PartialFunction[T, CompletionStrategy],
      failureMatcher: PartialFunction[T, Throwable]): Source[T, ActorRef[T]] =
    Source
      .actorRefWithAck[T](
        Some(ackTo.toClassic),
        ackMessage,
        completionMatcher.asInstanceOf[PartialFunction[Any, CompletionStrategy]],
        failureMatcher.asInstanceOf[PartialFunction[Any, Throwable]])
      .mapMaterializedValue(actorRefAdapter)
}
