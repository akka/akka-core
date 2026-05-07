/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.typed.javadsl

import akka.NotUsed
import akka.actor.typed._
import akka.stream.javadsl._
import akka.stream.typed

/**
 * Collection of Sinks aimed at integrating with typed Actors.
 */
object ActorSink {

  /**
   * Sends the elements of the stream to the given `ActorRef`.
   * If the target actor terminates the stream will be canceled.
   * When the stream is completed successfully the given `onCompleteMessage`
   * will be sent to the destination actor.
   * When the stream is completed with failure a the throwable that was signaled
   * to the stream is adapted to the Actors protocol using `onFailureMessage` and
   * then then sent to the destination actor.
   *
   * It will request at most `maxInputBufferSize` number of elements from
   * upstream, but there is no back-pressure signal from the destination actor,
   * i.e. if the actor is not consuming the messages fast enough the mailbox
   * of the actor will grow. For potentially slow consumer actors it is recommended
   * to use a bounded mailbox with zero `mailbox-push-timeout-time` or use a rate
   * limiting operator in front of this `Sink`.
   */
  def actorRef[T](
      ref: ActorRef[T],
      onCompleteMessage: T,
      onFailureMessage: akka.japi.function.Function[Throwable, T]): Sink[T, NotUsed] =
    typed.scaladsl.ActorSink.actorRef(ref, onCompleteMessage, onFailureMessage.apply).asJava

  /**
   * Sends the elements of the stream to the given `ActorRef` that sends back back-pressure signal.
   * First element is always `onInitMessage`, then stream is waiting for acknowledgement message
   * `ackMessage` from the given actor which means that it is ready to process
   * elements. It also requires `ackMessage` message after each stream element
   * to make backpressure work.
   *
   * If the target actor terminates the stream will be canceled.
   * When the stream is completed successfully the given `onCompleteMessage`
   * will be sent to the destination actor.
   * When the stream is completed with failure - result of `onFailureMessage(throwable)`
   * function will be sent to the destination actor.
   *
   * @param ref the receiving actor as `ActorRef<T>` (where `T` must include the control messages below)
   * @param messageAdapter a function that wraps the stream elements to be sent to the actor together with an `ActorRef[A]` which accepts the ack message
   * @param onInitMessage a function that wraps an `ActorRef<A>` into a messages to couple the receiving actor to the sink
   * @param ackMessage a fixed message that is expected after every element sent to the receiving actor
   * @param onCompleteMessage the message to be sent to the actor when the stream completes
   * @param onFailureMessage a function that creates a message to be sent to the actor in case the stream fails from a `Throwable`
   */
  def actorRefWithBackpressure[T, M, A](
      ref: ActorRef[M],
      messageAdapter: akka.japi.function.Function2[ActorRef[A], T, M],
      onInitMessage: akka.japi.function.Function[ActorRef[A], M],
      ackMessage: A,
      onCompleteMessage: M,
      onFailureMessage: akka.japi.function.Function[Throwable, M]): Sink[T, NotUsed] =
    typed.scaladsl.ActorSink
      .actorRefWithBackpressure(
        ref,
        messageAdapter.apply,
        onInitMessage.apply,
        ackMessage,
        onCompleteMessage,
        onFailureMessage.apply)
      .asJava

  /**
   * Sends the elements of the stream to the given `ActorRef` that sends back back-pressure signal.
   * First element is always `onInitMessage`, then stream is waiting for acknowledgement message
   * from the given actor which means that it is ready to process
   * elements. It also requires an ack message after each stream element
   * to make backpressure work. This variant will consider any message as ack message.
   *
   * If the target actor terminates the stream will be canceled.
   * When the stream is completed successfully the given `onCompleteMessage`
   * will be sent to the destination actor.
   * When the stream is completed with failure - result of `onFailureMessage(throwable)`
   * function will be sent to the destination actor.
   *
   * @param ref the receiving actor as `ActorRef<T>` (where `T` must include the control messages below)
   * @param messageAdapter a function that wraps the stream elements to be sent to the actor together with an `ActorRef[A]` which accepts the ack message
   * @param onInitMessage a function that wraps an `ActorRef<A>` into a messages to couple the receiving actor to the sink
   * @param onCompleteMessage the message to be sent to the actor when the stream completes
   * @param onFailureMessage a function that creates a message to be sent to the actor in case the stream fails from a `Throwable`
   */
  def actorRefWithBackpressure[T, M, A](
      ref: ActorRef[M],
      messageAdapter: akka.japi.function.Function2[ActorRef[A], T, M],
      onInitMessage: akka.japi.function.Function[ActorRef[A], M],
      onCompleteMessage: M,
      onFailureMessage: akka.japi.function.Function[Throwable, M]): Sink[T, NotUsed] =
    typed.scaladsl.ActorSink
      .actorRefWithBackpressure(
        ref,
        messageAdapter.apply,
        onInitMessage.apply,
        onCompleteMessage,
        onFailureMessage.apply)
      .asJava
}
