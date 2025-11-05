/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.typed.scaladsl

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.actor.typed.pubsub.Topic
import akka.annotation.ApiMayChange
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

/**
 * Sources and sinks to integrate [[akka.actor.typed.pubsub.Topic]] with streams allowing for local or distributed
 * publishing and subscribing of elements through a stream.
 */
object PubSub {

  /**
   * Create a source that will subscribe to a topic and stream messages published to the topic. Can be materialized
   * multiple times, each materialized stream will contain messages published after it was started.
   *
   * Note that it is not possible to propagate the backpressure from the running stream to the pub sub topic,
   * if the stream is backpressuring published messages are buffered up to a limit and if the limit is hit
   * the configurable `OverflowStrategy` decides what happens. It is not possible to use the `Backpressure`
   * strategy.
   *
   * @param topicActor The actor ref for an `akka.actor.typed.pubsub.Topic` actor representing a specific topic.
   * @param bufferSize The maximum number of messages to buffer if the stream applies backpressure
   * @param overflowStrategy Strategy to use once the buffer is full.
   * @tparam T The type of the published messages
   */
  @ApiMayChange
  def source[T](
      topicActor: ActorRef[Topic.Command[T]],
      bufferSize: Int,
      overflowStrategy: OverflowStrategy): Source[T, NotUsed] =
    ActorSource
      .actorRef[T](PartialFunction.empty, PartialFunction.empty, bufferSize, overflowStrategy)
      .mapMaterializedValue { ref =>
        topicActor ! Topic.Subscribe(ref)
        NotUsed
      }

  /**
   * Create a sink that will publish each message to the given topic. Note that there is no backpressure
   * from the topic, so care must be taken to not publish messages at a higher rate than that can be handled
   * by subscribers. If the topic does not have any subscribers when a message is published or the topic actor is stopped,
   * the message is sent to dead letters.
   *
   * @param topicActor The actor ref for an `akka.actor.typed.pubsub.Topic` actor representing a specific topic.
   * @tparam T the type of the messages that can be published
   */
  @ApiMayChange
  def sink[T](topicActor: ActorRef[Topic.Command[T]]): Sink[T, NotUsed] = {
    Sink
      .foreach[T] { message =>
        topicActor ! Topic.Publish(message)
      }
      .mapMaterializedValue(_ => NotUsed)
  }
}
