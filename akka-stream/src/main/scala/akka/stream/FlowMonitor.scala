/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import akka.stream.FlowMonitorState.StreamState

/**
 * Used to monitor the state of a stream
 *
 * @tparam T Type of messages passed by the stream
 */
trait FlowMonitor[+T] {
  def state: StreamState[T]
}

object FlowMonitorState {
  sealed trait StreamState[+U]

  /**
   * Stream was created, but no events have passed through it
   */
  case object Initialized extends StreamState[Nothing]

  /**
   * Java API
   */
  def initialized[U](): StreamState[U] = Initialized

  /**
   * Stream processed a message
   *
   * @param msg The processed message
   */
  final case class Received[+U](msg: U) extends StreamState[U]

  /**
   * Java API
   */
  def received[U](msg: U): StreamState[U] = Received(msg)

  /**
   * Stream failed
   *
   * @param cause The cause of the failure
   */
  final case class Failed(cause: Throwable) extends StreamState[Nothing]

  /**
   * Java API
   */
  def failed[U](cause: Throwable): StreamState[U] = Failed(cause)

  /**
   * Stream completed successfully
   */
  case object Finished extends StreamState[Nothing]

  /**
   * Java API
   */
  def finished[U](): StreamState[U] = Finished
}
