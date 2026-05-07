/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import java.util.Optional

import scala.jdk.DurationConverters._
import scala.jdk.OptionConverters._

import akka.japi.Pair
import akka.stream.scaladsl

object RetryFlow {

  /**
   * Allows retrying individual elements in the stream with an exponential backoff.
   *
   * The retry condition is controlled by the `decideRetry` function. It takes the originally emitted
   * element and the response emitted by `flow`, and may return a request to be retried.
   *
   * The implementation of the `RetryFlow` requires that `flow` follows one-in-one-out semantics,
   * the [[akka.stream.javadsl.Flow Flow]] may not filter elements,
   * nor emit more than one element per incoming element. The `RetryFlow` will fail if two elements are
   * emitted from the `flow`, it will be stuck "forever" if nothing is emitted. Just one element will
   * be emitted into the `flow` at any time. The `flow` needs to emit an element before the next
   * will be emitted to it.
   *
   * @param minBackoff minimum duration to backoff between issuing retries
   * @param maxBackoff maximum duration to backoff between issuing retries
   * @param randomFactor adds jitter to the retry delay. Use 0 for no jitter
   * @param flow a flow to retry elements from
   * @param decideRetry retry condition decision function
   */
  def withBackoff[In, Out, Mat](
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      maxRetries: Int,
      flow: Flow[In, Out, Mat],
      decideRetry: akka.japi.function.Function2[In, Out, Optional[In]]): Flow[In, Out, Mat] =
    scaladsl.RetryFlow
      .withBackoff[In, Out, Mat](minBackoff.toScala, maxBackoff.toScala, randomFactor, maxRetries, flow.asScala) {
        (in, out) =>
          decideRetry.apply(in, out).toScala
      }
      .asJava

  /**
   * Allows retrying individual elements in the stream with an exponential backoff.
   *
   * The retry condition is controlled by the `decideRetry` function. It takes the originally emitted
   * element with its context, and the response emitted by `flow`, and may return a request to be retried.
   *
   * The implementation of the `RetryFlow` requires that `flow` follows one-in-one-out semantics,
   * the [[akka.stream.javadsl.FlowWithContext FlowWithContext]] may not filter elements,
   * nor emit more than one element per incoming element. The `RetryFlow` will fail if two elements are
   * emitted from the `flow`, it will be stuck "forever" if nothing is emitted. Just one element will
   * be emitted into the `flow` at any time. The `flow` needs to emit an element before the next
   * will be emitted to it.
   *
   * The wrapped `flow` and `decideRetry` take the additional context parameters which can be a context,
   * or used to control retrying with other information.
   *
   * @param minBackoff minimum duration to backoff between issuing retries
   * @param maxBackoff maximum duration to backoff between issuing retries
   * @param randomFactor adds jitter to the retry delay. Use 0 for no jitter
   * @param flow a flow to retry elements from
   * @param decideRetry retry condition decision function
   */
  def withBackoffAndContext[In, InCtx, Out, OutCtx, Mat](
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      maxRetries: Int,
      flow: FlowWithContext[In, InCtx, Out, OutCtx, Mat],
      decideRetry: akka.japi.function.Function2[Pair[In, InCtx], Pair[Out, OutCtx], Optional[Pair[In, InCtx]]])
      : FlowWithContext[In, InCtx, Out, OutCtx, Mat] =
    scaladsl.RetryFlow
      .withBackoffAndContext[In, InCtx, Out, OutCtx, Mat](
        minBackoff.toScala,
        maxBackoff.toScala,
        randomFactor,
        maxRetries,
        flow.asScala) { (in, out) =>
        decideRetry.apply(Pair(in._1, in._2), Pair(out._1, out._2)).toScala.map(_.toScala)
      }
      .asJava[In, InCtx, Out, OutCtx, Mat]

}
