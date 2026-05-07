/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import akka.NotUsed
import akka.japi.function.Creator
import akka.stream.RestartSettings

/**
 * A RestartFlow wraps a [[Flow]] that gets restarted when it completes or fails.
 *
 * They are useful for graphs that need to run for longer than the [[Flow]] can necessarily guarantee it will, for
 * example, for [[Flow]] streams that depend on a remote server that may crash or become partitioned. The
 * RestartFlow ensures that the graph can continue running while the [[Flow]] restarts.
 */
object RestartFlow {

  /**
   * Wrap the given [[Flow]] with a [[Flow]] that will restart it when it fails or complete using an exponential
   * backoff.
   *
   * This [[Flow]] will not cancel, complete or emit a failure, until the opposite end of it has been cancelled or
   * completed. Any termination by the [[Flow]] before that time will be handled by restarting it. Any termination
   * signals sent to this [[Flow]] however will terminate the wrapped [[Flow]], if it's running, and then the [[Flow]]
   * will be allowed to terminate without being restarted.
   *
   * The restart process is inherently lossy, since there is no coordination between cancelling and the sending of
   * messages. A termination signal from either end of the wrapped [[Flow]] will cause the other end to be terminated,
   * and any in transit messages will be lost. During backoff, this [[Flow]] will backpressure.
   *
   * This uses the same exponential backoff algorithm as [[akka.pattern.BackoffOpts]].
   *
   * @param minBackoff minimum (initial) duration until the child actor will
   *   started again, if it is terminated
   * @param maxBackoff the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *   random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *   In order to skip this additional delay pass in `0`.
   * @param flowFactory A factory for producing the [[Flow]] to wrap.
   */
  @Deprecated
  @deprecated("Use the overloaded method which accepts akka.stream.RestartSettings instead.", since = "2.6.10")
  def withBackoff[In, Out](
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      flowFactory: Creator[Flow[In, Out, _]]): Flow[In, Out, NotUsed] = {
    val settings = RestartSettings.create(minBackoff, maxBackoff, randomFactor)
    withBackoff(settings, flowFactory)
  }

  /**
   * Wrap the given [[Flow]] with a [[Flow]] that will restart it when it fails or complete using an exponential
   * backoff.
   *
   * This [[Flow]] will not cancel, complete or emit a failure, until the opposite end of it has been cancelled or
   * completed. Any termination by the [[Flow]] before that time will be handled by restarting it as long as maxRestarts
   * is not reached. Any termination signals sent to this [[Flow]] however will terminate the wrapped [[Flow]], if it's
   * running, and then the [[Flow]] will be allowed to terminate without being restarted.
   *
   * The restart process is inherently lossy, since there is no coordination between cancelling and the sending of
   * messages. A termination signal from either end of the wrapped [[Flow]] will cause the other end to be terminated,
   * and any in transit messages will be lost. During backoff, this [[Flow]] will backpressure.
   *
   * This uses the same exponential backoff algorithm as [[akka.pattern.BackoffOpts]].
   *
   * @param minBackoff minimum (initial) duration until the child actor will
   *   started again, if it is terminated
   * @param maxBackoff the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *   random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *   In order to skip this additional delay pass in `0`.
   * @param maxRestarts the amount of restarts is capped to this amount within a time frame of minBackoff.
   *   Passing `0` will cause no restarts and a negative number will not cap the amount of restarts.
   * @param flowFactory A factory for producing the [[Flow]] to wrap.
   */
  @Deprecated
  @deprecated("Use the overloaded method which accepts akka.stream.RestartSettings instead.", since = "2.6.10")
  def withBackoff[In, Out](
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      maxRestarts: Int,
      flowFactory: Creator[Flow[In, Out, _]]): Flow[In, Out, NotUsed] = {
    val settings = RestartSettings.create(minBackoff, maxBackoff, randomFactor).withMaxRestarts(maxRestarts, minBackoff)
    withBackoff(settings, flowFactory)
  }

  /**
   * Wrap the given [[Flow]] with a [[Flow]] that will restart it when it fails or complete using an exponential
   * backoff.
   *
   * This [[Flow]] will not cancel, complete or emit a failure, until the opposite end of it has been cancelled or
   * completed. Any termination by the [[Flow]] before that time will be handled by restarting it as long as maxRestarts
   * is not reached. Any termination signals sent to this [[Flow]] however will terminate the wrapped [[Flow]], if it's
   * running, and then the [[Flow]] will be allowed to terminate without being restarted.
   *
   * The restart process is inherently lossy, since there is no coordination between cancelling and the sending of
   * messages. A termination signal from either end of the wrapped [[Flow]] will cause the other end to be terminated,
   * and any in transit messages will be lost. During backoff, this [[Flow]] will backpressure.
   *
   * This uses the same exponential backoff algorithm as [[akka.pattern.BackoffOpts]].
   *
   * @param settings [[RestartSettings]] defining restart configuration
   * @param flowFactory A factory for producing the [[Flow]] to wrap.
   */
  def withBackoff[In, Out](settings: RestartSettings, flowFactory: Creator[Flow[In, Out, _]]): Flow[In, Out, NotUsed] =
    akka.stream.scaladsl.RestartFlow
      .withBackoff(settings) { () =>
        flowFactory.create().asScala
      }
      .asJava

  /**
   * Wrap the given [[Flow]] with a [[Flow]] that will restart only when it fails that restarts
   * using an exponential backoff.
   *
   * This new [[Flow]] will not emit failures. Any failure by the original [[Flow]] (the wrapped one) before that
   * time will be handled by restarting it as long as maxRestarts  is not reached.
   * However, any termination signals, completion or cancellation sent to this [[Flow]] will terminate
   * the wrapped [[Flow]], if it's running, and then the [[Flow]] will be allowed to terminate without being restarted.
   *
   * The restart process is inherently lossy, since there is no coordination between cancelling and the sending of
   * messages. A termination signal from either end of the wrapped [[Flow]] will cause the other end to be terminated,
   * and any in transit messages will be lost. During backoff, this [[Flow]] will backpressure.
   *
   * This uses the same exponential backoff algorithm as [[akka.pattern.BackoffOpts]].
   *
   * @param minBackoff minimum (initial) duration until the child actor will
   *   started again, if it is terminated
   * @param maxBackoff the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *   random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *   In order to skip this additional delay pass in `0`.
   * @param maxRestarts the amount of restarts is capped to this amount within a time frame of minBackoff.
   *   Passing `0` will cause no restarts and a negative number will not cap the amount of restarts.
   * @param flowFactory A factory for producing the [[Flow]] to wrap.
   */
  @Deprecated
  @deprecated("Use the overloaded method which accepts akka.stream.RestartSettings instead.", since = "2.6.10")
  def onFailuresWithBackoff[In, Out](
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      maxRestarts: Int,
      flowFactory: Creator[Flow[In, Out, _]]): Flow[In, Out, NotUsed] = {
    val settings = RestartSettings.create(minBackoff, maxBackoff, randomFactor).withMaxRestarts(maxRestarts, minBackoff)
    onFailuresWithBackoff(settings, flowFactory)
  }

  /**
   * Wrap the given [[Flow]] with a [[Flow]] that will restart only when it fails that restarts
   * using an exponential backoff.
   *
   * This new [[Flow]] will not emit failures. Any failure by the original [[Flow]] (the wrapped one) before that
   * time will be handled by restarting it as long as maxRestarts  is not reached.
   * However, any termination signals, completion or cancellation sent to this [[Flow]] will terminate
   * the wrapped [[Flow]], if it's running, and then the [[Flow]] will be allowed to terminate without being restarted.
   *
   * The restart process is inherently lossy, since there is no coordination between cancelling and the sending of
   * messages. A termination signal from either end of the wrapped [[Flow]] will cause the other end to be terminated,
   * and any in transit messages will be lost. During backoff, this [[Flow]] will backpressure.
   *
   * This uses the same exponential backoff algorithm as [[akka.pattern.BackoffOpts]].
   *
   * @param settings [[RestartSettings]] defining restart configuration
   * @param flowFactory A factory for producing the [[Flow]] to wrap.
   */
  def onFailuresWithBackoff[In, Out](
      settings: RestartSettings,
      flowFactory: Creator[Flow[In, Out, _]]): Flow[In, Out, NotUsed] =
    akka.stream.scaladsl.RestartFlow
      .onFailuresWithBackoff(settings) { () =>
        flowFactory.create().asScala
      }
      .asJava
}
