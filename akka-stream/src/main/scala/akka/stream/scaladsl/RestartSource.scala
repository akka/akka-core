/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration.FiniteDuration
import akka.NotUsed
import akka.stream.{ Attributes, Outlet, RestartSettings, SourceShape }
import akka.stream.stage.{ GraphStage, GraphStageLogic }

/**
 * A RestartSource wraps a [[Source]] that gets restarted when it completes or fails.
 *
 * They are useful for graphs that need to run for longer than the [[Source]] can necessarily guarantee it will, for
 * example, for [[Source]] streams that depend on a remote server that may crash or become partitioned. The
 * RestartSource ensures that the graph can continue running while the [[Source]] restarts.
 */
object RestartSource {

  /**
   * Wrap the given [[Source]] with a [[Source]] that will restart it when it fails or complete using an exponential
   * backoff.
   *
   * This [[Source]] will never emit a complete or failure, since the completion or failure of the wrapped [[Source]]
   * is always handled by restarting it. The wrapped [[Source]] can however be cancelled by cancelling this [[Source]].
   * When that happens, the wrapped [[Source]], if currently running will be cancelled, and it will not be restarted.
   * This can be triggered simply by the downstream cancelling, or externally by introducing a [[KillSwitch]] right
   * after this [[Source]] in the graph.
   *
   * This uses the same exponential backoff algorithm as [[akka.pattern.BackoffOpts]].
   *
   * @param minBackoff minimum (initial) duration until the child actor will
   *   started again, if it is terminated
   * @param maxBackoff the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *   random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *   In order to skip this additional delay pass in `0`.
   * @param sourceFactory A factory for producing the [[Source]] to wrap.
   */
  @Deprecated
  @deprecated("Use the overloaded method which accepts akka.stream.RestartSettings instead.", since = "2.6.10")
  def withBackoff[T](minBackoff: FiniteDuration, maxBackoff: FiniteDuration, randomFactor: Double)(
      sourceFactory: () => Source[T, _]): Source[T, NotUsed] = {
    val settings = RestartSettings(minBackoff, maxBackoff, randomFactor)
    withBackoff(settings)(sourceFactory)
  }

  /**
   * Wrap the given [[Source]] with a [[Source]] that will restart it when it fails or complete using an exponential
   * backoff.
   *
   * This [[Source]] will not emit a complete or failure as long as maxRestarts is not reached, since the completion
   * or failure of the wrapped [[Source]] is handled by restarting it. The wrapped [[Source]] can however be cancelled
   * by cancelling this [[Source]]. When that happens, the wrapped [[Source]], if currently running will be cancelled,
   * and it will not be restarted.
   * This can be triggered simply by the downstream cancelling, or externally by introducing a [[KillSwitch]] right
   * after this [[Source]] in the graph.
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
   * @param sourceFactory A factory for producing the [[Source]] to wrap.
   */
  @Deprecated
  @deprecated("Use the overloaded method which accepts akka.stream.RestartSettings instead.", since = "2.6.10")
  def withBackoff[T](minBackoff: FiniteDuration, maxBackoff: FiniteDuration, randomFactor: Double, maxRestarts: Int)(
      sourceFactory: () => Source[T, _]): Source[T, NotUsed] = {
    val settings = RestartSettings(minBackoff, maxBackoff, randomFactor).withMaxRestarts(maxRestarts, minBackoff)
    withBackoff(settings)(sourceFactory)
  }

  /**
   * Wrap the given [[Source]] with a [[Source]] that will restart it when it fails or complete using an exponential
   * backoff.
   *
   * This [[Source]] will not emit a complete or failure as long as maxRestarts is not reached, since the completion
   * or failure of the wrapped [[Source]] is handled by restarting it. The wrapped [[Source]] can however be cancelled
   * by cancelling this [[Source]]. When that happens, the wrapped [[Source]], if currently running will be cancelled,
   * and it will not be restarted.
   * This can be triggered simply by the downstream cancelling, or externally by introducing a [[KillSwitch]] right
   * after this [[Source]] in the graph.
   *
   * This uses the same exponential backoff algorithm as [[akka.pattern.BackoffOpts]].
   *
   * @param settings [[RestartSettings]] defining restart configuration
   * @param sourceFactory A factory for producing the [[Source]] to wrap.
   */
  def withBackoff[T](settings: RestartSettings)(sourceFactory: () => Source[T, _]): Source[T, NotUsed] =
    Source.fromGraph(new RestartWithBackoffSource(sourceFactory, settings, onlyOnFailures = false))

  /**
   * Wrap the given [[Source]] with a [[Source]] that will restart it when it fails using an exponential backoff.
   *
   * This [[Source]] will not emit a failure as long as maxRestarts is not reached, since failure of the wrapped [[Source]]
   * is handled by restarting it. The wrapped [[Source]] can be cancelled
   * by cancelling this [[Source]].
   * When that happens, the wrapped [[Source]], if currently running will be cancelled, and it will not be restarted.
   * This can be triggered simply by the downstream cancelling, or externally by introducing a [[KillSwitch]] right
   * after this [[Source]] in the graph.
   *
   * This uses the same exponential backoff algorithm as [[akka.pattern.BackoffOpts]].
   *
   * @param minBackoff minimum (initial) duration until the child actor will
   *   started again, if it is terminated
   * @param maxBackoff the exponential back-off is capped to this duration
   * @param randomFactor after calculation of the exponential back-off an additional
   *   random delay based on this factor is added, e.g. `0.2` adds up to `20%` delay.
   *   In order to skip this additional delay pass in `0`.
   * @param sourceFactory A factory for producing the [[Source]] to wrap.
   *
   */
  @Deprecated
  @deprecated("Use the overloaded method which accepts akka.stream.RestartSettings instead.", since = "2.6.10")
  def onFailuresWithBackoff[T](minBackoff: FiniteDuration, maxBackoff: FiniteDuration, randomFactor: Double)(
      sourceFactory: () => Source[T, _]): Source[T, NotUsed] = {
    val settings = RestartSettings(minBackoff, maxBackoff, randomFactor)
    onFailuresWithBackoff(settings)(sourceFactory)
  }

  /**
   * Wrap the given [[Source]] with a [[Source]] that will restart it when it fails using an exponential backoff.
   *
   * This [[Source]] will not emit a failure as long as maxRestarts is not reached, since failure of the wrapped [[Source]]
   * is handled by restarting it. The wrapped [[Source]] can be cancelled
   * by cancelling this [[Source]]. When that happens, the wrapped [[Source]], if currently running will be cancelled,
   * and it will not be restarted.
   * This can be triggered simply by the downstream cancelling, or externally by introducing a [[KillSwitch]] right
   * after this [[Source]] in the graph.
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
   * @param sourceFactory A factory for producing the [[Source]] to wrap.
   *
   */
  @Deprecated
  @deprecated("Use the overloaded method which accepts akka.stream.RestartSettings instead.", since = "2.6.10")
  def onFailuresWithBackoff[T](
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      maxRestarts: Int)(sourceFactory: () => Source[T, _]): Source[T, NotUsed] = {
    val settings = RestartSettings(minBackoff, maxBackoff, randomFactor).withMaxRestarts(maxRestarts, minBackoff)
    onFailuresWithBackoff(settings)(sourceFactory)
  }

  /**
   * Wrap the given [[Source]] with a [[Source]] that will restart it when it fails using an exponential backoff.
   *
   * This [[Source]] will not emit a failure as long as maxRestarts is not reached, since failure of the wrapped [[Source]]
   * is handled by restarting it. The wrapped [[Source]] can be cancelled
   * by cancelling this [[Source]]. When that happens, the wrapped [[Source]], if currently running will be cancelled,
   * and it will not be restarted.
   * This can be triggered simply by the downstream cancelling, or externally by introducing a [[KillSwitch]] right
   * after this [[Source]] in the graph.
   *
   * This uses the same exponential backoff algorithm as [[akka.pattern.BackoffOpts]].
   *
   * @param settings [[RestartSettings]] defining restart configuration
   * @param sourceFactory A factory for producing the [[Source]] to wrap.
   *
   */
  def onFailuresWithBackoff[T](settings: RestartSettings)(sourceFactory: () => Source[T, _]): Source[T, NotUsed] =
    Source.fromGraph(new RestartWithBackoffSource(sourceFactory, settings, onlyOnFailures = true))
}

private final class RestartWithBackoffSource[T](
    sourceFactory: () => Source[T, _],
    settings: RestartSettings,
    onlyOnFailures: Boolean)
    extends GraphStage[SourceShape[T]] { self =>

  val out = Outlet[T]("RestartWithBackoffSource.out")

  override def shape = SourceShape(out)
  override def createLogic(inheritedAttributes: Attributes) =
    new RestartWithBackoffLogic("Source", shape, inheritedAttributes, settings, onlyOnFailures) {

      override protected def logSource = self.getClass

      override protected def startGraph() = {
        val sinkIn = createSubInlet(out)
        subFusingMaterializer.materialize(sourceFactory().to(sinkIn.sink), inheritedAttributes)
        if (isAvailable(out)) {
          sinkIn.pull()
        }
      }

      override protected def backoff() = {
        setHandler(out, GraphStageLogic.EagerTerminateOutput)
      }

      backoff()
    }
}
