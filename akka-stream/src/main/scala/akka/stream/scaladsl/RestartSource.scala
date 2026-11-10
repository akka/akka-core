/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

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
