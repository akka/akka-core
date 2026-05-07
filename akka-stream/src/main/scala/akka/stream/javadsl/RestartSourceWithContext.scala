/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import akka.NotUsed
import akka.japi.function.Creator
import akka.stream.RestartSettings
import akka.stream.scaladsl

/**
 * A RestartSourceWithContext wraps a [[SourceWithContext]] that gets restarted when it completes or fails.
 *
 * They are useful for graphs that need to run for longer than the [[SourceWithContext]] can necessarily guarantee it will,
 * e.g. for [[SourceWithContext]] streams that depend on a remote service to which connectivity may be lost (crash or partition).  The RestartSourceWithContext ensures that the graph can continue running while the SourceWithContext restarts.
 */
object RestartSourceWithContext {

  /**
   *  Wrap the given [[SourceWithContext]] with a SourceWithContext that will restart it when it fails or completes using an exponential backoff.
   *
   *  The returned [[SourceWithContext]] will not emit a complete or failure as long as maxRestarts is not reached, since the completion or failure of the wrapped SourceWithContext is handled by restarting it.  The wrapped SourceWithContext can however be canceled by canceling the returned SourceWithContext.  When that happens, the wrapped SourceWithContext will be canceled and will not be restarted.
   *
   *  @param settings [[RestartSettings]] defining the restart configuration
   *  @param sourceFactory A factory for producing the SourceWithContext to wrap
   */
  def withBackoff[T, C](
      settings: RestartSettings,
      sourceFactory: Creator[SourceWithContext[T, C, _]]): SourceWithContext[T, C, NotUsed] = {
    val underlyingFactory = () => sourceFactory.create().asScala
    new SourceWithContext(scaladsl.RestartSourceWithContext.withBackoff(settings)(underlyingFactory))
  }

  /**
   * Wrap the given [[SourceWithContext]] with a SourceWithContext that will restart it when it fails using an exponential backoff.
   *
   * The returned [[SourceWithContext]] will not emit a failure as long as maxRestarts is not reached, since the failure of the wrapped SourceWithContext is handled by restarting it.  The wrapped SourceWithContext can however be canceled by canceling the returned SourceWithContext.  When that happens, the wrapped SourceWithContext if currently running will be canceled and will not be restarted.
   *
   * @param settings [[RestartSettings]] defining the restart configuration
   * @param sourceFactory A factory for producing the SourceWithContext to wrap
   */
  def onFailuresWithBackoff[T, C](
      settings: RestartSettings,
      sourceFactory: Creator[SourceWithContext[T, C, _]]): SourceWithContext[T, C, NotUsed] = {
    val underlyingFactory = () => sourceFactory.create().asScala
    new SourceWithContext(scaladsl.RestartSourceWithContext.onFailuresWithBackoff(settings)(underlyingFactory))
  }
}
