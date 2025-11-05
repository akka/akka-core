/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.stage

import akka.event.LoggingAdapter
import akka.event.NoLogging
import akka.stream.MaterializerLoggingProvider

/**
 * Simple way to obtain a [[akka.event.LoggingAdapter]] when used together with an [[akka.stream.Materializer]].
 * If used with a different materializer [[akka.event.NoLogging]] will be returned.
 *
 * Make sure to only access `log` from GraphStage callbacks (such as `pull`, `push` or the async-callback).
 *
 * Note, abiding to [[akka.stream.ActorAttributes.logLevels]] has to be done manually,
 * the logger itself is configured based on the logSource provided to it. Also, the `log`
 * itself would not know if you're calling it from a "on element" context or not, which is why
 * these decisions have to be handled by the operator itself.
 */
trait StageLogging { self: GraphStageLogic =>
  private[this] var _log: LoggingAdapter = _

  /** Override to customise reported log source */
  protected def logSource: Class[_] = this.getClass

  def log: LoggingAdapter = {
    // only used in StageLogic, i.e. thread safe
    if (_log eq null) {
      materializer match {
        case p: MaterializerLoggingProvider =>
          _log = p.makeLogger(logSource.asInstanceOf[Class[Any]])
        case _ =>
          _log = NoLogging
      }
    }
    _log
  }

}
