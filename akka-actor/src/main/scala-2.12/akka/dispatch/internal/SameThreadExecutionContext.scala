/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.dispatch.internal

import scala.concurrent.ExecutionContext

import akka.annotation.InternalApi
import akka.dispatch.BatchingExecutor

/**
 * Factory to create same thread ec. Not intended to be called from any other site than to create [[akka.dispatch.ExecutionContexts#parasitic]]
 *
 * INTERNAL API
 */
@InternalApi
private[dispatch] object SameThreadExecutionContext {

  private val sameThread = new ExecutionContext with BatchingExecutor {
    override protected def unbatchedExecute(runnable: Runnable): Unit = runnable.run()
    override protected def resubmitOnBlock: Boolean = false // No point since we execute on same thread
    override def reportFailure(t: Throwable): Unit =
      throw new IllegalStateException("exception in sameThreadExecutionContext", t)
  }

  def apply(): ExecutionContext = sameThread

}
