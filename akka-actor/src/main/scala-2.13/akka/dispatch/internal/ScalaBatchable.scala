/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object ScalaBatchable {

  /*
   * In Scala 2.13.0 `OnCompleteRunnable` was deprecated, and in 2.13.4
   * `impl.Promise.Transformation` no longer extend the deprecated `OnCompleteRunnable`
   * but instead `scala.concurrent.Batchable` (which anyway extends `OnCompleteRunnable`).
   * On top of that we have or own legacy version `akka.dispatch.Batchable`.
   */

  def isBatchable(runnable: Runnable): Boolean = runnable match {
    case b: akka.dispatch.Batchable    => b.isBatchable
    case _: scala.concurrent.Batchable => true
    case _                             => false
  }

}
