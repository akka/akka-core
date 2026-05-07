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

  // See Scala 2.13 ScalaBatchable for explanation
  def isBatchable(runnable: Runnable): Boolean = runnable match {
    case b: akka.dispatch.Batchable    => b.isBatchable
    case _: scala.concurrent.Batchable => true
    case _                             => false
  }

}
