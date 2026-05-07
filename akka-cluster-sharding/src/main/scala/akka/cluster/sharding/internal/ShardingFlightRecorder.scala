/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import akka.annotation.InternalApi
import akka.cluster.sharding.internal.jfr.Passivate
import akka.cluster.sharding.internal.jfr.PassivateRestart
import akka.cluster.sharding.internal.jfr.RememberEntityAdd
import akka.cluster.sharding.internal.jfr.RememberEntityRemove
import akka.cluster.sharding.internal.jfr.RememberEntityWrite

/**
 * INTERNAL API
 */
@InternalApi
object ShardingFlightRecorder {
  def rememberEntityOperation(duration: Long): Unit =
    new RememberEntityWrite(duration).commit()
  def rememberEntityAdd(entityId: String): Unit =
    new RememberEntityAdd(entityId).commit()
  def rememberEntityRemove(entityId: String): Unit =
    new RememberEntityRemove(entityId).commit()
  def entityPassivate(entityId: String): Unit =
    new Passivate(entityId).commit()
  def entityPassivateRestart(entityId: String): Unit =
    new PassivateRestart(entityId).commit()
}
