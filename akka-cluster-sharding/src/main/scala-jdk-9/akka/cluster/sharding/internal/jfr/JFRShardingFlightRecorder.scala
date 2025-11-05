/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.cluster.sharding.internal.jfr

import akka.cluster.sharding.ShardingFlightRecorder

class JFRShardingFlightRecorder extends ShardingFlightRecorder {
  override def rememberEntityOperation(duration: Long): Unit =
    new RememberEntityWrite(duration).commit()
  override def rememberEntityAdd(entityId: String): Unit =
    new RememberEntityAdd(entityId).commit()
  override def rememberEntityRemove(entityId: String): Unit =
    new RememberEntityRemove(entityId).commit()
  override def entityPassivate(entityId: String): Unit =
    new Passivate(entityId).commit()
  override def entityPassivateRestart(entityId: String): Unit =
    new PassivateRestart(entityId).commit()
}
