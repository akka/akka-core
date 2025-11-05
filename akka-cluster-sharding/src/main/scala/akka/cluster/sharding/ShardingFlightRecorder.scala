/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.cluster.sharding
import akka.actor.{ ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }
import akka.annotation.InternalApi
import akka.util.FlightRecorderLoader

/**
 * INTERNAL API
 */
@InternalApi
object ShardingFlightRecorder extends ExtensionId[ShardingFlightRecorder] with ExtensionIdProvider {

  override def lookup: ExtensionId[_ <: Extension] = this

  override def createExtension(system: ExtendedActorSystem): ShardingFlightRecorder =
    FlightRecorderLoader.load[ShardingFlightRecorder](
      system,
      "akka.cluster.sharding.internal.jfr.JFRShardingFlightRecorder",
      NoOpShardingFlightRecorder)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait ShardingFlightRecorder extends Extension {
  def rememberEntityOperation(duration: Long): Unit
  def rememberEntityAdd(entityId: String): Unit
  def rememberEntityRemove(entityId: String): Unit
  def entityPassivate(entityId: String): Unit
  def entityPassivateRestart(entityId: String): Unit
}

/**
 * INTERNAL
 */
@InternalApi
private[akka] case object NoOpShardingFlightRecorder extends ShardingFlightRecorder {
  override def rememberEntityOperation(duration: Long): Unit = ()
  override def rememberEntityAdd(entityId: String): Unit = ()
  override def rememberEntityRemove(entityId: String): Unit = ()
  override def entityPassivate(entityId: String): Unit = ()
  override def entityPassivateRestart(entityId: String): Unit = ()
}
