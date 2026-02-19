/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalStableApi
import scala.collection.immutable
import scala.jdk.CollectionConverters._

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.event.Logging
import akka.util.TopologicalSort.topologicalSort

/**
 * INTERNAL API
 */
@InternalStableApi
object ClusterShardingInstrumentationProvider
    extends ExtensionId[ClusterShardingInstrumentationProvider]
    with ExtensionIdProvider {
  override def get(system: ActorSystem): ClusterShardingInstrumentationProvider = super.get(system)

  override def get(system: ClassicActorSystemProvider): ClusterShardingInstrumentationProvider = super.get(system)

  override def lookup = ClusterShardingInstrumentationProvider

  override def createExtension(system: ExtendedActorSystem): ClusterShardingInstrumentationProvider =
    new ClusterShardingInstrumentationProvider(system)
}

/**
 * INTERNAL API
 */
@InternalStableApi
class ClusterShardingInstrumentationProvider(system: ExtendedActorSystem) extends Extension {
  private val fqcnConfigPath = "akka.cluster.sharding.telemetry.instrumentations"

  system.log.error("opeeee")

  lazy val instrumentation: ClusterShardingInstrumentation = {
    if (!system.settings.config.hasPath(fqcnConfigPath)) {
      system.log.error("opeeee noe")
      EmptyClusterShardingInstrumentation
    } else {
      system.log.error("opeeee noe jo")
      val fqcns = system.settings.config.getStringList(fqcnConfigPath).asScala.toVector
      system.log.error(s"opeeee noe jo ${fqcns.size}")
      fqcns.size match {
        case 0 =>
          system.log.error(s"opeeee noe jo empt")
          EmptyClusterShardingInstrumentation
        case 1 =>
          system.log.error(s"opeeee noe jo frist")
          create(fqcns.head)
        case _ =>
          val instrumentationsByFqcn = fqcns.iterator.map(fqcn => fqcn -> create(fqcn)).toMap
          val sortedNames = topologicalSort[String](fqcns, fqcn => instrumentationsByFqcn(fqcn).dependencies.toSet)
          val instrumentations = sortedNames.map(instrumentationsByFqcn).toVector
          new ClusterShardingTelemetryEnsemble(instrumentations)
      }
    }
  }

  private def create(fqcn: String): ClusterShardingInstrumentation = {
    try {
      system.dynamicAccess
        .createInstanceFor[ClusterShardingInstrumentation](fqcn, immutable.Seq(classOf[ExtendedActorSystem] -> system))
        .get
    } catch {
      case t: Throwable => // Throwable, because instrumentation failure should not cause fatal shutdown
        Logging(system.classicSystem, classOf[ClusterShardingInstrumentationProvider])
          .warning(t, "Cannot create instrumentation [{}]", fqcn)
        EmptyClusterShardingInstrumentation
    }
  }
}

/**
 * INTERNAL API
 */
@InternalStableApi
class ClusterShardingTelemetryEnsemble(val instrumentations: Seq[ClusterShardingInstrumentation])
    extends ClusterShardingInstrumentation {

  override def shardRegionBufferSize(typeName: String, size: Int): Unit =
    instrumentations.foreach(_.shardRegionBufferSize(typeName, size))

  override def shardRegionBufferSizeIncremented(typeName: String): Unit =
    instrumentations.foreach(_.shardRegionBufferSizeIncremented(typeName))

  override def regionRequestedShardHome(typeName: String, shardId: String): Unit =
    instrumentations.foreach(_.regionRequestedShardHome(typeName, shardId))

  override def messageDropped(typeName: String): Unit =
    instrumentations.foreach(_.messageDropped(typeName))

  override def receivedShardHome(typeName: String, shardId: String): Unit =
    instrumentations.foreach(_.receivedShardHome(typeName, shardId))

  override def shardHandoffStarted(typeName: String, shard: String): Unit =
    instrumentations.foreach(_.shardHandoffStarted(typeName, shard))

  override def shardHandoffFinished(typeName: String, shard: String, ok: Boolean): Unit =
    instrumentations.foreach(_.shardHandoffFinished(typeName, shard, ok))

  override def dependencies: immutable.Seq[String] =
    instrumentations.flatMap(_.dependencies)

}

/**
 * INTERNAL API
 */
@InternalStableApi
object EmptyClusterShardingInstrumentation extends EmptyClusterShardingInstrumentation

/**
 * INTERNAL API
 */
@InternalStableApi
class EmptyClusterShardingInstrumentation extends ClusterShardingInstrumentation {

  override def shardRegionBufferSize(typeName: String, size: Int): Unit = ()

  override def shardRegionBufferSizeIncremented(typeName: String): Unit = ()

  override def regionRequestedShardHome(typeName: String, shardId: String): Unit = ()

  override def receivedShardHome(typeName: String, shardId: String): Unit = ()

  override def messageDropped(typeName: String): Unit = ()

  override def shardHandoffStarted(typeName: String, shard: String): Unit = ()

  override def shardHandoffFinished(typeName: String, shard: String, ok: Boolean): Unit = ()

  override def dependencies: immutable.Seq[String] = Nil
}

/**
 * INTERNAL API: Instrumentation SPI for Akka Cluster.
 */
@InternalStableApi
trait ClusterShardingInstrumentation {

  def shardRegionBufferSize(typeName: String, size: Int): Unit

  def shardRegionBufferSizeIncremented(typeName: String): Unit

  def regionRequestedShardHome(typeName: String, shardId: String): Unit

  def receivedShardHome(typeName: String, shardId: String): Unit

  def messageDropped(typeName: String): Unit

  def shardHandoffStarted(typeName: String, shard: String): Unit

  def shardHandoffFinished(typeName: String, shard: String, ok: Boolean): Unit

  /**
   * Optional dependencies for this instrumentation.
   *
   * Dependency instrumentations will always be ordered before this instrumentation.
   *
   * @return list of class names for optional instrumentation dependencies
   */
  def dependencies: immutable.Seq[String]
}
