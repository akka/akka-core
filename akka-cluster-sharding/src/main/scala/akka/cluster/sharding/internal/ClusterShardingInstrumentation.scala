/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalStableApi
import scala.collection.immutable
import scala.jdk.CollectionConverters._

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
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

  lazy val instrumentation: ClusterShardingInstrumentation = {
    if (!system.settings.config.hasPath(fqcnConfigPath)) {
      EmptyClusterShardingInstrumentation
    } else {
      val fqcns = system.settings.config.getStringList(fqcnConfigPath).asScala.toVector
      fqcns.size match {
        case 0 => EmptyClusterShardingInstrumentation
        case 1 => create(fqcns.head)
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

  override def shardRegionBufferSize(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      size: Int): Unit =
    instrumentations.foreach(_.shardRegionBufferSize(selfAddress, shardRegionActor, typeName, size))

  override def shardRegionBufferSizeIncremented(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String): Unit =
    instrumentations.foreach(_.shardRegionBufferSizeIncremented(selfAddress, shardRegionActor, typeName))

  override def regionRequestedShardHome(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      shardId: String): Unit =
    instrumentations.foreach(_.regionRequestedShardHome(selfAddress, shardRegionActor, typeName, shardId))

  override def messageDropped(selfAddress: Address, self: ActorRef, typeName: String): Unit =
    instrumentations.foreach(_.messageDropped(selfAddress, self, typeName))

  override def receivedShardHome(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      shardId: String): Unit =
    instrumentations.foreach(_.receivedShardHome(selfAddress, shardRegionActor, typeName, shardId))

  override def shardHandoffStarted(
      selfAddress: Address,
      shardCoordinatorActor: ActorRef,
      typeName: String,
      shard: String): Unit =
    instrumentations.foreach(_.shardHandoffStarted(selfAddress, shardCoordinatorActor, typeName, shard))

  override def shardHandoffFinished(
      selfAddress: Address,
      shardCoordinatorActor: ActorRef,
      typeName: String,
      shard: String,
      ok: Boolean): Unit =
    instrumentations.foreach(_.shardHandoffFinished(selfAddress, shardCoordinatorActor, typeName, shard, ok))

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

  override def shardRegionBufferSize(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      size: Int): Unit = ()

  override def shardRegionBufferSizeIncremented(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String): Unit = ()

  override def regionRequestedShardHome(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      shardId: String): Unit = ()

  override def receivedShardHome(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      shardId: String): Unit = ()

  override def messageDropped(selfAddress: Address, self: ActorRef, typeName: String): Unit = ()

  override def shardHandoffStarted(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      shard: String): Unit = ()

  override def shardHandoffFinished(
      selfAddress: Address,
      self: ActorRef,
      typeName: String,
      shard: String,
      ok: Boolean): Unit = ()

  override def dependencies: immutable.Seq[String] = Nil
}

/**
 * INTERNAL API: Instrumentation SPI for Akka Cluster.
 */
@InternalStableApi
trait ClusterShardingInstrumentation {

  def shardRegionBufferSize(selfAddress: Address, shardRegionActor: ActorRef, typeName: String, size: Int): Unit

  def shardRegionBufferSizeIncremented(selfAddress: Address, shardRegionActor: ActorRef, typeName: String): Unit

  def regionRequestedShardHome(
      selfAddress: Address,
      shardRegionActor: ActorRef,
      typeName: String,
      shardId: String): Unit

  def receivedShardHome(selfAddress: Address, shardRegionActor: ActorRef, typeName: String, shardId: String): Unit

  def messageDropped(selfAddress: Address, self: ActorRef, typeName: String): Unit

  def shardHandoffStarted(selfAddress: Address, shardCoordinatorActor: ActorRef, typeName: String, shard: String): Unit

  def shardHandoffFinished(
      selfAddress: Address,
      shardCoordinatorActor: ActorRef,
      typeName: String,
      shard: String,
      ok: Boolean): Unit

  /**
   * Optional dependencies for this instrumentation.
   *
   * Dependency instrumentations will always be ordered before this instrumentation.
   *
   * @return list of class names for optional instrumentation dependencies
   */
  def dependencies: immutable.Seq[String]
}
