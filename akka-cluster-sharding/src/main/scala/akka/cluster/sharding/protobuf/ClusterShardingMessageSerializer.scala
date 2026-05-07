/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.protobuf

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
import java.io.NotSerializableException
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.cluster.sharding.Shard
import akka.cluster.sharding.ShardCoordinator
import akka.cluster.sharding.ShardRegion._
import akka.cluster.sharding.internal.EventSourcedRememberEntitiesCoordinatorStore.{
  MigrationMarker,
  State => RememberShardsState
}
import akka.cluster.sharding.internal.EventSourcedRememberEntitiesShardStore.{ EntitiesStarted, EntitiesStopped }
import akka.cluster.sharding.internal.EventSourcedRememberEntitiesShardStore.{ State => EntityState }
import akka.cluster.sharding.protobuf.msg.{ ClusterShardingMessages => sm }
import akka.cluster.sharding.protobuf.msg.ClusterShardingMessages
import akka.protobufv3.internal.MessageLite
import akka.serialization.BaseSerializer
import akka.serialization.Serialization
import akka.serialization.SerializerWithStringManifest
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API: Protobuf serializer of ClusterSharding messages.
 */
private[akka] class ClusterShardingMessageSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {
  import Shard.{ CurrentShardState, GetCurrentShardState }
  import Shard.{ GetShardStats, ShardStats }
  import ShardCoordinator.Internal._

  private final val BufferSize = 1024 * 4

  private val CoordinatorStateManifest = "AA"
  private val ShardRegionRegisteredManifest = "AB"
  private val ShardRegionProxyRegisteredManifest = "AC"
  private val ShardRegionTerminatedManifest = "AD"
  private val ShardRegionProxyTerminatedManifest = "AE"
  private val ShardHomeAllocatedManifest = "AF"
  private val ShardHomeDeallocatedManifest = "AG"

  private val RegisterManifest = "BA"
  private val RegisterProxyManifest = "BB"
  private val RegisterAckManifest = "BC"
  private val GetShardHomeManifest = "BD"
  private val ShardHomeManifest = "BE"
  private val HostShardManifest = "BF"
  private val ShardStartedManifest = "BG"
  private val BeginHandOffManifest = "BH"
  private val BeginHandOffAckManifest = "BI"
  private val HandOffManifest = "BJ"
  private val ShardStoppedManifest = "BK"
  private val GracefulShutdownReqManifest = "BL"
  private val RegionStoppedManifest = "BM"
  private val ShardHomesManifest = "BN"

  private val EntityStateManifest = "CA"
  private val EntityStartedManifest = "CB"
  private val EntityStoppedManifest = "CD"
  private val EntitiesStartedManifest = "CE"
  private val EntitiesStoppedManifest = "CF"

  private val StartEntityManifest = "EA"
  private val StartEntityAckManifest = "EB"

  private val GetShardStatsManifest = "DA"
  private val ShardStatsManifest = "DB"
  private val GetShardRegionStatsManifest = "DC"
  private val ShardRegionStatsManifest = "DD"
  private val GetClusterShardingStatsManifest = "DE"
  private val ClusterShardingStatsManifest = "DF"
  private val GetCurrentRegionsManifest = "DG"
  private val CurrentRegionsManifest = "DH"

  private val GetCurrentShardStateManifest = "FA"
  private val CurrentShardStateManifest = "FB"
  private val GetShardRegionStateManifest = "FC"
  private val ShardStateManifest = "FD"
  private val CurrentShardRegionStateManifest = "FE"

  private val EventSourcedRememberShardsMigrationMarkerManifest = "SM"
  private val EventSourcedRememberShardsStateManifest = "SS"

  private val StopShardsManifest = "ST"

  private val fromBinaryMap = collection.immutable.HashMap[String, Array[Byte] => AnyRef](
    EntityStateManifest -> entityStateFromBinary,
    EntityStartedManifest -> entityStartedFromBinary,
    EntitiesStartedManifest -> entitiesStartedFromBinary,
    EntityStoppedManifest -> entityStoppedFromBinary,
    EntitiesStoppedManifest -> entitiesStoppedFromBinary,
    CoordinatorStateManifest -> coordinatorStateFromBinary,
    ShardRegionRegisteredManifest -> { bytes =>
      ShardRegionRegistered(actorRefMessageFromBinary(bytes))
    },
    ShardRegionProxyRegisteredManifest -> { bytes =>
      ShardRegionProxyRegistered(actorRefMessageFromBinary(bytes))
    },
    ShardRegionTerminatedManifest -> { bytes =>
      ShardRegionTerminated(actorRefMessageFromBinary(bytes))
    },
    ShardRegionProxyTerminatedManifest -> { bytes =>
      ShardRegionProxyTerminated(actorRefMessageFromBinary(bytes))
    },
    ShardHomeAllocatedManifest -> shardHomeAllocatedFromBinary,
    ShardHomeDeallocatedManifest -> { bytes =>
      ShardHomeDeallocated(shardIdMessageFromBinary(bytes))
    },
    RegisterManifest -> { bytes =>
      Register(actorRefMessageFromBinary(bytes))
    },
    RegisterProxyManifest -> { bytes =>
      RegisterProxy(actorRefMessageFromBinary(bytes))
    },
    RegisterAckManifest -> { bytes =>
      RegisterAck(actorRefMessageFromBinary(bytes))
    },
    GetShardHomeManifest -> { bytes =>
      GetShardHome(shardIdMessageFromBinary(bytes))
    },
    ShardHomeManifest -> shardHomeFromBinary,
    ShardHomesManifest -> shardHomesFromBinary,
    HostShardManifest -> { bytes =>
      HostShard(shardIdMessageFromBinary(bytes))
    },
    ShardStartedManifest -> { bytes =>
      ShardStarted(shardIdMessageFromBinary(bytes))
    },
    BeginHandOffManifest -> { bytes =>
      BeginHandOff(shardIdMessageFromBinary(bytes))
    },
    BeginHandOffAckManifest -> { bytes =>
      BeginHandOffAck(shardIdMessageFromBinary(bytes))
    },
    HandOffManifest -> { bytes =>
      HandOff(shardIdMessageFromBinary(bytes))
    },
    ShardStoppedManifest -> { bytes =>
      ShardStopped(shardIdMessageFromBinary(bytes))
    },
    GracefulShutdownReqManifest -> { bytes =>
      GracefulShutdownReq(actorRefMessageFromBinary(bytes))
    },
    RegionStoppedManifest -> { bytes =>
      RegionStopped(actorRefMessageFromBinary(bytes))
    },
    GetShardStatsManifest -> { _ =>
      GetShardStats
    },
    ShardStatsManifest -> { bytes =>
      shardStatsFromBinary(bytes)
    },
    GetShardRegionStatsManifest -> { _ =>
      GetShardRegionStats
    },
    ShardRegionStatsManifest -> { bytes =>
      shardRegionStatsFromBinary(bytes)
    },
    GetClusterShardingStatsManifest -> { bytes =>
      getClusterShardingStatsFromBinary(bytes)
    },
    ClusterShardingStatsManifest -> { bytes =>
      clusterShardingStatsFromBinary(bytes)
    },
    GetCurrentRegionsManifest -> { _ =>
      GetCurrentRegions
    },
    CurrentRegionsManifest -> { bytes =>
      currentRegionsFromBinary(bytes)
    },
    StartEntityManifest -> { startEntityFromBinary },
    StartEntityAckManifest -> { startEntityAckFromBinary },
    GetCurrentShardStateManifest -> { _ =>
      GetCurrentShardState
    },
    CurrentShardStateManifest -> { bytes =>
      currentShardStateFromBinary(bytes)
    },
    GetShardRegionStateManifest -> { _ =>
      GetShardRegionState
    },
    ShardStateManifest -> { bytes =>
      shardStateFromBinary(bytes)
    },
    CurrentShardRegionStateManifest -> { bytes =>
      currentShardRegionStateFromBinary(bytes)
    },
    EventSourcedRememberShardsMigrationMarkerManifest -> { _ =>
      MigrationMarker
    },
    EventSourcedRememberShardsStateManifest -> { bytes =>
      rememberShardsStateFromBinary(bytes)
    },
    StopShardsManifest -> stopShardsFromBinary)

  override def manifest(obj: AnyRef): String = obj match {
    case _: EntityState     => EntityStateManifest
    case _: EntitiesStarted => EntitiesStartedManifest
    case _: EntitiesStopped => EntitiesStoppedManifest

    case _: State                      => CoordinatorStateManifest
    case _: ShardRegionRegistered      => ShardRegionRegisteredManifest
    case _: ShardRegionProxyRegistered => ShardRegionProxyRegisteredManifest
    case _: ShardRegionTerminated      => ShardRegionTerminatedManifest
    case _: ShardRegionProxyTerminated => ShardRegionProxyTerminatedManifest
    case _: ShardHomeAllocated         => ShardHomeAllocatedManifest
    case _: ShardHomeDeallocated       => ShardHomeDeallocatedManifest

    case _: Register            => RegisterManifest
    case _: RegisterProxy       => RegisterProxyManifest
    case _: RegisterAck         => RegisterAckManifest
    case _: GetShardHome        => GetShardHomeManifest
    case _: ShardHome           => ShardHomeManifest
    case _: ShardHomes          => ShardHomesManifest
    case _: HostShard           => HostShardManifest
    case _: ShardStarted        => ShardStartedManifest
    case _: BeginHandOff        => BeginHandOffManifest
    case _: BeginHandOffAck     => BeginHandOffAckManifest
    case _: HandOff             => HandOffManifest
    case _: ShardStopped        => ShardStoppedManifest
    case _: GracefulShutdownReq => GracefulShutdownReqManifest
    case _: RegionStopped       => RegionStoppedManifest

    case _: StartEntity    => StartEntityManifest
    case _: StartEntityAck => StartEntityAckManifest

    case GetShardStats              => GetShardStatsManifest
    case _: ShardStats              => ShardStatsManifest
    case GetShardRegionStats        => GetShardRegionStatsManifest
    case _: ShardRegionStats        => ShardRegionStatsManifest
    case _: GetClusterShardingStats => GetClusterShardingStatsManifest
    case _: ClusterShardingStats    => ClusterShardingStatsManifest
    case GetCurrentRegions          => GetCurrentRegionsManifest
    case _: CurrentRegions          => CurrentRegionsManifest

    case GetCurrentShardState       => GetCurrentShardStateManifest
    case _: CurrentShardState       => CurrentShardStateManifest
    case GetShardRegionState        => GetShardRegionStateManifest
    case _: ShardState              => ShardStateManifest
    case _: CurrentShardRegionState => CurrentShardRegionStateManifest

    case MigrationMarker        => EventSourcedRememberShardsMigrationMarkerManifest
    case _: RememberShardsState => EventSourcedRememberShardsStateManifest

    case _: StopShards => StopShardsManifest

    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${obj.getClass} in [${getClass.getName}]")
  }

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case m: State                        => compress(coordinatorStateToProto(m))
    case ShardRegionRegistered(ref)      => actorRefMessageToProto(ref).toByteArray
    case ShardRegionProxyRegistered(ref) => actorRefMessageToProto(ref).toByteArray
    case ShardRegionTerminated(ref)      => actorRefMessageToProto(ref).toByteArray
    case ShardRegionProxyTerminated(ref) => actorRefMessageToProto(ref).toByteArray
    case m: ShardHomeAllocated           => shardHomeAllocatedToProto(m).toByteArray
    case ShardHomeDeallocated(shardId)   => shardIdMessageToProto(shardId).toByteArray

    case Register(ref)            => actorRefMessageToProto(ref).toByteArray
    case RegisterProxy(ref)       => actorRefMessageToProto(ref).toByteArray
    case RegisterAck(ref)         => actorRefMessageToProto(ref).toByteArray
    case GetShardHome(shardId)    => shardIdMessageToProto(shardId).toByteArray
    case m: ShardHome             => shardHomeToProto(m).toByteArray
    case sh: ShardHomes           => shardHomesToProto(sh).toByteArray
    case HostShard(shardId)       => shardIdMessageToProto(shardId).toByteArray
    case ShardStarted(shardId)    => shardIdMessageToProto(shardId).toByteArray
    case BeginHandOff(shardId)    => shardIdMessageToProto(shardId).toByteArray
    case BeginHandOffAck(shardId) => shardIdMessageToProto(shardId).toByteArray
    case HandOff(shardId)         => shardIdMessageToProto(shardId).toByteArray
    case ShardStopped(shardId)    => shardIdMessageToProto(shardId).toByteArray
    case GracefulShutdownReq(ref) =>
      actorRefMessageToProto(ref).toByteArray
    case RegionStopped(ref) => actorRefMessageToProto(ref).toByteArray

    case m: EntityState     => entityStateToProto(m).toByteArray
    case m: EntitiesStarted => entitiesStartedToProto(m).toByteArray
    case m: EntitiesStopped => entitiesStoppedToProto(m).toByteArray

    case s: StartEntity    => startEntityToByteArray(s)
    case s: StartEntityAck => startEntityAckToByteArray(s)

    case GetShardStats              => Array.emptyByteArray
    case m: ShardStats              => shardStatsToProto(m).toByteArray
    case GetShardRegionStats        => Array.emptyByteArray
    case m: ShardRegionStats        => shardRegionStatsToProto(m).toByteArray
    case m: GetClusterShardingStats => getClusterShardingStatsToProto(m).toByteArray
    case m: ClusterShardingStats    => clusterShardingStatsToProto(m).toByteArray
    case GetCurrentRegions          => Array.emptyByteArray
    case m: CurrentRegions          => currentRegionsToProto(m).toByteArray

    case GetCurrentShardState       => Array.emptyByteArray
    case m: CurrentShardState       => currentShardStateToProto(m).toByteArray
    case GetShardRegionState        => Array.emptyByteArray
    case m: ShardState              => shardStateToProto(m).toByteArray
    case m: CurrentShardRegionState => currentShardRegionStateToProto(m).toByteArray

    case MigrationMarker        => Array.emptyByteArray
    case m: RememberShardsState => rememberShardsStateToProto(m).toByteArray

    case ss: StopShards => stopShardsToProto(ss).toByteArray

    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${obj.getClass} in [${getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    fromBinaryMap.get(manifest) match {
      case Some(f) => f(bytes)
      case None =>
        throw new NotSerializableException(
          s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]")
    }

  private def rememberShardsStateToProto(state: RememberShardsState): sm.RememberedShardState = {
    val builder = sm.RememberedShardState.newBuilder()
    builder.addAllShardId(state.shards.toList.asJava)
    builder.setMarker(state.writtenMigrationMarker)
    builder.build()
  }

  private def rememberShardsStateFromBinary(bytes: Array[Byte]): RememberShardsState = {
    val proto = sm.RememberedShardState.parseFrom(bytes)
    RememberShardsState(proto.getShardIdList.asScala.toSet, proto.getMarker)
  }

  private def stopShardsFromBinary(bytes: Array[Byte]): StopShards = {
    val proto = sm.StopShards.parseFrom(bytes)
    StopShards(proto.getShardsList.asScala.toSet)
  }

  private def stopShardsToProto(ss: StopShards): sm.StopShards = {
    val builder = sm.StopShards.newBuilder()
    builder.addAllShards(ss.shards.asJava)
    builder.build()
  }

  private def coordinatorStateToProto(state: State): sm.CoordinatorState = {
    val builder = sm.CoordinatorState.newBuilder()

    state.shards.foreach {
      case (shardId, regionRef) =>
        val b = sm.CoordinatorState.ShardEntry
          .newBuilder()
          .setShardId(shardId)
          .setRegionRef(Serialization.serializedActorPath(regionRef))
        builder.addShards(b)
    }
    state.regions.foreach {
      case (regionRef, _) => builder.addRegions(Serialization.serializedActorPath(regionRef))
    }
    state.regionProxies.foreach { ref =>
      builder.addRegionProxies(Serialization.serializedActorPath(ref))
    }
    state.unallocatedShards.foreach { builder.addUnallocatedShards }

    builder.build()
  }

  private def coordinatorStateFromBinary(bytes: Array[Byte]): State =
    coordinatorStateFromProto(sm.CoordinatorState.parseFrom(decompress(bytes)))

  private def coordinatorStateFromProto(state: sm.CoordinatorState): State = {
    val shards: Map[String, ActorRef] =
      state.getShardsList.asScala.toVector.iterator.map { entry =>
        entry.getShardId -> resolveActorRef(entry.getRegionRef)
      }.toMap

    val regionsZero: Map[ActorRef, Vector[String]] =
      state.getRegionsList.asScala.toVector.iterator.map(resolveActorRef(_) -> Vector.empty[String]).toMap
    val regions: Map[ActorRef, Vector[String]] =
      shards.foldLeft(regionsZero) {
        case (acc, (shardId, regionRef)) => acc.updated(regionRef, acc(regionRef) :+ shardId)
      }

    val proxies: Set[ActorRef] = state.getRegionProxiesList.asScala.iterator.map { resolveActorRef }.to(immutable.Set)
    val unallocatedShards: Set[String] = state.getUnallocatedShardsList.asScala.toSet

    State(shards, regions, proxies, unallocatedShards)
  }

  private def actorRefMessageToProto(ref: ActorRef): sm.ActorRefMessage =
    sm.ActorRefMessage.newBuilder().setRef(Serialization.serializedActorPath(ref)).build()

  private def actorRefMessageFromBinary(bytes: Array[Byte]): ActorRef =
    resolveActorRef(sm.ActorRefMessage.parseFrom(bytes).getRef)

  private def shardIdMessageToProto(shardId: String): sm.ShardIdMessage =
    sm.ShardIdMessage.newBuilder().setShard(shardId).build()

  private def shardIdMessageFromBinary(bytes: Array[Byte]): String =
    sm.ShardIdMessage.parseFrom(bytes).getShard

  private def shardHomeAllocatedToProto(evt: ShardHomeAllocated): sm.ShardHomeAllocated =
    sm.ShardHomeAllocated
      .newBuilder()
      .setShard(evt.shard)
      .setRegion(Serialization.serializedActorPath(evt.region))
      .build()

  private def shardHomeAllocatedFromBinary(bytes: Array[Byte]): ShardHomeAllocated = {
    val m = sm.ShardHomeAllocated.parseFrom(bytes)
    ShardHomeAllocated(m.getShard, resolveActorRef(m.getRegion))
  }

  private def shardHomeToProto(m: ShardHome): sm.ShardHome =
    sm.ShardHome.newBuilder().setShard(m.shard).setRegion(Serialization.serializedActorPath(m.ref)).build()

  private def shardHomeFromBinary(bytes: Array[Byte]): ShardHome = {
    val m = sm.ShardHome.parseFrom(bytes)
    ShardHome(m.getShard, resolveActorRef(m.getRegion))
  }

  private def shardHomesToProto(sh: ShardHomes): sm.ShardHomes = {
    sm.ShardHomes
      .newBuilder()
      .addAllHomes(sh.homes.map {
        case (regionRef, shards) =>
          sm.ShardHomesEntry
            .newBuilder()
            .setRegion(Serialization.serializedActorPath(regionRef))
            .addAllShard(shards.asJava)
            .build()
      }.asJava)
      .build()
  }

  private def shardHomesFromBinary(bytes: Array[Byte]): ShardHomes = {
    val sh = sm.ShardHomes.parseFrom(bytes)
    ShardHomes(sh.getHomesList.asScala.map { she =>
      resolveActorRef(she.getRegion) -> she.getShardList.asScala.toVector
    }.toMap)
  }

  private def entityStateToProto(m: EntityState): sm.EntityState = {
    val b = sm.EntityState.newBuilder()
    m.entities.foreach(b.addEntities)
    b.build()
  }

  private def entityStateFromBinary(bytes: Array[Byte]): EntityState =
    EntityState(sm.EntityState.parseFrom(bytes).getEntitiesList.asScala.toSet)

  private def entityStartedFromBinary(bytes: Array[Byte]): EntitiesStarted =
    EntitiesStarted(Set(sm.EntityStarted.parseFrom(bytes).getEntityId))

  private def entitiesStartedToProto(evt: EntitiesStarted): sm.EntitiesStarted =
    sm.EntitiesStarted.newBuilder().addAllEntityId(evt.entities.asJava).build()

  private def entitiesStartedFromBinary(bytes: Array[Byte]): EntitiesStarted =
    EntitiesStarted(sm.EntitiesStarted.parseFrom(bytes).getEntityIdList.asScala.toSet)

  private def entitiesStoppedToProto(evt: EntitiesStopped): sm.EntitiesStopped =
    sm.EntitiesStopped.newBuilder().addAllEntityId(evt.entities.asJava).build()

  private def entityStoppedFromBinary(bytes: Array[Byte]): EntitiesStopped =
    EntitiesStopped(Set(sm.EntityStopped.parseFrom(bytes).getEntityId))

  private def entitiesStoppedFromBinary(bytes: Array[Byte]): EntitiesStopped =
    EntitiesStopped(sm.EntitiesStopped.parseFrom(bytes).getEntityIdList.asScala.toSet)

  private def shardStatsToProto(evt: ShardStats): sm.ShardStats =
    sm.ShardStats.newBuilder().setShard(evt.shardId).setEntityCount(evt.entityCount).build()

  private def shardStatsFromBinary(bytes: Array[Byte]): ShardStats = {
    val parsed = sm.ShardStats.parseFrom(bytes)
    ShardStats(parsed.getShard, parsed.getEntityCount)
  }

  private def shardRegionStatsToProto(evt: ShardRegionStats): sm.ShardRegionStats = {
    val b = sm.ShardRegionStats.newBuilder()
    evt.stats.foreach {
      case (sid, no) =>
        b.addStats(sm.MapFieldEntry.newBuilder().setKey(sid).setValue(no).build())
    }
    evt.failed.foreach { sid =>
      b.addFailed(sid).build()
    }
    b.build()
  }

  private def shardRegionStatsFromBinary(bytes: Array[Byte]): ShardRegionStats = {
    val parsed = sm.ShardRegionStats.parseFrom(bytes)
    shardRegionStatsFromProto(parsed)
  }

  private def shardRegionStatsFromProto(parsed: ClusterShardingMessages.ShardRegionStats): ShardRegionStats = {
    val stats: Map[String, Int] = parsed.getStatsList.asScala.iterator.map(e => e.getKey -> e.getValue).toMap
    val failed: Set[String] = parsed.getFailedList.asScala.toSet
    ShardRegionStats(stats, failed)
  }

  private def clusterShardingStatsToProto(evt: ClusterShardingStats): sm.ClusterShardingStats = {
    val b = sm.ClusterShardingStats.newBuilder()
    evt.regions.foreach {
      case (address, shardRegionStats) =>
        b.addStats(
          sm.ClusterShardingStatsEntry
            .newBuilder()
            .setAddress(serializeAddress(address))
            .setStats(shardRegionStatsToProto(shardRegionStats))
            .build())
    }
    b.build()
  }

  private def clusterShardingStatsFromBinary(bytes: Array[Byte]): ClusterShardingStats = {
    val parsed = sm.ClusterShardingStats.parseFrom(bytes)
    val stats: Map[Address, ShardRegionStats] =
      parsed.getStatsList.asScala.iterator
        .map(e => deserializeAddress(e.getAddress) -> shardRegionStatsFromProto(e.getStats))
        .toMap
    ClusterShardingStats(stats)
  }

  private def getClusterShardingStatsToProto(evt: GetClusterShardingStats): sm.GetClusterShardingStats = {
    sm.GetClusterShardingStats.newBuilder().setTimeoutNanos(evt.timeout.toNanos).build()
  }

  private def getClusterShardingStatsFromBinary(bytes: Array[Byte]): GetClusterShardingStats = {
    val parsed = sm.GetClusterShardingStats.parseFrom(bytes)
    GetClusterShardingStats(parsed.getTimeoutNanos.nanos)
  }

  private def currentRegionsToProto(evt: CurrentRegions): sm.CurrentRegions = {
    val b = sm.CurrentRegions.newBuilder()
    evt.regions.foreach { address =>
      b.addRegions(serializeAddress(address))
    }
    b.build()
  }

  private def currentRegionsFromBinary(bytes: Array[Byte]): CurrentRegions = {
    val parsed = sm.CurrentRegions.parseFrom(bytes)
    val regions: Set[Address] = parsed.getRegionsList.asScala.iterator.map(deserializeAddress).toSet
    CurrentRegions(regions)
  }

  private def startEntityToByteArray(s: StartEntity): Array[Byte] = {
    val builder = sm.StartEntity.newBuilder()
    builder.setEntityId(s.entityId)
    builder.build().toByteArray
  }

  private def startEntityFromBinary(bytes: Array[Byte]): StartEntity = {
    val se = sm.StartEntity.parseFrom(bytes)
    StartEntity(se.getEntityId)
  }

  private def startEntityAckToByteArray(s: StartEntityAck): Array[Byte] = {
    val builder = sm.StartEntityAck.newBuilder()
    builder.setEntityId(s.entityId)
    builder.setShardId(s.shardId)
    builder.build().toByteArray
  }

  private def startEntityAckFromBinary(bytes: Array[Byte]): StartEntityAck = {
    val sea = sm.StartEntityAck.parseFrom(bytes)
    StartEntityAck(sea.getEntityId, sea.getShardId)
  }

  private def shardStateToProto(evt: ShardState): sm.ShardState = {
    sm.ShardState.newBuilder().setShardId(evt.shardId).addAllEntityIds(evt.entityIds.asJava).build()
  }

  private def currentShardStateToProto(evt: CurrentShardState): sm.CurrentShardState = {
    sm.CurrentShardState.newBuilder().setShardId(evt.shardId).addAllEntityIds(evt.entityIds.asJava).build()
  }

  private def currentShardStateFromBinary(bytes: Array[Byte]): CurrentShardState = {
    val parsed = sm.CurrentShardState.parseFrom(bytes)
    CurrentShardState(parsed.getShardId, parsed.getEntityIdsList.asScala.toSet)
  }

  private def shardStateFromProto(parsed: ClusterShardingMessages.ShardState): ShardState = {
    ShardState(parsed.getShardId, parsed.getEntityIdsList.asScala.toSet)
  }

  private def shardStateFromBinary(bytes: Array[Byte]): ShardState = {
    val parsed = sm.ShardState.parseFrom(bytes)
    ShardState(parsed.getShardId, parsed.getEntityIdsList.asScala.toSet)
  }

  private def currentShardRegionStateToProto(evt: CurrentShardRegionState): sm.CurrentShardRegionState = {
    val shards = evt.shards.map(shardStateToProto).asJava
    val failed = evt.failed.asJava
    sm.CurrentShardRegionState.newBuilder().addAllShards(shards).addAllFailed(failed).build()
  }

  private def currentShardRegionStateFromBinary(bytes: Array[Byte]): CurrentShardRegionState = {
    val parsed = sm.CurrentShardRegionState.parseFrom(bytes)
    val state: Set[ShardState] = parsed.getShardsList.asScala.map(shardStateFromProto).toSet
    val failed: Set[String] = parsed.getFailedList.asScala.toSet
    CurrentShardRegionState(state, failed)
  }

  def serializeAddress(address: Address): sm.Address =
    address match {
      case Address(protocol, systemName, Some(host), Some(port)) =>
        sm.Address.newBuilder().setProtocol(protocol).setSystem(systemName).setHostname(host).setPort(port).build()
      case _ => throw new IllegalArgumentException(s"Address [$address] could not be serialized: host or port missing.")
    }

  def deserializeAddress(address: sm.Address): Address =
    Address(address.getProtocol, address.getSystem, address.getHostname, address.getPort)

  private def resolveActorRef(path: String): ActorRef = {
    system.provider.resolveActorRef(path)
  }

  private def compress(msg: MessageLite): Array[Byte] = {
    val bos = new ByteArrayOutputStream(BufferSize)
    val zip = new GZIPOutputStream(bos)
    try msg.writeTo(zip)
    finally zip.close()
    bos.toByteArray
  }

  private def decompress(bytes: Array[Byte]): Array[Byte] = {
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val out = new ByteArrayOutputStream()
    val buffer = new Array[Byte](BufferSize)

    @tailrec def readChunk(): Unit = in.read(buffer) match {
      case -1 => ()
      case n =>
        out.write(buffer, 0, n)
        readChunk()
    }

    try readChunk()
    finally in.close()
    out.toByteArray
  }

}
