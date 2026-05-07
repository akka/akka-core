/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.protobuf

import scala.concurrent.duration._

import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.cluster.sharding.Shard
import akka.cluster.sharding.ShardCoordinator
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.internal.EventSourcedRememberEntitiesShardStore
import akka.cluster.sharding.internal.EventSourcedRememberEntitiesShardStore.EntitiesStarted
import akka.serialization.SerializationExtension
import akka.testkit.AkkaSpec

class ClusterShardingMessageSerializerSpec extends AkkaSpec {
  import ShardCoordinator.Internal._

  val serializer = new ClusterShardingMessageSerializer(system.asInstanceOf[ExtendedActorSystem])

  val region1 = system.actorOf(Props.empty, "region1")
  val region2 = system.actorOf(Props.empty, "region2")
  val region3 = system.actorOf(Props.empty, "region3")
  val regionProxy1 = system.actorOf(Props.empty, "regionProxy1")
  val regionProxy2 = system.actorOf(Props.empty, "regionProxy2")

  def checkSerialization(obj: AnyRef): Unit = {
    SerializationExtension(system).findSerializerFor(obj).identifier should ===(serializer.identifier)
    val blob = serializer.toBinary(obj)
    val ref = serializer.fromBinary(blob, serializer.manifest(obj))
    ref should ===(obj)
  }

  "ClusterShardingMessageSerializer" must {

    "be able to serialize ShardCoordinator snapshot State" in {
      val state = State(
        shards = Map("a" -> region1, "b" -> region2, "c" -> region2),
        regions = Map(region1 -> Vector("a"), region2 -> Vector("b", "c"), region3 -> Vector.empty[String]),
        regionProxies = Set(regionProxy1, regionProxy2),
        unallocatedShards = Set("d"))
      checkSerialization(state)
    }

    "be able to serialize ShardCoordinator domain events" in {
      checkSerialization(ShardRegionRegistered(region1))
      checkSerialization(ShardRegionProxyRegistered(regionProxy1))
      checkSerialization(ShardRegionTerminated(region1))
      checkSerialization(ShardRegionProxyTerminated(regionProxy1))
      checkSerialization(ShardHomeAllocated("a", region1))
      checkSerialization(ShardHomeDeallocated("a"))
    }

    "be able to serialize ShardCoordinator remote messages" in {
      checkSerialization(Register(region1))
      checkSerialization(RegisterProxy(regionProxy1))
      checkSerialization(RegisterAck(region1))
      checkSerialization(GetShardHome("a"))
      checkSerialization(ShardHome("a", region1))
      checkSerialization(HostShard("a"))
      checkSerialization(ShardStarted("a"))
      checkSerialization(BeginHandOff("a"))
      checkSerialization(BeginHandOffAck("a"))
      checkSerialization(HandOff("a"))
      checkSerialization(ShardStopped("a"))
      checkSerialization(GracefulShutdownReq(region1))
    }

    "be able to serialize PersistentShard snapshot state" in {
      checkSerialization(EventSourcedRememberEntitiesShardStore.State(Set("e1", "e2", "e3")))
    }

    "be able to serialize PersistentShard domain events" in {
      checkSerialization(EventSourcedRememberEntitiesShardStore.EntitiesStarted(Set("e1", "e2")))
      checkSerialization(EventSourcedRememberEntitiesShardStore.EntitiesStopped(Set("e1", "e2")))
    }

    "be able to deserialize old entity started event into entities started" in {
      import akka.cluster.sharding.protobuf.msg.{ ClusterShardingMessages => sm }

      val asBytes = sm.EntityStarted.newBuilder().setEntityId("e1").build().toByteArray
      SerializationExtension(system).deserialize(asBytes, 13, "CB").get shouldEqual EntitiesStarted(Set("e1"))
    }

    "be able to serialize GetShardStats" in {
      checkSerialization(Shard.GetShardStats)
    }

    "be able to serialize ShardStats" in {
      checkSerialization(Shard.ShardStats("a", 23))
    }

    "be able to serialize GetShardRegionStats" in {
      checkSerialization(ShardRegion.GetShardRegionStats)
    }

    "be able to serialize ShardRegionStats" in {
      checkSerialization(ShardRegion.ShardRegionStats(Map.empty[ShardId, Int], Set.empty[ShardId]))
      checkSerialization(ShardRegion.ShardRegionStats(Map[ShardId, Int]("a" -> 23), Set("b")))
    }

    "be able to serialize StartEntity" in {
      checkSerialization(ShardRegion.StartEntity("42"))
      checkSerialization(ShardRegion.StartEntityAck("13", "37"))
    }

    "be able to serialize GetCurrentRegions" in {
      checkSerialization(ShardRegion.GetCurrentRegions)
      checkSerialization(
        ShardRegion.CurrentRegions(Set(Address("akka", "sys", "a", 2552), Address("akka", "sys", "b", 2552))))
    }

    "be able to serialize GetClusterShardingStats" in {
      checkSerialization(ShardRegion.GetClusterShardingStats(3.seconds))
      checkSerialization(
        ShardRegion.ClusterShardingStats(Map(
          Address("akka", "sys", "a", 2552) -> ShardRegion.ShardRegionStats(Map[ShardId, Int]("a" -> 23), Set("b")),
          Address("akka", "sys", "b", 2552) -> ShardRegion.ShardRegionStats(Map[ShardId, Int]("a" -> 23), Set("b")))))
    }
  }
}
