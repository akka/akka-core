/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.collection.immutable
import scala.collection.immutable.SortedSet

import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorRefProvider
import akka.actor.Address
import akka.actor.MinimalActorRef
import akka.actor.RootActorPath
import akka.cluster.ClusterEvent
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterSettings
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.UniqueAddress
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.RegionEntry
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.ShardSuitabilityOrdering
import akka.cluster.sharding.internal.LeastShardAllocationStrategy
import akka.testkit.AkkaSpec
import akka.util.Version

object LeastShardAllocationStrategySpec {

  private object DummyActorRef extends MinimalActorRef {
    override val path: ActorPath = RootActorPath(Address("akka", "myapp")) / "system" / "fake"

    override def provider: ActorRefProvider = ???
  }

  def afterRebalance(
      allocationStrategy: ShardAllocationStrategy,
      allocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
      rebalance: Set[ShardId]): Map[ActorRef, immutable.IndexedSeq[ShardId]] = {
    val allocationsAfterRemoval = allocations.map {
      case (region, shards) => region -> shards.filterNot(rebalance)
    }

    rebalance.toList.sorted.foldLeft(allocationsAfterRemoval) {
      case (acc, shard) =>
        val region = allocationStrategy.allocateShard(DummyActorRef, shard, acc).value.get.get
        acc.updated(region, acc(region) :+ shard)
    }
  }

  def countShardsPerRegion(newAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Vector[Int] = {
    newAllocations.valuesIterator.map(_.size).toVector
  }

  def countShards(allocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Int = {
    countShardsPerRegion(allocations).sum
  }

  def allocationCountsAfterRebalance(
      allocationStrategy: ShardAllocationStrategy,
      allocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
      rebalance: Set[ShardId]): Vector[Int] = {
    countShardsPerRegion(afterRebalance(allocationStrategy, allocations, rebalance))
  }

  final class DummyActorRef(val path: ActorPath) extends MinimalActorRef {
    override def provider: ActorRefProvider = ???
  }

  def newUpMember(host: String, port: Int = 252525, version: Version = Version("1.0.0")) =
    Member(
      UniqueAddress(Address("akka", "myapp", host, port), 1L),
      Set(ClusterSettings.DcRolePrefix + ClusterSettings.DefaultDataCenter),
      version).copy(MemberStatus.Up)

  def newFakeRegion(idForDebug: String, member: Member): ActorRef =
    new DummyActorRef(RootActorPath(member.address) / "system" / "fake" / idForDebug)
}

class LeastShardAllocationStrategySpec extends AkkaSpec {
  import LeastShardAllocationStrategySpec._

  val memberA = newUpMember("127.0.0.1")
  val memberB = newUpMember("127.0.0.2")
  val memberC = newUpMember("127.0.0.3")

  val regionA = newFakeRegion("regionA", memberA)
  val regionB = newFakeRegion("regionB", memberB)
  val regionC = newFakeRegion("regionC", memberC)

  private val shards = (1 to 999).map(n => ("00" + n.toString).takeRight(3))

  def createAllocations(aCount: Int, bCount: Int = 0, cCount: Int = 0): Map[ActorRef, Vector[String]] = {
    Map(
      regionA -> shards.take(aCount).toVector,
      regionB -> shards.slice(aCount, aCount + bCount).toVector,
      regionC -> shards.slice(aCount + bCount, aCount + bCount + cCount).toVector)
  }

  private val strategyWithoutLimits =
    strategyWithFakeCluster(absoluteLimit = 1000, relativeLimit = 1.0)

  private def strategyWithFakeCluster(absoluteLimit: Int, relativeLimit: Double) =
    // we don't really "start" it as we fake the cluster access
    new LeastShardAllocationStrategy(absoluteLimit, relativeLimit) {
      override protected def clusterState: ClusterEvent.CurrentClusterState =
        CurrentClusterState(SortedSet(memberA, memberB, memberC))
      override protected def selfMember: Member = memberA
    }

  "LeastShardAllocationStrategy" must {
    "allocate to region with least number of shards" in {
      val allocationStrategy = strategyWithoutLimits
      val allocations = createAllocations(aCount = 1, bCount = 1)
      allocationStrategy.allocateShard(regionA, "003", allocations).futureValue should ===(regionC)
    }

    "rebalance shards [1, 2, 0]" in {
      val allocationStrategy = strategyWithoutLimits
      val allocations = createAllocations(aCount = 1, bCount = 2)
      val result = allocationStrategy.rebalance(allocations, Set.empty).futureValue
      result should ===(Set("002"))
      allocationCountsAfterRebalance(allocationStrategy, allocations, result) should ===(Vector(1, 1, 1))
    }

    "rebalance shards [2, 0, 0]" in {
      val allocationStrategy = strategyWithoutLimits
      val allocations = createAllocations(aCount = 2)
      val result = allocationStrategy.rebalance(allocations, Set.empty).futureValue
      result should ===(Set("001"))
      allocationCountsAfterRebalance(allocationStrategy, allocations, result) should ===(Vector(1, 1, 0))
    }

    "not rebalance shards [1, 1, 0]" in {
      val allocationStrategy = strategyWithoutLimits
      val allocations = createAllocations(aCount = 1, bCount = 1)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set.empty[String])
    }

    "rebalance shards [3, 0, 0]" in {
      val allocationStrategy = strategyWithoutLimits
      val allocations = createAllocations(aCount = 3)
      val result = allocationStrategy.rebalance(allocations, Set.empty).futureValue
      result should ===(Set("001", "002"))
      allocationCountsAfterRebalance(allocationStrategy, allocations, result) should ===(Vector(1, 1, 1))
    }

    "rebalance shards [4, 4, 0]" in {
      val allocationStrategy = strategyWithoutLimits
      val allocations = createAllocations(aCount = 4, bCount = 4)
      val result = allocationStrategy.rebalance(allocations, Set.empty).futureValue
      result should ===(Set("001", "005"))
      allocationCountsAfterRebalance(allocationStrategy, allocations, result) should ===(Vector(3, 3, 2))
    }

    "rebalance shards [4, 4, 2]" in {
      // this is handled by phase 2, to find diff of 2
      val allocationStrategy = strategyWithoutLimits
      val allocations = createAllocations(aCount = 4, bCount = 4, cCount = 2)
      val result = allocationStrategy.rebalance(allocations, Set.empty).futureValue
      result should ===(Set("001"))
      allocationCountsAfterRebalance(allocationStrategy, allocations, result) should ===(Vector(3, 4, 3))
    }

    "rebalance shards [5, 5, 0]" in {
      val allocationStrategy = strategyWithoutLimits
      val allocations = createAllocations(aCount = 5, bCount = 5)
      val result1 = allocationStrategy.rebalance(allocations, Set.empty).futureValue
      result1 should ===(Set("001", "006"))

      // so far [4, 4, 2]
      allocationCountsAfterRebalance(allocationStrategy, allocations, result1) should ===(Vector(4, 4, 2))
      val allocations2 = afterRebalance(allocationStrategy, allocations, result1)
      // second phase will find the diff of 2, resulting in [3, 4, 3]
      val result2 = allocationStrategy.rebalance(allocations2, Set.empty).futureValue
      result2 should ===(Set("002"))
      allocationCountsAfterRebalance(allocationStrategy, allocations2, result2) should ===(Vector(3, 4, 3))
    }

    "rebalance shards [50, 50, 0]" in {
      val allocationStrategy = strategyWithoutLimits
      val allocations = createAllocations(aCount = 50, cCount = 50)
      val result1 = allocationStrategy.rebalance(allocations, Set.empty).futureValue
      result1 should ===(shards.take(50 - 34).toSet ++ shards.drop(50).take(50 - 34))

      // so far [34, 34, 32]
      allocationCountsAfterRebalance(allocationStrategy, allocations, result1).sorted should ===(
        Vector(34, 34, 32).sorted)
      val allocations2 = afterRebalance(allocationStrategy, allocations, result1)
      // second phase will find the diff of 2, resulting in [33, 34, 33]
      val result2 = allocationStrategy.rebalance(allocations2, Set.empty).futureValue
      result2 should ===(Set("017"))
      allocationCountsAfterRebalance(allocationStrategy, allocations2, result2).sorted should ===(
        Vector(33, 34, 33).sorted)
    }

    "respect absolute limit of number shards" in {
      val allocationStrategy =
        strategyWithFakeCluster(absoluteLimit = 3, relativeLimit = 1.0)
      val allocations = createAllocations(aCount = 1, bCount = 9)
      val result = allocationStrategy.rebalance(allocations, Set.empty).futureValue
      result should ===(Set("002", "003", "004"))
      allocationCountsAfterRebalance(allocationStrategy, allocations, result) should ===(Vector(2, 6, 2))
    }

    "respect relative limit of number shards" in {
      val allocationStrategy =
        strategyWithFakeCluster(absoluteLimit = 5, relativeLimit = 0.3)
      val allocations = createAllocations(aCount = 1, bCount = 9)
      val result = allocationStrategy.rebalance(allocations, Set.empty).futureValue
      result should ===(Set("002", "003", "004"))
      allocationCountsAfterRebalance(allocationStrategy, allocations, result) should ===(Vector(2, 6, 2))
    }

    "not rebalance when in progress" in {
      val allocationStrategy = strategyWithoutLimits
      val allocations = createAllocations(aCount = 10)
      allocationStrategy.rebalance(allocations, Set("002", "003")).futureValue should ===(Set.empty[String])
    }

    "prefer least shards, latest version, non downed, leaving or exiting nodes" in {
      // old version, up
      val oldMember = newUpMember("127.0.0.1", version = Version("1.0.0"))
      // leaving, new version
      val leavingMember = newUpMember("127.0.0.2", version = Version("1.0.0")).copy(MemberStatus.Leaving)
      // new version, up
      val newVersionMember1 = newUpMember("127.0.0.3", version = Version("1.0.1"))
      // new version, up
      val newVersionMember2 = newUpMember("127.0.0.4", version = Version("1.0.1"))
      // new version, up
      val newVersionMember3 = newUpMember("127.0.0.5", version = Version("1.0.1"))

      val fakeLocalRegion = newFakeRegion("oldapp", oldMember)
      val fakeRegionA = newFakeRegion("leaving", leavingMember)
      val fakeRegionB = newFakeRegion("fewest", newVersionMember1)
      val fakeRegionC = newFakeRegion("oneshard", newVersionMember2)
      val fakeRegionD = newFakeRegion("most", newVersionMember3)

      val shardsAndMembers =
        Seq(
          RegionEntry(fakeRegionB, newVersionMember1, Vector.empty),
          RegionEntry(fakeRegionA, leavingMember, Vector.empty),
          RegionEntry(fakeRegionD, newVersionMember3, Vector("ShardId2", "ShardId3")),
          RegionEntry(fakeLocalRegion, oldMember, Vector.empty),
          RegionEntry(fakeRegionC, newVersionMember2, Vector("ShardId1")))

      val sortedRegions =
        shardsAndMembers.sorted(ShardSuitabilityOrdering).map(_.region)

      // only node b has the new version
      sortedRegions should ===(
        Seq(
          fakeRegionB, // fewest shards, newest version, up
          fakeRegionC, // newest version, up
          fakeRegionD, // most shards, up
          fakeLocalRegion, // old app version
          fakeRegionA)) // leaving
    }

    "not rebalance when rolling update in progress" in {
      val allocationStrategy =
        new LeastShardAllocationStrategy(absoluteLimit = 1000, relativeLimit = 1.0) {

          val member1 = newUpMember("127.0.0.1", version = Version("1.0.0"))
          val member2 = newUpMember("127.0.0.2", version = Version("1.0.1"))
          val member3 = newUpMember("127.0.0.3", version = Version("1.0.0"))

          // multiple versions to simulate rolling update in progress
          override protected def clusterState: CurrentClusterState =
            CurrentClusterState(SortedSet(member1, member2, member3))

          override protected def selfMember: Member = member1
        }
      val allocations = createAllocations(aCount = 5, bCount = 5)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set.empty[String])
      allocationStrategy.rebalance(allocations, Set("001", "002")).futureValue should ===(Set.empty[String])
      allocationStrategy.rebalance(allocations, Set("001", "002", "051", "052")).futureValue should ===(
        Set.empty[String])
    }

    "not rebalance when regions are unreachable" in {
      val allocationStrategy =
        new LeastShardAllocationStrategy(absoluteLimit = 1000, relativeLimit = 1.0) {

          override protected def clusterState: CurrentClusterState =
            CurrentClusterState(SortedSet(memberA, memberB, memberC), unreachable = Set(memberB))
          override protected def selfMember: Member = memberB
        }
      val allocations = createAllocations(aCount = 5, bCount = 5)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set.empty[String])
      allocationStrategy.rebalance(allocations, Set("001", "002")).futureValue should ===(Set.empty[String])
      allocationStrategy.rebalance(allocations, Set("001", "002", "051", "052")).futureValue should ===(
        Set.empty[String])
    }
    "not rebalance when members are joining dc" in {
      val allocationStrategy =
        new LeastShardAllocationStrategy(absoluteLimit = 1000, relativeLimit = 1.0) {

          val member1 = newUpMember("127.0.0.1")
          val member2 =
            Member(
              UniqueAddress(Address("akka", "myapp", "127.0.0.2", 252525), 1L),
              Set(ClusterSettings.DcRolePrefix + ClusterSettings.DefaultDataCenter),
              member1.appVersion)
          val member3 = newUpMember("127.0.0.3")

          override protected def clusterState: CurrentClusterState =
            CurrentClusterState(SortedSet(member1, member2, member3), unreachable = Set.empty)
          override protected def selfMember: Member = member2
        }
      val allocations = createAllocations(aCount = 5, bCount = 5)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set.empty[String])
      allocationStrategy.rebalance(allocations, Set("001", "002")).futureValue should ===(Set.empty[String])
      allocationStrategy.rebalance(allocations, Set("001", "002", "051", "052")).futureValue should ===(
        Set.empty[String])
    }

  }
}
