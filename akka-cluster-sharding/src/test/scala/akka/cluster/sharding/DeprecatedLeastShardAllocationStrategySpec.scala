/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.collection.immutable.SortedSet

import akka.actor.ActorRef
import akka.actor.Address
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterSettings
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.UniqueAddress
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.RegionEntry
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.ShardSuitabilityOrdering
import akka.testkit.AkkaSpec
import akka.util.Version

class DeprecatedLeastShardAllocationStrategySpec extends AkkaSpec {
  import LeastShardAllocationStrategySpec._

  val memberA = newUpMember("127.0.0.1")
  val memberB = newUpMember("127.0.0.2")
  val memberC = newUpMember("127.0.0.3")

  val regionA = newFakeRegion("regionA", memberA)
  val regionB = newFakeRegion("regionB", memberB)
  val regionC = newFakeRegion("regionC", memberC)

  def createAllocations(aCount: Int, bCount: Int = 0, cCount: Int = 0): Map[ActorRef, Vector[String]] = {
    val shards = (1 to (aCount + bCount + cCount)).map(n => ("00" + n.toString).takeRight(3))
    Map(
      regionA -> shards.take(aCount).toVector,
      regionB -> shards.slice(aCount, aCount + bCount).toVector,
      regionC -> shards.takeRight(cCount).toVector)
  }

  private def allocationStrategyWithFakeCluster(rebalanceThreshold: Int, maxSimultaneousRebalance: Int) =
    // we don't really "start" it as we fake the cluster access
    new ShardCoordinator.LeastShardAllocationStrategy(rebalanceThreshold, maxSimultaneousRebalance) {
      override protected def clusterState: CurrentClusterState =
        CurrentClusterState(SortedSet(memberA, memberB, memberC))
      override protected def selfMember: Member = memberA
    }

  "DeprecatedLeastShardAllocationStrategy" must {
    "allocate to region with least number of shards [1, 1, 0]" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 3, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 1, bCount = 1)
      allocationStrategy.allocateShard(regionA, "003", allocations).futureValue should ===(regionC)
    }

    "rebalance from region with most number of shards [2, 0, 0], rebalanceThreshold=1" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 1, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 2)

      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("001"))
      allocationStrategy.rebalance(allocations, Set("001")).futureValue should ===(Set.empty[String])
    }

    "not rebalance when diff equal to threshold, [1, 1, 0], rebalanceThreshold=1" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 1, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 1, bCount = 1)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set.empty[String])
    }

    "rebalance from region with most number of shards [1, 2, 0], rebalanceThreshold=1" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 1, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 1, bCount = 2)

      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("002"))
      allocationStrategy.rebalance(allocations, Set("002")).futureValue should ===(Set.empty[String])
    }

    "rebalance from region with most number of shards [3, 0, 0], rebalanceThreshold=1" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 1, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 3)

      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("001"))
      allocationStrategy.rebalance(allocations, Set("001")).futureValue should ===(Set("002"))
    }

    "rebalance from region with most number of shards [4, 4, 0], rebalanceThreshold=1" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 1, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 4, bCount = 4)

      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("001"))
      allocationStrategy.rebalance(allocations, Set("001")).futureValue should ===(Set("005"))
    }

    "rebalance from region with most number of shards [4, 4, 2], rebalanceThreshold=1" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 1, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 4, bCount = 4, cCount = 2)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("001"))
      // not optimal, 005 stopped and started again, but ok
      allocationStrategy.rebalance(allocations, Set("001")).futureValue should ===(Set("005"))
    }

    "rebalance from region with most number of shards [1, 3, 0], rebalanceThreshold=2" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 2, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 1, bCount = 2)

      // so far regionB has 2 shards and regionC has 0 shards, but the diff is <= rebalanceThreshold
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set.empty[String])

      val allocations2 = createAllocations(aCount = 1, bCount = 3)
      allocationStrategy.rebalance(allocations2, Set.empty).futureValue should ===(Set("002"))
      allocationStrategy.rebalance(allocations2, Set("002")).futureValue should ===(Set.empty[String])
    }

    "not rebalance when diff equal to threshold, [2, 2, 0], rebalanceThreshold=2" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 2, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 2, bCount = 2)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set.empty[String])
    }

    "rebalance from region with most number of shards [3, 3, 0], rebalanceThreshold=2" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 2, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 3, bCount = 3)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("001"))
      allocationStrategy.rebalance(allocations, Set("001")).futureValue should ===(Set("004"))
      allocationStrategy.rebalance(allocations, Set("001", "004")).futureValue should ===(Set.empty[String])
    }

    "rebalance from region with most number of shards [4, 4, 0], rebalanceThreshold=2" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 2, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 4, bCount = 4)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("001", "002"))
      allocationStrategy.rebalance(allocations, Set("001", "002")).futureValue should ===(Set("005", "006"))
      allocationStrategy.rebalance(allocations, Set("001", "002", "005", "006")).futureValue should ===(
        Set.empty[String])
    }

    "rebalance from region with most number of shards [5, 5, 0], rebalanceThreshold=2" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 2, maxSimultaneousRebalance = 10)
      val allocations = createAllocations(aCount = 5, bCount = 5)
      // optimal would => [4, 4, 2] or even => [3, 4, 3]
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("001", "002"))
      // if 001 and 002 are not started quickly enough this is stopping more than optimal
      allocationStrategy.rebalance(allocations, Set("001", "002")).futureValue should ===(Set("006", "007"))
      allocationStrategy.rebalance(allocations, Set("001", "002", "006", "007")).futureValue should ===(Set("003"))
    }

    "rebalance from region with most number of shards [50, 50, 0], rebalanceThreshold=2" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 2, maxSimultaneousRebalance = 100)
      val allocations = createAllocations(aCount = 50, cCount = 50)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("001", "002"))
      allocationStrategy.rebalance(allocations, Set("001", "002")).futureValue should ===(Set("051", "052"))
      allocationStrategy.rebalance(allocations, Set("001", "002", "051", "052")).futureValue should ===(
        Set("003", "004"))
    }

    "limit number of simultaneous rebalance [1, 10, 0]" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 3, maxSimultaneousRebalance = 2)
      val allocations = createAllocations(aCount = 1, bCount = 10)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("002", "003"))
      allocationStrategy.rebalance(allocations, Set("002", "003")).futureValue should ===(Set.empty[String])
    }

    "not pick shards that are in progress [10, 0, 0]" in {
      val allocationStrategy = allocationStrategyWithFakeCluster(rebalanceThreshold = 3, maxSimultaneousRebalance = 4)
      val allocations = createAllocations(aCount = 10)
      allocationStrategy.rebalance(allocations, Set("002", "003")).futureValue should ===(Set("001", "004"))
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
        new ShardCoordinator.LeastShardAllocationStrategy(rebalanceThreshold = 2, maxSimultaneousRebalance = 100) {

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
        new ShardCoordinator.LeastShardAllocationStrategy(rebalanceThreshold = 2, maxSimultaneousRebalance = 100) {

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
        new ShardCoordinator.LeastShardAllocationStrategy(rebalanceThreshold = 2, maxSimultaneousRebalance = 100) {

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
