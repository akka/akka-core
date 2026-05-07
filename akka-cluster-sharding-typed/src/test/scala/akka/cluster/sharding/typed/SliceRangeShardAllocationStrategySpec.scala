/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.immutable.SortedSet
import scala.util.Random

import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorRefProvider
import akka.actor.Address
import akka.actor.MinimalActorRef
import akka.actor.RootActorPath
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterSettings
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.UniqueAddress
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.typed.SliceRangeShardAllocationStrategy.ShardBySliceMessageExtractor
import akka.persistence.Persistence
import akka.testkit.AkkaSpec
import akka.util.Version

object SliceRangeShardAllocationStrategySpec {

  final class DummyActorRef(val path: ActorPath) extends MinimalActorRef {
    override def provider: ActorRefProvider = ???
  }

  def newUpMember(host: String, upNbr: Int, port: Int = 252525, version: Version = Version("1.0.0")) =
    Member(
      UniqueAddress(Address("akka", "myapp", host, port), 1L),
      Set(ClusterSettings.DcRolePrefix + ClusterSettings.DefaultDataCenter),
      version).copy(MemberStatus.Up).copyUp(upNbr)

  def newFakeRegion(idForDebug: String, member: Member): ActorRef =
    new DummyActorRef(RootActorPath(member.address) / "system" / "fake" / idForDebug)
}

class SliceRangeShardAllocationStrategySpec extends AkkaSpec {
  import SliceRangeShardAllocationStrategySpec._

  val memberA = newUpMember("127.0.0.1", upNbr = 1)
  val memberB = newUpMember("127.0.0.2", upNbr = 2)
  val memberC = newUpMember("127.0.0.3", upNbr = 3)
  val memberD = newUpMember("127.0.0.4", upNbr = 4)

  val regionA = newFakeRegion("regionA", memberA)
  val regionB = newFakeRegion("regionB", memberB)
  val regionC = newFakeRegion("regionC", memberC)
  val regionD = newFakeRegion("regionD", memberD)

  private def createAllocationStrategy(
      members: IndexedSeq[Member],
      absoluteLimit: Int = 10,
      relativeLimit: Double = 0.1) = {
    new SliceRangeShardAllocationStrategy(absoluteLimit, relativeLimit) {
      override protected def clusterState: CurrentClusterState =
        CurrentClusterState(SortedSet(members: _*))

      override protected def selfMember: Member = members.head
    }
  }

  private def sliceRanges(numberOfRanges: Int): immutable.IndexedSeq[Range] = {
    val numberOfSlices = 1024
    val rangeSize = numberOfSlices / numberOfRanges
    require(
      numberOfRanges * rangeSize == numberOfSlices,
      s"numberOfRanges [$numberOfRanges] must be a whole number divisor of numberOfSlices [$numberOfSlices].")
    (0 until numberOfRanges).map { i =>
      (i * rangeSize until i * rangeSize + rangeSize)
    }.toVector
  }

  private def numberOfSliceRangesPerRegion(
      numberOfRanges: Int,
      shardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Map[ActorRef, Int] = {
    val ranges = sliceRanges(numberOfRanges)
    shardAllocations.map {
      case (region, shards) =>
        region -> shards.map(s => ranges.find(_.contains(s.toInt)).get).toSet.size
    }
  }

  class Setup(initialNumberOfMembers: Int, shuffle: Boolean = false) {

    def rndSeed: Long = System.currentTimeMillis()

    val rnd = new Random(rndSeed)

    private var _members: Vector[Member] =
      (1 to initialNumberOfMembers).map(n => newUpMember(s"127.0.0.$n", upNbr = n)).toVector

    private var _regions: Vector[ActorRef] =
      _members.map(m => newFakeRegion(s"region${m.upNumber}", m))

    def regions: Vector[ActorRef] = _regions

    private var _allocations: Map[ActorRef, Vector[ShardId]] =
      _regions.map(_ -> Vector.empty[String]).toMap

    def allocations: Map[ActorRef, Vector[Int]] =
      _allocations.map { case (region, shards) => region -> shards.map(_.toInt) }

    def allocation(slice: Int): Option[ActorRef] = {
      val shardId = slice.toString
      _allocations.collectFirst { case (region, shards) if shards.contains(shardId) => region }
    }

    private var oldAllocations: Map[ActorRef, Vector[ShardId]] =
      Map.empty

    private val allSlices: Vector[Int] =
      (0 to 1023).toVector

    private var strategy = createAllocationStrategy(_members)

    def numberOfMembers: Int = _members.size

    def allocateAll(): Map[ActorRef, Vector[ShardId]] =
      allocate(allSlices)

    def allocate(slice: Int): Map[ActorRef, Vector[ShardId]] =
      allocate(Vector(slice))

    def allocate(slices: Vector[Int]): Map[ActorRef, Vector[ShardId]] = {
      val shuffledSlices = if (shuffle) rnd.shuffle(slices) else slices
      shuffledSlices.foreach { slice =>
        val region = strategy.allocateShard(regionA, slice.toString, _allocations).futureValue
        _allocations = _allocations.updated(region, _allocations(region) :+ slice.toString)
      }
      if (oldAllocations.isEmpty)
        oldAllocations = _allocations
      _allocations
    }

    def allocateMissingSlices(atMost: Int): Map[ActorRef, Vector[ShardId]] = {
      val allocatedSlices = _allocations.valuesIterator.flatten.map(_.toInt).toSeq
      val missingSlices = allSlices.diff(allocatedSlices)
      val allocateSlices = if (shuffle) rnd.shuffle(missingSlices).take(atMost) else missingSlices.take(atMost)
      allocate(allocateSlices)
    }

    def rebalance(): Vector[Int] = {
      val rebalancedShards = strategy.rebalance(_allocations, Set.empty).futureValue
      _allocations = _allocations.map { case (region, shards) => region -> shards.filterNot(rebalancedShards.contains) }
      rebalancedShards.map(_.toInt).toVector
    }

    def rebalanceAndAllocate(): Vector[Int] = {
      val rebalanced = rebalance()
      allocate(rebalanced)
      rebalanced
    }

    def repeatRebalanceAndAllocate(): Unit = {
      @tailrec def loop(n: Int): Unit = {
        if (n <= 100) {
          val rebalancedSlices = rebalance()
          if (rebalancedSlices.nonEmpty) {
            allocate(rebalancedSlices)
            loop(n + 1)
          }
        }
      }

      loop(1)
    }

    def removeMember(i: Int): Vector[Int] = {
      val member = _members(i)
      _members = _members.filterNot(_ == member)
      val region = _regions(i)
      _regions = _regions.filterNot(_ == region)
      val removedShards = _allocations(region)
      _allocations = _allocations - region
      strategy = createAllocationStrategy(_members)
      removedShards.map(_.toInt)
    }

    def addMember(): Unit = {
      val n = _members.last.upNumber + 1
      val newMember = newUpMember(s"127.0.0.$n", upNbr = n)
      _members :+= newMember
      val newRegion = newFakeRegion(s"region${newMember.upNumber}", newMember)
      _regions :+= newRegion
      _allocations = _allocations.updated(newRegion, Vector.empty)
      strategy = createAllocationStrategy(_members)
    }

    private def sort(shards: Vector[ShardId]): Vector[Int] =
      shards.map(_.toInt).sorted

    def printAllocations(verbose: Boolean = true): Unit = {
      val rangesPerRegion8 = numberOfSliceRangesPerRegion(8, _allocations)
      val rangesPerRegion16 = numberOfSliceRangesPerRegion(16, _allocations)
      val hasOldAllocations = oldAllocations.nonEmpty && (oldAllocations ne _allocations)
      val oldRangesPerRegion8 = numberOfSliceRangesPerRegion(8, oldAllocations)
      val oldRangesPerRegion16 = numberOfSliceRangesPerRegion(16, oldAllocations)

      val optimalSize = 1024.0 / _allocations.size
      var varianceSum = 0.0

      var totalSame = 0
      //also add old allocations to new allocations, but with empty shards
      val allocationsWithEmptyOldEntries =
        if (hasOldAllocations) {
          _allocations ++ oldAllocations.iterator.collect {
            case (region, _) if !_allocations.contains(region) =>
              region -> Vector.empty[String]
          }
        } else
          _allocations
      allocationsWithEmptyOldEntries.toIndexedSeq
        .sortBy { case (_, shards) => if (shards.isEmpty) Int.MaxValue else shards.minBy(_.toInt).toInt }
        .foreach {
          case (region, shards) =>
            val deltaFromOptimal = if (_allocations.contains(region)) shards.size - optimalSize else 0.0
            varianceSum += deltaFromOptimal * deltaFromOptimal

            if (hasOldAllocations) {
              val oldShards = oldAllocations.getOrElse(region, Vector.empty)
              val added = shards.diff(oldShards)
              val removed = oldShards.diff(shards)
              val same = shards.intersect(oldShards)
              totalSame += same.size

              println(
                f"${region.path.name}: ${oldShards.size}->${shards.size}, +${added.size}, -${removed.size}, Δ$deltaFromOptimal%1.1f")
              if (verbose) {
                println(s"old ${region.path.name}: ${oldShards.size}, ${oldRangesPerRegion8.getOrElse(region, 0)} of 8 ranges, " +
                s"${oldRangesPerRegion16.getOrElse(region, 0)} of 16 ranges \n    (${sort(oldShards).mkString(", ")})")
                println(
                  s"new ${region.path.name}: ${shards.size}, ${rangesPerRegion8.getOrElse(region, 0)} of 8 ranges, " +
                  s"${rangesPerRegion16.getOrElse(region, 0)} of 16 ranges \n    (${sort(shards).mkString(", ")})")
              }
            } else if (verbose) {
              println(
                f"${region.path.name}: ${shards.size}, Δ$deltaFromOptimal%1.1f, ${rangesPerRegion8.getOrElse(region, 0)} of 8 ranges, " +
                s"${rangesPerRegion16.getOrElse(region, 0)} of 16 ranges \n    (${sort(shards).mkString(", ")})")
            } else {
              println(s"${region.path.name}: ${shards.size}")
            }
        }

      val stdDeviation = math.sqrt(varianceSum / _allocations.size)
      println(f"Standard deviation from optimal size $stdDeviation%1.1f")

      if (hasOldAllocations) {
        println(s"$totalSame shards kept at same region")
        println(
          s"old total of ${oldRangesPerRegion8.valuesIterator.sum} connections from ${oldAllocations.size} nodes " +
          s"to 8 backend ranges, reduction by ${oldAllocations.size * 8 - oldRangesPerRegion8.valuesIterator.sum}")
        println(
          s"old total of ${oldRangesPerRegion16.valuesIterator.sum} connections from ${oldAllocations.size} nodes " +
          s"to 16 backend ranges, reduction by ${oldAllocations.size * 16 - oldRangesPerRegion16.valuesIterator.sum}")
      }
      println(
        s"total of ${rangesPerRegion8.valuesIterator.sum} connections from ${_allocations.size} nodes " +
        s"to 8 backend ranges, reduction by ${_allocations.size * 8 - rangesPerRegion8.valuesIterator.sum}")
      println(
        s"total of ${rangesPerRegion16.valuesIterator.sum} connections from ${_allocations.size} nodes " +
        s"to 16 backend ranges, reduction by ${_allocations.size * 16 - rangesPerRegion16.valuesIterator.sum}")
    }

  }

  "SliceRangeShardAllocationStrategy" must {
    "allocate to regions" in new Setup(3) {
      allocateAll()
      val allocationsA = allocations(regions(0))
      val allocationsB = allocations(regions(1))
      val allocationsC = allocations(regions(2))

      // 3 regions => optimal 341.33 shards per region
      // 343: +1 due to overfill, +1 due to even index
      allocationsA should ===((0 to 342).toVector)
      // 342: +1 due to overfill
      allocationsB should ===((343 to 684).toVector)
      // 339: remainaing, 1024-343-342=339
      allocationsC should ===((685 to 1023).toVector)
    }

    "allocate in almost optimal way when allocated in order" in new Setup(64) {
      // TODO: maybe we could pre-allocate in order after reaching min-nr-of-members.

      // optimal would be 16 per region, but the overfill is needed for rebalance iterations
      allocateAll()
      allocations.foreach {
        case (_, slices) =>
          slices.size shouldBe <=(17)
      }

      allocation(slice = 0) should ===(allocation(slice = 15))
      allocation(slice = 17) should ===(allocation(slice = 31))
      allocation(slice = 512) should ===(allocation(slice = 526))
      allocation(slice = 1008) should ===(allocation(slice = 1019))
    }

    "allocate to the other regions when node is removed" in new Setup(3) {
      allocateAll()
      val removed = removeMember(2)
      val allocations2 = allocate(removed)
      allocations2(regions(0)).size should (be >= 511 and be <= 513)
      allocations2(regions(1)).size should (be >= 511 and be <= 513)
    }

    "not rebalance when nodes not changed" in new Setup(32) {
      allocateAll()
      repeatRebalanceAndAllocate()
      rebalance() should ===(Vector.empty[Int])
      rebalance() should ===(Vector.empty[Int])
    }

    "rebalance when node is added" in new Setup(3) {
      allocateAll()
      addMember()
      val rebalanced = rebalanceAndAllocate()
      rebalanced.foreach { slice =>
        allocation(slice).get should ===(regions.last)
      }

      repeatRebalanceAndAllocate()
      (0 until 4).foreach { i =>
        allocations(regions(i)).size should (be >= 253 and be <= 257)
      }
    }

    "not rebalance more than limit" in {
      val allocationStrategy = createAllocationStrategy(Vector(memberA, memberB, memberC, memberD), absoluteLimit = 2)
      val allocations = Map(
        regionA -> (0 until 300).map(_.toString).toVector,
        regionB -> Vector.empty,
        regionC -> Vector.empty,
        regionD -> Vector.empty)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("0", "299"))

      val allocationStrategy2 = createAllocationStrategy(Vector(memberA, memberB, memberC, memberD), absoluteLimit = 2)
      val allocations2 = Map(
        regionA -> (0 until 300).map(_.toString).toVector,
        regionB -> (300 until 600).map(_.toString).toVector,
        regionC -> Vector.empty,
        regionD -> Vector.empty)
      allocationStrategy2.rebalance(allocations2, Set.empty).futureValue should ===(Set("0", "299"))
    }

    "not rebalance those that have recently been rebalanced" in {
      val allocationStrategy = createAllocationStrategy(Vector(memberA, memberB, memberC, memberD), absoluteLimit = 2)
      val allocations = Map(
        regionA -> (0 until 300).map(_.toString).toVector,
        regionB -> Vector.empty,
        regionC -> Vector.empty,
        regionD -> Vector.empty)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("0", "299"))
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("1", "298"))
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("2", "297"))
    }

    "not rebalance when rebalance in progress" in {
      val allocationStrategy = createAllocationStrategy(Vector(memberA, memberB, memberC, memberD), absoluteLimit = 2)
      val allocations = Map(
        regionA -> (0 until 300).map(_.toString).toVector,
        regionB -> Vector.empty,
        regionC -> Vector.empty,
        regionD -> Vector.empty)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set("0", "299"))
      allocationStrategy.rebalance(allocations, Set("0", "299")).futureValue should ===(Set.empty[String])
    }

    "not rebalance when rolling update in progress" in {
      // multiple versions to simulate rolling update in progress
      val member1 = newUpMember("127.0.0.1", upNbr = 1, version = Version("1.0.0"))
      val member2 = newUpMember("127.0.0.2", upNbr = 2, version = Version("1.0.1"))
      val member3 = newUpMember("127.0.0.3", upNbr = 3, version = Version("1.0.0"))

      val allocationStrategy = createAllocationStrategy(Vector(member1, member2, member3))
      val allocations = Map(
        regionA -> (0 until 300).map(_.toString).toVector,
        regionB -> (300 until 600).map(_.toString).toVector,
        regionC -> Vector.empty)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set.empty[String])
    }

    "not rebalance when regions are unreachable" in {
      val allocationStrategy =
        new SliceRangeShardAllocationStrategy(10, 0.1) {
          override protected def clusterState: CurrentClusterState =
            CurrentClusterState(SortedSet(memberA, memberB, memberC), unreachable = Set(memberB))

          override protected def selfMember: Member = memberB
        }
      val allocations = Map(
        regionA -> (0 until 300).map(_.toString).toVector,
        regionB -> (300 until 600).map(_.toString).toVector,
        regionC -> Vector.empty)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set.empty[String])
    }

    "not rebalance when members are joining dc" in {
      val allocationStrategy =
        new SliceRangeShardAllocationStrategy(10, 0.1) {

          val member1 = newUpMember("127.0.0.1", upNbr = 1)
          val member2 =
            Member(
              UniqueAddress(Address("akka", "myapp", "127.0.0.2", 252525), 1L),
              Set(ClusterSettings.DcRolePrefix + ClusterSettings.DefaultDataCenter),
              member1.appVersion).copyUp(2)
          val member3 = newUpMember("127.0.0.3", upNbr = 3)

          override protected def clusterState: CurrentClusterState =
            CurrentClusterState(SortedSet(member1, member2, member3), unreachable = Set.empty)
          override protected def selfMember: Member = member2
        }
      val allocations = Map(
        regionA -> (0 until 300).map(_.toString).toVector,
        regionB -> (300 until 600).map(_.toString).toVector,
        regionC -> Vector.empty)
      allocationStrategy.rebalance(allocations, Set.empty).futureValue should ===(Set.empty[String])

    }

  }

  "ShardBySliceMessageExtractor" must {
    "extract slice as shardId" in {
      val persistence = Persistence(system)
      val extractor = new ShardBySliceMessageExtractor("TestEntity", persistence)
      val shardId = extractor.shardId("abc")
      val slice = shardId.toInt
      slice should ===(persistence.sliceForPersistenceId("TestEntity|abc"))
    }
  }

  // These are not real tests, but can be useful for exploring the algorithm and tuning
  "SliceRangeShardAllocationStrategy simulations" must {

    "try distributions" ignore {
      (1 to 100).foreach { N =>
        val setup = new Setup(N, shuffle = true)

        info(s"rnd seed ${setup.rndSeed}")

        println(s"# N=$N")
        setup.allocateAll()
        setup.printAllocations()

        println("\n")
        println("\n")
      }
    }

    "try member change impact" ignore new Setup(50, shuffle = true) {
      info(s"rnd seed $rndSeed")

      allocateAll()

      // remove one and add one member, pick one in the middle
      val removedSlices = removeMember(numberOfMembers / 2)
      addMember()
      allocate(removedSlices)
      printAllocations()
    }

    "try rebalance impact" ignore new Setup(50, shuffle = true) {
      info(s"rnd seed $rndSeed")

      allocateAll()
      val rebalanced1 = repeatRebalanceAndAllocate()
      println(s"rebalanced #1: $rebalanced1.sorted}")

      (2 to 100).foreach { n =>
        addMember()
        val rebalanced2 = repeatRebalanceAndAllocate()
        println(s"rebalanced #$n: $rebalanced2.sorted}")
        printAllocations(verbose = false)
      }
    }

    "try simulation" ignore new Setup(100, shuffle = true) {
      info(s"rnd seed $rndSeed")

      allocateAll()

      (1 to 100).foreach { _ =>
        rnd.nextInt(21) match {
          case 0 =>
            addMember()
          case 1 =>
            removeMember(rnd.nextInt(numberOfMembers))
          case 2 =>
            removeMember(rnd.nextInt(numberOfMembers))
            addMember()
          case n if 3 <= n && n <= 12 =>
            allocateMissingSlices(rnd.nextInt(10))
          case n if 13 <= n && n <= 20 =>
            val rebalancedSlices = rebalance()
            allocate(rebalancedSlices)
        }
      }

      allocateMissingSlices(1024)

      val rebalanced = repeatRebalanceAndAllocate()
      println(s"rebalanced: $rebalanced.sorted}")

      printAllocations(verbose = false)

    }

  }

}
