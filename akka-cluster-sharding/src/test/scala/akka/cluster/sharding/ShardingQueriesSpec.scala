/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import akka.cluster.sharding.Shard.{ CurrentShardState, ShardStats }
import akka.cluster.sharding.ShardRegion.ShardState
import akka.cluster.sharding.ShardingQueries.ShardsQueryResult
import akka.testkit.AkkaSpec

class ShardingQueriesSpec extends AkkaSpec {

  private val shards = Seq("a", "b", "busy")
  private val failures = Set("busy")
  private val timeout = ClusterShardingSettings(system).shardRegionQueryTimeout

  "ShardsQueryResult" must {

    def nonEmpty(qr: ShardsQueryResult[_]): Boolean =
      qr.total > 0 && qr.queried > 0

    def isTotalFailed(qr: ShardsQueryResult[_]): Boolean =
      nonEmpty(qr) && qr.failed.size == qr.total

    def isAllSubsetFailed(qr: ShardsQueryResult[_]): Boolean =
      nonEmpty(qr) && qr.queried < qr.total && qr.failed.size == qr.queried

    "reflect nothing to acquire metadata from - 0 shards" in {
      val qr = ShardsQueryResult[ShardState](Seq.empty, 0, timeout)
      qr.total shouldEqual qr.queried
      isTotalFailed(qr) shouldBe false // you'd have to make > 0 attempts in order to fail
      isAllSubsetFailed(qr) shouldBe false // same
      qr.toString shouldEqual "Shard region had zero shards to gather metadata from."
    }

    "partition failures and responses by type and by convention (failed Left, T Right)" in {
      def assert[T](responses: Seq[T]) = {
        val results = responses.map(Right(_)) ++ failures.map(Left(_))
        val qr = ShardsQueryResult[T](results, shards.size, timeout)
        qr.failed shouldEqual failures
        qr.responses shouldEqual responses
        isTotalFailed(qr) shouldBe false
        isAllSubsetFailed(qr) shouldBe false
        qr.toString shouldEqual s"Queried [3] shards: [2] responsive, [1] failed after $timeout."
      }

      assert[ShardStats](Seq(ShardStats("a", 1), ShardStats("b", 1)))
      assert[CurrentShardState](Seq(CurrentShardState("a", Set("a1")), CurrentShardState("b", Set("b1"))))
    }

    "detect a subset query - not all queried" in {
      def assert[T](responses: Seq[T]) = {
        val results = responses.map(Right(_)) ++ failures.map(Left(_))
        val qr = ShardsQueryResult[T](results, shards.size + 1, timeout)
        qr.total > qr.queried shouldBe true
        qr.queried < shards.size
        qr.toString shouldEqual s"Queried [3] shards of [4]: [2] responsive, [1] failed after $timeout."
      }

      assert[ShardStats](Seq(ShardStats("a", 1), ShardStats("b", 1)))
      assert[CurrentShardState](Seq(CurrentShardState("a", Set("a1")), CurrentShardState("b", Set("b1"))))
    }

    "partition when all failed" in {
      val results = Seq(Left("c"), Left("d"))
      val qr = ShardsQueryResult[ShardState](results, results.size, timeout)
      qr.total shouldEqual qr.queried
      isTotalFailed(qr) shouldBe true
      isAllSubsetFailed(qr) shouldBe false // not a subset
      qr.toString shouldEqual s"Queried [2] shards: [0] responsive, [2] failed after $timeout."
    }
  }

}
