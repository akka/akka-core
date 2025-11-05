/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.internal

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.cluster.sharding.typed.internal.ShardedDaemonProcessId.DecodedId

class ShardedDaemonProcessIdSpec extends AnyWordSpecLike with Matchers {

  "The sharded daemon process ids" should {

    "decode single int in id as id for non-rescaling" in {
      // number of processes and revision not used so 0 in this case
      ShardedDaemonProcessId.decodeEntityId("1", 8) should ===(DecodedId(0L, 8, 1))
    }

    "decode composed id as id for rescaling" in {
      ShardedDaemonProcessId.decodeEntityId("1|4|3", 8) should ===(DecodedId(1L, 4, 3))
    }

    "decode single int in id as id for rescaling (to support rolling upgrade)" in {
      ShardedDaemonProcessId.decodeEntityId("1", 8) should ===(DecodedId(0L, 8, 1))
    }

    "encode initial revision with old scheme (to support rolling upgrade)" in {
      val id = DecodedId(0L, 8, 4)
      id.encodeEntityId() should ===("4")
      id.encodeEntityId() should ===("4")
    }

    "encode non 0 revisions with new scheme" in {
      val id = DecodedId(1L, 8, 4)
      id.encodeEntityId() should ===("1|8|4")
    }

    "spread out workers over shards for non-rescaling" in {
      ShardedDaemonProcessId.allShardsFor(1, 8).size should ===(8)
    }

    "spread out workers over shards for rescaling" in {
      ShardedDaemonProcessId.allShardsFor(1, 8).size should ===(8)
    }

    "extract new id as shard in message extractor" in {
      val extractor = new ShardedDaemonProcessId.MessageExtractor[Any]()
      extractor.shardId("1|8|4") should ===("4")
    }

    "extract old id as shard in message extractor (to support rolling upgrade)" in {
      val extractor = new ShardedDaemonProcessId.MessageExtractor[Any]()
      extractor.shardId("1") should ===("1")
    }
  }

}
