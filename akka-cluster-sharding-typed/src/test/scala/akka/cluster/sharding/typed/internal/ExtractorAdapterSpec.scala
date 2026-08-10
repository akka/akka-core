/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.internal

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.cluster.sharding.ShardRegion.{ StartEntity => ClassicStartEntity }
import akka.cluster.sharding.internal.RememberEntitiesShardStore
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.ShardingMessageExtractor

class ExtractorAdapterSpec extends AnyWordSpecLike with Matchers {

  private val extractor = new ShardingMessageExtractor[String, String] {
    override def entityId(message: String): String =
      if (message.startsWith("entity-")) message.substring(7, message.indexOf(':'))
      else null

    override def shardId(entityId: String): String = entityId.hashCode.abs.toString

    override def unwrapMessage(message: String): String = message.substring(message.indexOf(':') + 1)
  }

  private val adapter = new ExtractorAdapter(extractor)

  "ExtractorAdapter" must {

    "extract entity id from ShardingEnvelope" in {
      adapter.entityId(ShardingEnvelope("entity-1", "hello")) should ===("entity-1")
    }

    "extract entity id from ClassicStartEntity" in {
      adapter.entityId(ClassicStartEntity("entity-1")) should ===("entity-1")
    }

    "delegate to the user extractor for user messages" in {
      adapter.entityId("entity-1:hello") should ===("1")
    }

    "not throw and return null for a stray RememberEntitiesShardStore.UpdateDone" in {
      adapter.entityId(RememberEntitiesShardStore.UpdateDone(Set("entity-1"), Set.empty)) should be(null)
    }

    "not throw and return null for a stray RememberEntitiesShardStore.RememberedEntities" in {
      adapter.entityId(RememberEntitiesShardStore.RememberedEntities(Set("entity-1", "entity-2"))) should be(null)
    }

    "unwrap a ShardingEnvelope message" in {
      adapter.unwrapMessage(ShardingEnvelope("entity-1", "hello")) should ===("hello")
    }

    "unwrap a ClassicStartEntity message" in {
      val msg = ClassicStartEntity("entity-1")
      // widened to Any to avoid the erased checkcast to M at the call site, which ClassicStartEntity isn't
      (adapter.unwrapMessage(msg): Any) should ===(msg)
    }

    "delegate unwrapMessage to the user extractor for user messages" in {
      adapter.unwrapMessage("entity-1:hello") should ===("hello")
    }

    "not throw and return null from unwrapMessage for a stray UpdateDone" in {
      adapter.unwrapMessage(RememberEntitiesShardStore.UpdateDone(Set("entity-1"), Set.empty)) should be(null)
    }

    "not throw and return null from unwrapMessage for a stray RememberedEntities" in {
      adapter.unwrapMessage(RememberEntitiesShardStore.RememberedEntities(Set("entity-1"))) should be(null)
    }
  }
}
