/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

import akka.actor._
import akka.testkit.AkkaSpec

class OutboundCompressionSpec extends AkkaSpec {
  import CompressionTestUtils._

  "Outbound ActorRef compression" must {
    val alice = minimalRef("alice")
    val bob = minimalRef("bob")

    "not compress unknown actor ref" in {
      val table = CompressionTable.empty[ActorRef]
      table.compress(alice) should ===(-1) // not compressed
    }

    "compress previously registered actor ref" in {
      val table = CompressionTable(17L, 1, Map(system.deadLetters -> 0, alice -> 1))
      table.compress(alice) should ===(1) // compressed
      table.compress(bob) should ===(-1) // not compressed

      val table2 = CompressionTable(table.originUid, table.version, table.dictionary.updated(bob, 2))
      table2.compress(alice) should ===(1) // compressed
      table2.compress(bob) should ===(2) // compressed
    }
  }

}
