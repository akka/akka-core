/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.util
import java.util.UUID

import akka.japi.Pair
import akka.testkit.ImplicitSender

class SliceRangesSpec extends PersistenceSpec(PersistenceSpec.config("inmem", "LoadJournalSpec")) with ImplicitSender {

  private val persistence = Persistence(system)

  "Persistence slices" must {
    "have fixed numberOfSlices" in {
      persistence.numberOfSlices should ===(1024)
    }

    "be deterministic from persistence id" in {
      persistence.sliceForPersistenceId("pid-1") should ===(111)
      persistence.sliceForPersistenceId("pid-2") should ===(112)
      persistence.sliceForPersistenceId("pid-6712") should ===(92)
    }

    "be within the numberOfSlices" in {
      val pid = s"pid-${UUID.randomUUID()}"
      withClue(s"$pid ") {
        val slice = persistence.sliceForPersistenceId(pid)
        slice should be >= 0
        slice should be < persistence.numberOfSlices
      }
    }

    "create ranges" in {
      persistence.sliceRanges(4) should ===(Vector(0 to 255, 256 to 511, 512 to 767, 768 to 1023))
      persistence.sliceRanges(1) should ===(Vector(0 to 1023))
    }

    "create ranges for Java" in {
      persistence.getSliceRanges(4) shouldBe
      util.Arrays.asList(Pair.create(0, 255), Pair.create(256, 511), Pair.create(512, 767), Pair.create(768, 1023))
    }
  }
}
