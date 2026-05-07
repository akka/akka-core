/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

import java.util.UUID

import scala.util.Random

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OffsetSpec extends AnyWordSpecLike with Matchers {

  "TimeBasedUUID offset" must {

    "be ordered correctly" in {
      val uuid1 = TimeBasedUUID(UUID.fromString("49225740-2019-11ea-a752-ffae2393b6e4")) //2019-12-16T15:32:36.148Z[UTC]
      val uuid2 = TimeBasedUUID(UUID.fromString("91be23d0-2019-11ea-a752-ffae2393b6e4")) //2019-12-16T15:34:37.965Z[UTC]
      val uuid3 = TimeBasedUUID(UUID.fromString("91f95810-2019-11ea-a752-ffae2393b6e4")) //2019-12-16T15:34:38.353Z[UTC]
      uuid1.value.timestamp() should be < uuid2.value.timestamp()
      uuid2.value.timestamp() should be < uuid3.value.timestamp()
      List(uuid2, uuid1, uuid3).sorted shouldEqual List(uuid1, uuid2, uuid3)
      List(uuid3, uuid2, uuid1).sorted shouldEqual List(uuid1, uuid2, uuid3)

    }
  }

  "Sequence offset" must {

    "be ordered correctly" in {
      val sequenceBasedList = List(1L, 2L, 3L).map(Sequence(_))
      Random.shuffle(sequenceBasedList).sorted shouldEqual sequenceBasedList
    }
  }

}
