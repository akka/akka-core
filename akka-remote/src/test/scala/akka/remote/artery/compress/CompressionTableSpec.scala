/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

import akka.testkit.AkkaSpec

class CompressionTableSpec extends AkkaSpec {

  "CompressionTable" must {
    "should invert" in {
      val decomp = CompressionTable(17L, 1, Map("0" -> 0, "1" -> 1, "2" -> 2, "3" -> 3)).invert
      decomp.table should ===(Array("0", "1", "2", "3"))
      decomp.originUid should ===(17L)
      decomp.version should ===(1.toByte)
    }

    "enforce to start allocating from 0th index" in {
      val compressionTable = CompressionTable(17L, 1, Map("1" -> 1, "3" -> 3)) // missing 0 is a gap too

      val ex = intercept[IllegalArgumentException] {
        compressionTable.invert
      }
      ex.getMessage should include("Compression table should start allocating from 0, yet lowest allocated id was 1")
    }

    "should not allow having gaps in compression ids (inversion would fail)" in {
      val compressionTable = CompressionTable(17L, 1, Map("0" -> 0, "1" -> 1, "3" -> 3)) // missing 0 is a gap too

      val ex = intercept[IllegalArgumentException] {
        compressionTable.invert
      }
      ex.getMessage should include("Given compression map does not seem to be gap-less")
    }
  }

}
