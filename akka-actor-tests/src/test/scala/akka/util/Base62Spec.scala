/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.Base64
import scala.util.Random

class Base62Spec extends AnyWordSpec with Matchers {

  "The Base62 encoder" should {

    def roundTrip(str: String): Unit = {
      val encoded = Base62.encode(str.getBytes("utf-8"))
      val decoded = new String(Base62.decode(encoded), "utf-8")
      decoded should ===(str)
    }

    "round trip different string lengths" in {
      roundTrip("")
      roundTrip("1")
      roundTrip("12")
      roundTrip("123")
      roundTrip("1234")
      roundTrip("12345")
      roundTrip("123456")
      roundTrip("1234567")
      roundTrip("12345678")
      roundTrip("123456789")
      roundTrip("1234567890")
    }

    "round trip the largest possible encoded block" in {
      // Special value, 0xffffffffffffffff / 62, to ensure our overflow checks aren 't overly zealous
      val bytes = Array(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF0).map(_.toByte)
      val encoded = "LygHa16AHY0"

      Base62.encode(bytes) should ===(encoded)
      Base64.getEncoder.encodeToString(Base62.decode(encoded)) should ===(Base64.getEncoder.encodeToString(bytes))
    }

    "round trip strings of random lengths" in {
      for (_ <- 1 to 10000) {
        val length = Random.nextInt(500) + 1
        val bytes = new Array[Byte](length)
        Random.nextBytes(bytes)
        val encoded = Base62.encode(bytes)
        val decoded = Base62.decode(encoded)
        Base64.getEncoder.encodeToString(decoded) should ===(Base64.getEncoder.encodeToString(bytes))
      }
    }

    def testFail(invalid: String): Unit = {
      a[Base62.Base62EncodingException] should be thrownBy Base62.decode(invalid)
    }

    "gracefully fail for various invalid base62 strings" in {
      // Fail due to invalid characters
      testFail("$")
      testFail("0@")
      testFail("00-")
      testFail("000+")
      testFail("0000#")
      testFail("00000!")
      testFail("000000?")
      testFail("0000000(")
      testFail("00000000)")
      testFail("000000000=")
      testFail("0000000000_")
      // Fail due to invalid length of last block
      testFail("0")
      testFail("0000")
      testFail("00000000")
      testFail("000000000000")
      testFail("000000000000000")
      testFail("0000000000000000000")
      // Fail due to invalid value in last block
      testFail("z")
      testFail("zzz")
      testFail("zzzzz")
      testFail("zzzzzz")
      testFail("zzzzzzz")
      testFail("zzzzzzzzz")
      testFail("zzzzzzzzzz")
      testFail("00000000000zzz")
      // Fail due to a full block containing an invalid value
      // This is the largest possible base 62 block, which is obviously invalid, but it 's also useful because when it
      // overflows during decoding, its value is still larger than the value after decoding the previous digit.
      testFail("zzzzzzzzzzz")
      // This is one more than 0xffffffffffffffff encoded, it triggers the addition overflow check
      testFail("LygHa16AHYG")
      // And this is (0xffffffffffffffff / 62 + 1) * 62, it triggers the multiplication overflow check
      testFail("LygHa16AHZ0")
    }
  }
}
