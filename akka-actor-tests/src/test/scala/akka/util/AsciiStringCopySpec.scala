/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util
import java.nio.charset.StandardCharsets

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AsciiStringCopySpec extends AnyWordSpec with Matchers {

  "The copyUSAsciiStrToBytes optimization" must {

    "select working algorithm" in {
      if (Unsafe.isIsJavaVersion9Plus) {
        Unsafe.testUSAsciiStrToBytesAlgorithm0("abc") should ===(true)
        // this is known to fail with JDK 11 on ARM32 (Raspberry Pi),
        // and therefore algorithm 0 is selected on that architecture
        Unsafe.testUSAsciiStrToBytesAlgorithm1("abc") should ===(true)
        Unsafe.testUSAsciiStrToBytesAlgorithm2("abc") should ===(false)
      } else {
        Unsafe.testUSAsciiStrToBytesAlgorithm0("abc") should ===(true)
        Unsafe.testUSAsciiStrToBytesAlgorithm1("abc") should ===(false)
        Unsafe.testUSAsciiStrToBytesAlgorithm2("abc") should ===(true)
      }
    }

    "copy string internal representation successfully" in {
      val ascii = "abcdefghijklmnopqrstuvxyz"
      val byteArray = new Array[Byte](ascii.length)
      Unsafe.copyUSAsciiStrToBytes(ascii, byteArray)
      new String(byteArray, StandardCharsets.US_ASCII) should ===(ascii)
    }
  }

  "The fast hash optimization" must {

    "hash strings successfully" in {
      Unsafe.fastHash("abcdefghijklmnopqrstuvxyz")
    }

  }

}
