/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.io.dns.{ RecordClass, RecordType }
import akka.util.ByteString

class MessageSpec extends AnyWordSpec with Matchers {
  "The Message" should {
    "parse a response that is truncated mid-message" in {
      val bytes = ByteString(0, 4, -125, -128, 0, 1, 0, 48, 0, 0, 0, 0, 4, 109, 97, 110, 121, 4, 98, 122, 122, 116, 3,
        110, 101, 116, 0, 0, 28, 0, 1)
      val msg = Message.parse(bytes)
      msg.id should be(4)
      msg.flags.isTruncated should be(true)
      msg.questions.length should be(1)
      msg.questions.head should be(Question("many.bzzt.net", RecordType.AAAA, RecordClass.IN))
      msg.answerRecs.length should be(0)
      msg.authorityRecs.length should be(0)
      msg.additionalRecs.length should be(0)
    }
  }

}
