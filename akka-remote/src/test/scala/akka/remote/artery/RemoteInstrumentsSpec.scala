/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.Checkers

class RemoteInstrumentsSpec extends AnyWordSpec with Matchers with Checkers {

  case class KeyLen(k: Key, l: Len) {
    override def toString = s" key = ${k}, len = ${l}"
  }
  type Key = Byte
  type Len = Int

  implicit val arbitraryKeyLength: Arbitrary[KeyLen] = Arbitrary {
    for {
      key <- Gen.chooseNum(0.toByte, 31.toByte)
      len <- Gen.chooseNum(1, 1024)
    } yield KeyLen(key, len)
  }

  "RemoteInstruments" must {

    "combine and decompose single key and length" in {
      val key: Byte = 17
      val len = 812
      val kl = RemoteInstruments.combineKeyLength(key, len)

      val key2 = RemoteInstruments.getKey(kl)
      key2 should ===(key)
      val len2 = RemoteInstruments.getLength(kl)
      len2 should ===(len)
    }

    "combine and decompose key with 0 multiple times" in {
      check { (kl: KeyLen) =>
        val k = kl.k

        val masked = RemoteInstruments.combineKeyLength(k, 0)
        val uk = RemoteInstruments.getKey(masked)
        uk should ===(k)
        uk == k
      }
    }

    "combine and decompose length with 0 multiple times" in {
      check { (kl: KeyLen) =>
        val l = kl.l

        val masked = RemoteInstruments.combineKeyLength(0, l)
        val ul = RemoteInstruments.getLength(masked)
        ul should ===(l)
        ul == l
      }
    }

    "combine and decompose key and length multiple times" in {
      check { (kl: KeyLen) =>
        val k = kl.k
        val l = kl.l

        val masked = RemoteInstruments.combineKeyLength(k, l)
        val uk = RemoteInstruments.getKey(masked)
        uk should ===(k)
        val ul = RemoteInstruments.getLength(masked)
        ul should ===(l)
        ul == l && uk == k
      }
    }

  }
}
