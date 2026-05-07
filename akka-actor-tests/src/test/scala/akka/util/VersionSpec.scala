/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class VersionSpec extends AnyWordSpec with Matchers {

  "Version" should {

    "compare 3 digit version" in {
      Version("1.2.3") should ===(Version("1.2.3"))
      Version("1.2.3") should !==(Version("1.2.4"))
      Version("1.2.4") should be > Version("1.2.3")
      Version("3.2.1") should be > Version("1.2.3")
      Version("3.2.1") should be < Version("3.3.1")
      Version("3.2.0") should be < Version("3.2.1")
    }

    "not support more than 3 digits version" in {
      intercept[IllegalArgumentException](Version("1.2.3.1"))
    }

    "compare 2 digit version" in {
      Version("1.2") should ===(Version("1.2"))
      Version("1.2") should ===(Version("1.2.0"))
      Version("1.2") should !==(Version("1.3"))
      Version("1.2.1") should be > Version("1.2")
      Version("2.4") should be > Version("2.3")
      Version("3.2") should be < Version("3.2.7")
    }

    "compare single digit version" in {
      Version("1") should ===(Version("1"))
      Version("1") should ===(Version("1.0"))
      Version("1") should ===(Version("1.0.0"))
      Version("1") should !==(Version("2"))
      Version("3") should be > Version("2")

      Version("2b") should be > Version("2a")
      Version("2020-09-07") should be > Version("2020-08-30")
    }

    "compare extra" in {
      Version("1.2.3-M1") should ===(Version("1.2.3-M1"))
      Version("1.2-M1") should ===(Version("1.2-M1"))
      Version("1.2.0-M1") should ===(Version("1.2-M1"))
      Version("1.2.3-M1") should !==(Version("1.2.3-M2"))
      Version("1.2-M1") should be < Version("1.2.0")
      Version("1.2.0-M1") should be < Version("1.2.0")
      Version("1.2.3-M2") should be > Version("1.2.3-M1")
      Version("2.10.0-M1") should be < Version("2.10.0")
      Version("2.10.0") should be > Version("2.10.0-M1")
    }

    "require digits" in {
      intercept[NumberFormatException](Version("1.x.3"))
      intercept[NumberFormatException](Version("1.2x.3"))
      intercept[NumberFormatException](Version("1.2.x"))
      intercept[NumberFormatException](Version("1.2.3x"))

      intercept[NumberFormatException](Version("x.3"))
      intercept[NumberFormatException](Version("1.x"))
      intercept[NumberFormatException](Version("1.2x"))
    }

    "compare dynver format" in {
      // dynver format
      Version("1.0.10+3-1234abcd") should be < Version("1.0.11")
      Version("1.0.10+3-1234abcd") should be < Version("1.0.10+10-1234abcd")
      Version("1.2+3-1234abcd") should be < Version("1.2+10-1234abcd")
      Version("1.0.0+3-1234abcd+20140707-1030") should be < Version("1.0.0+3-1234abcd+20140707-1130")
      Version("0.0.0+3-2234abcd") should be < Version("0.0.0+4-1234abcd")
      Version("HEAD+20140707-1030") should be < Version("HEAD+20140707-1130")

      Version("1.0.10-3-1234abcd") should be < Version("1.0.10-10-1234abcd")
      Version("1.0.0-3-1234abcd+20140707-1030") should be < Version("1.0.0-3-1234abcd+20140707-1130")

      // not real dynver, but should still work
      Version("1.0.10+3a-1234abcd") should be < Version("1.0.10+3b-1234abcd")

    }

    "compare extra without digits" in {
      Version("foo") should ===(Version("foo"))
      Version("foo") should !==(Version("bar"))
      Version("foo") should be < Version("1.2.3")
      Version("foo") should be > Version("bar")

      Version("1-foo") should !==(Version("01-foo"))
      Version("1-foo") should be > (Version("02-foo"))
    }
  }
}
