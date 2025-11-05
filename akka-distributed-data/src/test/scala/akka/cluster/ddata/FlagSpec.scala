/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.cluster.ddata.Replicator.Changed

class FlagSpec extends AnyWordSpec with Matchers {

  "A Flag" must {

    "be able to switch on once" in {
      val f1 = Flag()
      val f2 = f1.switchOn
      val f3 = f2.switchOn
      f1.enabled should be(false)
      f2.enabled should be(true)
      f3.enabled should be(true)
    }

    "merge by picking true" in {
      val f1 = Flag()
      val f2 = f1.switchOn
      val m1 = f1.merge(f2)
      m1.enabled should be(true)
      val m2 = f2.merge(f1)
      m2.enabled should be(true)
    }

    "have unapply extractor" in {
      val f1 = Flag.Disabled.switchOn
      val Flag(value1) = f1
      val value2: Boolean = value1
      value2 should be(true)

      Changed(FlagKey("key"))(f1) match {
        case c @ Changed(FlagKey("key")) =>
          val Flag(value3) = c.dataValue
          val value4: Boolean = value3
          value4 should be(true)
        case changed =>
          fail(s"Failed to match [$changed]")
      }
    }
  }
}
