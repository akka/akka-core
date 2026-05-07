/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SwitchSpec extends AnyWordSpec with Matchers {

  "Switch" must {

    "on and off" in {
      val s = new Switch(false)
      s.isOff should ===(true)
      s.isOn should ===(false)

      s.switchOn(()) should ===(true)
      s.isOn should ===(true)
      s.isOff should ===(false)
      s.switchOn(()) should ===(false)
      s.isOn should ===(true)
      s.isOff should ===(false)

      s.switchOff(()) should ===(true)
      s.isOff should ===(true)
      s.isOn should ===(false)
      s.switchOff(()) should ===(false)
      s.isOff should ===(true)
      s.isOn should ===(false)
    }

    "revert when exception" in {
      val s = new Switch(false)
      intercept[RuntimeException] {
        s.switchOn(throw new RuntimeException)
      }
      s.isOff should ===(true)
    }

    "run action without locking" in {
      val s = new Switch(false)
      s.ifOffYield("yes") should ===(Some("yes"))
      s.ifOnYield("no") should ===(None)
      s.ifOff(()) should ===(true)
      s.ifOn(()) should ===(false)

      s.switchOn(())
      s.ifOnYield("yes") should ===(Some("yes"))
      s.ifOffYield("no") should ===(None)
      s.ifOn(()) should ===(true)
      s.ifOff(()) should ===(false)
    }

    "run action with locking" in {
      val s = new Switch(false)
      s.whileOffYield("yes") should ===(Some("yes"))
      s.whileOnYield("no") should ===(None)
      s.whileOff(()) should ===(true)
      s.whileOn(()) should ===(false)

      s.switchOn(())
      s.whileOnYield("yes") should ===(Some("yes"))
      s.whileOffYield("no") should ===(None)
      s.whileOn(()) should ===(true)
      s.whileOff(()) should ===(false)
    }

    "run first or second action depending on state" in {
      val s = new Switch(false)
      s.fold("on")("off") should ===("off")
      s.switchOn(())
      s.fold("on")("off") should ===("on")
    }

    "do proper locking" in {
      val s = new Switch(false)

      s.locked {
        Thread.sleep(500)
        s.switchOn(())
        s.isOn should ===(true)
      }

      val latch = new CountDownLatch(1)
      new Thread {
        override def run(): Unit = {
          s.switchOff(())
          latch.countDown()
        }
      }.start()

      latch.await(5, TimeUnit.SECONDS)
      s.isOff should ===(true)
    }
  }
}
