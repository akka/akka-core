/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.cluster.sharding.internal.HillClimbingAdmissionOptimizer

class HillClimbingAdmissionOptimizerSpec extends AnyWordSpec with Matchers {

  def create(
      initialLimit: Int = 5,
      adjustMultiplier: Double = 2.0,
      initialStep: Double = 0.01,
      restartThreshold: Double = 0.05,
      stepDecay: Double = 0.98): HillClimbingAdmissionOptimizer =
    new HillClimbingAdmissionOptimizer(initialLimit, adjustMultiplier, initialStep, restartThreshold, stepDecay)

  "HillClimbingAdmissionOptimizer" must {

    "start in decreasing direction" in {
      val optimizer = create(initialStep = 0.123)
      for (_ <- 1 to 10) optimizer.recordPassive()
      optimizer.calculateAdjustment() shouldBe -0.123
    }

    "only adjust every 'adjust size' accesses" in {
      val optimizer = create()
      for (i <- 1 to 100) {
        optimizer.recordPassive()
        if (i % 10 == 0) optimizer.calculateAdjustment() should not be 0.0
        else optimizer.calculateAdjustment() shouldBe 0.0
      }
    }

    "decay each step when active rate is under restart threshold" in {
      val step = 0.1
      val decay = 0.5
      val optimizer = create(initialStep = step, stepDecay = decay)
      for (i <- 1 to 100) {
        optimizer.recordPassive()
        if (i % 10 == 0) optimizer.calculateAdjustment() shouldBe math.pow(decay, i.toDouble / 10 - 1) * -step
      }
    }

    "restart when active rate is over restart threshold" in {
      val step = 0.1
      val decay = 0.5
      val optimizer = create(initialStep = step, stepDecay = decay)
      for (i <- 1 to 100) {
        // increase (and maintain) active rate after 40 accesses to trigger restart at 60 then decay again
        if (i > 40) optimizer.recordActive() else optimizer.recordPassive()
        if (i % 10 == 0) {
          val shift = if (i < 60) 1 else 6
          optimizer.calculateAdjustment() shouldBe math.pow(decay, i.toDouble / 10 - shift) * -step
        }
      }
    }

    "change direction when active rate drops" in {
      val optimizer = create()
      for (i <- 1 to 500) {
        // decrease active rate every 50 accesses to switch direction
        val activeRate = math.max(1, 10 - (i / 50))
        if (i % 10 < activeRate) optimizer.recordActive() else optimizer.recordPassive()
        if (i % 10 == 0) {
          // at 10-50 should be negative direction, at 60-100 should be positive direction, and so on
          val adjustment = optimizer.calculateAdjustment()
          if (((i - 1) / 50) % 2 == 0) adjustment should be < 0.0 else adjustment should be > 0.0
        }
      }
    }

  }
}
