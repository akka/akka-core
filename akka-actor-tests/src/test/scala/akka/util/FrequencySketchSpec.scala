/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.collection.mutable

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FrequencySketchSpec extends AnyWordSpec with Matchers {

  "FrequencySketch" must {

    "increment counters" in {
      val sketch = FrequencySketch[String](capacity = 100)
      sketch.increment("foo")
      sketch.increment("bar")
      sketch.increment("bar")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.frequency("foo") shouldBe 1
      sketch.frequency("bar") shouldBe 2
      sketch.frequency("baz") shouldBe 3
      sketch.size shouldBe 6 // total number of increments
    }

    "increment counters to max value" in {
      // default of 4 bit counters, max value = 15
      val sketch = FrequencySketch[String](capacity = 100)
      for (_ <- 1 to 20) sketch.increment("foo")
      sketch.frequency("foo") shouldBe 15
      sketch.size shouldBe 15 // total number of increments
    }

    "reset counters when reset size is reached" in {
      val sketch = FrequencySketch[String](capacity = 100, resetMultiplier = 10)

      // increment counters until the reset size
      for (i <- 1 to 500) sketch.increment(i.toString)
      for (i <- 1 to 499) sketch.increment(i.toString)
      sketch.size shouldBe 999

      val frequencies1 = (1 to 499).map(i => i -> sketch.frequency(i.toString))

      // the 1000th increment will trigger a reset operation (halving all counters)
      sketch.increment("500")
      sketch.size shouldBe 500 // all counters (including hash collisions) will be even, so a perfect reset

      // frequencies should be halved now (ignore value 500, the reset trigger)
      val frequencies2 = (1 to 499).map(i => i -> sketch.frequency(i.toString))
      val halved1 = frequencies1.zip(frequencies2).foldLeft(0) {
        case (correct, ((_, f1), (_, f2))) => if (f2 == (f1 / 2)) correct + 1 else correct
      }
      // note: it's possible that the value that triggers the reset has a hash collision and this ends up
      // bumping the minimum value for another counter, so that the expected halved frequency is off-by-one
      // this could happen to up to four other counters in the worst case, and is only an issue for testing
      halved1 should be >= 499 - 4

      // increment more values, creating odd counts and more hash collisions
      for (i <- 501 to 999) sketch.increment(i.toString)
      sketch.size shouldBe 999

      val frequencies3 = (1 to 999).map(i => i -> sketch.frequency(i.toString))

      // the 1000th increment will trigger a reset operation (halving counters)
      sketch.increment("1000")
      sketch.size should (be > 300 and be < 500) // some counters will be odd numbers, rounded down when halved

      // frequencies should be halved now (ignore value 1000, the reset trigger)
      val frequencies4 = (1 to 999).map(i => i -> sketch.frequency(i.toString))
      val halved2 = frequencies3.zip(frequencies4).foldLeft(0) {
        case (correct, ((_, f3), (_, f4))) => if (f4 == (f3 / 2)) correct + 1 else correct
      }
      // note: it's possible that the value that triggers the reset has a hash collision and this ends up
      // bumping the minimum value for another counter, so that the expected halved frequency is off-by-one
      // this could happen to up to four other counters in the worst case, and is only an issue for testing
      halved2 should be >= 999 - 4
    }

    "compare frequencies for more popular items with reasonable accuracy" in {
      val sketch = FrequencySketch[String](capacity = 100, resetMultiplier = 10)
      for (i <- 1000 to 10000) sketch.increment(i.toString) // add some noise to the sketch
      for (i <- 1 to 5; _ <- 1 to (6 - i) * 2) sketch.increment(i.toString) // 1-5 are most popular, in order
      for (i <- 1 to 5) sketch.frequency(i.toString) should be >= sketch.frequency((i + 1).toString)
      for (i <- 6 to 10) sketch.frequency("5") should be >= sketch.frequency(i.toString)
    }

    "compare frequencies for random zipfian distribution with reasonable accuracy" in {
      val numberOfIds = 1000
      val mostPopular = 100
      val sketch = FrequencySketch[String](capacity = 100)
      val zipfian = ZipfianGenerator(numberOfIds) // zipfian distribution, lower numbers are more popular
      val actualFrequencies = mutable.Map.empty[Int, Int]
      for (_ <- 1 to 100 * numberOfIds) {
        val id = zipfian.next()
        sketch.increment(id.toString)
        actualFrequencies.update(id, actualFrequencies.getOrElse(id, 0) + 1)
      }
      // compare the most popular item frequencies with every other frequency, using order of actual frequency counts
      val sortedActualFrequencies = actualFrequencies.toIndexedSeq.sortBy(_._2)(Ordering.Int.reverse)
      var comparisons = 0
      var correct = 0
      for (i <- 0 until mostPopular) {
        val (id, _) = sortedActualFrequencies(i)
        val frequency = sketch.frequency(id.toString)
        for (j <- (i + 1) until sortedActualFrequencies.size) {
          val (otherId, _) = sortedActualFrequencies(j)
          val otherFrequency = sketch.frequency(otherId.toString)
          if (frequency >= otherFrequency) correct += 1
          comparisons += 1
        }
      }
      val accuracy = correct.toDouble / comparisons
      accuracy should be > 0.95 // note: depends on the hash collisions, and random distribution
    }

    "allow counter size to be configured as 2 bits" in {
      val sketch = FrequencySketch[String](capacity = 100, counterBits = 2, resetMultiplier = 1)

      // check increments
      sketch.increment("foo")
      sketch.increment("bar")
      sketch.increment("bar")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.frequency("foo") shouldBe 1
      sketch.frequency("bar") shouldBe 2
      sketch.frequency("baz") shouldBe 3
      sketch.size shouldBe 6 // total increments

      // check max value
      for (_ <- 1 to 10) sketch.increment("foo")
      sketch.frequency("foo") shouldBe 3 // max value
      sketch.size shouldBe (6 + 2) // total increments

      // check reset
      for (i <- 1 to (99 - 8)) sketch.increment(i.toString) // up to reset size
      sketch.size shouldBe 99
      sketch.increment("qux") // trigger reset
      sketch.size should be <= (100 / 2)
    }

    "allow counter size to be configured as 8 bits" in {
      val sketch = FrequencySketch[String](capacity = 100, counterBits = 8)

      // check increments
      sketch.increment("foo")
      sketch.increment("bar")
      sketch.increment("bar")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.frequency("foo") shouldBe 1
      sketch.frequency("bar") shouldBe 2
      sketch.frequency("baz") shouldBe 3
      sketch.size shouldBe 6 // total increments

      // check max value
      for (_ <- 1 to 1000) sketch.increment("foo")
      sketch.frequency("foo") shouldBe 255 // max value
      sketch.size shouldBe (6 + 254) // total increments

      // check reset
      for (i <- 1 to (999 - 260)) sketch.increment(i.toString) // up to reset size
      sketch.size shouldBe 999
      sketch.increment("qux") // trigger reset
      sketch.size should be <= (1000 / 2)
    }

    "allow counter size to be configured as 16 bits" in {
      val sketch = FrequencySketch[String](capacity = 100, counterBits = 16, resetMultiplier = 1000)

      // check increments
      sketch.increment("foo")
      sketch.increment("bar")
      sketch.increment("bar")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.frequency("foo") shouldBe 1
      sketch.frequency("bar") shouldBe 2
      sketch.frequency("baz") shouldBe 3
      sketch.size shouldBe 6 // total increments

      // check max value
      for (_ <- 1 to 100000) sketch.increment("foo")
      sketch.frequency("foo") shouldBe 65535 // max value
      sketch.size shouldBe (6 + 65534) // total increments

      // check reset
      for (i <- 1 to (99999 - 65540)) sketch.increment(i.toString) // up to reset size
      sketch.size shouldBe 99999
      sketch.increment("qux") // trigger reset
      sketch.size should be <= (100000 / 2)
    }

    "allow counter size to be configured as 32 bits" in {
      val sketch = FrequencySketch[String](capacity = 100, counterBits = 32)

      // check increments
      sketch.increment("foo")
      sketch.increment("bar")
      sketch.increment("bar")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.frequency("foo") shouldBe 1
      sketch.frequency("bar") shouldBe 2
      sketch.frequency("baz") shouldBe 3
      sketch.size shouldBe 6 // total increments

      // max value is getting big to check, assume it's working

      // check reset (at 1000)
      for (i <- 1 to (999 - 6)) sketch.increment(i.toString) // up to reset size
      sketch.size shouldBe 999
      sketch.increment("qux") // trigger reset
      sketch.size should be <= (1000 / 2)
    }

    "allow counter size to be configured as 64 bits" in {
      val sketch = FrequencySketch[String](capacity = 100, counterBits = 64)

      // check increments
      sketch.increment("foo")
      sketch.increment("bar")
      sketch.increment("bar")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.frequency("foo") shouldBe 1
      sketch.frequency("bar") shouldBe 2
      sketch.frequency("baz") shouldBe 3
      sketch.size shouldBe 6 // total increments

      // max value is getting big to check, assume it's working

      // check reset (at 1000)
      for (i <- 1 to (999 - 6)) sketch.increment(i.toString) // up to reset size
      sketch.size shouldBe 999
      sketch.increment("qux") // trigger reset
      sketch.size should be <= (1000 / 2)
    }

    "allow depth to be configured" in {
      // different depths uses different number of hash functions
      // force a frequency sketch with just a single counter and hash function, which will always collide
      val sketch = FrequencySketch[String](capacity = 1, widthMultiplier = 1, counterBits = 64, depth = 1)
      sketch.increment("foo")
      sketch.increment("bar")
      sketch.frequency("foo") shouldBe 2
      sketch.frequency("bar") shouldBe 2
    }
  }

  "FastFrequencySketch" must {

    "increment counters" in {
      val sketch = FastFrequencySketch[String](capacity = 100)
      sketch.increment("foo")
      sketch.increment("bar")
      sketch.increment("bar")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.increment("baz")
      sketch.frequency("foo") shouldBe 1
      sketch.frequency("bar") shouldBe 2
      sketch.frequency("baz") shouldBe 3
      sketch.size shouldBe 6 // total number of increments
    }

    "increment counters to max value" in {
      val sketch = FastFrequencySketch[String](capacity = 100)
      for (_ <- 1 to 20) sketch.increment("foo")
      sketch.frequency("foo") shouldBe 15
      sketch.size shouldBe 15 // total number of increments
    }

    "reset counters when reset size is reached" in {
      val sketch = FastFrequencySketch[String](capacity = 100, resetMultiplier = 10)

      // increment counters until the reset size
      for (i <- 1 to 500) sketch.increment(i.toString)
      for (i <- 1 to 499) sketch.increment(i.toString)
      sketch.size shouldBe 999

      val frequencies1 = (1 to 499).map(i => i -> sketch.frequency(i.toString))

      // the 1000th increment will trigger a reset operation (halving all counters)
      sketch.increment("500")
      sketch.size shouldBe 500 // all counters (including hash collisions) will be even, so a perfect reset

      // frequencies should be halved now (ignore value 500, the reset trigger)
      val frequencies2 = (1 to 499).map(i => i -> sketch.frequency(i.toString))
      val halved1 = frequencies1.zip(frequencies2).foldLeft(0) {
        case (correct, ((_, f1), (_, f2))) => if (f2 == (f1 / 2)) correct + 1 else correct
      }
      // note: it's possible that the value that triggers the reset has a hash collision and this ends up
      // bumping the minimum value for another counter, so that the expected halved frequency is off-by-one
      // this could happen to up to four other counters in the worst case, and is only an issue for testing
      halved1 should be >= 499 - 4

      // increment more values, creating odd counts and more hash collisions
      for (i <- 501 to 999) sketch.increment(i.toString)
      sketch.size shouldBe 999

      val frequencies3 = (1 to 999).map(i => i -> sketch.frequency(i.toString))

      // the 1000th increment will trigger a reset operation (halving counters)
      sketch.increment("1000")
      sketch.size should (be > 300 and be < 500) // some counters will be odd numbers, rounded down when halved

      // frequencies should be halved now (ignore value 1000, the reset trigger)
      val frequencies4 = (1 to 999).map(i => i -> sketch.frequency(i.toString))
      val halved2 = frequencies3.zip(frequencies4).foldLeft(0) {
        case (correct, ((_, f3), (_, f4))) => if (f4 == (f3 / 2)) correct + 1 else correct
      }
      // note: it's possible that the value that triggers the reset has a hash collision and this ends up
      // bumping the minimum value for another counter, so that the expected halved frequency is off-by-one
      // this could happen to up to four other counters in the worst case, and is only an issue for testing
      halved2 should be >= 999 - 4
    }

    "compare frequencies for more popular items with reasonable accuracy" in {
      val sketch = FastFrequencySketch[String](capacity = 100, resetMultiplier = 10)
      for (i <- 1000 to 10000) sketch.increment(i.toString) // add some noise to the sketch
      for (i <- 1 to 5; _ <- 1 to (6 - i) * 2) sketch.increment(i.toString) // 1-5 are most popular, in order
      for (i <- 1 to 5) sketch.frequency(i.toString) should be >= sketch.frequency((i + 1).toString)
      for (i <- 6 to 10) sketch.frequency("5") should be >= sketch.frequency(i.toString)
    }

    "compare frequencies for random zipfian distribution with reasonable accuracy" in {
      val numberOfIds = 1000
      val mostPopular = 100
      val sketch = FastFrequencySketch[String](capacity = 100)
      val zipfian = ZipfianGenerator(numberOfIds) // zipfian distribution, lower numbers are more popular
      val actualFrequencies = mutable.Map.empty[Int, Int]
      for (_ <- 1 to 100 * numberOfIds) {
        val id = zipfian.next()
        sketch.increment(id.toString)
        actualFrequencies.update(id, actualFrequencies.getOrElse(id, 0) + 1)
      }
      // compare the most popular item frequencies with every other frequency, using order of actual frequency counts
      val sortedActualFrequencies = actualFrequencies.toIndexedSeq.sortBy(_._2)(Ordering.Int.reverse)
      var comparisons = 0
      var correct = 0
      for (i <- 0 until mostPopular) {
        val (id, _) = sortedActualFrequencies(i)
        val frequency = sketch.frequency(id.toString)
        for (j <- (i + 1) until sortedActualFrequencies.size) {
          val (otherId, _) = sortedActualFrequencies(j)
          val otherFrequency = sketch.frequency(otherId.toString)
          if (frequency >= otherFrequency) correct += 1
          comparisons += 1
        }
      }
      val accuracy = correct.toDouble / comparisons
      accuracy should be > 0.95 // note: depends on the hash collisions, and random distribution
    }
  }
}
