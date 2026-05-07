/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.util.Random

import akka.testkit.AkkaSpec

class TokenBucketSpec extends AkkaSpec {

  class TestBucket(_cap: Long, _period: Long) extends TokenBucket(_cap, _period) {
    var currentTime: Long = 0L
  }

  "A Token Bucket" must {

    "start full" in {
      val bucket = new TestBucket(10, 1)
      bucket.init()

      bucket.offer(1) should ===(0L)
      bucket.offer(1) should ===(0L)
      bucket.offer(1) should ===(0L)
      bucket.offer(7) should ===(0L)

      bucket.offer(3) should ===(3L)
    }

    "calculate correctly with different rates and capacities" in {
      val bucketRate2 = new TestBucket(10, 2)
      bucketRate2.init()

      bucketRate2.offer(5) should ===(0L)
      bucketRate2.offer(5) should ===(0L)
      bucketRate2.offer(5) should ===(10L)

      val bucketRate3 = new TestBucket(8, 3)
      bucketRate3.init()
      bucketRate3.offer(5) should ===(0L)
      bucketRate3.offer(5) should ===(6L)

      bucketRate3.currentTime = 6
      bucketRate3.offer(3) should ===(9L)
    }

    "allow sending elements larger than capacity" in {
      val bucket = new TestBucket(10, 2)
      bucket.init()

      bucket.offer(5) should ===(0L)
      bucket.offer(20) should ===(30L)

      bucket.currentTime = 30
      bucket.offer(1) should ===(2L)

      bucket.currentTime = 34
      bucket.offer(1) should ===(0L)
      bucket.offer(1) should ===(2L)
    }

    "work with zero capacity" in {
      val bucket = new TestBucket(0, 2)
      bucket.init()

      bucket.offer(10) should ===(20L)

      bucket.currentTime = 40
      bucket.offer(10) should ===(20L)
    }

    "not delay if rate is higher than production" in {
      val bucket = new TestBucket(1, 10)
      bucket.init()

      for (time <- 0 to 100 by 10) {
        bucket.currentTime = time
        bucket.offer(1) should ===(0L)
      }

    }

    "maintain maximum capacity" in {
      val bucket = new TestBucket(10, 1)
      bucket.init()
      bucket.offer(10) should ===(0L)

      bucket.currentTime = 100000
      bucket.offer(20) should ===(10L)
    }

    "work if currentTime is negative" in {
      val bucket = new TestBucket(10, 1)
      bucket.currentTime = -100 // Must be set before init()!
      bucket.init()

      bucket.offer(5) should ===(0L)
      bucket.offer(10) should ===(5L)

      bucket.currentTime += 10

      bucket.offer(5) should ===(0L)
    }

    "work if currentTime wraps over" in {
      val bucket = new TestBucket(10, 1)
      bucket.currentTime = Long.MaxValue - 5 // Must be set before init()!
      bucket.init()

      bucket.offer(5) should ===(0L)
      bucket.offer(10) should ===(5L)

      bucket.currentTime += 10

      bucket.offer(5) should ===(0L)
    }

    "(attempt to) maintain equal time between token renewal intervals" in {
      val bucket = new TestBucket(5, 3)
      bucket.init()

      bucket.offer(10) should ===(15L)

      bucket.currentTime = 16
      // At this point there is no token in the bucket (we consumed it at T15) but the next token will arrive at T18!
      // A naive calculation would consider that there is 1 token needed hence we need to wait 3 units, but in fact
      // we only need to wait 2 units, otherwise we shift the whole token arrival sequence resulting in lower rates.
      //
      // 0   3   9  12  15  18  21  24  27
      // +---+---+---+---+---+---+---+---+
      //                 ^ ^
      //  emitted here --+ +---- currently here (T16)
      //

      bucket.offer(1) should ===(2L)

      bucket.currentTime = 19
      // At 18 bucket is empty, and so is at 19. For a cost of 2 we need to wait until T24 which is 5 units.
      //
      // 0   3   9  12  15  18  21  24  27
      // +---+---+---+---+---+---+---+---+
      //                     ^ ^
      //      emptied here --+ +---- currently here (T19)
      //
      bucket.offer(2) should ===(5L)

      // Another case
      val bucket2 = new TestBucket(10, 3)
      bucket2.init()

      bucket2.currentTime = 4
      bucket2.offer(6) should ===(0L)

      // 4 tokens remain and new tokens arrive at T6 and T9 so here we have 6 tokens remaining.
      // We need 1 more, which will arrive at T12
      bucket2.currentTime = 10
      bucket2.offer(7) should ===(2L)
    }

    "work with cost of zero" in {
      val bucket = new TestBucket(10, 1)
      bucket.init()

      // Can be called any number of times
      bucket.offer(0)
      bucket.offer(0)
      bucket.offer(0)

      bucket.offer(10) should ===(0L)

      // Bucket is empty now
      // Still can be called any number of times
      bucket.offer(0)
      bucket.offer(0)
      bucket.offer(0)
    }

    "work with very slow rates" in {
      val T = Long.MaxValue >> 10
      val bucket = new TestBucket(10, T)
      bucket.init()

      bucket.offer(20) should ===(10 * T)
      bucket.currentTime += 10 * T

      // Collect 5 tokens
      bucket.currentTime += 5 * T

      bucket.offer(4) should ===(0L)
      bucket.offer(2) should ===(T)
    }

    "behave exactly as the ideal (naive) token bucket if offer is called with perfect timing" in {
      val Debug = false

      for {
        capacity <- List(0, 1, 5, 10)
        period <- List(1, 3, 5)
        arrivalPeriod <- List(1, 3, 5)
        startTime <- List(Long.MinValue, -1L, 0L, Long.MaxValue)
        maxCost <- List(1, 5, 10)
      } {

        val bucket = new TestBucket(capacity, period)
        bucket.currentTime = startTime
        bucket.init()

        var idealBucket = capacity
        var untilNextTick = period
        var untilNextElement = Random.nextInt(arrivalPeriod) + 1
        var nextEmit = 0L
        var delaying = false

        for (time <- 0 to 1000) {
          if (untilNextTick == 0) {
            untilNextTick = period
            idealBucket = math.min(idealBucket + 1, capacity)
          }

          if (Debug) println(s"T:$time  bucket:$idealBucket")

          if (delaying && idealBucket == 0) {
            // Actual emit time should equal to what the optimized token bucket calculates
            time.toLong should ===(nextEmit)
            untilNextElement = time + Random.nextInt(arrivalPeriod)
            if (Debug) println(s"  EMITTING")
            delaying = false
          }

          if (untilNextElement == 0) {
            // Allow cost of zer
            val cost = Random.nextInt(maxCost + 1)
            idealBucket -= cost // This can go negative
            bucket.currentTime = startTime + time
            val delay = bucket.offer(cost)
            nextEmit = time + delay
            if (Debug) println(s"  ARRIVAL cost: $cost at: $nextEmit")
            if (delay == 0) {
              (idealBucket >= 0) should be(true)
              untilNextElement = time + Random.nextInt(arrivalPeriod)
            } else delaying = true
          }

          untilNextTick -= 1
          untilNextElement -= 1
        }
      }

    }

  }

}
