/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.util.Random
import akka.testkit.AkkaSpec
import akka.util.Unsafe

class LruBoundedCacheSpec extends AkkaSpec {

  class TestCache(_capacity: Int, threshold: Int, hashSeed: String = "")
      extends LruBoundedCache[String, String](_capacity, threshold) {
    private var cntr = 0

    override protected def compute(k: String): String = {
      val id = cntr
      cntr += 1
      k + ":" + id
    }

    override protected def hash(k: String): Int = Unsafe.fastHash(hashSeed + k + hashSeed)

    override protected def isCacheable(v: String): Boolean = !v.startsWith("#")
    override protected def isKeyCacheable(k: String): Boolean = !k.startsWith("!")

    def internalProbeDistanceOf(idealSlot: Int, actualSlot: Int): Int = probeDistanceOf(idealSlot, actualSlot)

    def expectComputed(key: String, value: String): Unit = {
      get(key) should ===(None)
      getOrCompute(key) should ===(value)
      get(key) should ===(Some(value))
    }

    def expectCached(key: String, value: String): Unit = {
      get(key) should ===(Some(value))
      getOrCompute(key) should ===(value)
      get(key) should ===(Some(value))
    }

    def expectComputedOnly(key: String, value: String): Unit = {
      get(key) should ===(None)
      getOrCompute(key) should ===(value)
      get(key) should ===(None)
    }
  }

  final class BrokenHashFunctionTestCache(_capacity: Int, _threshold: Int) extends TestCache(_capacity, _threshold) {
    override protected def hash(k: String): Int = 0
  }

  "LruBoundedCache" must {

    "work in the happy case" in {
      val cache = new TestCache(4, 4)

      cache.expectComputed("A", "A:0")
      cache.expectComputed("B", "B:1")
      cache.expectComputed("C", "C:2")
      cache.expectComputed("D", "D:3")

      cache.expectCached("A", "A:0")
      cache.expectCached("B", "B:1")
      cache.expectCached("C", "C:2")
      cache.expectCached("D", "D:3")
    }

    "evict oldest when full" in {
      for (_ <- 1 to 10) {
        val seed = Random.nextInt(1024)
        info(s"Variant $seed")
        val cache = new TestCache(4, 4, seed.toString)

        cache.expectComputed("A", "A:0")
        cache.expectComputed("B", "B:1")
        cache.expectComputed("C", "C:2")
        cache.expectComputed("D", "D:3")
        cache.expectComputed("E", "E:4")

        cache.expectCached("B", "B:1")
        cache.expectCached("C", "C:2")
        cache.expectCached("D", "D:3")
        cache.expectCached("E", "E:4")

        cache.expectComputed("A", "A:5")
        cache.expectComputed("B", "B:6")
        cache.expectComputed("C", "C:7")
        cache.expectComputed("D", "D:8")
        cache.expectComputed("E", "E:9")

        cache.expectCached("B", "B:6")
        cache.expectCached("C", "C:7")
        cache.expectCached("D", "D:8")
        cache.expectCached("E", "E:9")
      }
    }

    "work with low quality hash function" in {
      val cache = new BrokenHashFunctionTestCache(4, 4)

      cache.expectComputed("A", "A:0")
      cache.expectComputed("B", "B:1")
      cache.expectComputed("C", "C:2")
      cache.expectComputed("D", "D:3")
      cache.expectComputed("E", "E:4")

      cache.expectCached("B", "B:1")
      cache.expectCached("C", "C:2")
      cache.expectCached("D", "D:3")
      cache.expectCached("E", "E:4")

      cache.expectComputed("A", "A:5")
      cache.expectComputed("B", "B:6")
      cache.expectComputed("C", "C:7")
      cache.expectComputed("D", "D:8")
      cache.expectComputed("E", "E:9")

      cache.expectCached("B", "B:6")
      cache.expectCached("C", "C:7")
      cache.expectCached("D", "D:8")
      cache.expectCached("E", "E:9")
    }

    "calculate probe distance correctly" in {
      val cache = new TestCache(4, 4)

      cache.internalProbeDistanceOf(0, 0) should ===(0)
      cache.internalProbeDistanceOf(0, 1) should ===(1)
      cache.internalProbeDistanceOf(0, 2) should ===(2)
      cache.internalProbeDistanceOf(0, 3) should ===(3)

      cache.internalProbeDistanceOf(1, 1) should ===(0)
      cache.internalProbeDistanceOf(1, 2) should ===(1)
      cache.internalProbeDistanceOf(1, 3) should ===(2)
      cache.internalProbeDistanceOf(1, 0) should ===(3)

      cache.internalProbeDistanceOf(2, 2) should ===(0)
      cache.internalProbeDistanceOf(2, 3) should ===(1)
      cache.internalProbeDistanceOf(2, 0) should ===(2)
      cache.internalProbeDistanceOf(2, 1) should ===(3)

      cache.internalProbeDistanceOf(3, 3) should ===(0)
      cache.internalProbeDistanceOf(3, 0) should ===(1)
      cache.internalProbeDistanceOf(3, 1) should ===(2)
      cache.internalProbeDistanceOf(3, 2) should ===(3)
    }

    "work with a lower age threshold" in {
      for (_ <- 1 to 10) {
        val seed = Random.nextInt(1024)
        info(s"Variant $seed")
        val cache = new TestCache(4, 2, seed.toString)

        cache.expectComputed("A", "A:0")
        cache.expectComputed("B", "B:1")
        cache.expectComputed("C", "C:2")
        cache.expectComputed("D", "D:3")
        cache.expectComputed("E", "E:4")

        cache.expectCached("D", "D:3")
        cache.expectCached("E", "E:4")

        cache.expectComputed("F", "F:5")
        cache.expectComputed("G", "G:6")
        cache.expectComputed("H", "H:7")
        cache.expectComputed("I", "I:8")
        cache.expectComputed("J", "J:9")

        cache.expectCached("I", "I:8")
        cache.expectCached("J", "J:9")
      }
    }

    "must not cache noncacheable values" in {
      val cache = new TestCache(4, 4)

      cache.expectComputedOnly("#A", "#A:0")
      cache.expectComputedOnly("#A", "#A:1")
      cache.expectComputedOnly("#A", "#A:2")
      cache.expectComputedOnly("#A", "#A:3")

      cache.expectComputed("A", "A:4")
      cache.expectComputed("B", "B:5")
      cache.expectComputed("C", "C:6")
      cache.expectComputed("D", "D:7")
      cache.expectComputed("E", "E:8")

      cache.expectCached("B", "B:5")
      cache.expectCached("C", "C:6")
      cache.expectCached("D", "D:7")
      cache.expectCached("E", "E:8")

      cache.expectComputedOnly("#A", "#A:9")
      cache.expectComputedOnly("#A", "#A:10")
      cache.expectComputedOnly("#A", "#A:11")
      cache.expectComputedOnly("#A", "#A:12")

      // Cacheable values are not affected
      cache.expectCached("B", "B:5")
      cache.expectCached("C", "C:6")
      cache.expectCached("D", "D:7")
      cache.expectCached("E", "E:8")
    }

    "not cache non cacheable keys" in {
      val cache = new TestCache(4, 4)

      cache.expectComputedOnly("!A", "!A:0")
      cache.expectComputedOnly("!A", "!A:1")
      cache.expectComputedOnly("!A", "!A:2")
      cache.expectComputedOnly("!A", "!A:3")

      cache.expectComputed("A", "A:4")
      cache.expectComputed("B", "B:5")
      cache.expectComputed("C", "C:6")
      cache.expectComputed("D", "D:7")
      cache.expectComputed("E", "E:8")

      cache.expectCached("B", "B:5")
      cache.expectCached("C", "C:6")
      cache.expectCached("D", "D:7")
      cache.expectCached("E", "E:8")

      cache.expectComputedOnly("!A", "!A:9")
      cache.expectComputedOnly("!A", "!A:10")
      cache.expectComputedOnly("!A", "!A:11")
      cache.expectComputedOnly("!A", "!A:12")

      // Cacheable values are not affected
      cache.expectCached("B", "B:5")
      cache.expectCached("C", "C:6")
      cache.expectCached("D", "D:7")
      cache.expectCached("E", "E:8")
    }

    "maintain a good average probe distance" in {
      for (_ <- 1 to 10) {
        val seed = Random.nextInt(1024)
        info(s"Variant $seed")
        // Cache emulating 60% fill rate
        val cache = new TestCache(1024, 600, seed.toString)

        // Fill up cache
        for (_ <- 1 to 10000) cache.getOrCompute(Random.nextString(32))

        val stats = cache.stats
        // Have not seen lower than 890
        stats.entries should be > 750
        // Have not seen higher than 1.8

        stats.averageProbeDistance should be < 2.5
        // Have not seen higher than 15
        stats.maxProbeDistance should be < 25
      }

    }

  }

}
