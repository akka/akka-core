/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util
import java.util.concurrent.TimeUnit

import scala.util.Random

import org.openjdk.jmh.annotations.{ Param, _ }

import akka.remote.artery.LruBoundedCache

@State(Scope.Benchmark)
@Measurement(timeUnit = TimeUnit.MICROSECONDS)
class LruBoundedCacheBench {

  var javaHashMap: java.util.HashMap[String, String] = _

  @Param(Array("1024", "8192"))
  var count = 0

  @Param(Array("128", "256"))
  var stringSize = 0
  private var lruCache: LruBoundedCache[String, String] = _

  @Param(Array("90", "99"))
  var loadFactor: Int = _

  var toAdd: String = _
  var toRemove: String = _
  var toGet: String = _

  @Setup
  def setup(): Unit = {
    val loadF: Double = loadFactor / 100.0
    val threshold = (loadF * count).toInt

    val random = Random
    javaHashMap = new util.HashMap[String, String](count)
    lruCache = new LruBoundedCache[String, String](count, threshold) {
      override protected def compute(k: String): String = k
      override protected def hash(k: String): Int = k.hashCode
      override protected def isCacheable(v: String): Boolean = true
      override protected def isKeyCacheable(k: String): Boolean = true
    }

    // Loading
    for (i <- 1 to threshold) {
      val value = random.nextString(stringSize)
      if (i == 1) toGet = value
      toRemove = value
      javaHashMap.put(value, value)
      lruCache.get(value)
    }

    toAdd = random.nextString(stringSize)

  }

  @Benchmark
  def addOne_lruCache(): String = {
    lruCache.getOrCompute(toAdd)
  }

  @Benchmark
  def addOne_hashMap(): String = {
    javaHashMap.put(toAdd, toAdd)
    javaHashMap.get(toAdd)
  }

  @Benchmark
  def addOne_hashMap_remove_put_get(): String = {
    javaHashMap.remove(toRemove)
    javaHashMap.put(toAdd, toAdd)
    javaHashMap.get(toAdd)
  }

}
