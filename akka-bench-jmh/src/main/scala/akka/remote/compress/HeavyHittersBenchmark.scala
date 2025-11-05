/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.compress

import java.util.Random

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import akka.remote.artery.compress.TopHeavyHitters

/**
 * On Macbook pro:
 * [info] Benchmark                                  (n)   Mode  Cnt          Score         Error  Units
 * [info] HeavyHittersBenchmark.updateHitter        8192  thrpt   40      357 405.512 ±    3329.008  ops/s
 * [info] HeavyHittersBenchmark.updateNotHitter     8192  thrpt   40  259 032 711.743 ± 7199514.142  ops/s
 * [info] HeavyHittersBenchmark.updateRandomHitter  8192  thrpt   40    2 105 102.088 ±   18214.624  ops/s
 *
 * ===
 * on our benchmarking box:
 * ubuntu@ip-172-31-43-199:~/akka-ktoso$ lscpu
 * Architecture:          x86_64
 * CPU op-mode(s):        32-bit, 64-bit
 * Byte Order:            Little Endian
 * CPU(s):                2
 * Thread(s) per core:    2
 * CPU MHz:               2494.068
 * Hypervisor vendor:     Xen
 * Virtualization type:   full
 * L1d cache:             32K
 * L1i cache:             32K
 * L2 cache:              256K
 * L3 cache:              25600K
 *
 * ubuntu@ip-172-31-43-199:~/akka-ktoso$ cpuid | grep nm
 * (simple synth)  = Intel Core i9-4000 / Xeon E5-1600/E5-2600 v2 (Ivy Bridge-EP C1/M1/S1), 22nm
 *
 * [info] Benchmark                                  (n)   Mode  Cnt          Score         Error  Units
 * [info] HeavyHittersBenchmark.updateHitter        8192  thrpt   40      309 512.584 ±     153.248  ops/s
 * [info] HeavyHittersBenchmark.updateNotHitter     8192  thrpt   40  248 170 545.577 ± 1244986.765  ops/s
 * [info] HeavyHittersBenchmark.updateRandomHitter  8192  thrpt   40    1 207 521.674 ±     912.676  ops/s
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(2)
class HeavyHittersBenchmark {

  //  @Param(Array("512", "8192"))
  @Param(Array("8192"))
  var n: Int = 0

  private var topN: TopHeavyHitters[String] = _

  val rand = new Random(1001021)

  val preallocatedNums: Array[Long] = Array.ofDim(8192)
  val preallocatedStrings: Array[String] = Array.ofDim(8192)

  @Setup
  def init(): Unit = {
    topN = new TopHeavyHitters(n)
    var i = 0
    while (i < n) {
      topN.update(i.toString, i)
      preallocatedNums(i) = rand.nextLong()
      preallocatedStrings(i) = i.toString
      i += 1
    }
  }

  @Benchmark
  @OperationsPerInvocation(8192)
  def updateNotHitter(blackhole: Blackhole): Unit = {
    var i = 0
    while (i < 8192) {
      blackhole.consume(topN.update("NOT", 1)) // definitely not a heavy hitter
      i += 1
    }
  }

  @Benchmark
  @OperationsPerInvocation(8192)
  def updateExistingHitter(blackhole: Blackhole): Unit = {
    var i: Int = 0
    while (i < 8192) {
      blackhole.consume(topN.update(preallocatedStrings(i % 16), Long.MaxValue)) // definitely a heavy hitter
      i += 1
    }
  }

  @Benchmark
  def updateNewHitter(blackhole: Blackhole): Unit = {
    var i = 0
    while (i < 8192) {
      blackhole.consume(topN.update(preallocatedStrings(i), Long.MaxValue))
      i += 1
    }
  }

  @Benchmark
  @OperationsPerInvocation(8192)
  def updateRandomHitter(blackhole: Blackhole): Unit = {
    var i = 0
    while (i < 8192) {
      blackhole.consume(topN.update(preallocatedStrings(i), preallocatedNums(i))) // maybe a heavy hitter
      i += 1
    }
  }

}
