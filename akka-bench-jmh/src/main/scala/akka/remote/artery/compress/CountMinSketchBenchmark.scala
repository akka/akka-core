/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

import java.util.Random

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(2)
class CountMinSketchBenchmark {

  //  @Param(Array("4", "8", "12", "16"))
  @Param(Array("16", "256", "4096", "65536"))
  var w: Int = _
  @Param(Array("16", "128", "1024"))
  var d: Int = _

  private val seed: Int = 20160726

  val rand = new Random(seed)

  val preallocateIds = Array.ofDim[Int](8192)
  val preallocateValues = Array.ofDim[Long](8192)

  var countMinSketch: CountMinSketch = _

  @Setup
  def init(): Unit = {
    countMinSketch = new CountMinSketch(d, w, seed)
    (0 to 8191).foreach { index =>
      preallocateIds(index) = rand.nextInt()
      preallocateValues(index) = Math.abs(rand.nextInt())
    }
  }

  @Benchmark
  @OperationsPerInvocation(8192)
  def updateRandomNumbers(blackhole: Blackhole): Unit = {
    var i: Int = 0
    while (i < 8192) {
      blackhole.consume(countMinSketch.addObjectAndEstimateCount(preallocateIds(i), preallocateValues(i)))
      i += 1
    }
  }

}
