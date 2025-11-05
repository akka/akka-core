/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(2)
@Warmup(iterations = 4)
@Measurement(iterations = 10)
class ByteString_append_Benchmark {

  private val bs = ByteString(Array.ofDim[Byte](10))

  @Benchmark
  @OperationsPerInvocation(10000)
  def appendThree(bh: Blackhole): Unit = {
    var result = ByteString.empty
    var i = 0
    while (i < 10000) {
      result = result ++ bs
      if (i % 3 == 0) {
        bh.consume(result)
        result = ByteString.empty
      }
      i += 1
    }
    bh.consume(result)
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def appendMany(bh: Blackhole): Unit = {
    var result = ByteString.empty
    var i = 0
    while (i < 10000) {
      result = result ++ bs
      i += 1
    }
    bh.consume(result)
  }
  @Benchmark
  @OperationsPerInvocation(10000)
  def builderOne(bh: Blackhole): Unit = {
    val builder = ByteString.newBuilder
    var i = 0
    while (i < 10000) {
      builder ++= bs
      bh.consume(builder.result())
      builder.clear()
      i += 1
    }
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def builderThree(bh: Blackhole): Unit = {
    val builder = ByteString.newBuilder
    var i = 0
    while (i < 10000) {
      builder ++= bs
      if (i % 3 == 0) {
        bh.consume(builder.result())
        builder.clear()
      }
      i += 1
    }
    bh.consume(builder.result())
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def builderFive(bh: Blackhole): Unit = {
    val builder = ByteString.newBuilder
    var i = 0
    while (i < 10000) {
      builder ++= bs
      if (i % 5 == 0) {
        bh.consume(builder.result())
        builder.clear()
      }
      i += 1
    }
    bh.consume(builder.result())
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  def builderMany(bh: Blackhole): Unit = {
    val builder = ByteString.newBuilder
    var i = 0
    while (i < 10000) {
      builder ++= bs
      i += 1
    }
    bh.consume(builder.result())
  }
}
