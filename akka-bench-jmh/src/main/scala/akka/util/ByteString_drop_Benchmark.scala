/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.TimeUnit

import scala.util.Random

import org.openjdk.jmh.annotations._

import akka.util.ByteString.{ ByteString1, ByteStrings }

@State(Scope.Benchmark)
@Measurement(timeUnit = TimeUnit.MILLISECONDS)
class ByteString_drop_Benchmark {

  val str = List.fill[Byte](4)(0).mkString
  val numVec = 1024
  val bss = ByteStrings(Vector.fill(numVec)(ByteString1.fromString(str)))

  val rand = new Random()
  val len = str.size * numVec
  val n_greater_or_eq_to_len = len + rand.nextInt(Int.MaxValue - len)
  val n_neg = rand.nextInt(Int.MaxValue) * -1
  val n_avg = len / 2
  val n_best = 1
  val n_worst = len - 1

  /*
   --------------------------------- BASELINE ------------------------------------------------------------------
   [info] Benchmark                                             Mode  Cnt           Score          Error  Units
   [info] ByteString_drop_Benchmark.bss_avg                   thrpt   40      544841.222 ±    12917.565  ops/s
   [info] ByteString_drop_Benchmark.bss_best                  thrpt   40    10141204.609 ±   415441.925  ops/s
   [info] ByteString_drop_Benchmark.bss_greater_or_eq_to_len  thrpt   40   902173327.723 ±  9921650.983  ops/s
   [info] ByteString_drop_Benchmark.bss_negative              thrpt   40  1179430602.793 ± 12193702.247  ops/s
   [info] ByteString_drop_Benchmark.bss_worst                 thrpt   40      297489.038 ±     5534.801  ops/s

   */

  @Benchmark
  def bss_negative(): Unit = {
    @volatile var m: ByteString = null
    m = bss.drop(n_neg)
  }

  @Benchmark
  def bss_greater_or_eq_to_len(): Unit = {
    @volatile var m: ByteString = null
    m = bss.drop(n_greater_or_eq_to_len)
  }

  @Benchmark
  def bss_avg(): Unit = {
    @volatile var m: ByteString = null
    m = bss.drop(n_avg)
  }

  @Benchmark
  def bss_best(): Unit = {
    @volatile var m: ByteString = null
    m = bss.drop(n_best)
  }

  @Benchmark
  def bss_worst(): Unit = {
    @volatile var m: ByteString = null
    m = bss.drop(n_worst)
  }
}
