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
class ByteString_dropRight_Benchmark {

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
   --------------------------------- BASELINE -----------------------------------------------------------------------
   commit 0f2da7b26b5c4af35be87d2bd4a1a2392365df15
   [info] Benchmark                                                 Mode  Cnt           Score          Error  Units
   [info] ByteString_dropRight_Benchmark.bss_avg                   thrpt   40       25626.311 ±     1395.662  ops/s
   [info] ByteString_dropRight_Benchmark.bss_best                  thrpt   40     8667558.031 ±   200233.008  ops/s
   [info] ByteString_dropRight_Benchmark.bss_greater_or_eq_to_len  thrpt   40       12658.684 ±      376.730  ops/s
   [info] ByteString_dropRight_Benchmark.bss_negative              thrpt   40  1214680926.895 ± 10661843.507  ops/s
   [info] ByteString_dropRight_Benchmark.bss_worst                 thrpt   40       13087.245 ±      246.911  ops/s

   --------------------------------- AFTER --------------------------------------------------------------------------

   ------ TODAY –––––––
   [info] Benchmark                                                 Mode  Cnt           Score         Error  Units
   [info] ByteString_dropRight_Benchmark.bss_avg                   thrpt   40      528969.025 ±    6039.001  ops/s
   [info] ByteString_dropRight_Benchmark.bss_best                  thrpt   40     7925951.396 ±  249279.950  ops/s
   [info] ByteString_dropRight_Benchmark.bss_greater_or_eq_to_len  thrpt   40   893475724.604 ± 9836471.105  ops/s
   [info] ByteString_dropRight_Benchmark.bss_negative              thrpt   40  1182275022.613 ± 9710755.955  ops/s
   [info] ByteString_dropRight_Benchmark.bss_worst                 thrpt   40      244599.957 ±    3276.140  ops/s

   */

  @Benchmark
  def bss_negative(): ByteString =
    bss.dropRight(n_neg)

  @Benchmark
  def bss_greater_or_eq_to_len(): ByteString =
    bss.dropRight(n_greater_or_eq_to_len)

  @Benchmark
  def bss_avg(): ByteString =
    bss.dropRight(n_avg)

  @Benchmark
  def bss_best(): ByteString =
    bss.dropRight(n_best)

  @Benchmark
  def bss_worst(): ByteString =
    bss.dropRight(n_worst)
}
