/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import akka.util.ByteString.{ ByteString1, ByteStrings }

@State(Scope.Benchmark)
@Measurement(timeUnit = TimeUnit.MILLISECONDS)
class ByteString_apply_Benchmark {

  val bss = ByteStrings(Vector.fill(1024)(ByteString1(Array(0.toByte))))

  /*
    akka-bench-jmh/jmh:run -f 1 -wi 3 -i 3 .*ByteString_apply_Benchmark.*

    2.12 original
    ByteString_apply_Benchmark.bss_apply_best_case   thrpt    3  204261596.303 ± 94507102.894  ops/s
    ByteString_apply_Benchmark.bss_apply_worst_case  thrpt    3     170359.149 ±   102901.206  ops/s

    2.12 optimized
    ByteString_apply_Benchmark.bss_apply_best_case   thrpt    3  206985005.270 ± 7855543.098  ops/s
    ByteString_apply_Benchmark.bss_apply_worst_case  thrpt    3     437929.845 ±   27264.190  ops/s

    2.13 original
    ByteString_apply_Benchmark.bss_apply_best_case   thrpt    3  206854021.793 ± 81500220.451  ops/s
    ByteString_apply_Benchmark.bss_apply_worst_case  thrpt    3     237125.194 ±   128394.832  ops/s

    2.13 optimized
    ByteString_apply_Benchmark.bss_apply_best_case   thrpt    3  209266780.913 ± 6821134.296  ops/s
    ByteString_apply_Benchmark.bss_apply_worst_case  thrpt    3     430348.094 ±   24412.915  ops/s
   */

  @Benchmark
  def bss_apply_best_case: Byte = bss(0)

  @Benchmark
  def bss_apply_worst_case: Byte = bss(1023)
}
