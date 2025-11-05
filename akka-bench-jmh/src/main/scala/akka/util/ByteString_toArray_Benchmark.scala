/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
@Measurement(timeUnit = TimeUnit.MILLISECONDS)
class ByteString_toArray_Benchmark {

  var bs: ByteString = _

  var composed: ByteString = _

  @Param(Array("10", "100", "1000"))
  var kb = 0

  /*
    akka-bench-jmh/jmh:run -f 1 -wi 5 -i 5 .*ByteString_toArray_Benchmark.*


    Benchmark             (kb)   Mode  Cnt        Score       Error  Units
    2.12
    composed_bs_to_array    10  thrpt    5     5625.395 ±   145.999  ops/s
    composed_bs_to_array   100  thrpt    5      464.893 ±    33.579  ops/s
    composed_bs_to_array  1000  thrpt    5       45.482 ±     4.217  ops/s
    single_bs_to_array      10  thrpt    5  1450077.491 ± 27964.993  ops/s
    single_bs_to_array     100  thrpt    5    72201.806 ±   637.800  ops/s
    single_bs_to_array    1000  thrpt    5     5491.724 ±   178.696  ops/s

    2.13 before (second) fix
    composed_bs_to_array    10  thrpt    5      128.706 ±      4.590  ops/s
    composed_bs_to_array   100  thrpt    5       13.147 ±      0.435  ops/s
    composed_bs_to_array  1000  thrpt    5        1.255 ±      0.057  ops/s
    single_bs_to_array      10  thrpt    5  1336977.040 ± 719295.197  ops/s
    single_bs_to_array     100  thrpt    5    70202.111 ±    522.435  ops/s
    single_bs_to_array    1000  thrpt    5     5444.186 ±    224.677  ops/s

    2.13 with (second) fix
    composed_bs_to_array    10  thrpt    5     5970.395 ±   348.356  ops/s
    composed_bs_to_array   100  thrpt    5      479.762 ±    15.819  ops/s
    composed_bs_to_array  1000  thrpt    5       45.940 ±     1.306  ops/s
    single_bs_to_array      10  thrpt    5  1468430.822 ± 38730.696  ops/s
    single_bs_to_array     100  thrpt    5    71313.855 ±   983.643  ops/s
    single_bs_to_array    1000  thrpt    5     5526.564 ±   143.654  ops/s
   */

  @Setup
  def setup(): Unit = {
    val bytes = Array.ofDim[Byte](1024 * kb)
    bs = ByteString(bytes)
    composed = ByteString.empty
    for (_ <- 0 to 100) {
      composed = composed ++ bs
    }
  }

  @Benchmark
  def single_bs_to_array(blackhole: Blackhole): Unit = {
    blackhole.consume(bs.toArray[Byte])
  }

  @Benchmark
  def composed_bs_to_array(blackhole: Blackhole): Unit = {
    blackhole.consume(composed.toArray[Byte])
  }

}
