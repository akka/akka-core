/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import akka.util.ByteString.{ ByteString1C, ByteStrings }

@State(Scope.Benchmark)
@Measurement(timeUnit = TimeUnit.MILLISECONDS)
class ByteString_dropSliceTake_Benchmark {

  val _bs_mini = ByteString(Array.ofDim[Byte](128 * 4))
  val _bs_small = ByteString(Array.ofDim[Byte](1024 * 1))
  val _bs_large = ByteString(Array.ofDim[Byte](1024 * 4))

  val bs_mini = ByteString(Array.ofDim[Byte](128 * 4 * 4))
  val bs_small = ByteString(Array.ofDim[Byte](1024 * 1 * 4))
  val bs_large = ByteString(Array.ofDim[Byte](1024 * 4 * 4))

  val bss_mini = ByteStrings(Vector.fill(4)(bs_mini.asInstanceOf[ByteString1C].toByteString1), 4 * bs_mini.length)
  val bss_small = ByteStrings(Vector.fill(4)(bs_small.asInstanceOf[ByteString1C].toByteString1), 4 * bs_small.length)
  val bss_large = ByteStrings(Vector.fill(4)(bs_large.asInstanceOf[ByteString1C].toByteString1), 4 * bs_large.length)
  val bss_pc_large = bss_large.compact

  /*
   --------------------------------- BASELINE --------------------------------------------------------------------
   [info] Benchmark                                                         Mode  Cnt            Score         Error  Units
   [info] ByteString_dropSliceTake_Benchmark.bs_large_dropRight_100        thrpt   20  111 122 621.983 ± 6172679.160  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bs_large_dropRight_256        thrpt   20  110 238 003.870 ± 4042572.908  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bs_large_dropRight_2000       thrpt   20  106 435 449.123 ± 2972282.531  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_dropRight_100       thrpt   20    1 155 292.430 ±   23096.219  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_dropRight_256       thrpt   20    1 191 713.229 ±   15910.426  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_dropRight_2000      thrpt   20    1 201 342.579 ±   21119.392  ops/s

   [info] ByteString_dropSliceTake_Benchmark.bs_large_drop_100             thrpt   20  108 252 561.824 ± 3841392.346  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bs_large_drop_256             thrpt   20  112 515 936.237 ± 5651549.124  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bs_large_drop_2000            thrpt   20  110 851 553.706 ± 3327510.108  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_drop_18             thrpt   20      983 544.541 ±   46299.808  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_drop_100            thrpt   20      875 345.433 ±   44760.533  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_drop_256            thrpt   20      864 182.258 ±  111172.303  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_drop_2000           thrpt   20      997 459.151 ±   33627.993  ops/s

   [info] ByteString_dropSliceTake_Benchmark.bs_large_slice_80_80          thrpt   20  112 299 538.691 ± 7259114.294  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bs_large_slice_129_129        thrpt   20  105 640 836.625 ± 9112709.942  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_slice_80_80         thrpt   20   10 868 202.262 ±  526537.133  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_slice_129_129       thrpt   20    9 429 199.802 ± 1321542.453  ops/s

   --------------------------------- AFTER -----------------------------------------------------------------------

   ------ TODAY –––––––
   [info] Benchmark                                                         Mode  Cnt            Score         Error  Units
   [info] ByteString_dropSliceTake_Benchmark.bs_large_dropRight_100        thrpt   20  126 091 961.654 ± 2813125.268  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bs_large_dropRight_256        thrpt   20  118 393 394.350 ± 2934782.759  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bs_large_dropRight_2000       thrpt   20  119 183 386.004 ± 4445324.298  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_dropRight_100       thrpt   20    8 813 065.392 ±  234570.880  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_dropRight_256       thrpt   20    9 039 585.934 ±  297168.301  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_dropRight_2000      thrpt   20    9 629 458.168 ±  124846.904  ops/s

   [info] ByteString_dropSliceTake_Benchmark.bs_large_drop_100             thrpt   20  111 666 137.955 ± 4846727.674  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bs_large_drop_256             thrpt   20  114 405 514.622 ± 4985750.805  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bs_large_drop_2000            thrpt   20  114 364 716.297 ± 2512280.603  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_drop_18             thrpt   20   10 040 457.962 ±  527850.116  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_drop_100            thrpt   20    9 184 934.769 ±  549140.840  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_drop_256            thrpt   20   10 887 437.121 ±  195606.240  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_drop_2000           thrpt   20   10 725 300.292 ±  403470.413  ops/s

   [info] ByteString_dropSliceTake_Benchmark.bs_large_slice_80_80          thrpt   20  233 017 314.148 ± 7070246.826  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bs_large_slice_129_129        thrpt   20  275 245 086.247 ± 4969752.048  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_slice_80_80         thrpt   20  264 963 420.976 ± 4259289.143  ops/s
   [info] ByteString_dropSliceTake_Benchmark.bss_large_slice_129_129       thrpt   20  265 477 577.022 ± 4623974.283  ops/s

   */

  // 18 == "http://example.com", a typical url length

  @Benchmark
  def bs_large_drop_0: ByteString =
    bs_large.drop(0)
  @Benchmark
  def bss_large_drop_0: ByteString =
    bss_large.drop(0)

  @Benchmark
  def bs_large_drop_18: ByteString =
    bs_large.drop(18)
  @Benchmark
  def bss_large_drop_18: ByteString =
    bss_large.drop(18)

  @Benchmark
  def bs_large_drop_100: ByteString =
    bs_large.drop(100)
  @Benchmark
  def bss_large_drop_100: ByteString =
    bss_large.drop(100)

  @Benchmark
  def bs_large_drop_256: ByteString =
    bs_large.drop(256)
  @Benchmark
  def bss_large_drop_256: ByteString =
    bss_large.drop(256)

  @Benchmark
  def bs_large_drop_2000: ByteString =
    bs_large.drop(2000)
  @Benchmark
  def bss_large_drop_2000: ByteString =
    bss_large.drop(2000)

  /* these force 2 array drops, and 1 element drop inside the 2nd to first/last; can be considered as "bad case" */

  @Benchmark
  def bs_large_slice_129_129: ByteString =
    bs_large.slice(129, 129)
  @Benchmark
  def bss_large_slice_129_129: ByteString =
    bss_large.slice(129, 129)

  /* these only move the indexes, don't drop any arrays "happy case" */

  @Benchmark
  def bs_large_slice_80_80: ByteString =
    bs_large.slice(80, 80)
  @Benchmark
  def bss_large_slice_80_80: ByteString =
    bss_large.slice(80, 80)

  // drop right ---

  @Benchmark
  def bs_large_dropRight_100: ByteString =
    bs_large.dropRight(100)
  @Benchmark
  def bss_large_dropRight_100: ByteString =
    bss_large.dropRight(100)

  @Benchmark
  def bs_large_dropRight_256: ByteString =
    bs_large.dropRight(256)
  @Benchmark
  def bss_large_dropRight_256: ByteString =
    bss_large.dropRight(256)

  @Benchmark
  def bs_large_dropRight_2000: ByteString =
    bs_large.dropRight(2000)
  @Benchmark
  def bss_large_dropRight_2000: ByteString =
    bss_large.dropRight(2000)

}
