/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import akka.util.ByteString.{ ByteString1C, ByteStrings }

@State(Scope.Benchmark)
@Measurement(timeUnit = TimeUnit.MILLISECONDS)
class ByteString_decode_Benchmark {

  val _bs_large = ByteString(Array.ofDim[Byte](1024 * 4))

  val bs_large = ByteString(Array.ofDim[Byte](1024 * 4 * 4))

  val bss_large = ByteStrings(Vector.fill(4)(bs_large.asInstanceOf[ByteString1C].toByteString1), 4 * bs_large.length)
  val bc_large = bss_large.compact // compacted

  val utf8String = "utf-8"
  val utf8 = Charset.forName(utf8String)

  /*
    Using Charset helps a bit, but nothing impressive:

    [info] ByteString_decode_Benchmark.bc_large_decodeString_stringCharset_utf8        thrpt   20  21 612.293 ±  825.099  ops/s
      =>
    [info] ByteString_decode_Benchmark.bc_large_decodeString_charsetCharset_utf8       thrpt   20  22 473.372 ±  851.597  ops/s


    [info] ByteString_decode_Benchmark.bs_large_decodeString_stringCharset_utf8        thrpt   20  84 443.674 ± 3723.987  ops/s
      =>
    [info] ByteString_decode_Benchmark.bs_large_decodeString_charsetCharset_utf8       thrpt   20  93 865.033 ± 2052.476  ops/s


    [info] ByteString_decode_Benchmark.bss_large_decodeString_stringCharset_utf8       thrpt   20  14 886.553 ±  326.752  ops/s
      =>
    [info] ByteString_decode_Benchmark.bss_large_decodeString_charsetCharset_utf8      thrpt   20  16 031.670 ±  474.565  ops/s
   */

  @Benchmark
  def bc_large_decodeString_stringCharset_utf8: String =
    bc_large.decodeString(utf8String)
  @Benchmark
  def bs_large_decodeString_stringCharset_utf8: String =
    bs_large.decodeString(utf8String)
  @Benchmark
  def bss_large_decodeString_stringCharset_utf8: String =
    bss_large.decodeString(utf8String)

  @Benchmark
  def bc_large_decodeString_charsetCharset_utf8: String =
    bc_large.decodeString(utf8)
  @Benchmark
  def bs_large_decodeString_charsetCharset_utf8: String =
    bs_large.decodeString(utf8)
  @Benchmark
  def bss_large_decodeString_charsetCharset_utf8: String =
    bss_large.decodeString(utf8)

}
