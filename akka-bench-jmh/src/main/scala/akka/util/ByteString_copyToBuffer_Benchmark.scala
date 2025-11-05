/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import akka.util.ByteString.{ ByteString1C, ByteStrings }

@State(Scope.Benchmark)
@Measurement(timeUnit = TimeUnit.MILLISECONDS)
class ByteString_copyToBuffer_Benchmark {

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

  val buf = ByteBuffer.allocate(1024 * 4 * 4)

  /*
    BEFORE

    [info] Benchmark                                       Mode  Cnt            Score          Error  Units
    [info] ByteStringBenchmark.bs_large_copyToBuffer      thrpt   40  142 163 289.866 ± 21751578.294  ops/s
    [info] ByteStringBenchmark.bss_large_copyToBuffer     thrpt   40    1 489 195.631 ±   209165.487  ops/s << that's the interesting case, we needlessly fold and allocate tons of Stream etc
    [info] ByteStringBenchmark.bss_large_pc_copyToBuffer  thrpt   40  184 466 756.364 ±  9169108.378  ops/s // "can't beat that"


    [info] ....[Thread state: RUNNABLE]........................................................................
    [info]  35.9%  35.9% scala.collection.Iterator$class.toStream
    [info]  20.2%  20.2% scala.collection.immutable.Stream.foldLeft
    [info]  11.6%  11.6% scala.collection.immutable.Stream$StreamBuilder.<init>
    [info]  10.9%  10.9% akka.util.ByteIterator.<init>
    [info]   6.1%   6.1% scala.collection.mutable.ListBuffer.<init>
    [info]   5.2%   5.2% akka.util.ByteString.copyToBuffer
    [info]   5.2%   5.2% scala.collection.AbstractTraversable.<init>
    [info]   2.2%   2.2% scala.collection.immutable.VectorIterator.initFrom
    [info]   1.2%   1.2% akka.util.generated.ByteStringBenchmark_bss_large_copyToBuffer.bss_large_copyToBuffer_thrpt_jmhStub
    [info]   0.3%   0.3% akka.util.ByteIterator$MultiByteArrayIterator.copyToBuffer
    [info]   1.2%   1.2% <other>


    AFTER specializing impls

    [info] ....[Thread state: RUNNABLE]........................................................................
    [info]  99.5%  99.6% akka.util.generated.ByteStringBenchmark_bss_large_copyToBuffer_jmhTest.bss_large_copyToBuffer_thrpt_jmhStub
    [info]   0.1%   0.1% java.util.concurrent.CountDownLatch.countDown
    [info]   0.1%   0.1% sun.reflect.NativeMethodAccessorImpl.invoke0
    [info]   0.1%   0.1% sun.misc.Unsafe.putObject
    [info]   0.1%   0.1% org.openjdk.jmh.infra.IterationParamsL2.getBatchSize
    [info]   0.1%   0.1% java.lang.Thread.currentThread
    [info]   0.1%   0.1% sun.misc.Unsafe.compareAndSwapInt
    [info]   0.1%   0.1% sun.reflect.AccessorGenerator.internalize

    [info] Benchmark                                       Mode  Cnt            Score         Error  Units
    [info] ByteStringBenchmark.bs_large_copyToBuffer      thrpt   40  177 328 585.473 ± 7742067.648  ops/s
    [info] ByteStringBenchmark.bss_large_copyToBuffer     thrpt   40  113 535 003.488 ± 3899763.124  ops/s // previous bad case now very good (was 2M/s)
    [info] ByteStringBenchmark.bss_large_pc_copyToBuffer  thrpt   40  203 590 896.493 ± 7582752.024  ops/s // "can't beat that"

   */

  @Benchmark
  def bs_large_copyToBuffer(): Int = {
    buf.flip()
    bs_large.copyToBuffer(buf)
  }

  @Benchmark
  def bss_large_copyToBuffer(): Int = {
    buf.flip()
    bss_large.copyToBuffer(buf)
  }

  /** Pre-compacted */
  @Benchmark
  def bss_large_pc_copyToBuffer(): Int = {
    buf.flip()
    bss_pc_large.copyToBuffer(buf)
  }
}
