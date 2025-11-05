/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.TimeUnit

import scala.annotation.tailrec

import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 5, timeUnit = TimeUnit.MICROSECONDS, batchSize = 1)
@Measurement(iterations = 10, time = 15, timeUnit = TimeUnit.MICROSECONDS, batchSize = 1)
class ImmutableIntMapBench {

  @tailrec private[this] final def add(n: Int, c: ImmutableIntMap = ImmutableIntMap.empty): ImmutableIntMap =
    if (n >= 0) add(n - 1, c.updated(n, n))
    else c

  @tailrec private[this] final def contains(n: Int, by: Int, to: Int, in: ImmutableIntMap, b: Boolean): Boolean =
    if (n <= to) {
      val result = in.contains(n)
      contains(n + by, by, to, in, result)
    } else b

  @tailrec private[this] final def get(n: Int, by: Int, to: Int, in: ImmutableIntMap, b: Int): Int =
    if (n <= to) {
      val result = in.get(n)
      get(n + by, by, to, in, result)
    } else b

  @tailrec private[this] final def hashCode(n: Int, in: ImmutableIntMap, b: Int): Int =
    if (n >= 0) {
      val result = in.hashCode
      hashCode(n - 1, in, result)
    } else b

  @tailrec private[this] final def updateIfAbsent(n: Int, by: Int, to: Int, in: ImmutableIntMap): ImmutableIntMap =
    if (n <= to) updateIfAbsent(n + by, by, to, in.updateIfAbsent(n, n))
    else in

  @tailrec private[this] final def getKey(iterations: Int, key: Int, from: ImmutableIntMap): ImmutableIntMap = {
    if (iterations > 0 && key != Int.MinValue) {
      val k = from.get(key)
      getKey(iterations - 1, k, from)
    } else from
  }

  val odd1000 = (0 to 1000).iterator.filter(_ % 2 == 1).foldLeft(ImmutableIntMap.empty)((l, i) => l.updated(i, i))

  @Benchmark
  @OperationsPerInvocation(1)
  def add1(): ImmutableIntMap = add(1)

  @Benchmark
  @OperationsPerInvocation(10)
  def add10(): ImmutableIntMap = add(10)

  @Benchmark
  @OperationsPerInvocation(100)
  def add100(): ImmutableIntMap = add(100)

  @Benchmark
  @OperationsPerInvocation(1000)
  def add1000(): ImmutableIntMap = add(1000)

  @Benchmark
  @OperationsPerInvocation(10000)
  def add10000(): ImmutableIntMap = add(10000)

  @Benchmark
  @OperationsPerInvocation(500)
  def contains(): Boolean = contains(n = 1, by = 2, to = odd1000.size, in = odd1000, b = false)

  @Benchmark
  @OperationsPerInvocation(500)
  def notcontains(): Boolean = contains(n = 0, by = 2, to = odd1000.size, in = odd1000, b = false)

  @Benchmark
  @OperationsPerInvocation(500)
  def get(): Int = get(n = 1, by = 2, to = odd1000.size, in = odd1000, b = Int.MinValue)

  @Benchmark
  @OperationsPerInvocation(500)
  def notget(): Int = get(n = 0, by = 2, to = odd1000.size, in = odd1000, b = Int.MinValue)

  @Benchmark
  @OperationsPerInvocation(500)
  def updateNotAbsent(): ImmutableIntMap = updateIfAbsent(n = 1, by = 2, to = odd1000.size, in = odd1000)

  @Benchmark
  @OperationsPerInvocation(500)
  def updateAbsent(): ImmutableIntMap = updateIfAbsent(n = 0, by = 2, to = odd1000.size, in = odd1000)

  @Benchmark
  @OperationsPerInvocation(10000)
  def hashcode(): Int = hashCode(10000, odd1000, 0)

  @Benchmark
  @OperationsPerInvocation(1000)
  def getMidElement(): ImmutableIntMap = getKey(iterations = 1000, key = 249, from = odd1000)

  @Benchmark
  @OperationsPerInvocation(1000)
  def getLoElement(): ImmutableIntMap = getKey(iterations = 1000, key = 1, from = odd1000)

  @Benchmark
  @OperationsPerInvocation(1000)
  def getHiElement(): ImmutableIntMap = getKey(iterations = 1000, key = 999, from = odd1000)
}
