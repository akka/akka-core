/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.{ Scope => JmhScope }
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup

/*
[info] Benchmark                                            Mode   Samples        Score  Score error    Units
[info] a.a.ActorPathValidationBenchmark.handLoop7000       thrpt        20        0.070        0.002   ops/us
[info] a.a.ActorPathValidationBenchmark.old7000            -- blows up (stack overflow) --

[info] a.a.ActorPathValidationBenchmark.handLoopActor_1    thrpt        20       38.825        3.378   ops/us
[info] a.a.ActorPathValidationBenchmark.oldActor_1         thrpt        20        1.585        0.090   ops/us
 */
@Fork(2)
@State(JmhScope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class ActorPathValidationBenchmark {

  final val a = "actor-1"
  final val s = "687474703a2f2f74686566727569742e636f6d2f26683d37617165716378357926656e" * 100

  final val ElementRegex = """(?:[-\w:@&=+,.!~*'_;]|%\p{XDigit}{2})(?:[-\w:@&=+,.!~*'$_;]|%\p{XDigit}{2})*""".r

  //  @Benchmark // blows up with stack overflow, we know
  def old7000: Option[List[String]] = ElementRegex.unapplySeq(s)

  @Benchmark
  def handLoop7000: Boolean = ActorPath.isValidPathElement(s)

  @Benchmark
  def oldActor_1: Option[List[String]] = ElementRegex.unapplySeq(a)

  @Benchmark
  def handLoopActor_1: Boolean = ActorPath.isValidPathElement(a)

}
