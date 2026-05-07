/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

import java.util.concurrent.ThreadLocalRandom

import org.openjdk.jmh.annotations._

@Fork(1)
@State(Scope.Benchmark)
class InvertCompressionTableBenchmark {

  /*
    TODO: Possibly specialise the inversion, it's not in hot path so not doing it for now
     a.r.artery.compress.CompressionTableBenchmark.invert_comp_to_decomp_1024           N/A  thrpt   20      5828.963 ±     281.631  ops/s
     a.r.artery.compress.CompressionTableBenchmark.invert_comp_to_decomp_256            N/A  thrpt   20     29040.889 ±     345.425  ops/s
   */

  def randomName = ThreadLocalRandom.current().nextInt(1000).toString
  val compTable_256 = CompressionTable(17L, 2, Map(Vector.fill[String](256)(randomName).zipWithIndex: _*))
  val compTable_1024 = CompressionTable(17L, 3, Map(Vector.fill[String](1024)(randomName).zipWithIndex: _*))

  @Benchmark def invert_comp_to_decomp_256 = compTable_256.invert
  @Benchmark def invert_comp_to_decomp_1024 = compTable_1024.invert
}
