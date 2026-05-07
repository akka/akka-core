/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka

import org.openjdk.jmh.results.RunResult
import org.openjdk.jmh.runner.Runner
import org.openjdk.jmh.runner.options.CommandLineOptions

object BenchRunner {
  def main(args: Array[String]) = {
    import scala.jdk.CollectionConverters._

    val args2 = args.toList.flatMap {
      case "quick"    => "-i 1 -wi 1 -f1 -t1".split(" ").toList
      case "full"     => "-i 10 -wi 4 -f3 -t1".split(" ").toList
      case "jitwatch" => "-jvmArgs=-XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation" :: Nil
      case other      => other :: Nil
    }

    val opts = new CommandLineOptions(args2: _*)
    val results = new Runner(opts).run()

    val report = results.asScala.map { (result: RunResult) =>
      val bench = result.getParams.getBenchmark
      val params =
        result.getParams.getParamsKeys.asScala.map(key => s"$key=${result.getParams.getParam(key)}").mkString("_")
      val score = result.getAggregatedResult.getPrimaryResult.getScore.round
      val unit = result.getAggregatedResult.getPrimaryResult.getScoreUnit
      s"\t${bench}_${params}\t$score\t$unit"
    }

    report.toList.sorted.foreach(println)
  }
}
