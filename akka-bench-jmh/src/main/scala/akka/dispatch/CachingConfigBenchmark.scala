/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 20)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class CachingConfigBenchmark {

  val deepKey = "akka.actor.deep.settings.something"
  val deepConfigString = s"""$deepKey = something"""
  val deepConfig = ConfigFactory.parseString(deepConfigString)
  val deepCaching = new CachingConfig(deepConfig)

  @Benchmark def deep_config = deepConfig.hasPath(deepKey)
  @Benchmark def deep_caching = deepCaching.hasPath(deepKey)

}
