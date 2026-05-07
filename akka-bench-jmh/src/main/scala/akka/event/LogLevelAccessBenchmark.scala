/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import akka.event.Logging.LogLevel

@Fork(3)
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 10)
@Measurement(iterations = 20, timeUnit = TimeUnit.MILLISECONDS)
class LogLevelAccessBenchmark {

  /*
  volatile logLevel, guard on loggers
  20 readers, 2 writers
    a.e.LogLevelAccessBenchmark.g                        thrpt        60  1862566.204    37860.541   ops/ms
    a.e.LogLevelAccessBenchmark.g:readLogLevel           thrpt        60  1860031.729    37834.335   ops/ms
    a.e.LogLevelAccessBenchmark.g:writeLogLevel_1        thrpt        60     1289.452       45.403   ops/ms
    a.e.LogLevelAccessBenchmark.g:writeLogLevel_2        thrpt        60     1245.023       51.071   ops/ms
   */

  val NoopBus = new LoggingBus {
    override def subscribe(subscriber: Subscriber, to: Classifier): Boolean = true
    override def publish(event: Event): Unit = ()
    override def unsubscribe(subscriber: Subscriber, from: Classifier): Boolean = true
    override def unsubscribe(subscriber: Subscriber): Unit = ()
  }

  var log: BusLogging = akka.event.Logging(NoopBus, "").asInstanceOf[BusLogging]

  @Benchmark
  @GroupThreads(20)
  @Group("g")
  def readLogLevel(): LogLevel =
    log.bus.logLevel

  @Benchmark
  @Group("g")
  def setLogLevel_1(): Unit =
    log.bus.setLogLevel(Logging.ErrorLevel)

  @Benchmark
  @Group("g")
  def setLogLevel_2(): Unit =
    log.bus.setLogLevel(Logging.DebugLevel)

}
