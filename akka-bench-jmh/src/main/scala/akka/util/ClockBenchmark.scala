/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Warmup

import akka.actor.ActorSystem

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
class ClockBenchmark {

  private val system: ActorSystem = ActorSystem("ClockBenchmark")
  private val _nanoClock = new NanoClock
  private val _scheduledClock = new ScheduledClock(1.second, system.scheduler, system.dispatcher)

  @Benchmark
  def nanoClock(): Long = {
    _nanoClock.currentTime()
  }

  @Benchmark
  def scheduledClock(): Long = {
    _scheduledClock.currentTime()
  }

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

}
