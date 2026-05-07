/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.TimeUnit

import scala.concurrent._
import scala.concurrent.duration._

import org.openjdk.jmh.annotations._

import akka.actor.ActorSystem
import akka.stream.scaladsl._

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class InvokeWithFeedbackBenchmark {
  implicit val system: ActorSystem = ActorSystem("InvokeWithFeedbackBenchmark")

  var sourceQueue: SourceQueueWithComplete[Int] = _
  var sinkQueue: SinkQueueWithCancel[Int] = _

  val waitForResult = 100.millis

  @Setup
  def setup(): Unit = {
    // these are currently the only two built in stages using invokeWithFeedback
    val (in, out) =
      Source
        .queue[Int](bufferSize = 1, overflowStrategy = OverflowStrategy.backpressure)
        .toMat(Sink.queue[Int]())(Keep.both)
        .run()

    sourceQueue = in
    sinkQueue = out

  }

  @OperationsPerInvocation(100000)
  @Benchmark
  def pass_through_100k_elements(): Unit = {
    (0 to 100000).foreach { n =>
      val f = sinkQueue.pull()
      Await.result(sourceQueue.offer(n), waitForResult)
      Await.result(f, waitForResult)
    }
  }

  @TearDown
  def tearDown(): Unit = {
    sourceQueue.complete()
    // no way to observe sink completion from the outside
    Await.result(system.terminate(), 5.seconds)
  }

}
