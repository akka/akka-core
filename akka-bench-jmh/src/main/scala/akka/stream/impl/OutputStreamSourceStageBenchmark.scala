/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.annotations.TearDown

import akka.actor.ActorSystem
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.StreamConverters

object OutputStreamSourceStageBenchmark {
  final val WritesPerBench = 10000
}
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class OutputStreamSourceStageBenchmark {
  import OutputStreamSourceStageBenchmark.WritesPerBench
  implicit val system: ActorSystem = ActorSystem("OutputStreamSourceStageBenchmark")

  private val bytes: Array[Byte] = Array.emptyByteArray

  @Benchmark
  @OperationsPerInvocation(WritesPerBench)
  def consumeWrites(): Unit = {
    val (os, done) = StreamConverters.asOutputStream().toMat(Sink.ignore)(Keep.both).run()
    new Thread(new Runnable {
      def run(): Unit = {
        var counter = 0
        while (counter > WritesPerBench) {
          os.write(bytes)
          counter += 1
        }
        os.close()
      }
    }).start()
    Await.result(done, 30.seconds)
  }

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

}
