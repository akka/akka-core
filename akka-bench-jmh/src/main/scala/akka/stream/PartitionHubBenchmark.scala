/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

import akka.NotUsed
import akka.actor.ActorSystem
import akka.remote.artery.BenchTestSource
import akka.remote.artery.FixedSizePartitionHub
import akka.remote.artery.LatchSink
import akka.stream.scaladsl._
import akka.stream.scaladsl.PartitionHub
import akka.stream.testkit.scaladsl.StreamTestKit

object PartitionHubBenchmark {
  final val OperationsPerInvocation = 100000
}

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class PartitionHubBenchmark {
  import PartitionHubBenchmark._

  val config = ConfigFactory.parseString("""
    akka.actor.default-dispatcher {
      executor = "fork-join-executor"
      fork-join-executor {
        parallelism-factor = 1
      }
    }
    """)

  implicit val system: ActorSystem = ActorSystem("PartitionHubBenchmark", config)

  @Param(Array("2", "5", "10", "20", "30"))
  var NumberOfStreams = 0

  @Param(Array("256"))
  var BufferSize = 0

  var testSource: Source[java.lang.Integer, NotUsed] = _

  @Setup
  def setup(): Unit = {
    // eager init of materializer
    SystemMaterializer(system).materializer
    testSource = Source.fromGraph(new BenchTestSource(OperationsPerInvocation))
  }

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(OperationsPerInvocation)
  def partition(): Unit = {
    val N = OperationsPerInvocation
    val latch = new CountDownLatch(NumberOfStreams)

    val source = testSource.runWith(
      PartitionHub.sink[java.lang.Integer](
        (_, elem) => elem.intValue % NumberOfStreams,
        startAfterNrOfConsumers = NumberOfStreams,
        bufferSize = BufferSize))

    for (_ <- 0 until NumberOfStreams)
      source.runWith(new LatchSink(N / NumberOfStreams, latch))

    if (!latch.await(30, TimeUnit.SECONDS)) {
      dumpMaterializer()
      throw new RuntimeException("Latch didn't complete in time")
    }
  }

  //  @Benchmark
  //  @OperationsPerInvocation(OperationsPerInvocation)
  def arteryLanes(): Unit = {
    val N = OperationsPerInvocation
    val latch = new CountDownLatch(NumberOfStreams)

    val source = testSource.runWith(
      Sink.fromGraph(
        new FixedSizePartitionHub(_.intValue % NumberOfStreams, lanes = NumberOfStreams, bufferSize = BufferSize)))

    for (_ <- 0 until NumberOfStreams)
      source.runWith(new LatchSink(N / NumberOfStreams, latch))

    if (!latch.await(30, TimeUnit.SECONDS)) {
      dumpMaterializer()
      throw new RuntimeException("Latch didn't complete in time")
    }
  }

  private def dumpMaterializer(): Unit = {
    implicit val ec = system.dispatcher
    StreamTestKit.printDebugDump(SystemMaterializer(system).materializer.supervisor)
  }

}
