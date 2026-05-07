/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
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
import akka.remote.artery.LatchSink
import akka.stream.impl.fusing.GraphStages
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.StreamTestKit

object FlatMapConcatBenchmark {
  final val OperationsPerInvocation = 100000
}

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class FlatMapConcatBenchmark {
  import FlatMapConcatBenchmark._

  private val config = ConfigFactory.parseString("""
    akka.actor.default-dispatcher {
      executor = "fork-join-executor"
      fork-join-executor {
        parallelism-factor = 1
      }
    }
    """)

  private implicit val system: ActorSystem = ActorSystem("FlatMapConcatBenchmark", config)

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
  def sourceDotSingle(): Unit = {
    val latch = new CountDownLatch(1)

    testSource.flatMapConcat(Source.single).runWith(new LatchSink(OperationsPerInvocation, latch))

    awaitLatch(latch)
  }

  @Benchmark
  @OperationsPerInvocation(OperationsPerInvocation)
  def internalSingleSource(): Unit = {
    val latch = new CountDownLatch(1)

    testSource
      .flatMapConcat(elem => new GraphStages.SingleSource(elem))
      .runWith(new LatchSink(OperationsPerInvocation, latch))

    awaitLatch(latch)
  }

  @Benchmark
  @OperationsPerInvocation(OperationsPerInvocation)
  def oneElementList(): Unit = {
    val latch = new CountDownLatch(1)

    testSource.flatMapConcat(n => Source(n :: Nil)).runWith(new LatchSink(OperationsPerInvocation, latch))

    awaitLatch(latch)
  }

  @Benchmark
  @OperationsPerInvocation(OperationsPerInvocation)
  def mapBaseline(): Unit = {
    val latch = new CountDownLatch(1)

    testSource.map(elem => elem).runWith(new LatchSink(OperationsPerInvocation, latch))

    awaitLatch(latch)
  }

  private def awaitLatch(latch: CountDownLatch): Unit = {
    if (!latch.await(30, TimeUnit.SECONDS)) {
      implicit val ec = system.dispatcher
      StreamTestKit.printDebugDump(SystemMaterializer(system).materializer.supervisor)
      throw new RuntimeException("Latch didn't complete in time")
    }
  }

}
