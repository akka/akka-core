/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.remote.artery.BenchTestSourceSameElement
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

object ZipWithIndexBenchmark {
  final val OperationsPerInvocation = 100000
}

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class ZipWithIndexBenchmark {
  import ZipWithIndexBenchmark._

  private val config = ConfigFactory.parseString("""
    akka.actor.default-dispatcher {
      executor = "fork-join-executor"
      fork-join-executor {
        parallelism-factor = 1
      }
    }
    """)

  private implicit val system: ActorSystem = ActorSystem("ZipWithIndexBenchmark", config)

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  def createSource(count: Int): Source[Int, NotUsed] = Source.fromGraph(new BenchTestSourceSameElement(count, 1))

  private val newZipWithIndex =
    createSource(OperationsPerInvocation).zipWithIndex.toMat(Sink.ignore)(Keep.right)

  private val oldZipWithIndex = createSource(OperationsPerInvocation)
    .statefulMapConcat[(Int, Long)] { () =>
      var index: Long = 0L
      elem => {
        val zipped = (elem, index)
        index += 1
        immutable.Iterable[(Int, Long)](zipped)
      }
    }
    .toMat(Sink.ignore)(Keep.right)

  @Benchmark
  @OperationsPerInvocation(OperationsPerInvocation)
  def benchOldZipWithIndex(): Unit =
    Await.result(oldZipWithIndex.run(), Duration.Inf)

  @Benchmark
  @OperationsPerInvocation(OperationsPerInvocation)
  def benchNewZipWithIndex(): Unit =
    Await.result(newZipWithIndex.run(), Duration.Inf)

}
