/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

import akka.NotUsed
import akka.actor.ActorSystem
import akka.remote.artery.BenchTestSourceSameElement
import akka.stream.scaladsl.Framing
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class FramingBenchmark {

  val config: Config = ConfigFactory.parseString("""
      akka {
        log-config-on-start = off
        log-dead-letters-during-shutdown = off
        stdout-loglevel = "OFF"
        loglevel = "OFF"
        actor.default-dispatcher {
          #executor = "thread-pool-executor"
          throughput = 1024
        }
        actor.default-mailbox {
          mailbox-type = "akka.dispatch.SingleConsumerOnlyUnboundedMailbox"
        }
        test {
          timefactor =  1.0
          filter-leeway = 3s
          single-expect-default = 3s
          default-timeout = 5s
          calling-thread-dispatcher {
            type = akka.testkit.CallingThreadDispatcherConfigurator
          }
        }
      }""".stripMargin).withFallback(ConfigFactory.load())

  implicit val system: ActorSystem = ActorSystem("test", config)

  // Safe to be benchmark scoped because the flows we construct in this bench are stateless
  var flow: Source[ByteString, NotUsed] = _

  @Param(Array("32", "64", "128", "256", "512", "1024"))
  var messageSize = 0

  @Param(Array("1", "8", "16", "32", "64", "128"))
  var framePerSeq = 0

  @Setup
  def setup(): Unit = {
    SystemMaterializer(system).materializer

    val frame = List.range(0, messageSize, 1).map(_ => Random.nextPrintableChar()).mkString + "\n"
    val messageChunk = ByteString(List.range(0, framePerSeq, 1).map(_ => frame).mkString)

    Source
      .fromGraph(new BenchTestSourceSameElement(100000, messageChunk))
      .via(Framing.delimiter(ByteString("\n"), Int.MaxValue))
  }

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(100000)
  def framing(): Unit = {
    val lock = new Semaphore(1)
    lock.acquire()
    flow.runWith(Sink.onComplete(_ => lock.release()))
    lock.acquire()
  }

}
