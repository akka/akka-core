/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

import akka.actor._
import akka.testkit.TestProbe

object NodeQueueBenchmark {
  final val burst = 100000
  case object Stop
}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class NodeQueueBenchmark {
  import NodeQueueBenchmark._

  val config = ConfigFactory.parseString("""
dispatcher {
  executor = "thread-pool-executor"
  throughput = 1000
  thread-pool-executor {
    fixed-pool-size = 1
  }
}
mailbox {
  mailbox-type = "akka.dispatch.SingleConsumerOnlyUnboundedMailbox"
  mailbox-capacity = 1000000
}
""").withFallback(ConfigFactory.load())
  implicit val sys: ActorSystem = ActorSystem("ANQ", config)
  val ref = sys.actorOf(Props(new Actor {
    def receive = {
      case Stop => sender() ! Stop
      case _    =>
    }
  }).withDispatcher("dispatcher").withMailbox("mailbox"), "receiver")

  @TearDown
  def teardown(): Unit = Await.result(sys.terminate(), 5.seconds)

  @TearDown(Level.Invocation)
  def waitInBetween(): Unit = {
    val probe = TestProbe()
    probe.send(ref, Stop)
    probe.expectMsg(Stop)
    System.gc()
    System.gc()
    System.gc()
  }

  @Benchmark
  @OperationsPerInvocation(burst)
  def send(): Unit = {
    var todo = burst
    while (todo > 0) {
      ref ! "hello"
      todo -= 1
    }
  }

}
