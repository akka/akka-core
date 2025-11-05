/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._

import BenchmarkActors._
import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

import akka.testkit.TestProbe

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS, batchSize = 1)
@Measurement(iterations = 10, time = 15, timeUnit = TimeUnit.SECONDS, batchSize = 1)
class ForkJoinActorBenchmark {
  import ForkJoinActorBenchmark._

  @Param(Array("50"))
  var tpt = 0

  @Param(Array(coresStr)) // coresStr, cores2xStr, cores4xStr
  var threads = ""

  @Param(
    Array(
      "akka.dispatch.SingleConsumerOnlyUnboundedMailbox",
      "akka.actor.ManyToOneArrayMailbox",
      "akka.actor.JCToolsMailbox"))
  var mailbox = ""

  implicit var system: ActorSystem = _

  @Setup(Level.Trial)
  def setup(): Unit = {

    requireRightNumberOfCores(cores)

    system = ActorSystem(
      "ForkJoinActorBenchmark",
      ConfigFactory.parseString(s"""
        akka {
           log-dead-letters = off
           default-mailbox.mailbox-capacity = 512
           actor {
             fjp-dispatcher {
               executor = "fork-join-executor"
               fork-join-executor {
                 parallelism-min = $threads
                 parallelism-factor = 1.0
                 parallelism-max = $threads
               }
               throughput = $tpt
               mailbox-type = "$mailbox"
             }
           }
         }
      """))
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 15.seconds)
  }

  //  @Benchmark
  //  @OperationsPerInvocation(totalMessagesTwoActors)
  //  def pingPong(): Unit = benchmarkPingPongActors(messages, twoActors, "fjp-dispatcher", tpt, timeout)

  @Benchmark
  @OperationsPerInvocation(totalMessagesLessThanCores)
  def pingPongLessActorsThanCores(): Unit =
    benchmarkPingPongActors(messages, lessThanCoresActors, "fjp-dispatcher", tpt, timeout)

  //  @Benchmark
  //  @OperationsPerInvocation(totalMessagesSameAsCores)
  //  def pingPongSameNumberOfActorsAsCores(): Unit = benchmarkPingPongActors(messages, sameAsCoresActors, "fjp-dispatcher", tpt, timeout)

  @Benchmark
  @OperationsPerInvocation(totalMessagesMoreThanCores)
  def pingPongMoreActorsThanCores(): Unit =
    benchmarkPingPongActors(messages, moreThanCoresActors, "fjp-dispatcher", tpt, timeout)

  //  @Benchmark
  //  @Measurement(timeUnit = TimeUnit.MILLISECONDS)
  //  @OperationsPerInvocation(messages)
  def floodPipe(): Unit = {

    val end = system.actorOf(Props(classOf[Pipe], None))
    val middle = system.actorOf(Props(classOf[Pipe], Some(end)))
    val penultimate = system.actorOf(Props(classOf[Pipe], Some(middle)))
    val beginning = system.actorOf(Props(classOf[Pipe], Some(penultimate)))

    val p = TestProbe()
    p.watch(end)

    @tailrec def send(left: Int): Unit =
      if (left > 0) {
        beginning ! Message
        send(left - 1)
      }

    send(messages / 4) // we have 4 actors in the pipeline

    beginning ! Stop

    p.expectTerminated(end, timeout)
  }
}

object ForkJoinActorBenchmark {
  final val messages = 2000000 // messages per actor pair

  // Constants because they are used in annotations
  // update according to cpu
  final val cores = 8
  final val coresStr = "8"
  final val cores2xStr = "16"
  final val cores4xStr = "24"

  final val twoActors = 2
  final val moreThanCoresActors = cores * 2
  final val lessThanCoresActors = cores / 2
  final val sameAsCoresActors = cores

  final val totalMessagesTwoActors = messages
  final val totalMessagesMoreThanCores = (moreThanCoresActors * messages) / 2
  final val totalMessagesLessThanCores = (lessThanCoresActors * messages) / 2
  final val totalMessagesSameAsCores = (sameAsCoresActors * messages) / 2

}
