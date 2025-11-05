/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import org.openjdk.jmh.annotations._

/*
regex checking:
[info] a.a.ActorCreationBenchmark.synchronousStarting       ss    120000       28.285        0.481       us

hand checking:
[info] a.a.ActorCreationBenchmark.synchronousStarting       ss    120000       21.496        0.502       us


 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SingleShotTime))
@Fork(5)
@Warmup(iterations = 1000)
@Measurement(iterations = 4000)
class ActorCreationBenchmark {
  implicit val system: ActorSystem = ActorSystem()

  final val props = Props[MyActor]()

  var i = 1
  def name = {
    i += 1
    "some-rather-long-actor-name-actor-" + i
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 15.seconds)
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def synchronousStarting =
    system.actorOf(props, name)
}

class MyActor extends Actor {
  override def receive: Receive = {
    case _ =>
  }
}
