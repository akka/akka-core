/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import org.openjdk.jmh.annotations._

import akka.routing.RoundRobinPool
import akka.testkit.TestActors
import akka.testkit.TestProbe

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.SingleShotTime))
@Fork(3)
@Warmup(iterations = 20)
@Measurement(iterations = 100)
class RouterPoolCreationBenchmark {
  implicit val system: ActorSystem = ActorSystem()
  val probe = TestProbe()

  Props[TestActors.EchoActor]()

  @Param(Array("1000", "2000", "3000", "4000"))
  var size = 0

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 15.seconds)
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def testCreation: Boolean = {
    val pool = system.actorOf(RoundRobinPool(size).props(TestActors.echoActorProps))
    pool.tell("hello", probe.ref)
    probe.expectMsg(5.seconds, "hello")
    true
  }
}
