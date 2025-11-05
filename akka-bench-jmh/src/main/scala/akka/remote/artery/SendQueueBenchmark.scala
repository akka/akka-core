/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue
import org.openjdk.jmh.annotations._

import akka.actor.ActorSystem
import akka.stream.KillSwitches
import akka.stream.OverflowStrategy
import akka.stream.SystemMaterializer
import akka.stream.scaladsl._

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@Fork(2)
@Warmup(iterations = 4)
@Measurement(iterations = 10)
class SendQueueBenchmark {

  val config = ConfigFactory.parseString("""
    """)

  implicit val system: ActorSystem = ActorSystem("SendQueueBenchmark", config)

  @Setup
  def setup(): Unit = {
    // eager init of materializer
    SystemMaterializer(system).materializer
  }

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(100000)
  def queue(): Unit = {
    val latch = new CountDownLatch(1)
    val barrier = new CyclicBarrier(2)
    val N = 100000
    val burstSize = 1000

    val source = Source.queue[Int](1024)

    val (queue, killSwitch) = source
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(new BarrierSink(N, latch, burstSize, barrier))(Keep.left)
      .run()

    var n = 1
    while (n <= N) {
      queue.offer(n)
      if (n % burstSize == 0 && n < N) {
        barrier.await()
      }
      n += 1
    }

    if (!latch.await(30, TimeUnit.SECONDS))
      throw new RuntimeException("Latch didn't complete in time")
    killSwitch.shutdown()
  }

  @Benchmark
  @OperationsPerInvocation(100000)
  def actorRef(): Unit = {
    val latch = new CountDownLatch(1)
    val barrier = new CyclicBarrier(2)
    val N = 100000
    val burstSize = 1000

    val source = Source.actorRef(PartialFunction.empty, PartialFunction.empty, 1024, OverflowStrategy.dropBuffer)

    val (ref, killSwitch) = source
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(new BarrierSink(N, latch, burstSize, barrier))(Keep.left)
      .run()

    var n = 1
    while (n <= N) {
      ref ! n
      if (n % burstSize == 0 && n < N) {
        barrier.await()
      }
      n += 1
    }

    if (!latch.await(30, TimeUnit.SECONDS))
      throw new RuntimeException("Latch didn't complete in time")
    killSwitch.shutdown()
  }

  @Benchmark
  @OperationsPerInvocation(100000)
  def sendQueue(): Unit = {
    val latch = new CountDownLatch(1)
    val barrier = new CyclicBarrier(2)
    val N = 100000
    val burstSize = 1000

    val queue = new ManyToOneConcurrentArrayQueue[Int](1024)
    val source = Source.fromGraph(new SendQueue[Int](_ => ()))

    val (sendQueue, killSwitch) = source
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(new BarrierSink(N, latch, burstSize, barrier))(Keep.left)
      .run()
    sendQueue.inject(queue)

    var n = 1
    while (n <= N) {
      if (!sendQueue.offer(n))
        println(s"offer failed $n") // should not happen
      if (n % burstSize == 0 && n < N) {
        barrier.await()
      }
      n += 1
    }

    if (!latch.await(30, TimeUnit.SECONDS))
      throw new RuntimeException("Latch didn't complete in time")
    killSwitch.shutdown()
  }

}
