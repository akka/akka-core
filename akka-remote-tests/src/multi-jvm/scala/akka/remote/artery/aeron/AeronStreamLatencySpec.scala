/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery
package aeron

import java.io.File
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray
import java.util.concurrent.locks.LockSupport

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import io.aeron.Aeron
import io.aeron.CncFileDescriptor
import org.HdrHistogram.Histogram
import org.agrona.IoUtil
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue

import akka.Done
import akka.actor._
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.STMultiNodeSpec
import akka.stream.KillSwitches
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.testkit._
import akka.util.ByteString

object AeronStreamLatencySpec extends MultiNodeConfig {
  // important to not use aeron-udp via system property override because that will cause port conflict
  System.setProperty("akka.remote.artery.transport", "tcp")

  val first = role("first")
  val second = role("second")

  val barrierTimeout = 5.minutes

  commonConfig(debugConfig(on = false).withFallback(ConfigFactory.parseString(s"""
       # for serious measurements you should increase the totalMessagesFactor (10) and repeatCount (3)
       akka.test.AeronStreamLatencySpec.totalMessagesFactor = 1.0
       akka.test.AeronStreamLatencySpec.repeatCount = 1
       akka {
         loglevel = INFO
         testconductor.barrier-timeout = ${barrierTimeout.toSeconds}s
         actor {
           provider = remote
         }
         remote.artery {
           advanced.aeron.idle-cpu-level=8
         }
       }
       """)))

  final case class TestSettings(
      testName: String,
      messageRate: Int, // msg/s
      payloadSize: Int,
      repeat: Int)

}

class AeronStreamLatencySpecMultiJvmNode1 extends AeronStreamLatencySpec
class AeronStreamLatencySpecMultiJvmNode2 extends AeronStreamLatencySpec

abstract class AeronStreamLatencySpec
    extends AeronStreamMultiNodeSpec(AeronStreamLatencySpec)
    with STMultiNodeSpec
    with ImplicitSender {

  import AeronStreamLatencySpec._

  val totalMessagesFactor = system.settings.config.getDouble("akka.test.AeronStreamLatencySpec.totalMessagesFactor")
  val repeatCount = system.settings.config.getInt("akka.test.AeronStreamLatencySpec.repeatCount")

  var plots = LatencyPlots()

  val driver = startDriver()

  val pool = new EnvelopeBufferPool(1024 * 1024, 128)

  val cncByteBuffer = IoUtil.mapExistingFile(new File(driver.aeronDirectoryName, CncFileDescriptor.CNC_FILE), "cnc")
  val stats =
    new AeronStat(AeronStat.mapCounters(cncByteBuffer))

  val aeron = {
    val ctx = new Aeron.Context
    ctx.aeronDirectoryName(driver.aeronDirectoryName)
    Aeron.connect(ctx)
  }

  val idleCpuLevel = system.settings.config.getInt("akka.remote.artery.advanced.aeron.idle-cpu-level")
  val taskRunner = {
    val r = new TaskRunner(system.asInstanceOf[ExtendedActorSystem], idleCpuLevel)
    r.start()
    r
  }

  override def initialParticipants = roles.size

  val streamId = 1
  val giveUpMessageAfter = 30.seconds

  lazy val reporterExecutor = Executors.newFixedThreadPool(1)
  def reporter(name: String): TestRateReporter = {
    val r = new TestRateReporter(name)
    reporterExecutor.execute(r)
    r
  }

  override def afterAll(): Unit = {
    reporterExecutor.shutdown()
    taskRunner.stop()
    aeron.close()
    driver.close()
    IoUtil.unmap(cncByteBuffer)
    IoUtil.delete(new File(driver.aeronDirectoryName), true)
    runOn(first) {
      println(plots.plot50.csv(system.name + "50"))
      println(plots.plot90.csv(system.name + "90"))
      println(plots.plot99.csv(system.name + "99"))
    }
    super.afterAll()
  }

  def printTotal(testName: String, histogram: Histogram, totalDurationNanos: Long, lastRepeat: Boolean): Unit = {
    def percentile(p: Double): Double = histogram.getValueAtPercentile(p) / 1000.0
    val throughput = 1000.0 * histogram.getTotalCount / totalDurationNanos.nanos.toMillis

    println(
      s"=== AeronStreamLatency $testName: RTT " +
      f"50%%ile: ${percentile(50.0)}%.0f µs, " +
      f"90%%ile: ${percentile(90.0)}%.0f µs, " +
      f"99%%ile: ${percentile(99.0)}%.0f µs, " +
      f"rate: ${throughput}%,.0f msg/s")
    println("Histogram of RTT latencies in microseconds.")
    histogram.outputPercentileDistribution(System.out, 1000.0)

    // only use the last repeat for the plots
    if (lastRepeat) {
      plots = plots.copy(
        plot50 = plots.plot50.add(testName, percentile(50.0)),
        plot90 = plots.plot90.add(testName, percentile(90.0)),
        plot99 = plots.plot99.add(testName, percentile(99.0)))
    }
  }

  def printStats(side: String): Unit = {
    println(side + " stats:")
    stats.print(System.out)
  }

  def sendToDeadLetters[T](pending: Vector[T]): Unit =
    pending.foreach(system.deadLetters ! _)

  val scenarios = List(
    TestSettings(testName = "rate-100-size-100", messageRate = 100, payloadSize = 100, repeat = repeatCount),
    TestSettings(testName = "rate-1000-size-100", messageRate = 1000, payloadSize = 100, repeat = repeatCount),
    TestSettings(testName = "rate-10000-size-100", messageRate = 10000, payloadSize = 100, repeat = repeatCount),
    TestSettings(testName = "rate-20000-size-100", messageRate = 20000, payloadSize = 100, repeat = repeatCount),
    TestSettings(testName = "rate-1000-size-1k", messageRate = 1000, payloadSize = 1000, repeat = repeatCount))

  def test(testSettings: TestSettings): Unit = {
    import testSettings._

    runOn(first) {
      val payload = ("1" * payloadSize).getBytes("utf-8")
      // by default run for 2 seconds, but can be adjusted with the totalMessagesFactor
      val totalMessages = (2 * messageRate * totalMessagesFactor).toInt
      val sendTimes = new AtomicLongArray(totalMessages)
      val histogram = new Histogram(SECONDS.toNanos(10), 3)

      val rep = reporter(testName)
      val barrier = new CyclicBarrier(2)
      val count = new AtomicInteger
      val startTime = new AtomicLong
      val lastRepeat = new AtomicBoolean(false)
      val killSwitch = KillSwitches.shared(testName)
      val started = TestProbe()
      val startMsg = "0".getBytes("utf-8")
      Source
        .fromGraph(new AeronSource(channel(first), streamId, aeron, taskRunner, pool, 0))
        .via(killSwitch.flow)
        .runForeach { envelope =>
          val bytes = ByteString.fromByteBuffer(envelope.byteBuffer)
          if (bytes.length == 1 && bytes(0) == startMsg(0))
            started.ref ! Done
          else {
            if (bytes.length != payloadSize) throw new IllegalArgumentException("Invalid message")
            rep.onMessage(1, payloadSize)
            val c = count.incrementAndGet()
            val d = System.nanoTime() - sendTimes.get(c - 1)
            histogram.recordValue(d)
            if (c == totalMessages) {
              val totalDurationNanos = System.nanoTime() - startTime.get
              printTotal(testName, histogram, totalDurationNanos, lastRepeat.get)
              barrier.await() // this is always the last party
            }
          }
          pool.release(envelope)
        }

      within(10.seconds) {
        Source(1 to 50)
          .map { _ =>
            val envelope = pool.acquire()
            envelope.byteBuffer.put(startMsg)
            envelope.byteBuffer.flip()
            envelope
          }
          .throttle(1, 200.milliseconds, 1, ThrottleMode.Shaping)
          .runWith(new AeronSink(channel(second), streamId, aeron, taskRunner, pool, giveUpMessageAfter))
        started.expectMsg(Done)
      }

      for (rep <- 1 to repeat) {
        histogram.reset()
        count.set(0)
        lastRepeat.set(rep == repeat)

        val sendFlow = Flow[Unit].map { _ =>
          val envelope = pool.acquire()
          envelope.byteBuffer.put(payload)
          envelope.byteBuffer.flip()
          envelope
        }

        val queueValue = Source
          .fromGraph(new SendQueue[Unit](sendToDeadLetters))
          .via(sendFlow)
          .to(new AeronSink(channel(second), streamId, aeron, taskRunner, pool, giveUpMessageAfter))
          .run()

        val queue = new ManyToOneConcurrentArrayQueue[Unit](1024)
        queueValue.inject(queue)
        Thread.sleep(3000) // let materialization complete

        startTime.set(System.nanoTime())

        var i = 0
        var adjust = 0L
        // increase the rate somewhat to compensate for overhead, based on heuristics
        val adjustRateFactor =
          if (messageRate <= 100) 1.05
          else if (messageRate <= 1000) 1.1
          else if (messageRate <= 10000) 1.2
          else if (messageRate <= 20000) 1.3
          else 1.4
        val targetDelay = (SECONDS.toNanos(1) / (messageRate * adjustRateFactor)).toLong
        while (i < totalMessages) {
          LockSupport.parkNanos(targetDelay - adjust)
          val now = System.nanoTime()
          sendTimes.set(i, now)
          if (i >= 1) {
            val diff = now - sendTimes.get(i - 1)
            adjust = math.max(0L, (diff - targetDelay) / 2)
          }

          if (!queueValue.offer(()))
            fail("sendQueue full")
          i += 1
        }

        barrier.await((totalMessages / messageRate) + 10, SECONDS)
      }

      killSwitch.shutdown()
      rep.halt()
    }

    printStats(myself.name)
    enterBarrier("after-" + testName)
  }

  "Latency of Aeron Streams" must {

    "start upd port" in {
      system.actorOf(Props[UdpPortActor](), "updPort")
      enterBarrier("udp-port-started")
    }

    "start echo" in {
      runOn(second) {
        // just echo back
        Source
          .fromGraph(new AeronSource(channel(second), streamId, aeron, taskRunner, pool, 0))
          .runWith(new AeronSink(channel(first), streamId, aeron, taskRunner, pool, giveUpMessageAfter))
      }
      enterBarrier("echo-started")
    }

    for (s <- scenarios) {
      s"be low for ${s.testName}, at ${s.messageRate} msg/s, payloadSize = ${s.payloadSize}" in test(s)
    }

  }
}
