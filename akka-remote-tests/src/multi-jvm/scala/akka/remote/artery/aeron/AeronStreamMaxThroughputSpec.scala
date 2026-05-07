/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery
package aeron

import java.io.File
import java.util.concurrent.Executors

import scala.collection.AbstractIterator
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import io.aeron.Aeron
import io.aeron.CncFileDescriptor
import org.agrona.IoUtil

import akka.actor._
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.STMultiNodeSpec
import akka.stream.KillSwitches
import akka.stream.scaladsl.Source
import akka.testkit._
import akka.util.ByteString

object AeronStreamMaxThroughputSpec extends MultiNodeConfig {
  // important to not use aeron-udp via system property override because that will cause port conflict
  System.setProperty("akka.remote.artery.transport", "tcp")

  val first = role("first")
  val second = role("second")

  val barrierTimeout = 5.minutes

  commonConfig(debugConfig(on = false).withFallback(ConfigFactory.parseString(s"""
       # for serious measurements you should increase the totalMessagesFactor (20)
       akka.test.AeronStreamMaxThroughputSpec.totalMessagesFactor = 1.0
       akka {
         loglevel = ERROR
         testconductor.barrier-timeout = ${barrierTimeout.toSeconds}s
         actor {
           provider = remote
         }
       }
       """)))

  final case class TestSettings(testName: String, totalMessages: Long, payloadSize: Int)

  def iterate(start: Long, end: Long): Iterator[Long] = new AbstractIterator[Long] {
    private[this] var first = true
    private[this] var acc = start
    def hasNext: Boolean = acc < end
    def next(): Long = {
      if (!hasNext) throw new NoSuchElementException("next on empty iterator")
      if (first) first = false
      else acc += 1

      acc
    }
  }

}

class AeronStreamMaxThroughputSpecMultiJvmNode1 extends AeronStreamMaxThroughputSpec
class AeronStreamMaxThroughputSpecMultiJvmNode2 extends AeronStreamMaxThroughputSpec

abstract class AeronStreamMaxThroughputSpec
    extends AeronStreamMultiNodeSpec(AeronStreamMaxThroughputSpec)
    with STMultiNodeSpec
    with ImplicitSender {

  import AeronStreamMaxThroughputSpec._

  val totalMessagesFactor =
    system.settings.config.getDouble("akka.test.AeronStreamMaxThroughputSpec.totalMessagesFactor")

  var plot = PlotResult()

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

  import system.dispatcher

  def adjustedTotalMessages(n: Long): Long = (n * totalMessagesFactor).toLong

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
    runOn(second) {
      println(plot.csv(system.name))
    }
    super.afterAll()
  }

  def printTotal(testName: String, total: Long, startTime: Long, payloadSize: Long): Unit = {
    val d = (System.nanoTime - startTime).nanos.toMillis
    val throughput = 1000.0 * total / d
    println(
      f"=== AeronStreamMaxThroughput $testName: " +
      f"${throughput}%,.0f msg/s, ${throughput * payloadSize}%,.0f bytes/s, " +
      s"payload size $payloadSize, " +
      s"$d ms to deliver $total messages")
    plot = plot.add(testName, throughput * payloadSize / 1024 / 1024)
  }

  def printStats(side: String): Unit = {
    println(side + " stats:")
    stats.print(System.out)
  }

  val scenarios = List(
    TestSettings(testName = "size-100", totalMessages = adjustedTotalMessages(1000000), payloadSize = 100),
    TestSettings(testName = "size-1k", totalMessages = adjustedTotalMessages(100000), payloadSize = 1000),
    TestSettings(testName = "size-10k", totalMessages = adjustedTotalMessages(10000), payloadSize = 10000))

  def test(testSettings: TestSettings): Unit = {
    import testSettings._
    val receiverName = testName + "-rcv"

    runOn(second) {
      val rep = reporter(testName)
      var t0 = System.nanoTime()
      var count = 0L
      val done = TestLatch(1)
      val killSwitch = KillSwitches.shared(testName)
      Source
        .fromGraph(new AeronSource(channel(second), streamId, aeron, taskRunner, pool, 0))
        .via(killSwitch.flow)
        .runForeach { envelope =>
          val bytes = ByteString.fromByteBuffer(envelope.byteBuffer)
          rep.onMessage(1, bytes.length)
          count += 1
          if (count == 1) {
            t0 = System.nanoTime()
          } else if (count == totalMessages) {
            printTotal(testName, totalMessages, t0, payloadSize)
            done.countDown()
            killSwitch.shutdown()
          }
          pool.release(envelope)
        }
        .failed
        .foreach { _.printStackTrace }

      enterBarrier(receiverName + "-started")
      Await.ready(done, barrierTimeout)
      rep.halt()
      printStats("receiver")
      enterBarrier(testName + "-done")
    }

    runOn(first) {
      enterBarrier(receiverName + "-started")

      val payload = ("0" * payloadSize).getBytes("utf-8")
      Source
        .fromIterator(() => iterate(1, totalMessages))
        .map { _ =>
          val envelope = pool.acquire()
          envelope.byteBuffer.put(payload)
          envelope.byteBuffer.flip()
          envelope
        }
        .runWith(new AeronSink(channel(second), streamId, aeron, taskRunner, pool, giveUpMessageAfter))

      printStats("sender")
      enterBarrier(testName + "-done")

    }

    enterBarrier("after-" + testName)
  }

  "Max throughput of Aeron Streams" must {

    "start upd port" in {
      system.actorOf(Props[UdpPortActor](), "updPort")
      enterBarrier("udp-port-started")
    }

    for (s <- scenarios) {
      s"be great for ${s.testName}, payloadSize = ${s.payloadSize}" in test(s)
    }

  }
}
