/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.concurrent.Executors

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.remote.{ RemoteActorRefProvider, RemotingMultiNodeSpec }
import akka.remote.artery.MaxThroughputSpec._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, PerfFlamesSupport }
import akka.testkit._

object FanInThroughputSpec extends MultiNodeConfig {
  val totalNumberOfNodes =
    System.getProperty("akka.test.FanInThroughputSpec.nrOfNodes") match {
      case null  => 4
      case value => value.toInt
    }
  val senderReceiverPairs = totalNumberOfNodes - 1

  for (n <- 1 to totalNumberOfNodes) role("node-" + n)

  val barrierTimeout = 5.minutes

  commonConfig(
    debugConfig(on = false)
      .withFallback(
        ConfigFactory.parseString("""
       # for serious measurements you should increase the totalMessagesFactor (20)
       akka.test.FanInThroughputSpec.totalMessagesFactor = 10.0
       akka.test.FanInThroughputSpec.real-message = off
       akka.test.FanInThroughputSpec.actor-selection = off
       akka.remote.artery.advanced {
         # inbound-lanes = 4
       }
       """))
      .withFallback(MaxThroughputSpec.cfg)
      .withFallback(RemotingMultiNodeSpec.commonConfig))

}

class FanInThroughputSpecMultiJvmNode1 extends FanInThroughputSpec
class FanInThroughputSpecMultiJvmNode2 extends FanInThroughputSpec
class FanInThroughputSpecMultiJvmNode3 extends FanInThroughputSpec
class FanInThroughputSpecMultiJvmNode4 extends FanInThroughputSpec
//class FanInThroughputSpecMultiJvmNode5 extends FanInThroughputSpec
//class FanInThroughputSpecMultiJvmNode6 extends FanInThroughputSpec
//class FanInThroughputSpecMultiJvmNode7 extends FanInThroughputSpec

abstract class FanInThroughputSpec extends RemotingMultiNodeSpec(FanInThroughputSpec) with PerfFlamesSupport {

  import FanInThroughputSpec._

  val totalMessagesFactor = system.settings.config.getDouble("akka.test.FanInThroughputSpec.totalMessagesFactor")
  val realMessage = system.settings.config.getBoolean("akka.test.FanInThroughputSpec.real-message")
  val actorSelection = system.settings.config.getBoolean("akka.test.FanInThroughputSpec.actor-selection")

  var plot = PlotResult()

  def adjustedTotalMessages(n: Long): Long = (n * totalMessagesFactor).toLong

  override def initialParticipants = roles.size

  def remoteSettings =
    system.asInstanceOf[ExtendedActorSystem].provider.asInstanceOf[RemoteActorRefProvider].remoteSettings

  lazy val reporterExecutor = Executors.newFixedThreadPool(1)
  def reporter(name: String): TestRateReporter = {
    val r = new TestRateReporter(name)
    reporterExecutor.execute(r)
    r
  }

  override def afterAll(): Unit = {
    reporterExecutor.shutdown()
    runOn(roles(1)) {
      println(plot.csv(system.name))
    }
    super.afterAll()
  }

  def identifyReceiver(name: String, r: RoleName): Target = {
    val sel = system.actorSelection(node(r) / "user" / name)
    sel ! Identify(None)
    val ref = expectMsgType[ActorIdentity](10.seconds).ref.get
    if (actorSelection) ActorSelectionTarget(sel, ref)
    else ActorRefTarget(ref)
  }

  val scenarios = List(
    TestSettings(
      testName = "warmup",
      totalMessages = adjustedTotalMessages(20000),
      burstSize = 1000,
      payloadSize = 100,
      senderReceiverPairs = senderReceiverPairs,
      realMessage),
    TestSettings(
      testName = "size-100",
      totalMessages = adjustedTotalMessages(50000),
      burstSize = 1000,
      payloadSize = 100,
      senderReceiverPairs = senderReceiverPairs,
      realMessage),
    TestSettings(
      testName = "size-1k",
      totalMessages = adjustedTotalMessages(10000),
      burstSize = 1000,
      payloadSize = 1000,
      senderReceiverPairs = senderReceiverPairs,
      realMessage),
    TestSettings(
      testName = "size-10k",
      totalMessages = adjustedTotalMessages(2000),
      burstSize = 1000,
      payloadSize = 10000,
      senderReceiverPairs = senderReceiverPairs,
      realMessage))

  def test(testSettings: TestSettings, resultReporter: BenchmarkFileReporter): Unit = {
    import testSettings._
    val receiverName = testName + "-rcv"

    val sendingNodes = roles.tail

    runPerfFlames(roles: _*)(delay = 5.seconds)

    runOn(roles.head) {
      val rep = reporter(testName)
      val receivers = (1 to sendingNodes.size).map { n =>
        system.actorOf(receiverProps(rep, payloadSize, senderReceiverPairs), receiverName + "-" + n)
      }
      enterBarrier(receiverName + "-started")
      enterBarrier(testName + "-done")
      receivers.foreach(_ ! PoisonPill)
      rep.halt()
    }

    runOn(sendingNodes: _*) {
      enterBarrier(receiverName + "-started")
      val receivers = (1 to sendingNodes.size)
        .map { n =>
          identifyReceiver(receiverName + "-" + n, roles.head)
        }
        .toArray[Target]

      val idx = roles.indexOf(myself) - 1
      val receiver = receivers(idx)
      val plotProbe = TestProbe()
      val snd = system.actorOf(
        senderProps(receiver, receivers, testSettings, plotProbe.ref, resultReporter),
        testName + "-snd" + idx)
      val terminationProbe = TestProbe()
      terminationProbe.watch(snd)
      snd ! Run

      terminationProbe.expectTerminated(snd, barrierTimeout)
      if (idx == 0) {
        val plotResult = plotProbe.expectMsgType[PlotResult]
        plot = plot.addAll(plotResult)
      }

      enterBarrier(testName + "-done")
    }

    enterBarrier("after-" + testName)
  }

  "Max throughput of fan-in" must {
    val reporter = BenchmarkFileReporter("FanInThroughputSpec", system)
    for (s <- scenarios) {
      s"be great for ${s.testName}, burstSize = ${s.burstSize}, payloadSize = ${s.payloadSize}" in test(s, reporter)
    }
  }
}
