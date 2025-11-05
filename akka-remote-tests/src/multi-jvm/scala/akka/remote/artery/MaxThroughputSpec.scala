/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.remote.{ RARP, RemoteActorRefProvider, RemotingMultiNodeSpec }
import akka.remote.artery.compress.CompressionProtocol.Events.ReceivedActorRefCompressionTable
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, PerfFlamesSupport }
import akka.serialization.{ ByteBufferSerializer, SerializerWithStringManifest }
import akka.serialization.jackson.CborSerializable
import akka.testkit._

object MaxThroughputSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  val barrierTimeout = 5.minutes

  val cfg = ConfigFactory.parseString(s"""
     # for serious measurements you should increase the totalMessagesFactor (80)
     akka.test.MaxThroughputSpec.totalMessagesFactor = 160.0
     akka.test.MaxThroughputSpec.real-message = off
     akka.test.MaxThroughputSpec.actor-selection = off
     akka {
       loglevel = INFO
       log-dead-letters = 100
       # avoid TestEventListener
       loggers = ["akka.event.Logging$$DefaultLogger"]
       testconductor.barrier-timeout = ${barrierTimeout.toSeconds}s
       actor {
         provider = remote

         serializers {
           test = "akka.remote.artery.MaxThroughputSpec$$TestSerializer"
           test-message = "akka.remote.artery.TestMessageSerializer"
         }
         serialization-bindings {
           "akka.remote.artery.MaxThroughputSpec$$FlowControl" = test
           "akka.remote.artery.TestMessage" = test-message
         }
       }
       remote.artery {

         # for serious measurements when running this test on only one machine
         # it is recommended to use external media driver
         # See akka-remote/src/test/resources/aeron.properties
         # advanced.aeron.embedded-media-driver = off
         # advanced.aeron.aeron-dir = "akka-remote/target/aeron"
         # on linux, use directory on ram disk, instead
         # advanced.aeron.aeron-dir = "/dev/shm/aeron"

         advanced.compression {
           actor-refs.advertisement-interval = 2 second
           manifests.advertisement-interval = 2 second
         }

         advanced {
           # inbound-lanes = 1
           # buffer-pool-size = 512
         }
       }
     }
     akka.remote.default-remote-dispatcher {
       fork-join-executor {
         # parallelism-factor = 0.5
         parallelism-min = 4
         parallelism-max = 4
       }
       # Set to 10 by default. Might be worthwhile to experiment with.
       # throughput = 100
     }
    """)

  commonConfig(debugConfig(on = false).withFallback(cfg).withFallback(RemotingMultiNodeSpec.commonConfig))

  case object Run
  sealed trait Echo extends DeadLetterSuppression with CborSerializable
  case object StartAck extends Echo
  final case class Start(correspondingReceiver: ActorRef) extends Echo
  case object End extends Echo
  final case class Warmup(payload: AnyRef) extends CborSerializable
  final case class EndResult(totalReceived: Long) extends CborSerializable
  final case class FlowControl(id: Int, burstStartTime: Long) extends Echo

  sealed trait Target {
    def tell(msg: Any, sender: ActorRef): Unit
    def ref: ActorRef
  }

  final case class ActorRefTarget(override val ref: ActorRef) extends Target {
    override def tell(msg: Any, sender: ActorRef) = ref.tell(msg, sender)
  }

  final case class ActorSelectionTarget(sel: ActorSelection, override val ref: ActorRef) extends Target {
    override def tell(msg: Any, sender: ActorRef) = sel.tell(msg, sender)
  }

  def receiverProps(reporter: RateReporter, payloadSize: Int, numSenders: Int): Props =
    Props(new Receiver(reporter, payloadSize, numSenders)).withDispatcher("akka.remote.default-remote-dispatcher")

  class Receiver(reporter: RateReporter, payloadSize: Int, numSenders: Int) extends Actor {
    private var c = 0L
    private var endMessagesMissing = numSenders
    private var correspondingSender
        : ActorRef = null // the Actor which send the Start message will also receive the report

    def receive = {
      case msg: Array[Byte] =>
        if (msg.length != payloadSize) throw new IllegalArgumentException("Invalid message")

        report()
      case _: TestMessage =>
        report()

      case Start(corresponding) =>
        if (corresponding == self) correspondingSender = sender()
        sender() ! StartAck

      case End if endMessagesMissing > 1 =>
        endMessagesMissing -= 1 // wait for End message from all senders

      case End =>
        correspondingSender ! EndResult(c)
        context.stop(self)

      case m: Echo =>
        sender() ! m
    }

    def report(): Unit = {
      reporter.onMessage(1, payloadSize)
      c += 1
    }
  }

  def senderProps(
      mainTarget: Target,
      targets: Array[Target],
      testSettings: TestSettings,
      plotRef: ActorRef,
      reporter: BenchmarkFileReporter): Props =
    Props(new Sender(mainTarget, targets, testSettings, plotRef, reporter))

  class Sender(
      target: Target,
      targets: Array[Target],
      testSettings: TestSettings,
      plotRef: ActorRef,
      reporter: BenchmarkFileReporter)
      extends Actor {
    val numTargets = targets.size

    import testSettings._
    val payload = ("0" * testSettings.payloadSize).getBytes("utf-8")
    var startTime = 0L
    var remaining = totalMessages
    var maxRoundTripMillis = 0L

    context.system.eventStream.subscribe(self, classOf[ReceivedActorRefCompressionTable])

    var flowControlId = 0
    var pendingFlowControl = Map.empty[Int, Int]

    val compressionEnabled =
      RARP(context.system).provider.remoteSettings.Artery.Advanced.Compression.Enabled

    def receive = {
      case Run =>
        if (compressionEnabled) {
          target.tell(Warmup(payload), self)
          context.setReceiveTimeout(1.second)
          context.become(waitingForCompression)
        } else runWarmup()
    }

    def waitingForCompression: Receive = {
      case ReceivedActorRefCompressionTable(_, table) =>
        val ref = target match {
          case ActorRefTarget(ref)          => ref
          case ActorSelectionTarget(sel, _) => sel.anchor
        }
        if (table.dictionary.contains(ref)) {
          context.setReceiveTimeout(Duration.Undefined)
          runWarmup()
        } else
          target.tell(Warmup(payload), self)
      case ReceiveTimeout =>
        target.tell(Warmup(payload), self)
    }

    def runWarmup(): Unit = {
      sendBatch(warmup = true) // first some warmup
      targets.foreach(_.tell(Start(target.ref), self)) // then Start, which will echo back here
      context.become(warmup)
    }

    def warmup: Receive = {
      case StartAck =>
        println(
          s"${self.path.name}: Starting benchmark of $totalMessages messages with burst size " +
          s"$burstSize and payload size $payloadSize")
        startTime = System.nanoTime
        remaining = totalMessages
        (0 until sent.size).foreach(i => sent(i) = 0)
        // have a few batches in flight to make sure there are always messages to send
        (1 to 3).foreach { _ =>
          val t0 = System.nanoTime()
          sendBatch(warmup = false)
          sendFlowControl(t0)
        }

        context.become(active)

      case _: Warmup =>
    }

    def active: Receive = {
      case _ @FlowControl(id, t0) =>
        val targetCount = pendingFlowControl(id)
        if (targetCount - 1 == 0) {
          pendingFlowControl -= id
          val now = System.nanoTime()
          val duration = NANOSECONDS.toMillis(now - t0)
          maxRoundTripMillis = math.max(maxRoundTripMillis, duration)

          sendBatch(warmup = false)
          sendFlowControl(now)
        } else {
          // waiting for FlowControl from more targets
          pendingFlowControl = pendingFlowControl.updated(id, targetCount - 1)
        }
    }

    val waitingForEndResult: Receive = {
      case EndResult(totalReceived) =>
        val took = NANOSECONDS.toMillis(System.nanoTime - startTime)
        val throughput = (totalReceived * 1000.0 / took)

        reporter.reportResults(s"=== ${reporter.testName} ${self.path.name}: " +
        f"throughput ${throughput * testSettings.senderReceiverPairs}%,.0f msg/s, " +
        f"${throughput * payloadSize * testSettings.senderReceiverPairs}%,.0f bytes/s (payload), " +
        f"${throughput * totalSize(context.system) * testSettings.senderReceiverPairs}%,.0f bytes/s (total" +
        (if (RARP(context.system).provider.remoteSettings.Artery.Advanced.Compression.Enabled) ",compression" else "") + "), " +
        (if (testSettings.senderReceiverPairs == 1) s"dropped ${totalMessages - totalReceived}, " else "") +
        s"max round-trip $maxRoundTripMillis ms, " +
        s"burst size $burstSize, " +
        s"payload size $payloadSize, " +
        s"total size ${totalSize(context.system)}, " +
        s"$took ms to deliver $totalReceived messages.")

        plotRef ! PlotResult().add(testName, throughput * payloadSize * testSettings.senderReceiverPairs / 1024 / 1024)
        context.stop(self)

      case _: ReceivedActorRefCompressionTable =>
    }

    val sent = new Array[Long](targets.size)
    def sendBatch(warmup: Boolean): Unit = {
      val batchSize = math.min(remaining, burstSize)
      var i = 0
      while (i < batchSize) {
        val msg0 =
          if (realMessage)
            TestMessage(
              id = totalMessages - remaining + i,
              name = "abc",
              status = i % 2 == 0,
              description = "ABC",
              payload = payload,
              items = Vector(TestMessage.Item(1, "A"), TestMessage.Item(2, "B")))
          else payload

        val msg1 = if (warmup) Warmup(msg0) else msg0

        targets(i % numTargets).tell(msg1, ActorRef.noSender)
        sent(i % numTargets) += 1
        i += 1
      }
      remaining -= batchSize
    }

    def sendFlowControl(t0: Long): Unit = {
      if (remaining <= 0) {
        context.become(waitingForEndResult)
        targets.foreach(_.tell(End, self))
      } else {
        flowControlId += 1
        pendingFlowControl = pendingFlowControl.updated(flowControlId, targets.size)
        val flowControlMsg = FlowControl(flowControlId, t0)
        targets.foreach(_.tell(flowControlMsg, self))
      }
    }
  }

  final case class TestSettings(
      testName: String,
      totalMessages: Long,
      burstSize: Int,
      payloadSize: Int,
      senderReceiverPairs: Int,
      realMessage: Boolean) {
    // data based on measurement
    def totalSize(system: ActorSystem) =
      payloadSize + (if (RARP(system).provider.remoteSettings.Artery.Advanced.Compression.Enabled) 38 else 110)
  }

  class TestSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest with ByteBufferSerializer {

    val FlowControlManifest = "A"

    override val identifier: Int = 100

    override def manifest(o: AnyRef): String =
      o match {
        case _: FlowControl => FlowControlManifest
        case _              => throw new NotSerializableException()
      }

    override def toBinary(o: AnyRef, buf: ByteBuffer): Unit =
      o match {
        case FlowControl(id, burstStartTime) =>
          buf.putInt(id)
          buf.putLong(burstStartTime)
        case _ => throw new NotSerializableException()
      }

    override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef =
      manifest match {
        case FlowControlManifest => FlowControl(buf.getInt, buf.getLong)
        case _                   => throw new NotSerializableException()
      }

    override def toBinary(o: AnyRef): Array[Byte] = o match {
      case FlowControl(_, _) =>
        val buf = ByteBuffer.allocate(12)
        toBinary(o, buf)
        buf.flip()
        val bytes = new Array[Byte](buf.remaining)
        buf.get(bytes)
        bytes
      case _ => throw new NotSerializableException()
    }

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
      fromBinary(ByteBuffer.wrap(bytes), manifest)
  }

}

class MaxThroughputSpecMultiJvmNode1 extends MaxThroughputSpec
class MaxThroughputSpecMultiJvmNode2 extends MaxThroughputSpec

abstract class MaxThroughputSpec extends RemotingMultiNodeSpec(MaxThroughputSpec) with PerfFlamesSupport {

  import MaxThroughputSpec._

  val totalMessagesFactor = system.settings.config.getDouble("akka.test.MaxThroughputSpec.totalMessagesFactor")
  val realMessage = system.settings.config.getBoolean("akka.test.MaxThroughputSpec.real-message")
  val actorSelection = system.settings.config.getBoolean("akka.test.MaxThroughputSpec.actor-selection")

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
    runOn(first) {
      println(plot.csv(system.name))
    }
    super.afterAll()
  }

  def identifyReceiver(name: String, r: RoleName = second): Target = {
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
      senderReceiverPairs = 1,
      realMessage),
    TestSettings(
      testName = "1-to-1",
      totalMessages = adjustedTotalMessages(50000),
      burstSize = 1000,
      payloadSize = 100,
      senderReceiverPairs = 1,
      realMessage),
    TestSettings(
      testName = "1-to-1-size-1k",
      totalMessages = adjustedTotalMessages(20000),
      burstSize = 1000,
      payloadSize = 1000,
      senderReceiverPairs = 1,
      realMessage),
    TestSettings(
      testName = "1-to-1-size-10k",
      totalMessages = adjustedTotalMessages(5000),
      burstSize = 1000,
      payloadSize = 10000,
      senderReceiverPairs = 1,
      realMessage),
    TestSettings(
      testName = "5-to-5",
      totalMessages = adjustedTotalMessages(20000),
      burstSize = 200, // don't exceed the send queue capacity 200*5*3=3000
      payloadSize = 100,
      senderReceiverPairs = 5,
      realMessage))

  def test(testSettings: TestSettings, resultReporter: BenchmarkFileReporter): Unit = {
    import testSettings._
    val receiverName = testName + "-rcv"

    runPerfFlames(first, second)(delay = 5.seconds)

    runOn(second) {
      val rep = reporter(testName)
      val receivers = (1 to senderReceiverPairs).map { n =>
        system.actorOf(receiverProps(rep, payloadSize, senderReceiverPairs), receiverName + n)
      }
      enterBarrier(receiverName + "-started")
      enterBarrier(testName + "-done")
      receivers.foreach(_ ! PoisonPill)
      rep.halt()
    }

    runOn(first) {
      enterBarrier(receiverName + "-started")
      val receivers = (for (n <- 1 to senderReceiverPairs) yield identifyReceiver(receiverName + n)).toArray
      val senders = for (n <- 1 to senderReceiverPairs) yield {
        val receiver = receivers(n - 1)
        val plotProbe = TestProbe()
        val snd = system
          .actorOf(senderProps(receiver, receivers, testSettings, plotProbe.ref, resultReporter), testName + "-snd" + n)
        val terminationProbe = TestProbe()
        terminationProbe.watch(snd)
        snd ! Run
        (snd, terminationProbe, plotProbe)
      }
      senders.foreach {
        case (snd, terminationProbe, plotProbe) =>
          terminationProbe.expectTerminated(snd, barrierTimeout)
          if (snd == senders.head._1) {
            val plotResult = plotProbe.expectMsgType[PlotResult]
            plot = plot.addAll(plotResult)
          }
      }
      enterBarrier(testName + "-done")
    }

    enterBarrier("after-" + testName)
  }

  "Max throughput of Artery" must {
    val reporter = BenchmarkFileReporter("MaxThroughputSpec", system)
    for (s <- scenarios) {
      s"be great for ${s.testName}, burstSize = ${s.burstSize}, payloadSize = ${s.payloadSize}" in test(s, reporter)
    }
  }
}
