/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.delivery

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.ProducerController
import akka.actor.typed.delivery.WorkPullingProducerController
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.MultiNodeClusterSpec
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.MultiNodeTypedClusterSpec
import akka.remote.artery.BenchmarkFileReporter
import akka.remote.artery.PlotResult
import akka.remote.artery.TestRateReporter
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.PerfFlamesSupport
import akka.serialization.jackson.CborSerializable

object DeliveryThroughputSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  val barrierTimeout = 5.minutes

  val cfg = ConfigFactory.parseString(s"""
     # for serious measurements you should increase the totalMessagesFactor (30)
     akka.test.DeliveryThroughputSpec.totalMessagesFactor = 10.0
     akka.reliable-delivery {
       consumer-controller.flow-control-window = 50
       sharding.consumer-controller.flow-control-window = 50
       sharding.producer-controller.cleanup-unused-after = 5s
     }
     akka {
       loglevel = INFO
       log-dead-letters = off
       testconductor.barrier-timeout = ${barrierTimeout.toSeconds}s
       actor.allow-java-serialization = off
       # quicker dissemination the service keys
       cluster.typed.receptionist.distributed-data.write-consistency = all
       cluster.sharding.passivate-idle-entity-after = 5s
     }
    """)

  commonConfig(debugConfig(on = false).withFallback(cfg).withFallback(MultiNodeClusterSpec.clusterConfig))

  nodeConfig(second, third) {
    ConfigFactory.parseString("""
    akka.cluster.roles = ["worker"]
    """)
  }

  lazy val reporterExecutor = Executors.newFixedThreadPool(10)
  def newRateReporter(name: String): TestRateReporter = {
    val r = new TestRateReporter(name)
    reporterExecutor.execute(r)
    r
  }

  object Consumer {
    sealed trait Command

    case object TheMessage extends Command with CborSerializable
    case object Stop extends Command

    private case class WrappedDelivery(d: ConsumerController.Delivery[Command]) extends Command

    def apply(consumerController: ActorRef[ConsumerController.Start[Command]]): Behavior[Command] = {
      Behaviors.setup { context =>
        val traceEnabled = context.log.isTraceEnabled
        val deliveryAdapter =
          context.messageAdapter[ConsumerController.Delivery[Command]](WrappedDelivery(_))
        consumerController ! ConsumerController.Start(deliveryAdapter)
        val rateReporter = newRateReporter(context.self.path.elements.mkString("/", "/", ""))

        var c = 0L

        def report(): Unit = {
          rateReporter.onMessage(1, 0) // not using the payload size
          c += 1
        }

        Behaviors
          .receiveMessagePartial[Command] {
            case WrappedDelivery(d @ ConsumerController.Delivery(_, confirmTo)) =>
              report()
              if (traceEnabled)
                context.log.trace("Processed {}", d.seqNr)
              confirmTo ! ConsumerController.Confirmed
              Behaviors.same
            case Stop =>
              Behaviors.stopped
          }
          .receiveSignal {
            case (_, PostStop) =>
              rateReporter.halt()
              Behaviors.same
          }

      }

    }
  }

  object Producer {
    sealed trait Command

    case object Run extends Command
    private case class WrappedRequestNext(r: ProducerController.RequestNext[Consumer.Command]) extends Command

    def apply(
        producerController: ActorRef[ProducerController.Command[Consumer.Command]],
        testSettings: TestSettings,
        plotRef: ActorRef[PlotResult],
        reporter: BenchmarkFileReporter): Behavior[Command] = {
      val numberOfMessages = testSettings.totalMessages

      Behaviors.setup { context =>
        val requestNextAdapter =
          context.messageAdapter[ProducerController.RequestNext[Consumer.Command]](WrappedRequestNext(_))
        var startTime = System.nanoTime()

        Behaviors.receiveMessage {
          case WrappedRequestNext(next) =>
            if (next.confirmedSeqNr >= numberOfMessages) {
              context.log.info("Completed {} messages", numberOfMessages)
              reportEnd(startTime, testSettings, plotRef, reporter)
              Behaviors.stopped
            } else {
              next.sendNextTo ! Consumer.TheMessage
              Behaviors.same
            }

          case Run =>
            context.log.info("Starting {} messages", numberOfMessages)
            startTime = System.nanoTime()
            producerController ! ProducerController.Start(requestNextAdapter)
            Behaviors.same
        }
      }
    }

    def reportEnd(
        startTime: Long,
        testSettings: TestSettings,
        plotRef: ActorRef[PlotResult],
        resultReporter: BenchmarkFileReporter): Unit = {
      val numberOfMessages = testSettings.totalMessages
      val took = NANOSECONDS.toMillis(System.nanoTime - startTime)
      val throughput = (numberOfMessages * 1000.0 / took)

      resultReporter.reportResults(
        s"=== ${resultReporter.testName} ${testSettings.testName}: " +
        f"throughput ${throughput}%,.0f msg/s, " +
        s"$took ms to deliver $numberOfMessages.")

      plotRef ! PlotResult().add(testSettings.testName, throughput)
    }
  }

  def serviceKey(testName: String) = ServiceKey[ConsumerController.Command[Consumer.Command]](testName)

  object WorkPullingProducer {
    sealed trait Command

    case object Run extends Command
    private case class WrappedRequestNext(r: WorkPullingProducerController.RequestNext[Consumer.Command])
        extends Command

    def apply(
        producerController: ActorRef[WorkPullingProducerController.Command[Consumer.Command]],
        testSettings: TestSettings,
        plotRef: ActorRef[PlotResult],
        resultReporter: BenchmarkFileReporter): Behavior[Command] = {
      val numberOfMessages = testSettings.totalMessages

      Behaviors.setup { context =>
        val requestNextAdapter =
          context.messageAdapter[WorkPullingProducerController.RequestNext[Consumer.Command]](WrappedRequestNext(_))
        var startTime = System.nanoTime()
        var remaining = numberOfMessages + context.system.settings.config
            .getInt("akka.reliable-delivery.consumer-controller.flow-control-window")

        Behaviors.receiveMessage {
          case WrappedRequestNext(next) =>
            remaining -= 1
            if (remaining == 0) {
              context.log.info("Completed {} messages", numberOfMessages)
              Producer.reportEnd(startTime, testSettings, plotRef, resultReporter)
              Behaviors.stopped
            } else {
              next.sendNextTo ! Consumer.TheMessage
              Behaviors.same
            }

          case Run =>
            context.log.info("Starting {} messages", numberOfMessages)
            startTime = System.nanoTime()
            producerController ! WorkPullingProducerController.Start(requestNextAdapter)
            Behaviors.same
        }
      }
    }
  }

  def typeKey(testName: String) = EntityTypeKey[ConsumerController.SequencedMessage[Consumer.Command]](testName)

  object ShardingProducer {
    sealed trait Command

    case object Run extends Command
    private case class WrappedRequestNext(r: ShardingProducerController.RequestNext[Consumer.Command]) extends Command
    private case object PrintStatus extends Command

    def apply(
        producerController: ActorRef[ShardingProducerController.Command[Consumer.Command]],
        testSettings: TestSettings,
        plotRef: ActorRef[PlotResult],
        resultReporter: BenchmarkFileReporter): Behavior[Command] = {
      val numberOfMessages = testSettings.totalMessages

      Behaviors.withTimers { timers =>
        Behaviors.setup { context =>
          timers.startTimerWithFixedDelay(PrintStatus, 1.second)
          val requestNextAdapter =
            context.messageAdapter[ShardingProducerController.RequestNext[Consumer.Command]](WrappedRequestNext(_))
          var startTime = System.nanoTime()
          var remaining = numberOfMessages + context.system.settings.config
              .getInt("akka.reliable-delivery.sharding.consumer-controller.flow-control-window")
          var latestDemand: ShardingProducerController.RequestNext[Consumer.Command] = null
          var messagesSentToEachEntity: Map[String, Long] = Map.empty[String, Long].withDefaultValue(0L)

          Behaviors.receiveMessage {
            case WrappedRequestNext(next) =>
              latestDemand = next
              remaining -= 1
              if (remaining == 0) {
                context.log.info("Completed {} messages", numberOfMessages)
                Producer.reportEnd(startTime, testSettings, plotRef, resultReporter)
                Behaviors.stopped
              } else {
                val entityId = (remaining % testSettings.numberOfConsumers).toString
                if (next.entitiesWithDemand(entityId) || !next.bufferedForEntitiesWithoutDemand.contains(entityId)) {
                  messagesSentToEachEntity =
                    messagesSentToEachEntity.updated(entityId, messagesSentToEachEntity(entityId) + 1L)

                  next.sendNextTo ! ShardingEnvelope(entityId, Consumer.TheMessage)
                }
                Behaviors.same
              }
            case Run =>
              context.log.info("Starting {} messages", numberOfMessages)
              startTime = System.nanoTime()
              producerController ! ShardingProducerController.Start(requestNextAdapter)
              Behaviors.same

            case PrintStatus =>
              context.log.info(
                "Remaining {}. Latest demand {}. Messages sent {}. Expecting demand from {}",
                remaining,
                latestDemand,
                messagesSentToEachEntity,
                (remaining % testSettings.numberOfConsumers))
              Behaviors.same
          }
        }
      }
    }

  }

  final case class TestSettings(testName: String, totalMessages: Long, numberOfConsumers: Int)

}

class DeliveryThroughputSpecMultiJvmNode1 extends DeliveryThroughputSpec
class DeliveryThroughputSpecMultiJvmNode2 extends DeliveryThroughputSpec
class DeliveryThroughputSpecMultiJvmNode3 extends DeliveryThroughputSpec

abstract class DeliveryThroughputSpec
    extends MultiNodeSpec(DeliveryThroughputSpec)
    with MultiNodeTypedClusterSpec
    with PerfFlamesSupport {

  import DeliveryThroughputSpec._

  private val totalMessagesFactor =
    system.settings.config.getDouble("akka.test.DeliveryThroughputSpec.totalMessagesFactor")

  private var plot = PlotResult()

  private def adjustedTotalMessages(n: Long): Long = (n * totalMessagesFactor).toLong

  override def initialParticipants = roles.size

  override def afterAll(): Unit = {
    reporterExecutor.shutdown()
    runOn(first) {
      println(plot.csv(system.name))
    }
    super.afterAll()
  }

  private val settingsToReport = List(
    "akka.test.DeliveryThroughputSpec.totalMessagesFactor",
    "akka.reliable-delivery.consumer-controller.flow-control-window")
  private val resultReporter = BenchmarkFileReporter("DeliveryThroughputSpec", system, settingsToReport)

  def testPointToPoint(testSettings: TestSettings): Unit = {
    import testSettings._

    runPerfFlames(first, second)(delay = 5.seconds)

    runOn(second) {
      val consumerController = spawn(ConsumerController[Consumer.Command](), s"consumerController-$testName")
      val consumer = spawn(Consumer(consumerController), s"consumer-$testName")
      enterBarrier(testName + "-consumer-started")
      enterBarrier(testName + "-done")
      consumer ! Consumer.Stop
    }

    runOn(first) {
      enterBarrier(testName + "-consumer-started")
      val consumerController = identify(s"consumerController-$testName", second)
      val plotProbe = TestProbe[PlotResult]()
      val producerController = spawn(
        ProducerController[Consumer.Command](testName, durableQueueBehavior = None),
        s"producerController-$testName")
      val producer =
        spawn(Producer(producerController, testSettings, plotProbe.ref, resultReporter), s"producer-$testName")
      producerController ! ProducerController.RegisterConsumer(consumerController)
      producer ! Producer.Run
      val terminationProbe = TestProbe()
      terminationProbe.expectTerminated(producer, 1.minute)
      val plotResult = plotProbe.receiveMessage()
      plot = plot.addAll(plotResult)
      enterBarrier(testName + "-done")
    }

    runOn(third) {
      enterBarrier(testName + "-consumer-started")
      enterBarrier(testName + "-done")
    }

    enterBarrier("after-" + testName)
  }

  def testWorkPulling(testSettings: TestSettings): Unit = {
    import testSettings._

    runPerfFlames(first, second, third)(delay = 5.seconds)

    runOn(second, third) {
      val range = if (myself == second) (1 to numberOfConsumers by 2) else (2 to numberOfConsumers by 2)
      val consumers = range.map { n =>
        val consumerController =
          spawn(ConsumerController[Consumer.Command](serviceKey(testName)), s"consumerController-$n-$testName")
        spawn(Consumer(consumerController), s"consumer-$n-$testName")
      }
      enterBarrier(testName + "-consumer-started")
      enterBarrier(testName + "-done")
      consumers.foreach(_ ! Consumer.Stop)
    }

    runOn(first) {
      enterBarrier(testName + "-consumer-started")
      val plotProbe = TestProbe[PlotResult]()
      val producerController = spawn(
        WorkPullingProducerController[Consumer.Command](testName, serviceKey(testName), durableQueueBehavior = None),
        s"producerController-$testName")
      val producer =
        spawn(
          WorkPullingProducer(producerController, testSettings, plotProbe.ref, resultReporter),
          s"producer-$testName")
      producer ! WorkPullingProducer.Run
      val terminationProbe = TestProbe()
      terminationProbe.expectTerminated(producer, 1.minute)
      val plotResult = plotProbe.receiveMessage()
      plot = plot.addAll(plotResult)
      enterBarrier(testName + "-done")
    }

    enterBarrier("after-" + testName)
  }

  def testSharding(testSettings: TestSettings): Unit = {
    import testSettings._

    runPerfFlames(first, second, third)(delay = 5.seconds)

    val region = ClusterSharding(typedSystem).init(Entity(typeKey(testName))(_ =>
      ShardingConsumerController(consumerController => Consumer(consumerController))).withRole("worker"))
    enterBarrier(testName + "-sharding-init")

    runOn(first) {
      val plotProbe = TestProbe[PlotResult]()
      val producerController = spawn(
        ShardingProducerController[Consumer.Command](testName, region, durableQueueBehavior = None),
        s"producerController-$testName")
      val producer =
        spawn(ShardingProducer(producerController, testSettings, plotProbe.ref, resultReporter), s"producer-$testName")
      producer ! ShardingProducer.Run
      val terminationProbe = TestProbe()
      terminationProbe.expectTerminated(producer, 1.minute)
      val plotResult = plotProbe.receiveMessage()
      plot = plot.addAll(plotResult)
    }

    enterBarrier("after-" + testName)
  }

  "Reliable delivery throughput" must {

    "form cluster" in {
      formCluster(first, second, third)
    }

    "warmup point-to-point" in {
      val testSettings = TestSettings("warmup-point-to-point", adjustedTotalMessages(20000), 1)
      testPointToPoint(testSettings)
    }

    "be measured for point-to-point" in {
      val testSettings = TestSettings("1-to-1", adjustedTotalMessages(50000), 1)
      testPointToPoint(testSettings)
    }

    "warmup work-pulling" in {
      val testSettings = TestSettings("warmup-work-pulling", adjustedTotalMessages(10000), 2)
      testWorkPulling(testSettings)
    }

    "be measured for work-pulling with 1 worker" in {
      val testSettings = TestSettings("work-pulling-1", adjustedTotalMessages(20000), 1)
      testWorkPulling(testSettings)
    }

    "be measured for work-pulling with 2 workers" in {
      val testSettings = TestSettings("work-pulling-2", adjustedTotalMessages(30000), 2)
      testWorkPulling(testSettings)
    }

    "be measured for work-pulling with 4 workers" in {
      val testSettings = TestSettings("work-pulling-4", adjustedTotalMessages(40000), 4)
      testWorkPulling(testSettings)
    }

    "be measured for work-pulling with 10 workers" in {
      val testSettings = TestSettings("work-pulling-20", adjustedTotalMessages(40000), 10)
      testWorkPulling(testSettings)
    }

    "warmup sharding" in {
      val testSettings = TestSettings("warmup-sharding", adjustedTotalMessages(10000), 2)
      testSharding(testSettings)
    }

    "be measured for sharding with 1 entity" in {
      val testSettings = TestSettings("sharding-1", adjustedTotalMessages(20000), 1)
      testSharding(testSettings)
    }

    "be measured for sharding with 2 entities" in {
      val testSettings = TestSettings("sharding-2", adjustedTotalMessages(20000), 2)
      testSharding(testSettings)
    }

    "be measured for sharding with 4 entities" in {
      val testSettings = TestSettings("sharding-4", adjustedTotalMessages(20000), 4)
      testSharding(testSettings)
    }

    "be measured for sharding with 10 entities" in {
      val testSettings = TestSettings("sharding-10", adjustedTotalMessages(20000), 10)
      testSharding(testSettings)
    }

  }
}
