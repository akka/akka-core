/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import scala.concurrent.duration._
import scala.util.Random

import com.typesafe.config.ConfigFactory
import org.HdrHistogram.Histogram

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.ProducerController
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.MultiNodeClusterSpec
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.serialization.jackson.CborSerializable

object ChunkLargeMessageSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(ConfigFactory.parseString("""
        akka.loglevel = INFO
        #akka.serialization.jackson.verbose-debug-logging = on
        akka.remote.artery {
          advanced.inbound-lanes = 1
          advanced.maximum-frame-size = 2 MB
        }
      """).withFallback(MultiNodeClusterSpec.clusterConfig))

  object Producer {
    sealed trait Command
    case object Stop extends Command
    final case class Reply(timestamp: Long) extends Command with CborSerializable

    private case class WrappedRequestNext(r: ProducerController.RequestNext[Consumer.TheMessage]) extends Command
    private case class SendNext(to: ActorRef[Consumer.TheMessage]) extends Command

    def apply(
        numberOfMessages: Int,
        large: Boolean,
        delay: FiniteDuration,
        producerController: ActorRef[ProducerController.Command[Consumer.TheMessage]]): Behavior[Command] = {

      Behaviors.setup { context =>
        val requestNextAdapter =
          context.messageAdapter[ProducerController.RequestNext[Consumer.TheMessage]](WrappedRequestNext(_))
        producerController ! ProducerController.Start(requestNextAdapter)

        val histogram = new Histogram(SECONDS.toNanos(10), 3)

        def percentile(p: Double): Double = histogram.getValueAtPercentile(p) / 1000.0

        val rnd = new Random
        val elements = if (large) Vector.fill(500)(rnd.nextString(1000)) else Vector("a")

        if (numberOfMessages == 0)
          Behaviors.stopped
        else
          Behaviors
            .receive[Command] { (context, msg) =>
              msg match {
                case WrappedRequestNext(next) =>
                  if (delay > Duration.Zero)
                    context.scheduleOnce(delay, context.self, SendNext(next.sendNextTo))
                  else
                    next.sendNextTo ! Consumer.TheMessage(System.nanoTime(), context.self, Vector("a"))
                  Behaviors.same
                case SendNext(to) =>
                  to ! Consumer.TheMessage(System.nanoTime(), context.self, elements)
                  Behaviors.same
                case Reply(timestamp) =>
                  histogram.recordValue(System.nanoTime() - timestamp)
                  if (histogram.getTotalCount == numberOfMessages)
                    Behaviors.stopped
                  else
                    Behaviors.same
                case Stop =>
                  Behaviors.stopped
              }
            }
            .receiveSignal {
              case (_, PostStop) =>
                if (histogram.getTotalCount > 0) {
                  context.log.info(
                    s"=== Latency for [${context.self.path.name}] " +
                    f"50%%ile: ${percentile(50.0)}%.0f µs, " +
                    f"90%%ile: ${percentile(90.0)}%.0f µs, " +
                    f"99%%ile: ${percentile(99.0)}%.0f µs")
                  println(s"Histogram for [${context.self.path.name}] of RTT latencies in microseconds.")
                  histogram.outputPercentileDistribution(System.out, 1000.0)
                }
                Behaviors.same
            }

      }
    }

  }

  object Consumer {

    sealed trait Command
    final case class TheMessage(sendTimstamp: Long, replyTo: ActorRef[Producer.Reply], elements: Vector[String])
        extends CborSerializable {
      override def toString: String = s"TheMessage($sendTimstamp,$replyTo,${elements.size})"
    }
    private final case class WrappedDelivery(d: ConsumerController.Delivery[TheMessage]) extends Command
    case object Stop extends Command

    def apply(consumerController: ActorRef[ConsumerController.Start[TheMessage]]): Behavior[Command] = {
      Behaviors.setup { context =>
        val deliveryAdapter =
          context.messageAdapter[ConsumerController.Delivery[TheMessage]](WrappedDelivery(_))
        consumerController ! ConsumerController.Start(deliveryAdapter)

        Behaviors.receiveMessage {
          case WrappedDelivery(d) =>
            d.message.replyTo ! Producer.Reply(d.message.sendTimstamp)
            d.confirmTo ! ConsumerController.Confirmed
            Behaviors.same
          case Stop =>
            Behaviors.stopped
        }
      }
    }
  }

}

class ChunkLargeMessageMultiJvmNode1 extends ChunkLargeMessageSpec
class ChunkLargeMessageMultiJvmNode2 extends ChunkLargeMessageSpec

abstract class ChunkLargeMessageSpec extends MultiNodeSpec(ChunkLargeMessageSpec) with MultiNodeTypedClusterSpec {
  import ChunkLargeMessageSpec._

  private def test(n: Int, numberOfMessages: Int, includeLarge: Boolean): Unit = {
    runOn(first) {
      val producerController = spawn(ProducerController[Consumer.TheMessage](s"p$n", None), s"producerController$n")
      val producer =
        spawn(Producer(numberOfMessages, large = false, delay = 10.millis, producerController), s"producer$n")
      val largeProducerController =
        spawn(
          ProducerController[Consumer.TheMessage](
            s"p$n",
            None,
            ProducerController.Settings(typedSystem).withChunkLargeMessagesBytes(50000)),
          s"largeProducerController$n")
      val largeProducer =
        spawn(
          Producer(if (includeLarge) Int.MaxValue else 0, large = true, delay = 10.millis, largeProducerController),
          s"largeProducer$n")
      enterBarrier(s"producer$n-started")
      val probe = TestProbe[Any]()
      probe.expectTerminated(producer, 25.seconds)
      largeProducer ! Producer.Stop
      enterBarrier(s"producer$n-stopped")
    }
    runOn(second) {
      enterBarrier(s"producer$n-started")
      val consumerController = spawn(ConsumerController[Consumer.TheMessage](), s"consumerController$n")
      val consumer = spawn(Consumer(consumerController), s"consumer$n")
      val largeConsumerController = spawn(ConsumerController[Consumer.TheMessage](), s"largeConsumerController$n")
      val largeConsumer = spawn(Consumer(largeConsumerController), s"largeConsumer$n")
      val producerController: ActorRef[ProducerController.Command[Consumer.TheMessage]] =
        identify(s"producerController$n", first)
      consumerController ! ConsumerController.RegisterToProducerController(producerController)
      val largeProducerController: ActorRef[ProducerController.Command[Consumer.TheMessage]] =
        identify(s"largeProducerController$n", first)
      largeConsumerController ! ConsumerController.RegisterToProducerController(largeProducerController)
      enterBarrier(s"producer$n-stopped")
      consumer ! Consumer.Stop
      largeConsumer ! Consumer.Stop
    }
    enterBarrier(s"after-$n")
  }

  "Reliable delivery with chunked messages" must {

    "form a cluster" in {
      formCluster(first, second)
      enterBarrier("cluster started")
    }

    "warmup" in {
      test(1, 100, includeLarge = true)
    }

    "measure latency without large messages" in {
      test(2, 250, includeLarge = false)
    }

    "measure latency with large messages" in {
      test(3, 250, includeLarge = true)
    }

  }

}
