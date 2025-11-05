/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit

object ReliableDeliverySpec {
  val config: Config = ConfigFactory.parseString("""
    akka.reliable-delivery.consumer-controller.flow-control-window = 20
    """)
}

class ReliableDeliverySpec(config: Config)
    extends ScalaTestWithActorTestKit(config)
    with AnyWordSpecLike
    with LogCapturing {
  import TestConsumer.defaultConsumerDelay
  import TestProducer.defaultProducerDelay

  def this() = this(ReliableDeliverySpec.config)

  private val chunked = ProducerController.Settings(system).chunkLargeMessagesBytes > 0

  private var idCount = 0
  private def nextId(): Int = {
    idCount += 1
    idCount
  }

  "ReliableDelivery" must {

    "illustrate point-to-point usage" in {
      nextId()
      val consumerEndProbe = createTestProbe[TestConsumer.Collected]()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
      spawn(
        TestConsumer(defaultConsumerDelay, 42, consumerEndProbe.ref, consumerController),
        name = s"destination-${idCount}")

      val producerController =
        spawn(ProducerController[TestConsumer.Job](s"p-${idCount}", None), s"producerController-${idCount}")
      val producer = spawn(TestProducer(defaultProducerDelay, producerController), name = s"producer-${idCount}")

      consumerController ! ConsumerController.RegisterToProducerController(producerController)

      consumerEndProbe.receiveMessage(5.seconds)

      testKit.stop(producer)
      testKit.stop(producerController)
      testKit.stop(consumerController)
    }

    "illustrate point-to-point usage with ask" in {
      nextId()
      val consumerEndProbe = createTestProbe[TestConsumer.Collected]()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
      spawn(
        TestConsumer(defaultConsumerDelay, 42, consumerEndProbe.ref, consumerController),
        name = s"destination-${idCount}")

      val replyProbe = createTestProbe[Long]()

      val producerController =
        spawn(ProducerController[TestConsumer.Job](s"p-${idCount}", None), s"producerController-${idCount}")
      val producer =
        spawn(
          TestProducerWithAsk(defaultProducerDelay, replyProbe.ref, producerController),
          name = s"producer-${idCount}")

      consumerController ! ConsumerController.RegisterToProducerController(producerController)

      val messageCount = consumerEndProbe.receiveMessage(5.seconds).messageCount
      if (chunked)
        replyProbe.receiveMessages(messageCount, 5.seconds)
      else
        replyProbe.receiveMessages(messageCount, 5.seconds).toSet should ===((1L to 42).toSet)

      testKit.stop(producer)
      testKit.stop(producerController)
      testKit.stop(consumerController)
    }

    def testWithDelays(producerDelay: FiniteDuration, consumerDelay: FiniteDuration): Unit = {
      nextId()
      val consumerEndProbe = createTestProbe[TestConsumer.Collected]()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
      spawn(TestConsumer(consumerDelay, 42, consumerEndProbe.ref, consumerController), name = s"destination-${idCount}")

      val producerController =
        spawn(ProducerController[TestConsumer.Job](s"p-${idCount}", None), s"producerController-${idCount}")
      val producer = spawn(TestProducer(producerDelay, producerController), name = s"producer-${idCount}")

      consumerController ! ConsumerController.RegisterToProducerController(producerController)

      consumerEndProbe.receiveMessage(5.seconds)

      testKit.stop(producer)
      testKit.stop(producerController)
      testKit.stop(consumerController)
    }

    "work with slow producer and fast consumer" in {
      testWithDelays(producerDelay = 30.millis, consumerDelay = Duration.Zero)
    }

    "work with fast producer and slow consumer" in {
      testWithDelays(producerDelay = Duration.Zero, consumerDelay = 30.millis)
    }

    "work with fast producer and fast consumer" in {
      testWithDelays(producerDelay = Duration.Zero, consumerDelay = Duration.Zero)
    }

    "allow replacement of destination" in {
      nextId()
      val consumerEndProbe = createTestProbe[TestConsumer.Collected]()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController1-${idCount}")
      spawn(TestConsumer(defaultConsumerDelay, 42, consumerEndProbe.ref, consumerController), s"consumer1-${idCount}")

      val producerController =
        spawn(ProducerController[TestConsumer.Job](s"p-${idCount}", None), s"producerController-${idCount}")
      val producer = spawn(TestProducer(defaultProducerDelay, producerController), name = s"producer-${idCount}")

      consumerController ! ConsumerController.RegisterToProducerController(producerController)

      consumerEndProbe.receiveMessage(5.seconds)
      consumerEndProbe.expectTerminated(consumerController)

      val consumerEndProbe2 = createTestProbe[TestConsumer.Collected]()
      val consumerController2 =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController2-${idCount}")
      spawn(TestConsumer(defaultConsumerDelay, 42, consumerEndProbe2.ref, consumerController2), s"consumer2-${idCount}")
      consumerController2 ! ConsumerController.RegisterToProducerController(producerController)

      consumerEndProbe2.receiveMessage(5.seconds)

      testKit.stop(producer)
      testKit.stop(producerController)
      testKit.stop(consumerController2)
    }

    "allow replacement of producer" in {
      nextId()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      val producerController1 =
        spawn(ProducerController[TestConsumer.Job](s"p-${idCount}", None), s"producerController1-${idCount}")
      val producerProbe1 = createTestProbe[ProducerController.RequestNext[TestConsumer.Job]]()
      producerController1 ! ProducerController.Start(producerProbe1.ref)

      producerController1 ! ProducerController.RegisterConsumer(consumerController)

      producerProbe1.receiveMessage().sendNextTo ! TestConsumer.Job("msg-1")
      val delivery1 = consumerProbe.receiveMessage()
      delivery1.message should ===(TestConsumer.Job("msg-1"))
      delivery1.confirmTo ! ConsumerController.Confirmed

      producerProbe1.receiveMessage().sendNextTo ! TestConsumer.Job("msg-2")
      val delivery2 = consumerProbe.receiveMessage()
      delivery2.message should ===(TestConsumer.Job("msg-2"))
      delivery2.confirmTo ! ConsumerController.Confirmed

      // replace producer
      testKit.stop(producerController1)
      producerProbe1.expectTerminated(producerController1)
      val producerController2 =
        spawn(ProducerController[TestConsumer.Job](s"p-${idCount}", None), s"producerController2-${idCount}")
      val producerProbe2 = createTestProbe[ProducerController.RequestNext[TestConsumer.Job]]()
      producerController2 ! ProducerController.Start(producerProbe2.ref)
      producerController2 ! ProducerController.RegisterConsumer(consumerController)

      producerProbe2.receiveMessage().sendNextTo ! TestConsumer.Job("msg-3")
      val delivery3 = consumerProbe.receiveMessage()
      delivery3.message should ===(TestConsumer.Job("msg-3"))
      delivery3.confirmTo ! ConsumerController.Confirmed

      producerProbe2.receiveMessage().sendNextTo ! TestConsumer.Job("msg-4")
      val delivery4 = consumerProbe.receiveMessage()
      delivery4.message should ===(TestConsumer.Job("msg-4"))
      delivery4.confirmTo ! ConsumerController.Confirmed

      testKit.stop(producerController2)
      testKit.stop(consumerController)
    }

  }

}

// Same tests but with chunked messages
class ReliableDeliveryChunkedSpec
    extends ReliableDeliverySpec(
      ConfigFactory.parseString("""
    akka.reliable-delivery.producer-controller.chunk-large-messages = 1b
    """).withFallback(TestSerializer.config).withFallback(ReliableDeliverySpec.config))
