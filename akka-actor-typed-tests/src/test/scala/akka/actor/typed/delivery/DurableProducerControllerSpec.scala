/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.delivery.DurableProducerQueue.MessageSent
import akka.actor.typed.delivery.ProducerController.MessageWithConfirmation
import akka.actor.typed.delivery.internal.ChunkedMessage
import akka.actor.typed.delivery.internal.ProducerControllerImpl
import akka.util.ByteString

class DurableProducerControllerSpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory.parseString("""
  akka.reliable-delivery.consumer-controller.flow-control-window = 20
  akka.reliable-delivery.consumer-controller.resend-interval-min = 1s
  """).withFallback(TestSerializer.config))
    with AnyWordSpecLike
    with LogCapturing {
  import DurableProducerQueue.NoQualifier
  import TestConsumer.sequencedMessage
  import TestDurableProducerQueue.TestTimestamp

  private var idCount = 0
  private def nextId(): Int = {
    idCount += 1
    idCount
  }

  private def producerId: String = s"p-$idCount"

  "ProducerController with durable queue" must {

    "load initial state and resend unconfirmed" in {
      nextId()
      val consumerControllerProbe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()

      val durable = TestDurableProducerQueue[TestConsumer.Job](
        Duration.Zero,
        DurableProducerQueue.State(
          currentSeqNr = 5,
          highestConfirmedSeqNr = 2,
          confirmedSeqNr = Map(NoQualifier -> (2L -> TestTimestamp)),
          unconfirmed = Vector(
            DurableProducerQueue.MessageSent(3, TestConsumer.Job("msg-3"), false, NoQualifier, TestTimestamp),
            DurableProducerQueue.MessageSent(4, TestConsumer.Job("msg-4"), false, NoQualifier, TestTimestamp))))

      val producerController =
        spawn(ProducerController[TestConsumer.Job](producerId, Some(durable)), s"producerController-${idCount}")
          .unsafeUpcast[ProducerControllerImpl.InternalCommand]
      val producerProbe = createTestProbe[ProducerController.RequestNext[TestConsumer.Job]]()
      producerController ! ProducerController.Start(producerProbe.ref)

      producerController ! ProducerController.RegisterConsumer(consumerControllerProbe.ref)

      // no request to producer since it has unconfirmed to begin with
      producerProbe.expectNoMessage()

      consumerControllerProbe.expectMessage(sequencedMessage(producerId, 3, producerController).asFirst)
      consumerControllerProbe.expectNoMessage(50.millis)
      producerController ! ProducerControllerImpl.Request(3L, 13L, true, false)
      consumerControllerProbe.expectMessage(sequencedMessage(producerId, 4, producerController))

      val sendTo = producerProbe.receiveMessage().sendNextTo
      sendTo ! TestConsumer.Job("msg-5")
      consumerControllerProbe.expectMessage(sequencedMessage(producerId, 5, producerController))

      testKit.stop(producerController)
    }

    "store confirmations" in {
      nextId()
      val consumerControllerProbe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()

      val stateHolder =
        new AtomicReference[DurableProducerQueue.State[TestConsumer.Job]](DurableProducerQueue.State.empty)
      val durable = TestDurableProducerQueue[TestConsumer.Job](
        Duration.Zero,
        stateHolder,
        (_: DurableProducerQueue.Command[_]) => false)

      val producerController =
        spawn(ProducerController[TestConsumer.Job](producerId, Some(durable)), s"producerController-${idCount}")
          .unsafeUpcast[ProducerControllerImpl.InternalCommand]
      val producerProbe = createTestProbe[ProducerController.RequestNext[TestConsumer.Job]]()
      producerController ! ProducerController.Start(producerProbe.ref)

      producerController ! ProducerController.RegisterConsumer(consumerControllerProbe.ref)

      producerProbe.receiveMessage().sendNextTo ! TestConsumer.Job("msg-1")
      consumerControllerProbe.expectMessage(sequencedMessage(producerId, 1, producerController))
      producerProbe.awaitAssert {
        stateHolder.get() should ===(
          DurableProducerQueue.State(
            2,
            0,
            Map.empty,
            Vector(MessageSent(1, TestConsumer.Job("msg-1"), ack = false, NoQualifier, TestTimestamp))))
      }
      producerController ! ProducerControllerImpl.Request(1L, 10L, true, false)
      producerProbe.awaitAssert {
        stateHolder.get() should ===(
          DurableProducerQueue.State(2, 1, Map(NoQualifier -> (1L -> TestTimestamp)), Vector.empty))
      }

      val replyTo = createTestProbe[Long]()
      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-2"), replyTo.ref)
      consumerControllerProbe.expectMessage(sequencedMessage(producerId, 2, producerController, ack = true))
      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-3"), replyTo.ref)
      consumerControllerProbe.expectMessage(sequencedMessage(producerId, 3, producerController, ack = true))
      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-4"), replyTo.ref)
      consumerControllerProbe.expectMessage(sequencedMessage(producerId, 4, producerController, ack = true))
      producerController ! ProducerControllerImpl.Ack(3)
      producerProbe.awaitAssert {
        stateHolder.get() should ===(
          DurableProducerQueue.State(
            5,
            3,
            Map(NoQualifier -> (3L -> TestTimestamp)),
            Vector(MessageSent(4, TestConsumer.Job("msg-4"), ack = true, NoQualifier, TestTimestamp))))
      }

      testKit.stop(producerController)
    }

    "reply to MessageWithConfirmation after storage" in {
      nextId()
      val consumerControllerProbe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()

      val durable =
        TestDurableProducerQueue[TestConsumer.Job](Duration.Zero, DurableProducerQueue.State.empty[TestConsumer.Job])

      val producerController =
        spawn(ProducerController[TestConsumer.Job](producerId, Some(durable)), s"producerController-${idCount}")
          .unsafeUpcast[ProducerControllerImpl.InternalCommand]
      val producerProbe = createTestProbe[ProducerController.RequestNext[TestConsumer.Job]]()
      producerController ! ProducerController.Start(producerProbe.ref)

      producerController ! ProducerController.RegisterConsumer(consumerControllerProbe.ref)

      val replyTo = createTestProbe[Long]()

      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-1"), replyTo.ref)
      replyTo.expectMessage(1L)

      consumerControllerProbe.expectMessage(sequencedMessage(producerId, 1, producerController, ack = true))
      producerController ! ProducerControllerImpl.Request(1L, 10L, true, false)

      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-2"), replyTo.ref)
      replyTo.expectMessage(2L)

      testKit.stop(producerController)
    }

    "store chunked messages" in {
      nextId()
      val consumerControllerProbe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()

      val stateHolder =
        new AtomicReference[DurableProducerQueue.State[TestConsumer.Job]](DurableProducerQueue.State.empty)
      val durable =
        TestDurableProducerQueue[TestConsumer.Job](
          Duration.Zero,
          stateHolder,
          (_: DurableProducerQueue.Command[_]) => false)

      val producerController =
        spawn(
          ProducerController[TestConsumer.Job](
            producerId,
            Some(durable),
            ProducerController.Settings(system).withChunkLargeMessagesBytes(1)),
          s"producerController-${idCount}").unsafeUpcast[ProducerControllerImpl.InternalCommand]
      val producerProbe = createTestProbe[ProducerController.RequestNext[TestConsumer.Job]]()
      producerController ! ProducerController.Start(producerProbe.ref)

      producerController ! ProducerController.RegisterConsumer(consumerControllerProbe.ref)

      producerProbe.receiveMessage().sendNextTo ! TestConsumer.Job("abc")
      consumerControllerProbe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]

      producerProbe.awaitAssert {
        val durableState = stateHolder.get()
        durableState.currentSeqNr should ===(2)
        durableState.unconfirmed.size should ===(1)
        durableState.unconfirmed.head.message.getClass should ===(classOf[ChunkedMessage])
      }

      producerController ! ProducerControllerImpl.Request(0L, 10L, true, false)

      consumerControllerProbe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]

      val seqMsg3 = consumerControllerProbe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
      seqMsg3.isFirstChunk should ===(false)
      seqMsg3.isLastChunk should ===(true)
      seqMsg3.seqNr should ===(3L)

      producerProbe.awaitAssert {
        val durableState = stateHolder.get()
        durableState.currentSeqNr should ===(4)
        durableState.unconfirmed.size should ===(3)
        durableState.unconfirmed.head.message.getClass should ===(classOf[ChunkedMessage])
      }

      testKit.stop(producerController)
    }

    "load initial state but don't resend partially stored chunked messages" in {
      // may happen if crashed before all chunked messages have been stored,
      // should be treated as if none of them were stored (they were not confirmed)
      nextId()
      val consumerControllerProbe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()

      val durable = TestDurableProducerQueue[TestConsumer.Job](
        Duration.Zero,
        DurableProducerQueue.State(
          currentSeqNr = 5,
          highestConfirmedSeqNr = 2,
          confirmedSeqNr = Map(NoQualifier -> (2L -> TestTimestamp)),
          unconfirmed = Vector(
            DurableProducerQueue.MessageSent.fromChunked[TestConsumer.Job](
              3,
              ChunkedMessage(ByteString.fromString("abc"), true, true, 20, ""),
              false,
              NoQualifier,
              TestTimestamp),
            DurableProducerQueue.MessageSent.fromChunked[TestConsumer.Job](
              4,
              ChunkedMessage(ByteString.fromString("d"), true, false, 20, ""),
              false,
              NoQualifier,
              TestTimestamp),
            DurableProducerQueue.MessageSent.fromChunked[TestConsumer.Job](
              5,
              ChunkedMessage(ByteString.fromString("e"), false, false, 20, ""),
              false,
              NoQualifier,
              TestTimestamp)
            // missing last chunk
          )))

      val producerController =
        spawn(
          ProducerController[TestConsumer.Job](
            producerId,
            Some(durable),
            ProducerController.Settings(system).withChunkLargeMessagesBytes(1)),
          s"producerController-${idCount}").unsafeUpcast[ProducerControllerImpl.InternalCommand]
      val producerProbe = createTestProbe[ProducerController.RequestNext[TestConsumer.Job]]()
      producerController ! ProducerController.Start(producerProbe.ref)

      producerController ! ProducerController.RegisterConsumer(consumerControllerProbe.ref)

      val seqMsg3 = consumerControllerProbe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
      seqMsg3.seqNr should ===(3)
      seqMsg3.isFirstChunk should ===(true)
      seqMsg3.isLastChunk should ===(true)

      producerController ! ProducerControllerImpl.Request(0L, 10L, true, false)

      // 4 and 5 discarded because missing last chunk
      consumerControllerProbe.expectNoMessage()

      producerProbe.receiveMessage().sendNextTo ! TestConsumer.Job("g")
      val seqMsg4 = consumerControllerProbe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
      seqMsg4.seqNr should ===(4)
      seqMsg4.isFirstChunk should ===(true)
      seqMsg4.isLastChunk should ===(true)

      testKit.stop(producerController)
    }
  }

}
