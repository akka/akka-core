/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.delivery.ConsumerController.DeliverThenStop
import akka.actor.typed.delivery.internal.ConsumerControllerImpl
import akka.actor.typed.delivery.internal.ProducerControllerImpl
import akka.serialization.SerializationExtension

class ConsumerControllerSpec
    extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
  akka.reliable-delivery.consumer-controller {
    flow-control-window = 20
    resend-interval-min = 1s
  }
  """).withFallback(TestSerializer.config))
    with AnyWordSpecLike
    with LogCapturing {
  import TestConsumer.sequencedMessage

  private var idCount = 0
  private def nextId(): Int = {
    idCount += 1
    idCount
  }

  private def producerId: String = s"p-$idCount"

  private val settings = ConsumerController.Settings(system)
  import settings.flowControlWindow

  private val serialization = SerializationExtension(system)

  "ConsumerController" must {
    "resend RegisterConsumer" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      consumerController ! ConsumerController.RegisterToProducerController(producerControllerProbe.ref)
      producerControllerProbe.expectMessage(ProducerController.RegisterConsumer(consumerController))
      // expected resend
      producerControllerProbe.expectMessage(ProducerController.RegisterConsumer(consumerController))

      testKit.stop(consumerController)
    }

    "resend RegisterConsumer when changed to different ProducerController" in {
      nextId()
      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
      val producerControllerProbe1 = createTestProbe[ProducerControllerImpl.InternalCommand]()

      consumerController ! ConsumerController.Start(consumerProbe.ref)
      consumerController ! ConsumerController.RegisterToProducerController(producerControllerProbe1.ref)
      producerControllerProbe1.expectMessage(ProducerController.RegisterConsumer(consumerController))
      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe1.ref)

      // change producer
      val producerControllerProbe2 = createTestProbe[ProducerControllerImpl.InternalCommand]()
      consumerController ! ConsumerController.RegisterToProducerController(producerControllerProbe2.ref)
      producerControllerProbe2.expectMessage(ProducerController.RegisterConsumer(consumerController))
      consumerProbe.receiveMessage().confirmTo ! ConsumerController.Confirmed
      // expected resend
      producerControllerProbe2.expectMessage(ProducerController.RegisterConsumer(consumerController))

      testKit.stop(consumerController)
    }

    "resend initial Request" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 20, true, false))
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(1, 20, true, false))

      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(1, 20, true, true))

      testKit.stop(consumerController)
    }

    "send Request after half window size" in {
      nextId()
      val windowSize = 20
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      (1 until windowSize / 2).foreach { n =>
        consumerController ! sequencedMessage(producerId, n, producerControllerProbe.ref)
      }

      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, windowSize, true, false))
      (1 until windowSize / 2).foreach { n =>
        consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
        consumerController ! ConsumerController.Confirmed
        if (n == 1)
          producerControllerProbe.expectMessage(ProducerControllerImpl.Request(1, windowSize, true, false))
      }

      producerControllerProbe.expectNoMessage()

      consumerController ! sequencedMessage(producerId, windowSize / 2, producerControllerProbe.ref)

      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      producerControllerProbe.expectNoMessage()
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(
        ProducerControllerImpl.Request(windowSize / 2, windowSize + windowSize / 2, true, false))

      testKit.stop(consumerController)
    }

    "detect lost message" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed

      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 20, true, false))
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(1, 20, true, false))

      consumerController ! sequencedMessage(producerId, 2, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed

      consumerController ! sequencedMessage(producerId, 5, producerControllerProbe.ref)
      producerControllerProbe.expectMessage(ProducerControllerImpl.Resend(3))

      consumerController ! sequencedMessage(producerId, 3, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 4, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 5, producerControllerProbe.ref)

      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(3)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(4)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(5)
      consumerController ! ConsumerController.Confirmed

      testKit.stop(consumerController)
    }

    "resend Request" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed

      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 20, true, false))
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(1, 20, true, false))

      consumerController ! sequencedMessage(producerId, 2, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(2, 20, true, true))

      consumerController ! sequencedMessage(producerId, 3, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(3)
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(3, 20, true, true))

      // exponential back, so now it should be more than 1 sec
      producerControllerProbe.expectNoMessage(1.1.second)
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(3, 20, true, true))

      testKit.stop(consumerController)
    }

    "stash while waiting for consumer confirmation" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed

      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 20, true, false))
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(1, 20, true, false))

      consumerController ! sequencedMessage(producerId, 2, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! sequencedMessage(producerId, 3, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 4, producerControllerProbe.ref)
      consumerProbe.expectNoMessage()

      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(3)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(4)
      consumerController ! ConsumerController.Confirmed

      consumerController ! sequencedMessage(producerId, 5, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 6, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 7, producerControllerProbe.ref)

      // ProducerController may resend unconfirmed
      consumerController ! sequencedMessage(producerId, 5, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 6, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 7, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 8, producerControllerProbe.ref)

      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(5)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(6)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(7)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(8)
      consumerController ! ConsumerController.Confirmed

      consumerProbe.expectNoMessage()

      testKit.stop(consumerController)
    }

    "optionally ack messages" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref, ack = true)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed

      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 20, true, false))
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(1, 20, true, false))

      consumerController ! sequencedMessage(producerId, 2, producerControllerProbe.ref, ack = true)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Ack(2))

      consumerController ! sequencedMessage(producerId, 3, producerControllerProbe.ref, ack = true)
      consumerController ! sequencedMessage(producerId, 4, producerControllerProbe.ref, ack = false)
      consumerController ! sequencedMessage(producerId, 5, producerControllerProbe.ref, ack = true)

      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(3)
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Ack(3))
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(4)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(5)
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Ack(5))

      testKit.stop(consumerController)
    }

    "allow restart of consumer" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe1 = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe1.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      consumerProbe1.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed

      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 20, true, false))
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(1, 20, true, false))

      consumerController ! sequencedMessage(producerId, 2, producerControllerProbe.ref)
      consumerProbe1.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed

      consumerController ! sequencedMessage(producerId, 3, producerControllerProbe.ref)
      consumerProbe1.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(3)

      // restart consumer, before Confirmed(3)
      val consumerProbe2 = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe2.ref)

      consumerProbe2.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(3)
      consumerController ! ConsumerController.Confirmed

      consumerController ! sequencedMessage(producerId, 4, producerControllerProbe.ref)
      consumerProbe2.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(4)
      consumerController ! ConsumerController.Confirmed

      testKit.stop(consumerController)
    }

    "stop ConsumerController when consumer is stopped" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]

      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe1 = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe1.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      consumerProbe1.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed

      consumerProbe1.stop()
      createTestProbe().expectTerminated(consumerController)
    }

    "stop ConsumerController when consumer is stopped before first message" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]

      val consumerProbe1 = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe1.ref)

      consumerProbe1.stop()
      createTestProbe().expectTerminated(consumerController)
    }

    "deduplicate resend of first message" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 20, true, false))
      // that Request will typically cancel the resending of first, but in unlucky timing it may happen
      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      consumerProbe.receiveMessage().confirmTo ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(1, 20, true, false))
      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      // deduplicated, not delivered again
      consumerProbe.expectNoMessage()

      // but if the ProducerController is changed it will not be deduplicated
      val producerControllerProbe2 = createTestProbe[ProducerControllerImpl.InternalCommand]()
      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe2.ref)
      producerControllerProbe2.expectMessage(ProducerControllerImpl.Request(0, 20, true, false))
      consumerProbe.receiveMessage().confirmTo ! ConsumerController.Confirmed
      producerControllerProbe2.expectMessage(ProducerControllerImpl.Request(1, 20, true, false))

      testKit.stop(consumerController)
    }

    "request window after first" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, flowControlWindow, true, false))
      consumerProbe.receiveMessage().confirmTo ! ConsumerController.Confirmed

      // and if the ProducerController is changed
      val producerControllerProbe2 = createTestProbe[ProducerControllerImpl.InternalCommand]()
      consumerController ! sequencedMessage(producerId, 23, producerControllerProbe2.ref).asFirst
      producerControllerProbe2.expectMessage(ProducerControllerImpl.Request(0, 23 + flowControlWindow - 1, true, false))
      consumerProbe.receiveMessage().confirmTo ! ConsumerController.Confirmed

      val producerControllerProbe3 = createTestProbe[ProducerControllerImpl.InternalCommand]()
      consumerController ! sequencedMessage(producerId, 7, producerControllerProbe3.ref).asFirst
      producerControllerProbe3.expectMessage(ProducerControllerImpl.Request(0, 7 + flowControlWindow - 1, true, false))
      consumerProbe.receiveMessage().confirmTo ! ConsumerController.Confirmed

      testKit.stop(consumerController)
    }

    "handle first message when waiting for lost (resending)" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()

      // while waiting for Start the SequencedMessage will be stashed
      consumerController ! sequencedMessage(producerId, 44, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 41, producerControllerProbe.ref).asFirst
      consumerController ! sequencedMessage(producerId, 45, producerControllerProbe.ref)

      consumerController ! ConsumerController.Start(consumerProbe.ref)
      // unstashed 44, 41, 45
      // 44 is not first so will trigger a full Resend, and also clears stashed messages
      producerControllerProbe.expectMessage(ProducerControllerImpl.Resend(0))
      consumerController ! sequencedMessage(producerId, 41, producerControllerProbe.ref).asFirst
      consumerController ! sequencedMessage(producerId, 42, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 43, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 44, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 45, producerControllerProbe.ref)

      // and 41 is first, which will trigger the initial Request
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 60, true, false))

      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(41)
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(41, 60, true, false))

      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(42)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(43)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(44)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(45)
      consumerController ! ConsumerController.Confirmed

      consumerController ! sequencedMessage(producerId, 46, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(46)
      consumerController ! ConsumerController.Confirmed

      testKit.stop(consumerController)
    }

    "send Ack when stopped" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]

      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe1 = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe1.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      consumerProbe1.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      producerControllerProbe.expectMessageType[ProducerControllerImpl.Request]
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessageType[ProducerControllerImpl.Request]

      consumerController ! sequencedMessage(producerId, 2, producerControllerProbe.ref)
      consumerProbe1.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed

      consumerController ! sequencedMessage(producerId, 3, producerControllerProbe.ref)
      consumerProbe1.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]

      // now we know that the ConsumerController has received Confirmed for 2,
      // and 3 is still not confirmed
      testKit.stop(consumerController)
      producerControllerProbe.expectMessage(ProducerControllerImpl.Ack(2L))
    }

    "support graceful stopping" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]

      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      producerControllerProbe.expectMessageType[ProducerControllerImpl.Request]
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessageType[ProducerControllerImpl.Request]

      consumerController ! sequencedMessage(producerId, 2, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].message should ===(
        TestConsumer.Job("msg-2"))
      consumerController ! sequencedMessage(producerId, 3, producerControllerProbe.ref)
      consumerController ! sequencedMessage(producerId, 4, producerControllerProbe.ref)

      consumerController ! DeliverThenStop()

      consumerController ! ConsumerController.Confirmed // 2
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].message should ===(
        TestConsumer.Job("msg-3"))
      consumerController ! sequencedMessage(producerId, 5, producerControllerProbe.ref)
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].message should ===(
        TestConsumer.Job("msg-4"))
      consumerController ! ConsumerController.Confirmed
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].message should ===(
        TestConsumer.Job("msg-5"))
      consumerController ! ConsumerController.Confirmed

      consumerProbe.expectTerminated(consumerController)

      testKit.stop(consumerController)
      // one Ack from postStop, and another from Behaviors.stopped callback after final Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Ack(4L))
      producerControllerProbe.expectMessage(ProducerControllerImpl.Ack(5L))
    }
  }

  "ConsumerController with chunked messages" must {

    "collect and assemble chunks" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      // one chunk for each letter, "123" => 3 chunks
      val chunks1 = ProducerControllerImpl.createChunks(TestConsumer.Job(s"123"), chunkSize = 1, serialization)
      val seqMessages1 = chunks1.zipWithIndex.map {
        case (chunk, i) =>
          ConsumerController.SequencedMessage.fromChunked(
            producerId,
            1 + i,
            chunk,
            first = i == 0,
            ack = false,
            producerControllerProbe.ref)
      }

      consumerController ! seqMessages1.head
      consumerProbe.expectNoMessage() // need all chunks before delivery
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 20, true, false))
      consumerController ! seqMessages1(1)
      consumerController ! seqMessages1(2)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].message.payload should ===("123")
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(3, 22, true, false))

      val chunks2 = ProducerControllerImpl.createChunks(TestConsumer.Job(s"45"), chunkSize = 1, serialization)
      val seqMessages2 = chunks2.zipWithIndex.map {
        case (chunk, i) =>
          ConsumerController.SequencedMessage.fromChunked(
            producerId,
            4 + i,
            chunk,
            first = false,
            ack = true,
            producerControllerProbe.ref)
      }

      consumerController ! seqMessages2.head
      consumerController ! seqMessages2(1)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].message.payload should ===("45")
      consumerController ! ConsumerController.Confirmed
      producerControllerProbe.expectMessage(ProducerControllerImpl.Ack(5))

      testKit.stop(consumerController)
    }

    "send Request after half window size when many chunks" in {
      nextId()
      val consumerController =
        spawn(ConsumerController[TestConsumer.Job](), s"consumerController-${idCount}")
          .unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      // one chunk for each letter, => 25 chunks
      val chunks1 =
        ProducerControllerImpl.createChunks(
          TestConsumer.Job(s"1234567890123456789012345"),
          chunkSize = 1,
          serialization)
      val seqMessages1 = chunks1.zipWithIndex.map {
        case (chunk, i) =>
          ConsumerController.SequencedMessage.fromChunked(
            producerId,
            1 + i,
            chunk,
            first = i == 0,
            ack = false,
            producerControllerProbe.ref)
      }

      consumerController ! seqMessages1.head
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 20, true, false))
      producerControllerProbe.expectNoMessage() // no more Request yet
      (1 to 8).foreach(i => consumerController ! seqMessages1(i))
      producerControllerProbe.expectNoMessage() // sent 9, no more Request yet

      consumerController ! seqMessages1(9)
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 30, true, false))

      (10 to 18).foreach(i => consumerController ! seqMessages1(i))
      producerControllerProbe.expectNoMessage() // sent 19, no more Request yet

      consumerController ! seqMessages1(19)
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 40, true, false))

      // not sending more for a while, timeout will trigger new Request
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 40, true, true))

      (20 to 24).foreach(i => consumerController ! seqMessages1(i))
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].message.payload should ===(
        "1234567890123456789012345")
      consumerController ! ConsumerController.Confirmed

      testKit.stop(consumerController)
    }
  }

  "ConsumerController without resending" must {
    "accept lost message" in {
      nextId()
      val consumerController =
        spawn(
          ConsumerController[TestConsumer.Job](ConsumerController.Settings(system).withOnlyFlowControl(true)),
          s"consumerController-${idCount}").unsafeUpcast[ConsumerControllerImpl.InternalCommand]
      val producerControllerProbe = createTestProbe[ProducerControllerImpl.InternalCommand]()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      consumerController ! sequencedMessage(producerId, 1, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]]
      consumerController ! ConsumerController.Confirmed

      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(0, 20, supportResend = false, false))
      producerControllerProbe.expectMessage(ProducerControllerImpl.Request(1, 20, supportResend = false, false))

      // skipping 2
      consumerController ! sequencedMessage(producerId, 3, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(3)
      consumerController ! ConsumerController.Confirmed
      consumerController ! sequencedMessage(producerId, 4, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(4)
      consumerController ! ConsumerController.Confirmed

      // skip many
      consumerController ! sequencedMessage(producerId, 35, producerControllerProbe.ref)
      consumerProbe.expectMessageType[ConsumerController.Delivery[TestConsumer.Job]].seqNr should ===(35)
      consumerController ! ConsumerController.Confirmed

      testKit.stop(consumerController)
    }
  }

}
