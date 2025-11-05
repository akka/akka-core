/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.delivery.internal.ProducerControllerImpl
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey

class WorkPullingSpec
    extends ScalaTestWithActorTestKit("""
  akka.reliable-delivery.consumer-controller.flow-control-window = 20
  """)
    with AnyWordSpecLike
    with LogCapturing {
  import TestConsumer.defaultConsumerDelay
  import TestProducer.defaultProducerDelay

  private var idCount = 0
  private def nextId(): Int = {
    idCount += 1
    idCount
  }

  private def producerId: String = s"p-$idCount"

  private def awaitWorkersRegistered(
      controller: ActorRef[WorkPullingProducerController.Command[TestConsumer.Job]],
      count: Int): Unit = {
    val probe = createTestProbe[WorkPullingProducerController.WorkerStats]()
    probe.awaitAssert {
      controller ! WorkPullingProducerController.GetWorkerStats(probe.ref)
      probe.receiveMessage().numberOfWorkers should ===(count)
    }
  }

  val workerServiceKey: ServiceKey[ConsumerController.Command[TestConsumer.Job]] = ServiceKey("worker")

  "ReliableDelivery with work-pulling" must {

    "illustrate work-pulling usage" in {
      nextId()
      val workPullingController =
        spawn(
          WorkPullingProducerController[TestConsumer.Job](producerId, workerServiceKey, None),
          s"workPullingController-${idCount}")
      val jobProducer =
        spawn(TestProducerWorkPulling(defaultProducerDelay, workPullingController), name = s"jobProducer-${idCount}")

      val consumerEndProbe1 = createTestProbe[TestConsumer.Collected]()
      val workerController1 =
        spawn(ConsumerController[TestConsumer.Job](workerServiceKey), s"workerController1-${idCount}")
      spawn(
        TestConsumer(defaultConsumerDelay, 42, consumerEndProbe1.ref, workerController1),
        name = s"worker1-${idCount}")

      val consumerEndProbe2 = createTestProbe[TestConsumer.Collected]()
      val workerController2 =
        spawn(ConsumerController[TestConsumer.Job](workerServiceKey), s"workerController2-${idCount}")
      spawn(
        TestConsumer(defaultConsumerDelay, 42, consumerEndProbe2.ref, workerController2),
        name = s"worker2-${idCount}")

      consumerEndProbe1.receiveMessage(10.seconds)
      consumerEndProbe2.receiveMessage()

      testKit.stop(workerController1)
      testKit.stop(workerController2)
      awaitWorkersRegistered(workPullingController, 0)
      testKit.stop(jobProducer)
      testKit.stop(workPullingController)
    }

    "resend unconfirmed to other if worker dies" in {
      nextId()
      val workPullingController =
        spawn(
          WorkPullingProducerController[TestConsumer.Job](producerId, workerServiceKey, None),
          s"workPullingController-${idCount}")
      val producerProbe = createTestProbe[WorkPullingProducerController.RequestNext[TestConsumer.Job]]()
      workPullingController ! WorkPullingProducerController.Start(producerProbe.ref)

      val workerController1Probe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()
      system.receptionist ! Receptionist.Register(workerServiceKey, workerController1Probe.ref)
      awaitWorkersRegistered(workPullingController, 1)

      producerProbe.receiveMessage().sendNextTo ! TestConsumer.Job("msg-1")
      val seqMsg1 = workerController1Probe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
      seqMsg1.message should ===(TestConsumer.Job("msg-1"))
      seqMsg1.producerController ! ProducerControllerImpl.Request(1L, 10L, true, false)

      producerProbe.receiveMessage().sendNextTo ! TestConsumer.Job("msg-2")
      workerController1Probe
        .expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
        .message should ===(TestConsumer.Job("msg-2"))
      producerProbe.receiveMessage().sendNextTo ! TestConsumer.Job("msg-3")
      workerController1Probe
        .expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
        .message should ===(TestConsumer.Job("msg-3"))

      val workerController2Probe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()
      system.receptionist ! Receptionist.Register(workerServiceKey, workerController2Probe.ref)
      awaitWorkersRegistered(workPullingController, 2)

      workerController1Probe.stop()
      awaitWorkersRegistered(workPullingController, 1)

      // msg-2 and msg3 were not confirmed and should be resent to another worker
      val seqMsg2 = workerController2Probe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
      seqMsg2.message should ===(TestConsumer.Job("msg-2"))
      seqMsg2.seqNr should ===(1)
      seqMsg2.producerController ! ProducerControllerImpl.Request(1L, 10L, true, false)

      workerController2Probe
        .expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
        .message should ===(TestConsumer.Job("msg-3"))

      producerProbe.receiveMessage().sendNextTo ! TestConsumer.Job("msg-4")
      workerController2Probe
        .expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
        .message should ===(TestConsumer.Job("msg-4"))

      workerController2Probe.stop()
      awaitWorkersRegistered(workPullingController, 0)
      testKit.stop(workPullingController)
    }

    "reply to MessageWithConfirmation" in {
      import WorkPullingProducerController.MessageWithConfirmation
      nextId()
      val workPullingController =
        spawn(
          WorkPullingProducerController[TestConsumer.Job](producerId, workerServiceKey, None),
          s"workPullingController-${idCount}")
      val producerProbe = createTestProbe[WorkPullingProducerController.RequestNext[TestConsumer.Job]]()
      workPullingController ! WorkPullingProducerController.Start(producerProbe.ref)

      val workerController1Probe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()
      system.receptionist ! Receptionist.Register(workerServiceKey, workerController1Probe.ref)
      awaitWorkersRegistered(workPullingController, 1)

      val replyProbe = createTestProbe[Done]()
      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-1"), replyProbe.ref)
      val seqMsg1 = workerController1Probe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
      seqMsg1.message should ===(TestConsumer.Job("msg-1"))
      seqMsg1.ack should ===(true)
      seqMsg1.producerController ! ProducerControllerImpl.Request(1L, 10L, true, false)
      replyProbe.receiveMessage()

      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-2"), replyProbe.ref)
      val seqMsg2 = workerController1Probe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
      seqMsg2.message should ===(TestConsumer.Job("msg-2"))
      seqMsg2.ack should ===(true)
      // no reply until ack
      replyProbe.expectNoMessage()
      seqMsg2.producerController ! ProducerControllerImpl.Ack(2L)
      replyProbe.receiveMessage()

      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-3"), replyProbe.ref)
      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-4"), replyProbe.ref)
      workerController1Probe.receiveMessages(2)
      seqMsg2.producerController ! ProducerControllerImpl.Ack(4L)
      replyProbe.receiveMessages(2)

      workerController1Probe.stop()
      awaitWorkersRegistered(workPullingController, 0)
      testKit.stop(workPullingController)
    }

    "reply to MessageWithConfirmation also when worker dies" in {
      import WorkPullingProducerController.MessageWithConfirmation
      nextId()
      val workPullingController =
        spawn(
          WorkPullingProducerController[TestConsumer.Job](producerId, workerServiceKey, None),
          s"workPullingController-${idCount}")
      val producerProbe = createTestProbe[WorkPullingProducerController.RequestNext[TestConsumer.Job]]()
      workPullingController ! WorkPullingProducerController.Start(producerProbe.ref)

      val workerController1Probe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()
      system.receptionist ! Receptionist.Register(workerServiceKey, workerController1Probe.ref)
      awaitWorkersRegistered(workPullingController, 1)

      val replyProbe = createTestProbe[Done]()
      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-1"), replyProbe.ref)
      val seqMsg1 = workerController1Probe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
      seqMsg1.producerController ! ProducerControllerImpl.Request(1L, 10L, true, false)
      replyProbe.receiveMessage()

      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-2"), replyProbe.ref)
      workerController1Probe.receiveMessage()
      seqMsg1.producerController ! ProducerControllerImpl.Ack(2L)
      replyProbe.receiveMessage()

      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-3"), replyProbe.ref)
      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(TestConsumer.Job("msg-4"), replyProbe.ref)
      workerController1Probe.receiveMessages(2)

      val workerController2Probe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()
      system.receptionist ! Receptionist.Register(workerServiceKey, workerController2Probe.ref)
      awaitWorkersRegistered(workPullingController, 2)

      workerController1Probe.stop()
      awaitWorkersRegistered(workPullingController, 1)
      replyProbe.expectNoMessage()

      // msg-3 and msg-4 were not confirmed and should be resent to another worker
      val seqMsg3 = workerController2Probe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
      seqMsg3.message should ===(TestConsumer.Job("msg-3"))
      seqMsg3.seqNr should ===(1)
      seqMsg3.producerController ! ProducerControllerImpl.Request(1L, 10L, true, false)
      replyProbe.receiveMessage()

      workerController2Probe
        .expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
        .message should ===(TestConsumer.Job("msg-4"))
      seqMsg3.producerController ! ProducerControllerImpl.Ack(2L)
      replyProbe.receiveMessage()

      workerController2Probe.stop()
      awaitWorkersRegistered(workPullingController, 0)
      testKit.stop(workPullingController)
    }

    "allow restart of producer" in {
      nextId()

      val workPullingController =
        spawn(
          WorkPullingProducerController[TestConsumer.Job](producerId, workerServiceKey, None),
          s"workPullingController-${idCount}")
      val producerProbe = createTestProbe[WorkPullingProducerController.RequestNext[TestConsumer.Job]]()
      workPullingController ! WorkPullingProducerController.Start(producerProbe.ref)

      val workerController1Probe = createTestProbe[ConsumerController.Command[TestConsumer.Job]]()
      system.receptionist ! Receptionist.Register(workerServiceKey, workerController1Probe.ref)
      awaitWorkersRegistered(workPullingController, 1)

      producerProbe.receiveMessage().sendNextTo ! TestConsumer.Job("msg-1")
      val seqMsg1 = workerController1Probe.expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
      seqMsg1.message should ===(TestConsumer.Job("msg-1"))
      seqMsg1.producerController ! ProducerControllerImpl.Request(1L, 10L, true, false)

      producerProbe.receiveMessage().sendNextTo ! TestConsumer.Job("msg-2")
      workerController1Probe
        .expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
        .message should ===(TestConsumer.Job("msg-2"))
      producerProbe.receiveMessage()

      // restart producer, new Start
      val producerProbe2 = createTestProbe[WorkPullingProducerController.RequestNext[TestConsumer.Job]]()
      workPullingController ! WorkPullingProducerController.Start(producerProbe2.ref)

      producerProbe2.receiveMessage().sendNextTo ! TestConsumer.Job("msg-3")
      workerController1Probe
        .expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
        .message should ===(TestConsumer.Job("msg-3"))
      producerProbe2.receiveMessage().sendNextTo ! TestConsumer.Job("msg-4")
      workerController1Probe
        .expectMessageType[ConsumerController.SequencedMessage[TestConsumer.Job]]
        .message should ===(TestConsumer.Job("msg-4"))

      testKit.stop(workPullingController)
    }

  }

}

// TODO #28723 add a random test for work pulling
