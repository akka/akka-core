/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.delivery

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.FishingOutcome
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.WorkPullingProducerController
import akka.actor.typed.receptionist.ServiceKey
import akka.persistence.typed.PersistenceId

object WorkPullingWithEventSourcedProducerQueueSpec {
  def conf: Config =
    ConfigFactory.parseString(s"""
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    akka.persistence.snapshot-store.local.dir = "target/WorkPullingWithEventSourcedProducerQueueSpec-${UUID
      .randomUUID()
      .toString}"
    akka.reliable-delivery.consumer-controller.flow-control-window = 20  
    """)
}

class WorkPullingWithEventSourcedProducerQueueSpec
    extends ScalaTestWithActorTestKit(WorkPullingWithEventSourcedProducerQueueSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  private val idCounter = new AtomicInteger(0)
  private def nextId(): String = s"${idCounter.incrementAndGet()}"

  private def workerServiceKey(): ServiceKey[ConsumerController.Command[String]] =
    ServiceKey(s"worker-${idCounter.get}")

  "WorkPulling with EventSourcedProducerQueue" must {

    "deliver messages after full producer and consumer restart" in {
      val producerId = s"p${nextId()}"
      val serviceKey = workerServiceKey()
      val producerProbe = createTestProbe[WorkPullingProducerController.RequestNext[String]]()

      val producerController = spawn(
        WorkPullingProducerController[String](
          producerId,
          serviceKey,
          Some(EventSourcedProducerQueue[String](PersistenceId.ofUniqueId(producerId)))))
      producerController ! WorkPullingProducerController.Start(producerProbe.ref)

      val consumerController = spawn(ConsumerController[String](serviceKey))
      val consumerProbe = createTestProbe[ConsumerController.Delivery[String]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      producerProbe.receiveMessage().sendNextTo ! "a"
      producerProbe.receiveMessage().sendNextTo ! "b"
      producerProbe.receiveMessage().sendNextTo ! "c"
      producerProbe.receiveMessage()

      consumerProbe.receiveMessage().message should ===("a")

      system.log.info("Stopping [{}]", producerController)
      testKit.stop(producerController)
      producerProbe.expectTerminated(producerController)
      testKit.stop(consumerController)
      consumerProbe.expectTerminated(consumerController)

      system.log.info("------------------ Start 2")

      val producerController2 = spawn(
        WorkPullingProducerController[String](
          producerId,
          serviceKey,
          Some(EventSourcedProducerQueue[String](PersistenceId.ofUniqueId(producerId)))))
      producerController2 ! WorkPullingProducerController.Start(producerProbe.ref)

      val consumerController2 = spawn(ConsumerController[String](serviceKey))
      consumerController2 ! ConsumerController.Start(consumerProbe.ref)

      // start two consumers (same consumerProbe) to reproduce issue #29854
      val consumerController3 = spawn(ConsumerController[String](serviceKey))
      consumerController3 ! ConsumerController.Start(consumerProbe.ref)

      val requestNext4 = producerProbe.receiveMessage()
      producerProbe.expectNoMessage()

      val delivery1 = consumerProbe.receiveMessage()
      delivery1.confirmTo ! ConsumerController.Confirmed

      val delivery2 = consumerProbe.receiveMessage()
      delivery2.confirmTo ! ConsumerController.Confirmed

      val delivery3 = consumerProbe.receiveMessage()
      delivery3.confirmTo ! ConsumerController.Confirmed

      // since we have two consumers with the same probe order of delivery to the
      // probe is not deterministic
      Set(delivery1.message, delivery2.message, delivery3.message) should ===(Set("a", "b", "c"))

      producerProbe.expectNoMessage()
      requestNext4.sendNextTo ! "d"

      val delivery4 = consumerProbe.receiveMessage()
      delivery4.message should ===("d")
      delivery4.confirmTo ! ConsumerController.Confirmed

      testKit.stop(producerController2)
      testKit.stop(consumerController2)
    }

    "deliver messages after producer restart, keeping same ConsumerController" in {
      val producerId = s"p${nextId()}"
      val serviceKey = workerServiceKey()
      val producerProbe = createTestProbe[WorkPullingProducerController.RequestNext[String]]()

      val producerController = spawn(
        WorkPullingProducerController[String](
          producerId,
          serviceKey,
          Some(EventSourcedProducerQueue[String](PersistenceId.ofUniqueId(producerId)))))
      producerController ! WorkPullingProducerController.Start(producerProbe.ref)

      val consumerController = spawn(ConsumerController[String](serviceKey))
      val consumerProbe = createTestProbe[ConsumerController.Delivery[String]]()
      consumerController ! ConsumerController.Start(consumerProbe.ref)

      producerProbe.receiveMessage().sendNextTo ! "a"
      producerProbe.receiveMessage().sendNextTo ! "b"
      producerProbe.receiveMessage().sendNextTo ! "c"
      producerProbe.receiveMessage()

      val delivery1 = consumerProbe.receiveMessage()
      delivery1.message should ===("a")

      system.log.info("Stopping [{}]", producerController)
      testKit.stop(producerController)

      val producerController2 = spawn(
        WorkPullingProducerController[String](
          producerId,
          serviceKey,
          Some(EventSourcedProducerQueue[String](PersistenceId.ofUniqueId(producerId)))))
      producerController2 ! WorkPullingProducerController.Start(producerProbe.ref)

      // Delivery in flight from old dead WorkPullingProducerController, confirmation will not be stored
      delivery1.confirmTo ! ConsumerController.Confirmed

      // from old, buffered in ConsumerController
      val delivery2 = consumerProbe.receiveMessage()
      delivery2.message should ===("b")
      delivery2.confirmTo ! ConsumerController.Confirmed

      // from old, buffered in ConsumerController
      val delivery3 = consumerProbe.receiveMessage()
      delivery3.message should ===("c")
      delivery3.confirmTo ! ConsumerController.Confirmed

      val requestNext4 = producerProbe.receiveMessage()
      producerProbe.expectNoMessage()
      requestNext4.sendNextTo ! "d"

      // TODO Should we try harder to deduplicate first?
      val redelivery1 = consumerProbe.receiveMessage()
      redelivery1.message should ===("a")
      redelivery1.confirmTo ! ConsumerController.Confirmed

      producerProbe.receiveMessage().sendNextTo ! "e"

      val redelivery2 = consumerProbe.receiveMessage()
      redelivery2.message should ===("b")
      redelivery2.confirmTo ! ConsumerController.Confirmed

      val redelivery3 = consumerProbe.receiveMessage()
      redelivery3.message should ===("c")
      redelivery3.confirmTo ! ConsumerController.Confirmed

      val delivery4 = consumerProbe.receiveMessage()
      delivery4.message should ===("d")
      delivery4.confirmTo ! ConsumerController.Confirmed

      val delivery5 = consumerProbe.receiveMessage()
      delivery5.message should ===("e")
      delivery5.confirmTo ! ConsumerController.Confirmed

      testKit.stop(producerController2)
      testKit.stop(consumerController)
    }

    "deliver messages after restart, when using several workers" in {
      val producerId = s"p${nextId()}"
      val serviceKey = workerServiceKey()
      val producerProbe = createTestProbe[WorkPullingProducerController.RequestNext[String]]()

      val producerController = spawn(
        WorkPullingProducerController[String](
          producerId,
          serviceKey,
          Some(EventSourcedProducerQueue[String](PersistenceId.ofUniqueId(producerId)))))
      producerController ! WorkPullingProducerController.Start(producerProbe.ref)

      // same consumerProbe for all workers, since we can't know the routing
      val consumerProbe = createTestProbe[ConsumerController.Delivery[String]]()
      var received = Vector.empty[ConsumerController.Delivery[String]]

      val consumerController1 = spawn(ConsumerController[String](serviceKey))
      consumerController1 ! ConsumerController.Start(consumerProbe.ref)
      val consumerController2 = spawn(ConsumerController[String](serviceKey))
      consumerController2 ! ConsumerController.Start(consumerProbe.ref)
      val consumerController3 = spawn(ConsumerController[String](serviceKey))
      consumerController3 ! ConsumerController.Start(consumerProbe.ref)

      val batch1 = 15
      val confirmed1 = 10
      (1 to batch1).foreach { n =>
        val reqNext = producerProbe.receiveMessage()
        if (n == 1 || n == 7 || n == 13) // not checking all because takes too much time
          producerProbe.expectNoMessage(50.millis) // issue #29854
        reqNext.sendNextTo ! s"msg-$n"
      }

      (1 to confirmed1).foreach { _ =>
        received :+= consumerProbe.receiveMessage()
        received.last.confirmTo ! ConsumerController.Confirmed
      }

      system.log.debug("Workers received [{}]", received.mkString(", "))
      received.map(_.message).toSet.size should ===(confirmed1)

      producerProbe.receiveMessage()

      system.log.info("Stopping [{}]", producerController)
      testKit.stop(producerController)
      system.log.info("Stopping [{}]", consumerController2)
      testKit.stop(consumerController2)

      val consumerController4 = spawn(ConsumerController[String](serviceKey))
      consumerController4 ! ConsumerController.Start(consumerProbe.ref)

      // start two consumers (same consumerProbe) to reproduce issue #29854
      val consumerController5 = spawn(ConsumerController[String](serviceKey))
      consumerController5 ! ConsumerController.Start(consumerProbe.ref)

      val producerController2 = spawn(
        WorkPullingProducerController[String](
          producerId,
          serviceKey,
          Some(EventSourcedProducerQueue[String](PersistenceId.ofUniqueId(producerId)))))
      producerController2 ! WorkPullingProducerController.Start(producerProbe.ref)

      val batch2 = 5
      (batch1 + 1 to batch1 + batch2).foreach { n =>
        val reqNext = producerProbe.receiveMessage()
        if (n == batch1 + 1 || n == batch1 + 3) // not checking all because takes too much time
          producerProbe.expectNoMessage(50.millis) // issue #29854
        reqNext.sendNextTo ! s"msg-$n"
      }

      consumerProbe.fishForMessage(consumerProbe.remainingOrDefault) { delivery =>
        received :+= delivery
        delivery.confirmTo ! ConsumerController.Confirmed
        if (received.map(_.message).toSet.size == batch1 + batch2)
          FishingOutcome.Complete
        else
          FishingOutcome.Continue
      }

      system.log.debug("Workers received [{}]", received.mkString(", "))
      received.map(_.message).toSet should ===((1 to batch1 + batch2).map(n => s"msg-$n").toSet)

      testKit.stop(producerController2)
      testKit.stop(consumerController1)
      testKit.stop(consumerController3)
      testKit.stop(consumerController4)
    }

  }

}
