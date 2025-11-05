/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.delivery

import java.util.UUID

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.ConsumerController.SequencedMessage
import akka.actor.typed.delivery.DurableProducerQueue
import akka.actor.typed.delivery.TestConsumer
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.persistence.journal.inmem.InmemJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.delivery.EventSourcedProducerQueue

object DurableShardingSpec {
  def conf: Config =
    ConfigFactory.parseString(s"""
    akka.actor.provider = cluster
    akka.remote.artery.canonical.port = 0
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    akka.persistence.snapshot-store.local.dir = "target/DurableShardingSpec-${UUID.randomUUID().toString}"
    akka.reliable-delivery.consumer-controller.flow-control-window = 20
    """)
}

class DurableShardingSpec
    extends ScalaTestWithActorTestKit(DurableShardingSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  private var idCount = 0
  private def nextId(): Int = {
    idCount += 1
    idCount
  }

  private def producerId: String = s"p-$idCount"

  private val journalOperations = createTestProbe[InmemJournal.Operation]()
  system.eventStream ! EventStream.Subscribe(journalOperations.ref)

  private def consumerBehavior(
      c: ActorRef[ConsumerController.Start[TestConsumer.Job]],
      consumerProbe: ActorRef[TestConsumer.JobDelivery]): Behavior[TestConsumer.Command] =
    Behaviors.setup[TestConsumer.Command] { context =>
      val deliveryAdapter = context.messageAdapter[ConsumerController.Delivery[TestConsumer.Job]] { d =>
        TestConsumer.JobDelivery(d.message, d.confirmTo, d.producerId, d.seqNr)
      }
      c ! ConsumerController.Start(deliveryAdapter)
      Behaviors.receiveMessagePartial {
        case jobDelivery: TestConsumer.JobDelivery =>
          consumerProbe.ref ! jobDelivery
          Behaviors.same
      }
    }

  "ReliableDelivery with sharding and durable queue" must {

    "join cluster" in {
      Cluster(system).manager ! Join(Cluster(system).selfMember.address)
    }

    "load initial state and resend unconfirmed" in {
      nextId()
      val typeKey = EntityTypeKey[SequencedMessage[TestConsumer.Job]](s"TestConsumer-$idCount")
      val consumerProbe = createTestProbe[TestConsumer.JobDelivery]()
      val sharding: ActorRef[ShardingEnvelope[SequencedMessage[TestConsumer.Job]]] =
        ClusterSharding(system).init(Entity(typeKey)(_ =>
          ShardingConsumerController[TestConsumer.Job, TestConsumer.Command](c =>
            consumerBehavior(c, consumerProbe.ref))))

      val shardingProducerController =
        spawn(
          ShardingProducerController[TestConsumer.Job](
            producerId,
            sharding,
            Some(EventSourcedProducerQueue[TestConsumer.Job](PersistenceId.ofUniqueId(producerId)))),
          s"shardingController-$idCount")
      val producerProbe = createTestProbe[ShardingProducerController.RequestNext[TestConsumer.Job]]()
      shardingProducerController ! ShardingProducerController.Start(producerProbe.ref)

      (1 to 4).foreach { n =>
        producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job(s"msg-$n"))
        journalOperations.expectMessageType[InmemJournal.Write].event.getClass should ===(
          classOf[DurableProducerQueue.MessageSent[_]])
      }

      journalOperations.expectNoMessage()

      val delivery1 = consumerProbe.receiveMessage()
      delivery1.confirmTo ! ConsumerController.Confirmed
      journalOperations.expectMessageType[InmemJournal.Write].event.getClass should ===(
        classOf[DurableProducerQueue.Confirmed])

      val delivery2 = consumerProbe.receiveMessage()
      delivery2.confirmTo ! ConsumerController.Confirmed
      journalOperations.expectMessageType[InmemJournal.Write].event.getClass should ===(
        classOf[DurableProducerQueue.Confirmed])

      producerProbe.receiveMessage()

      // let the initial messages reach the ShardingConsumerController before stopping ShardingProducerController
      val delivery3 = consumerProbe.receiveMessage()
      delivery3.msg should ===(TestConsumer.Job("msg-3"))
      delivery3.seqNr should ===(3)
      Thread.sleep(1000)

      system.log.info("Stopping [{}]", shardingProducerController)
      testKit.stop(shardingProducerController)

      val shardingProducerController2 =
        spawn(
          ShardingProducerController[TestConsumer.Job](
            producerId,
            sharding,
            Some(EventSourcedProducerQueue[TestConsumer.Job](PersistenceId.ofUniqueId(producerId)))),
          s"shardingController2-$idCount")
      shardingProducerController2 ! ShardingProducerController.Start(producerProbe.ref)

      // delivery3 and delivery4 are still from old shardingProducerController, that were queued in ConsumerController
      delivery3.confirmTo ! ConsumerController.Confirmed
      // that confirmation goes to old dead shardingProducerController, and therefore not stored
      journalOperations.expectNoMessage()

      val delivery4 = consumerProbe.receiveMessage()
      delivery4.msg should ===(TestConsumer.Job("msg-4"))
      delivery4.seqNr should ===(4)
      delivery4.confirmTo ! ConsumerController.Confirmed
      // that confirmation goes to old dead shardingProducerController, and therefore not stored
      journalOperations.expectNoMessage()

      // now the unconfirmed are redelivered
      val redelivery3 = consumerProbe.receiveMessage()
      redelivery3.msg should ===(TestConsumer.Job("msg-3"))
      redelivery3.seqNr should ===(1) // new ProducerController and there starting at 1
      redelivery3.confirmTo ! ConsumerController.Confirmed
      val confirmed3 =
        journalOperations.expectMessageType[InmemJournal.Write].event.asInstanceOf[DurableProducerQueue.Confirmed]
      confirmed3.seqNr should ===(3)
      confirmed3.confirmationQualifier should ===("entity-1")

      val redelivery4 = consumerProbe.receiveMessage()
      redelivery4.msg should ===(TestConsumer.Job("msg-4"))
      redelivery4.seqNr should ===(2)
      redelivery4.confirmTo ! ConsumerController.Confirmed
      val confirmed4 =
        journalOperations.expectMessageType[InmemJournal.Write].event.asInstanceOf[DurableProducerQueue.Confirmed]
      confirmed4.seqNr should ===(4)
      confirmed4.confirmationQualifier should ===("entity-1")

      val next5 = producerProbe.receiveMessage()
      next5.sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job(s"msg-5"))
      journalOperations.expectMessageType[InmemJournal.Write].event.getClass should ===(
        classOf[DurableProducerQueue.MessageSent[_]])

      // issue #30489: the consumer controller may have stopped after msg-5, so allow for resend on timeout (10-15s)
      val delivery5 = consumerProbe.receiveMessage(20.seconds)
      delivery5.msg should ===(TestConsumer.Job("msg-5"))
      delivery5.seqNr should ===(3)
      delivery5.confirmTo ! ConsumerController.Confirmed
      val confirmed5 =
        journalOperations.expectMessageType[InmemJournal.Write].event.asInstanceOf[DurableProducerQueue.Confirmed]
      confirmed5.seqNr should ===(5)
      confirmed5.confirmationQualifier should ===("entity-1")

      testKit.stop(shardingProducerController2)
    }

    "reply to MessageWithConfirmation after storage" in {
      import ShardingProducerController.MessageWithConfirmation
      nextId()
      val typeKey = EntityTypeKey[SequencedMessage[TestConsumer.Job]](s"TestConsumer-$idCount")
      val consumerProbe = createTestProbe[TestConsumer.JobDelivery]()

      val sharding: ActorRef[ShardingEnvelope[SequencedMessage[TestConsumer.Job]]] =
        ClusterSharding(system).init(Entity(typeKey)(_ =>
          ShardingConsumerController[TestConsumer.Job, TestConsumer.Command](c =>
            consumerBehavior(c, consumerProbe.ref))))

      val shardingProducerController =
        spawn(
          ShardingProducerController[TestConsumer.Job](
            producerId,
            sharding,
            Some(EventSourcedProducerQueue[TestConsumer.Job](PersistenceId.ofUniqueId(producerId)))),
          s"shardingController-$idCount")
      val producerProbe = createTestProbe[ShardingProducerController.RequestNext[TestConsumer.Job]]()
      shardingProducerController ! ShardingProducerController.Start(producerProbe.ref)

      val replyProbe = createTestProbe[Done]()
      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(
        "entity-1",
        TestConsumer.Job(s"msg-1"),
        replyProbe.ref)
      journalOperations.expectMessageType[InmemJournal.Write].event.getClass should ===(
        classOf[DurableProducerQueue.MessageSent[_]])
      replyProbe.expectMessage(Done)

      producerProbe.receiveMessage().askNextTo ! MessageWithConfirmation(
        "entity-2",
        TestConsumer.Job(s"msg-2"),
        replyProbe.ref)
      journalOperations.expectMessageType[InmemJournal.Write].event.getClass should ===(
        classOf[DurableProducerQueue.MessageSent[_]])
      replyProbe.expectMessage(Done)

      testKit.stop(shardingProducerController)
    }
  }

}
