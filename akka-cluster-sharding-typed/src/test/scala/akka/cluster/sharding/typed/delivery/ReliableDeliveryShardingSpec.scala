/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.delivery

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.ConsumerController.SequencedMessage
import akka.actor.typed.delivery.TestConsumer
import akka.actor.typed.delivery.internal.ProducerControllerImpl
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join

object ReliableDeliveryShardingSpec {
  val config = ConfigFactory.parseString("""
    akka.actor.provider = cluster
    akka.remote.artery.canonical.port = 0
    akka.reliable-delivery.consumer-controller.flow-control-window = 20
    """)

  object TestShardingProducer {

    sealed trait Command
    final case class RequestNext(sendToRef: ActorRef[ShardingEnvelope[TestConsumer.Job]]) extends Command

    private case object Tick extends Command

    def apply(producerController: ActorRef[ShardingProducerController.Start[TestConsumer.Job]]): Behavior[Command] = {
      Behaviors.setup { context =>
        context.setLoggerName("TestShardingProducer")
        val requestNextAdapter: ActorRef[ShardingProducerController.RequestNext[TestConsumer.Job]] =
          context.messageAdapter(req => RequestNext(req.sendNextTo))
        producerController ! ShardingProducerController.Start(requestNextAdapter)

        // simulate fast producer
        Behaviors.withTimers { timers =>
          timers.startTimerWithFixedDelay(Tick, Tick, 20.millis)
          idle(0)
        }
      }
    }

    private def idle(n: Int): Behavior[Command] = {
      Behaviors.receiveMessage {
        case Tick                => Behaviors.same
        case RequestNext(sendTo) => active(n + 1, sendTo)
      }
    }

    private def active(n: Int, sendTo: ActorRef[ShardingEnvelope[TestConsumer.Job]]): Behavior[Command] = {
      Behaviors.receive { (ctx, msg) =>
        msg match {
          case Tick =>
            val msg = s"msg-$n"
            val entityId = s"entity-${n % 3}"
            ctx.log.info("sent {} to {}", msg, entityId)
            sendTo ! ShardingEnvelope(entityId, TestConsumer.Job(msg))
            idle(n)

          case RequestNext(_) =>
            // already active
            Behaviors.same
        }
      }
    }

  }

}

class ReliableDeliveryShardingSpec
    extends ScalaTestWithActorTestKit(ReliableDeliveryShardingSpec.config)
    with AnyWordSpecLike
    with LogCapturing {
  import ReliableDeliveryShardingSpec._
  import TestConsumer.defaultConsumerDelay

  private var idCount = 0
  private def nextId(): Int = {
    idCount += 1
    idCount
  }

  private def producerId: String = s"p-$idCount"

  "ReliableDelivery with sharding" must {
    "join cluster" in {
      Cluster(system).manager ! Join(Cluster(system).selfMember.address)
    }

    "illustrate sharding usage" in {
      nextId()
      val consumerEndProbe = createTestProbe[TestConsumer.Collected]()
      val typeKey = EntityTypeKey[SequencedMessage[TestConsumer.Job]](s"TestConsumer-$idCount")
      val sharding: ActorRef[ShardingEnvelope[SequencedMessage[TestConsumer.Job]]] =
        ClusterSharding(system).init(Entity(typeKey)(_ =>
          ShardingConsumerController[TestConsumer.Job, TestConsumer.Command](c =>
            TestConsumer(defaultConsumerDelay, 42, consumerEndProbe.ref, c))))

      val shardingProducerController =
        spawn(ShardingProducerController[TestConsumer.Job](producerId, sharding, None), s"shardingController-$idCount")
      val producer = spawn(TestShardingProducer(shardingProducerController), name = s"shardingProducer-$idCount")

      // expecting 3 end messages, one for each entity: "entity-0", "entity-1", "entity-2"
      consumerEndProbe.receiveMessages(3, 5.seconds)

      testKit.stop(producer)
      testKit.stop(shardingProducerController)
    }

    "illustrate sharding usage with several producers" in {
      nextId()
      val consumerEndProbe = createTestProbe[TestConsumer.Collected]()
      val typeKey = EntityTypeKey[SequencedMessage[TestConsumer.Job]](s"TestConsumer-$idCount")
      val sharding: ActorRef[ShardingEnvelope[SequencedMessage[TestConsumer.Job]]] =
        ClusterSharding(system).init(Entity(typeKey)(_ =>
          ShardingConsumerController[TestConsumer.Job, TestConsumer.Command](c =>
            TestConsumer(defaultConsumerDelay, 42, consumerEndProbe.ref, c))))

      val shardingController1 =
        spawn(
          ShardingProducerController[TestConsumer.Job](
            s"p1-$idCount", // note different producerId
            sharding,
            None),
          s"shardingController1-$idCount")
      val producer1 = spawn(TestShardingProducer(shardingController1), name = s"shardingProducer1-$idCount")

      val shardingController2 =
        spawn(
          ShardingProducerController[TestConsumer.Job](
            s"p2-$idCount", // note different producerId
            sharding,
            None),
          s"shardingController2-$idCount")
      val producer2 = spawn(TestShardingProducer(shardingController2), name = s"shardingProducer2-$idCount")

      // expecting 3 end messages, one for each entity: "entity-0", "entity-1", "entity-2"
      val endMessages = consumerEndProbe.receiveMessages(3, 5.seconds)
      // verify that they received messages from both producers
      endMessages.flatMap(_.producerIds).toSet should ===(
        Set(
          s"p1-$idCount-entity-0",
          s"p1-$idCount-entity-1",
          s"p1-$idCount-entity-2",
          s"p2-$idCount-entity-0",
          s"p2-$idCount-entity-1",
          s"p2-$idCount-entity-2"))

      testKit.stop(producer1)
      testKit.stop(producer2)
      testKit.stop(shardingController1)
      testKit.stop(shardingController2)
    }

    "reply to MessageWithConfirmation" in {
      nextId()
      val consumerEndProbe = createTestProbe[TestConsumer.Collected]()
      val typeKey = EntityTypeKey[SequencedMessage[TestConsumer.Job]](s"TestConsumer-$idCount")
      val sharding: ActorRef[ShardingEnvelope[SequencedMessage[TestConsumer.Job]]] =
        ClusterSharding(system).init(Entity(typeKey)(_ =>
          ShardingConsumerController[TestConsumer.Job, TestConsumer.Command](c =>
            TestConsumer(defaultConsumerDelay, 3, consumerEndProbe.ref, c))))

      val shardingProducerController =
        spawn(ShardingProducerController[TestConsumer.Job](producerId, sharding, None), s"shardingController-$idCount")

      val producerProbe = createTestProbe[ShardingProducerController.RequestNext[TestConsumer.Job]]()
      shardingProducerController ! ShardingProducerController.Start(producerProbe.ref)

      val replyProbe = createTestProbe[Done]()
      producerProbe.receiveMessage().askNextTo ! ShardingProducerController.MessageWithConfirmation(
        "entity-0",
        TestConsumer.Job("msg-1"),
        replyProbe.ref)
      producerProbe.receiveMessage().askNextTo ! ShardingProducerController.MessageWithConfirmation(
        "entity-0",
        TestConsumer.Job("msg-2"),
        replyProbe.ref)
      producerProbe.receiveMessage().askNextTo ! ShardingProducerController.MessageWithConfirmation(
        "entity-1",
        TestConsumer.Job("msg-3"),
        replyProbe.ref)
      producerProbe.receiveMessage().askNextTo ! ShardingProducerController.MessageWithConfirmation(
        "entity-0",
        TestConsumer.Job("msg-4"),
        replyProbe.ref)

      consumerEndProbe.receiveMessage() // entity-0 received 3 messages
      consumerEndProbe.expectNoMessage()

      producerProbe.receiveMessage().askNextTo ! ShardingProducerController.MessageWithConfirmation(
        "entity-1",
        TestConsumer.Job("msg-5"),
        replyProbe.ref)
      producerProbe.receiveMessage().askNextTo ! ShardingProducerController.MessageWithConfirmation(
        "entity-1",
        TestConsumer.Job("msg-6"),
        replyProbe.ref)
      consumerEndProbe.receiveMessage() // entity-0 received 3 messages

      testKit.stop(shardingProducerController)
    }

    "include demand information in RequestNext" in {
      nextId()

      val shardingProbe =
        createTestProbe[ShardingEnvelope[SequencedMessage[TestConsumer.Job]]]()
      val shardingProducerController =
        spawn(
          ShardingProducerController[TestConsumer.Job](producerId, shardingProbe.ref, None),
          s"shardingController-$idCount")
      val producerProbe = createTestProbe[ShardingProducerController.RequestNext[TestConsumer.Job]]()
      shardingProducerController ! ShardingProducerController.Start(producerProbe.ref)

      val next1 = producerProbe.receiveMessage()
      next1.entitiesWithDemand should ===(Set.empty)
      next1.bufferedForEntitiesWithoutDemand should ===(Map.empty)

      next1.sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-1"))
      // for the first message no RequestNext until initial roundtrip
      producerProbe.expectNoMessage()

      val seq1 = shardingProbe.receiveMessage().message
      seq1.message should ===(TestConsumer.Job("msg-1"))
      seq1.producerController ! ProducerControllerImpl.Request(confirmedSeqNr = 0L, requestUpToSeqNr = 5, true, false)

      val next2 = producerProbe.receiveMessage()
      next2.entitiesWithDemand should ===(Set("entity-1"))
      next2.bufferedForEntitiesWithoutDemand should ===(Map.empty)

      next2.sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-2"))
      val next3 = producerProbe.receiveMessage()
      // could be sent immediately since had demand, and Request(requestUpToSeqNr-5)
      next3.entitiesWithDemand should ===(Set("entity-1"))
      next3.bufferedForEntitiesWithoutDemand should ===(Map.empty)

      next3.sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-3"))
      val next4 = producerProbe.receiveMessage()
      next4.entitiesWithDemand should ===(Set("entity-1"))
      next4.bufferedForEntitiesWithoutDemand should ===(Map.empty)

      next4.sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-4"))
      val next5 = producerProbe.receiveMessage()
      next5.entitiesWithDemand should ===(Set("entity-1"))
      next5.bufferedForEntitiesWithoutDemand should ===(Map.empty)

      next5.sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-5"))
      // no more demand Request(requestUpToSeqNr-5)
      producerProbe.expectNoMessage()
      // but we can anyway send more, which will be buffered
      next5.sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-6"))

      shardingProbe.receiveMessage()
      shardingProbe.receiveMessage()
      shardingProbe.receiveMessage()
      val seq5 = shardingProbe.receiveMessage().message
      seq5.message should ===(TestConsumer.Job("msg-5"))

      val next6 = producerProbe.receiveMessage()
      next6.entitiesWithDemand should ===(Set.empty)
      next6.bufferedForEntitiesWithoutDemand should ===(Map("entity-1" -> 1))

      // and we can send to another entity
      next6.sendNextTo ! ShardingEnvelope("entity-2", TestConsumer.Job("msg-7"))
      producerProbe.expectNoMessage()
      val seq7 = shardingProbe.receiveMessage().message
      seq7.message should ===(TestConsumer.Job("msg-7"))
      seq7.producerController ! ProducerControllerImpl.Request(confirmedSeqNr = 0L, requestUpToSeqNr = 5, true, false)

      val next8 = producerProbe.receiveMessage()
      next8.entitiesWithDemand should ===(Set("entity-2"))
      next8.bufferedForEntitiesWithoutDemand should ===(Map("entity-1" -> 1))

      // when new demand the buffered messages will be be sent
      seq5.producerController ! ProducerControllerImpl.Request(confirmedSeqNr = 5L, requestUpToSeqNr = 10, true, false)
      val seq6 = shardingProbe.receiveMessage().message
      seq6.message should ===(TestConsumer.Job("msg-6"))

      val next9 = producerProbe.receiveMessage()
      next9.entitiesWithDemand should ===(Set("entity-1", "entity-2"))
      next9.bufferedForEntitiesWithoutDemand should ===(Map.empty)

      testKit.stop(shardingProducerController)
    }

    "allow restart of producer" in {
      nextId()

      val shardingProbe =
        createTestProbe[ShardingEnvelope[SequencedMessage[TestConsumer.Job]]]()
      val shardingProducerController =
        spawn(
          ShardingProducerController[TestConsumer.Job](producerId, shardingProbe.ref, None),
          s"shardingController-$idCount")
      val producerProbe = createTestProbe[ShardingProducerController.RequestNext[TestConsumer.Job]]()
      shardingProducerController ! ShardingProducerController.Start(producerProbe.ref)

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-1"))
      val seq1 = shardingProbe.receiveMessage().message
      seq1.message should ===(TestConsumer.Job("msg-1"))
      seq1.producerController ! ProducerControllerImpl.Request(confirmedSeqNr = 0L, requestUpToSeqNr = 5, true, false)

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-2"))
      shardingProbe.receiveMessage().message.message should ===(TestConsumer.Job("msg-2"))

      // restart producer, new Start
      val producerProbe2 = createTestProbe[ShardingProducerController.RequestNext[TestConsumer.Job]]()
      shardingProducerController ! ShardingProducerController.Start(producerProbe2.ref)

      producerProbe2.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-3"))
      shardingProbe.receiveMessage().message.message should ===(TestConsumer.Job("msg-3"))

      testKit.stop(shardingProducerController)
    }

    "deliver unconfirmed if ShardingConsumerController is terminated" in {
      // for example if ShardingConsumerController is rebalanced, but no more messages are sent to the entity
      nextId()

      val consumerIncarnation = new AtomicInteger(0)
      val consumerProbes = Vector.fill(3)(createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]())

      val typeKey = EntityTypeKey[SequencedMessage[TestConsumer.Job]](s"TestConsumer-$idCount")
      val region = ClusterSharding(system).init(Entity(typeKey)(_ =>
        ShardingConsumerController[TestConsumer.Job, TestConsumer.Command] { cc =>
          cc ! ConsumerController.Start(consumerProbes(consumerIncarnation.getAndIncrement()).ref)
          Behaviors.empty
        }))

      val shardingProducerSettings =
        ShardingProducerController.Settings(system).withResendFirstUnconfirmedIdleTimeout(1500.millis)
      val shardingProducerController =
        spawn(
          ShardingProducerController[TestConsumer.Job](producerId, region, None, shardingProducerSettings),
          s"shardingController-$idCount")
      val producerProbe = createTestProbe[ShardingProducerController.RequestNext[TestConsumer.Job]]()
      shardingProducerController ! ShardingProducerController.Start(producerProbe.ref)

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-1"))
      val delivery1 = consumerProbes(0).receiveMessage()
      delivery1.message should ===(TestConsumer.Job("msg-1"))
      delivery1.confirmTo ! ConsumerController.Confirmed

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-2"))
      val delivery2 = consumerProbes(0).receiveMessage()
      delivery2.message should ===(TestConsumer.Job("msg-2"))
      delivery2.confirmTo ! ConsumerController.Confirmed

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-3"))
      val delivery3 = consumerProbes(0).receiveMessage()
      delivery3.message should ===(TestConsumer.Job("msg-3"))
      // msg-3 not Confirmed

      {
        consumerProbes(0).stop()
        Thread.sleep(1000) // let it terminate

        producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-4"))
        val delivery3b = consumerProbes(1).receiveMessage()
        // msg-3 is redelivered
        delivery3b.message should ===(TestConsumer.Job("msg-3"))
        delivery3b.confirmTo ! ConsumerController.Confirmed
        val delivery3cor4 = consumerProbes(1).receiveMessage()
        delivery3cor4.message match {
          case TestConsumer.Job("msg-3") =>
            // It is possible the ProducerController re-sends msg-3 again before it has processed its acknowledgement.
            // If the ConsumerController restarts between sending the acknowledgement and receiving that re-sent msg-3,
            // it will deliver msg-3 a second time. We then expect msg-4 next:
            delivery3cor4.confirmTo ! ConsumerController.Confirmed
            val delivery4 = consumerProbes(1).receiveMessage()
            delivery4.message should ===(TestConsumer.Job("msg-4"))
          case TestConsumer.Job("msg-4") =>
          // OK!
          case other =>
            throw new MatchError(other)
        }
      }

      // redeliver also when no more messages are sent
      {
        consumerProbes(1).stop()

        val delivery3cor4 = consumerProbes(2).receiveMessage()
        delivery3cor4.message match {
          case TestConsumer.Job("msg-3") =>
            // It is possible the ProducerController re-sends msg-3 again before it has processed its acknowledgement.
            // If the ConsumerController restarts between sending the acknowledgement and receiving that re-sent msg-3,
            // it will deliver msg-3 a second time. We then expect msg-4 next:
            delivery3cor4.confirmTo ! ConsumerController.Confirmed
            val delivery4 = consumerProbes(2).receiveMessage()
            delivery4.message should ===(TestConsumer.Job("msg-4"))
          case TestConsumer.Job("msg-4") =>
          // OK!
          case other =>
            throw new MatchError(other)
        }
      }

      consumerProbes(2).stop()
      testKit.stop(shardingProducerController)
    }

    "cleanup unused ProducerController" in {
      nextId()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()

      val typeKey = EntityTypeKey[SequencedMessage[TestConsumer.Job]](s"TestConsumer-$idCount")
      val region = ClusterSharding(system).init(Entity(typeKey)(_ =>
        ShardingConsumerController[TestConsumer.Job, TestConsumer.Command] { cc =>
          cc ! ConsumerController.Start(consumerProbe.ref)
          Behaviors.empty
        }))

      val shardingProducerSettings =
        ShardingProducerController.Settings(system).withCleanupUnusedAfter(1.second)
      val shardingProducerController =
        spawn(
          ShardingProducerController[TestConsumer.Job](producerId, region, None, shardingProducerSettings),
          s"shardingController-$idCount")
      val producerProbe = createTestProbe[ShardingProducerController.RequestNext[TestConsumer.Job]]()
      shardingProducerController ! ShardingProducerController.Start(producerProbe.ref)

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-1"))
      val delivery1 = consumerProbe.receiveMessage()
      delivery1.message should ===(TestConsumer.Job("msg-1"))
      delivery1.confirmTo ! ConsumerController.Confirmed

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-2"))
      val delivery2 = consumerProbe.receiveMessage()
      delivery2.message should ===(TestConsumer.Job("msg-2"))
      delivery2.confirmTo ! ConsumerController.Confirmed

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-2", TestConsumer.Job("msg-3"))
      val delivery3 = consumerProbe.receiveMessage()
      delivery3.message should ===(TestConsumer.Job("msg-3"))
      // msg-3 not Confirmed

      val next4 = producerProbe.receiveMessage()
      next4.entitiesWithDemand should ===(Set("entity-1", "entity-2"))

      Thread.sleep(2000)

      next4.sendNextTo ! ShardingEnvelope("entity-2", TestConsumer.Job("msg-4"))
      val next5 = producerProbe.receiveMessage()
      next5.entitiesWithDemand should ===(Set("entity-2")) // entity-1 removed

      delivery3.confirmTo ! ConsumerController.Confirmed
      val delivery4 = consumerProbe.receiveMessage()
      delivery4.message should ===(TestConsumer.Job("msg-4"))
      delivery4.confirmTo ! ConsumerController.Confirmed

      // send to entity-1 again
      next5.sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-5"))
      val delivery5 = consumerProbe.receiveMessage()
      delivery5.message should ===(TestConsumer.Job("msg-5"))
      delivery5.confirmTo ! ConsumerController.Confirmed

      consumerProbe.stop()
      testKit.stop(shardingProducerController)
    }

    "cleanup ConsumerController when ProducerController is terminated" in {
      nextId()

      val consumerProbe = createTestProbe[ConsumerController.Delivery[TestConsumer.Job]]()

      val typeKey = EntityTypeKey[SequencedMessage[TestConsumer.Job]](s"TestConsumer-$idCount")
      val region = ClusterSharding(system).init(Entity(typeKey)(_ =>
        ShardingConsumerController[TestConsumer.Job, TestConsumer.Command] { cc =>
          cc ! ConsumerController.Start(consumerProbe.ref)
          Behaviors.empty
        }))

      val shardingProducerController1 =
        spawn(ShardingProducerController[TestConsumer.Job](producerId, region, None), s"shardingController-$idCount")
      val producerProbe = createTestProbe[ShardingProducerController.RequestNext[TestConsumer.Job]]()
      shardingProducerController1 ! ShardingProducerController.Start(producerProbe.ref)

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-1"))
      val delivery1 = consumerProbe.receiveMessage()
      delivery1.message should ===(TestConsumer.Job("msg-1"))
      delivery1.confirmTo ! ConsumerController.Confirmed

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-2"))
      val delivery2 = consumerProbe.receiveMessage()
      delivery2.message should ===(TestConsumer.Job("msg-2"))
      delivery2.confirmTo ! ConsumerController.Confirmed
      producerProbe.receiveMessage()

      LoggingTestKit.empty
        .withMessageRegex("ProducerController.*terminated")
        .withLoggerName("akka.cluster.sharding.typed.delivery.ShardingConsumerController")
        .expect {
          testKit.stop(shardingProducerController1)
        }

      val shardingProducerController2 =
        spawn(ShardingProducerController[TestConsumer.Job](producerId, region, None), s"shardingController-$idCount")
      shardingProducerController2 ! ShardingProducerController.Start(producerProbe.ref)

      LoggingTestKit
        .debug("Starting ConsumerController")
        .withLoggerName("akka.cluster.sharding.typed.delivery.ShardingConsumerController")
        .expect {
          producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-3"))
        }
      val delivery3 = consumerProbe.receiveMessage()
      delivery3.message should ===(TestConsumer.Job("msg-3"))
      delivery3.confirmTo ! ConsumerController.Confirmed

      producerProbe.receiveMessage().sendNextTo ! ShardingEnvelope("entity-1", TestConsumer.Job("msg-4"))
      val delivery4 = consumerProbe.receiveMessage()
      delivery4.message should ===(TestConsumer.Job("msg-4"))
      delivery4.confirmTo ! ConsumerController.Confirmed

      consumerProbe.stop()
      testKit.stop(shardingProducerController2)
    }

  }

}

// TODO #28723 add a random test for sharding
