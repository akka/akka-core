/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._
import scala.util.Random

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.Behavior
import akka.actor.typed.BehaviorInterceptor
import akka.actor.typed.TypedActorContext
import akka.actor.typed.delivery.internal.ProducerControllerImpl
import akka.actor.typed.scaladsl.Behaviors

object ReliableDeliveryRandomSpec {
  val config: Config = ConfigFactory.parseString("""
    akka.reliable-delivery.consumer-controller {
      flow-control-window = 20
      resend-interval-min = 500 ms
      resend-interval-max = 2 s
    }
    """)

  object RandomFlakyNetwork {
    def apply[T](rnd: Random, dropProbability: Any => Double): BehaviorInterceptor[T, T] =
      new RandomFlakyNetwork(rnd, dropProbability).asInstanceOf[BehaviorInterceptor[T, T]]
  }

  class RandomFlakyNetwork(rnd: Random, dropProbability: Any => Double) extends BehaviorInterceptor[Any, Any] {
    override def aroundReceive(
        ctx: TypedActorContext[Any],
        msg: Any,
        target: BehaviorInterceptor.ReceiveTarget[Any]): Behavior[Any] = {
      if (rnd.nextDouble() < dropProbability(msg)) {
        ctx.asScala.log.info("dropped {}", msg)
        Behaviors.same
      } else {
        target(ctx, msg)
      }
    }

  }
}

class ReliableDeliveryRandomSpec(config: Config)
    extends ScalaTestWithActorTestKit(config)
    with AnyWordSpecLike
    with LogCapturing {
  import ReliableDeliveryRandomSpec._

  def this() = this(ReliableDeliveryRandomSpec.config)

  private var idCount = 0
  private def nextId(): Int = {
    idCount += 1
    idCount
  }

  private def producerId: String = s"p-$idCount"

  private def test(
      rndSeed: Long,
      rnd: Random,
      numberOfMessages: Int,
      producerDropProbability: Double,
      consumerDropProbability: Double,
      durableFailProbability: Option[Double],
      resendLost: Boolean): Unit = {

    val consumerControllerSettings = ConsumerController.Settings(system).withOnlyFlowControl(!resendLost)

    val consumerDelay = rnd.nextInt(40).millis
    val producerDelay = rnd.nextInt(40).millis
    val durableDelay = if (durableFailProbability.isDefined) rnd.nextInt(40).millis else Duration.Zero
    system.log.info(
      "Random seed [{}], consumerDropProbability [{}], producerDropProbability [{}], " +
      "consumerDelay [{}], producerDelay [{}], durableFailProbability [{}], durableDelay [{}]",
      rndSeed,
      consumerDropProbability,
      producerDropProbability,
      consumerDelay,
      producerDelay,
      durableFailProbability,
      durableDelay)

    // RandomFlakyNetwork to simulate lost messages from producerController to consumerController
    val consumerDrop: Any => Double = {
      case _: ConsumerController.SequencedMessage[_] => consumerDropProbability
      case _                                         => 0.0
    }

    val consumerEndProbe = createTestProbe[TestConsumer.Collected]()
    val consumerController =
      spawn(
        Behaviors.intercept(() => RandomFlakyNetwork[ConsumerController.Command[TestConsumer.Job]](rnd, consumerDrop))(
          ConsumerController[TestConsumer.Job](serviceKey = None, consumerControllerSettings)),
        s"consumerController-${idCount}")
    spawn(
      TestConsumer(consumerDelay, numberOfMessages, consumerEndProbe.ref, consumerController),
      name = s"destination-${idCount}")

    // RandomFlakyNetwork to simulate lost messages from consumerController to producerController
    val producerDrop: Any => Double = {
      case _: ProducerControllerImpl.Request         => producerDropProbability
      case _: ProducerControllerImpl.Resend          => producerDropProbability
      case _: ProducerController.RegisterConsumer[_] => producerDropProbability
      case _                                         => 0.0
    }

    val stateHolder = new AtomicReference[DurableProducerQueue.State[TestConsumer.Job]]
    val durableQueue = durableFailProbability.map { p =>
      TestDurableProducerQueue(
        durableDelay,
        stateHolder,
        (_: DurableProducerQueue.Command[TestConsumer.Job]) => rnd.nextDouble() < p)
    }

    val producerController = spawn(
      Behaviors.intercept(() => RandomFlakyNetwork[ProducerController.Command[TestConsumer.Job]](rnd, producerDrop))(
        ProducerController[TestConsumer.Job](producerId, durableQueue)),
      s"producerController-${idCount}")
    val producer = spawn(TestProducer(producerDelay, producerController), name = s"producer-${idCount}")

    consumerController ! ConsumerController.RegisterToProducerController(producerController)

    consumerEndProbe.receiveMessage(120.seconds)

    testKit.stop(producer)
    testKit.stop(producerController)
    testKit.stop(consumerController)
  }

  "ReliableDelivery with random failures" must {

    "work with flaky network" in {
      nextId()
      val rndSeed = System.currentTimeMillis()
      val rnd = new Random(rndSeed)
      val consumerDropProbability = 0.1 + rnd.nextDouble() * 0.2
      val producerDropProbability = 0.1 + rnd.nextDouble() * 0.2
      test(
        rndSeed,
        rnd,
        numberOfMessages = 63,
        producerDropProbability,
        consumerDropProbability,
        durableFailProbability = None,
        resendLost = true)
    }

    "work with flaky DurableProducerQueue" in {
      nextId()
      val rndSeed = System.currentTimeMillis()
      val rnd = new Random(rndSeed)
      val durableFailProbability = 0.1 + rnd.nextDouble() * 0.1
      test(
        rndSeed,
        rnd,
        numberOfMessages = 31,
        producerDropProbability = 0.0,
        consumerDropProbability = 0.0,
        Some(durableFailProbability),
        resendLost = true)
    }

    "work with flaky network and flaky DurableProducerQueue" in {
      nextId()
      val rndSeed = System.currentTimeMillis()
      val rnd = new Random(rndSeed)
      val consumerDropProbability = 0.1 + rnd.nextDouble() * 0.1
      val producerDropProbability = 0.1 + rnd.nextDouble() * 0.1
      val durableFailProbability = 0.1 + rnd.nextDouble() * 0.1
      test(
        rndSeed,
        rnd,
        numberOfMessages = 17,
        producerDropProbability,
        consumerDropProbability,
        Some(durableFailProbability),
        resendLost = true)
    }

    "work with flaky network without resending" in {
      nextId()
      val rndSeed = System.currentTimeMillis()
      val rnd = new Random(rndSeed)
      val consumerDropProbability = 0.1 + rnd.nextDouble() * 0.4
      val producerDropProbability = 0.1 + rnd.nextDouble() * 0.3
      test(
        rndSeed,
        rnd,
        numberOfMessages = 63,
        producerDropProbability,
        consumerDropProbability,
        durableFailProbability = None,
        resendLost = false)
    }

  }

}

// same tests but with chunked messages
class ReliableDeliveryRandomChunkedSpec
    extends ReliableDeliveryRandomSpec(
      ConfigFactory.parseString("""
        akka.reliable-delivery.producer-controller.chunk-large-messages = 1b
        """).withFallback(TestSerializer.config).withFallback(ReliableDeliveryRandomSpec.config))
