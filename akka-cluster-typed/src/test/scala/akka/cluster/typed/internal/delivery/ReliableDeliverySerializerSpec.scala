/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed.internal.delivery

import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.delivery.ConsumerController
import akka.actor.typed.delivery.DurableProducerQueue
import akka.actor.typed.delivery.ProducerController
import akka.actor.typed.delivery.internal.ChunkedMessage
import akka.actor.typed.delivery.internal.ProducerControllerImpl
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializationExtension
import akka.util.ByteString

class ReliableDeliverySerializerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  private val classicSystem = system.toClassic
  private val serializer = new ReliableDeliverySerializer(classicSystem.asInstanceOf[ExtendedActorSystem])
  private val ref = spawn(Behaviors.empty[Any])

  "ReliableDeliverySerializer" must {

    val timestamp = System.currentTimeMillis()
    Seq(
      "SequencedMessage-1" -> ConsumerController.SequencedMessage("prod-1", 17L, "msg17", false, false)(ref),
      "SequencedMessage-2" -> ConsumerController.SequencedMessage("prod-1", 1L, "msg01", true, true)(ref),
      "Ack" -> ProducerControllerImpl.Ack(5L),
      "Request" -> ProducerControllerImpl.Request(5L, 25L, true, true),
      "Resend" -> ProducerControllerImpl.Resend(5L),
      "RegisterConsumer" -> ProducerController.RegisterConsumer[Any](ref),
      "DurableProducerQueue.MessageSent-1" -> DurableProducerQueue.MessageSent(3L, "msg03", false, "", timestamp),
      "DurableProducerQueue.MessageSent-2" -> DurableProducerQueue.MessageSent(3L, "msg03", true, "q1", timestamp),
      "DurableProducerQueue.Confirmed" -> DurableProducerQueue.Confirmed(3L, "q2", timestamp),
      "DurableProducerQueue.State-1" -> DurableProducerQueue.State(3L, 2L, Map.empty, Vector.empty),
      "DurableProducerQueue.State-2" -> DurableProducerQueue.State(
        3L,
        2L,
        Map("" -> (2L -> timestamp)),
        Vector(DurableProducerQueue.MessageSent(3L, "msg03", false, "", timestamp))),
      "DurableProducerQueue.State-3" -> DurableProducerQueue.State(
        17L,
        12L,
        Map(
          "q1" -> (5L -> timestamp),
          "q2" -> (7L -> timestamp),
          "q3" -> (12L -> timestamp),
          "q4" -> (14L -> timestamp)),
        Vector(
          DurableProducerQueue.MessageSent(15L, "msg15", true, "q4", timestamp),
          DurableProducerQueue.MessageSent(16L, "msg16", true, "q4", timestamp))),
      "DurableProducerQueue.Cleanup" -> DurableProducerQueue.Cleanup(Set("q1", "q2", "q3")),
      "SequencedMessage-chunked-1" -> ConsumerController.SequencedMessage
        .fromChunked("prod-1", 1L, ChunkedMessage(ByteString.fromString("abc"), true, true, 20, ""), true, true, ref),
      "SequencedMessage-chunked-2" -> ConsumerController.SequencedMessage
        .fromChunked("prod-1", 1L, ChunkedMessage(ByteString(1, 2, 3), true, false, 123456, "A"), false, false, ref),
      "DurableProducerQueue.MessageSent-chunked" -> DurableProducerQueue.MessageSent.fromChunked(
        3L,
        ChunkedMessage(ByteString.fromString("abc"), true, true, 20, ""),
        false,
        "",
        timestamp)).foreach {
      case (scenario, item) =>
        s"resolve serializer for $scenario" in {
          val serializer = SerializationExtension(classicSystem)
          serializer.serializerFor(item.getClass).getClass should be(classOf[ReliableDeliverySerializer])
        }

        s"serialize and de-serialize $scenario" in {
          verifySerialization(item)
        }
    }
  }

  def verifySerialization(msg: AnyRef): Unit = {
    serializer.fromBinary(serializer.toBinary(msg), serializer.manifest(msg)) should be(msg)
  }

}
