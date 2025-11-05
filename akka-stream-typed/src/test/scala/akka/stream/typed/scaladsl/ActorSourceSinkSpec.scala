/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.typed.scaladsl

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.{ CompletionStrategy, OverflowStrategy }
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

object ActorSourceSinkSpec {

  sealed trait AckProto
  case class Init(sender: ActorRef[String]) extends AckProto
  case class Msg(sender: ActorRef[String], msg: String) extends AckProto
  case object Complete extends AckProto
  case object Failed extends AckProto
}

class ActorSourceSinkSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  import ActorSourceSinkSpec._

  "ActorSink" should {

    "accept messages" in {
      val p = TestProbe[String]()

      val in =
        Source
          .queue[String](10)
          .map(_ + "!")
          .to(ActorSink.actorRef(p.ref, "DONE", ex => "FAILED: " + ex.getMessage))
          .run()

      val msg = "Zug zug"

      in.offer(msg)
      p.expectMessage(msg + "!")
    }

    "obey protocol" in {
      val p = TestProbe[AckProto]()

      val autoPilot = Behaviors.receiveMessage[AckProto] {
        case m @ Init(sender) =>
          p.ref ! m
          sender ! "ACK"
          Behaviors.same
        case m @ Msg(sender, _) =>
          p.ref ! m
          sender ! "ACK"
          Behaviors.same
        case m =>
          p.ref ! m
          Behaviors.same
      }

      val pilotRef: ActorRef[AckProto] = spawn(autoPilot)

      val in =
        Source
          .queue[String](10)
          .to(ActorSink.actorRefWithBackpressure(pilotRef, Msg.apply, Init.apply, "ACK", Complete, _ => Failed))
          .run()

      p.expectMessageType[Init]

      in.offer("Dabu!")
      p.expectMessageType[Msg].msg shouldBe "Dabu!"

      in.offer("Lok'tar!")
      p.expectMessageType[Msg].msg shouldBe "Lok'tar!"

      in.offer("Swobu!")
      p.expectMessageType[Msg].msg shouldBe "Swobu!"
    }

    "obey protocol without specific ack message" in {
      val p = TestProbe[AckProto]()

      val autoPilot = Behaviors.receiveMessage[AckProto] {
        case m @ Init(sender) =>
          p.ref ! m
          sender ! "ACK"
          Behaviors.same
        case m @ Msg(sender, _) =>
          p.ref ! m
          sender ! "ACK"
          Behaviors.same
        case m =>
          p.ref ! m
          Behaviors.same
      }

      val pilotRef: ActorRef[AckProto] = spawn(autoPilot)

      val in =
        Source
          .queue[String](10)
          .to(ActorSink.actorRefWithBackpressure(pilotRef, Msg.apply, Init.apply, Complete, _ => Failed))
          .run()

      p.expectMessageType[Init]

      in.offer("Dabu!")
      p.expectMessageType[Msg].msg shouldBe "Dabu!"

      in.offer("Lok'tar!")
      p.expectMessageType[Msg].msg shouldBe "Lok'tar!"

      in.offer("Swobu!")
      p.expectMessageType[Msg].msg shouldBe "Swobu!"
    }
  }

  "ActorSource" should {
    "send messages and complete" in {
      val (in, out) = ActorSource
        .actorRef[String]({ case "complete" => }, PartialFunction.empty, 10, OverflowStrategy.dropBuffer)
        .toMat(Sink.seq)(Keep.both)
        .run()

      in ! "one"
      in ! "two"
      in ! "complete"

      out.futureValue should contain theSameElementsAs Seq("one", "two")
    }

    "fail the stream" in {
      val (in, out) = ActorSource
        .actorRef[String](PartialFunction.empty, { case msg => new Error(msg) }, 10, OverflowStrategy.dropBuffer)
        .toMat(Sink.seq)(Keep.both)
        .run()

      in ! "boom!"

      out.failed.futureValue.getCause.getMessage shouldBe "boom!"
    }

    "send message and ack" in {
      val p = TestProbe[String]()

      val (in, out) = ActorSource
        .actorRefWithBackpressure[String, String](
          p.ref,
          "ack", { case "complete" => CompletionStrategy.draining },
          PartialFunction.empty)
        .toMat(Sink.seq)(Keep.both)
        .run()

      in ! "one"
      p.expectMessage("ack")
      in ! "two"
      p.expectMessage("ack")
      in ! "complete"

      out.futureValue should contain theSameElementsAs Seq("one", "two")
    }
  }
}
