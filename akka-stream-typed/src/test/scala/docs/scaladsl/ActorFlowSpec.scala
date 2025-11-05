/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.pattern.StatusReply

//#imports
import akka.stream.scaladsl.{ Flow, Sink, Source }
import akka.stream.typed.scaladsl.ActorFlow
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout

//#imports
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.stream.testkit.TestSubscriber
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

object ActorFlowSpec {
  //#ask-actor
  final case class Asking(s: String, replyTo: ActorRef[Reply])
  final case class Reply(msg: String)

  final case class AskingWithStatus(s: String, replyTo: ActorRef[StatusReply[String]])

  //#ask-actor
}

class ActorFlowSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  import ActorFlowSpec._

  "ActorFlow" should {

    val replier = spawn(Behaviors.receiveMessage[Asking] {
      case Asking("TERMINATE", _) =>
        Behaviors.stopped

      case asking =>
        asking.replyTo ! Reply(asking.s + "!!!")
        Behaviors.same
    })

    val replierWithSuccess = spawn(Behaviors.receiveMessage[AskingWithStatus] {
      case AskingWithStatus("TERMINATE", _) =>
        Behaviors.stopped

      case asking =>
        asking.replyTo ! StatusReply.success(asking.s + "!!!")
        Behaviors.same
    })

    val replierWithError = spawn(Behaviors.receiveMessage[AskingWithStatus] {
      case AskingWithStatus("TERMINATE", _) =>
        Behaviors.stopped

      case asking =>
        asking.replyTo ! StatusReply.error("error!!!" + asking.s)
        Behaviors.same
    })

    "produce asked elements" in {
      val in: Future[immutable.Seq[Reply]] =
        Source
          .repeat("hello")
          .via(ActorFlow.ask(replier)((el, replyTo: ActorRef[Reply]) => Asking(el, replyTo)))
          .take(3)
          .runWith(Sink.seq)

      in.futureValue shouldEqual List.fill(3)(Reply("hello!!!"))
    }

    "produce asked elements with context " in {
      val in: Future[immutable.Seq[(Reply, Long)]] =
        Source
          .repeat("hello")
          .zipWithIndex
          .via(ActorFlow.askWithContext(replier)((el, replyTo: ActorRef[Reply]) => Asking(el, replyTo)))
          .take(3)
          .runWith(Sink.seq)

      in.futureValue shouldEqual List.fill(3)(Reply("hello!!!")).zipWithIndex.map { case (r, i) => r -> i.toLong }
    }

    "produced status success elements unwrap " in {
      val in: Future[immutable.Seq[String]] =
        Source
          .repeat("hello")
          .via(ActorFlow.askWithStatus(replierWithSuccess)((el, replyTo: ActorRef[StatusReply[String]]) =>
            AskingWithStatus(el, replyTo)))
          .take(3)
          .runWith(Sink.seq)

      in.futureValue shouldEqual List.fill(3)("hello!!!")
    }

    "produced status success elements unwrap with context " in {
      val in: Future[immutable.Seq[(String, Long)]] =
        Source
          .repeat("hello")
          .zipWithIndex
          .via(ActorFlow.askWithStatusAndContext(replierWithSuccess)((el, replyTo: ActorRef[StatusReply[String]]) =>
            AskingWithStatus(el, replyTo)))
          .take(3)
          .runWith(Sink.seq)

      in.futureValue shouldEqual List.fill(3)("hello!!!").zipWithIndex.map { case (r, i) => r -> i.toLong }
    }

    "produce status error elements unwrap " in {
      val in: Future[immutable.Seq[String]] =
        Source
          .repeat("hello")
          .via(ActorFlow.askWithStatus(replierWithError)((el, replyTo: ActorRef[StatusReply[String]]) =>
            AskingWithStatus(el, replyTo)))
          .take(3)
          .runWith(Sink.seq)

      val v = in.failed.futureValue
      v shouldBe a[StatusReply.ErrorMessage]
      v.getMessage shouldEqual "error!!!hello"
    }

    "produce status error elements unwrap with context" in {
      val in: Future[immutable.Seq[(String, Long)]] =
        Source
          .repeat("hello")
          .zipWithIndex
          .via(ActorFlow.askWithStatusAndContext(replierWithError)((el, replyTo: ActorRef[StatusReply[String]]) =>
            AskingWithStatus(el, replyTo)))
          .take(3)
          .runWith(Sink.seq)

      val v = in.failed.futureValue
      v shouldBe a[StatusReply.ErrorMessage]
      v.getMessage shouldEqual "error!!!hello"
    }

    "produce asked elements in order" in {
      //#ask-actor
      val ref = spawn(Behaviors.receiveMessage[Asking] { asking =>
        asking.replyTo ! Reply(asking.s + "!!!")
        Behaviors.same
      })

      //#ask-actor

      //#ask
      implicit val timeout: Timeout = 1.second

      val askFlow: Flow[String, Reply, NotUsed] =
        ActorFlow.ask(ref)(Asking.apply)

      // explicit creation of the sent message
      val askFlowExplicit: Flow[String, Reply, NotUsed] =
        ActorFlow.ask(ref)(makeMessage = (el, replyTo: ActorRef[Reply]) => Asking(el, replyTo))

      val in: Future[immutable.Seq[String]] =
        Source(1 to 50).map(_.toString).via(askFlow).map(_.msg).runWith(Sink.seq)
      //#ask
      askFlowExplicit.map(identity)

      in.futureValue shouldEqual List.tabulate(51)(i => s"$i!!!").drop(1)
    }

    "signal ask timeout failure" in {
      import akka.actor.typed.scaladsl.adapter._
      val dontReply = spawn(Behaviors.ignore[Asking])

      val c = TestSubscriber.manualProbe[Reply]()(system.toClassic)
      implicit val timeout: Timeout = 10.millis

      Source(1 to 5)
        .map(_.toString + " nope")
        .via(ActorFlow.ask[String, Asking, Reply](4)(dontReply)(Asking(_, _)))
        .to(Sink.fromSubscriber(c))
        .run()

      c.expectSubscription().request(10)
      c.expectError().getMessage should startWith("Ask timed out on [Actor")
    }

    "signal failure when target actor is terminated" in {
      val done = Source
        .maybe[String]
        .via(ActorFlow.ask(replier)((el, replyTo: ActorRef[Reply]) => Asking(el, replyTo)))
        .runWith(Sink.ignore)

      intercept[RuntimeException] {
        replier ! Asking("TERMINATE", system.deadLetters)
        Await.result(done, 3.seconds)
      }.getMessage should startWith("Actor watched by [ask()] has terminated! Was: Actor[akka://ActorFlowSpec")
    }

  }

}
