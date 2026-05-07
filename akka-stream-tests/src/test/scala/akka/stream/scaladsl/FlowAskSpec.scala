/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.stream.ActorAttributes
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Supervision
import akka.stream.Supervision.resumingDecider
import akka.stream.testkit._
import akka.testkit.TestActors
import akka.testkit.TestProbe
import akka.util.Timeout

object FlowAskSpec {
  case class Reply(payload: Int)

  class Replier extends Actor {
    override def receive: Receive = {
      case msg: Int => sender() ! Reply(msg)
    }
  }

  class ReplyAndProxy(to: ActorRef) extends Actor {
    override def receive: Receive = {
      case msg: Int =>
        to ! msg
        sender() ! Reply(msg)
    }
  }

  class RandomDelaysReplier extends Actor {
    override def receive: Receive = {
      case msg: Int =>
        import context.dispatcher

        val replyTo = sender()
        Future {
          Thread.sleep(ThreadLocalRandom.current().nextInt(1, 10))
          replyTo ! Reply(msg)
        }
    }
  }

  class StatusReplier extends Actor {
    override def receive: Receive = {
      case msg: Int => sender() ! akka.actor.Status.Success(Reply(msg))
    }
  }

  class FailOn(n: Int) extends Actor {
    override def receive: Receive = {
      case `n`      => sender() ! akka.actor.Status.Failure(new Exception(s"Booming for $n!"))
      case msg: Int => sender() ! akka.actor.Status.Success(Reply(msg))
    }
  }

  class FailOnAllExcept(n: Int) extends Actor {
    override def receive: Receive = {
      case `n`    => sender() ! akka.actor.Status.Success(Reply(n))
      case _: Int => sender() ! akka.actor.Status.Failure(new Exception(s"Booming for $n!"))
    }
  }

}

class FlowAskSpec extends StreamSpec {
  import FlowAskSpec._

  "A Flow with ask" must {

    implicit val timeout: Timeout = akka.util.Timeout(10.seconds)

    val replyOnInts =
      system.actorOf(Props(classOf[Replier]).withDispatcher("akka.test.stream-dispatcher"), "replyOnInts")

    val dontReply = system.actorOf(TestActors.blackholeProps.withDispatcher("akka.test.stream-dispatcher"), "dontReply")

    val replyRandomDelays =
      system.actorOf(
        Props(classOf[RandomDelaysReplier]).withDispatcher("akka.test.stream-dispatcher"),
        "replyRandomDelays")

    val statusReplier =
      system.actorOf(Props(new StatusReplier).withDispatcher("akka.test.stream-dispatcher"), "statusReplier")

    def replierFailOn(n: Int) =
      system.actorOf(Props(new FailOn(n)).withDispatcher("akka.test.stream-dispatcher"), s"failureReplier-$n")
    val failsOn1 = replierFailOn(1)
    val failsOn3 = replierFailOn(3)

    def replierFailAllExceptOn(n: Int) =
      system.actorOf(Props(new FailOnAllExcept(n)).withDispatcher("akka.test.stream-dispatcher"), s"failureReplier-$n")
    val failAllExcept6 = replierFailAllExceptOn(6)

    "produce asked elements" in {
      val c = TestSubscriber.manualProbe[Reply]()
      Source(1 to 3).ask[Reply](4)(replyOnInts).runWith(Sink.fromSubscriber(c))
      val sub = c.expectSubscription()
      sub.request(2)
      c.expectNext(Reply(1))
      c.expectNext(Reply(2))
      c.expectNoMessage()
      sub.request(2)
      c.expectNext(Reply(3))
      c.expectComplete()
    }
    "produce asked elements (simple ask)" in {
      val c = TestSubscriber.manualProbe[Reply]()
      Source(1 to 3).ask[Reply](replyOnInts).runWith(Sink.fromSubscriber(c))
      val sub = c.expectSubscription()
      sub.request(2)
      c.expectNext(Reply(1))
      c.expectNext(Reply(2))
      c.expectNoMessage()
      sub.request(2)
      c.expectNext(Reply(3))
      c.expectComplete()
    }
    "produce asked elements, when replies are akka.actor.Status.Success" in {
      val c = TestSubscriber.manualProbe[Reply]()
      Source(1 to 3).ask[Reply](4)(statusReplier).runWith(Sink.fromSubscriber(c))
      val sub = c.expectSubscription()
      sub.request(2)
      c.expectNext(Reply(1))
      c.expectNext(Reply(2))
      c.expectNoMessage()
      sub.request(2)
      c.expectNext(Reply(3))
      c.expectComplete()
    }

    "produce future elements in order" in {
      val c = TestSubscriber.manualProbe[Reply]()
      Source(1 to 50).ask[Reply](4)(replyRandomDelays).to(Sink.fromSubscriber(c)).run()
      val sub = c.expectSubscription()
      sub.request(1000)
      for (n <- 1 to 50) c.expectNext(Reply(n))
      c.expectComplete()
    }

    "signal ask timeout failure" in {
      val c = TestSubscriber.manualProbe[Reply]()
      Source(1 to 5)
        .map(s => s"$s + nope")
        .ask[Reply](4)(dontReply)(akka.util.Timeout(10.millis), implicitly[ClassTag[Reply]])
        .to(Sink.fromSubscriber(c))
        .run()
      c.expectSubscription().request(10)
      c.expectError().getMessage should startWith("Ask timed out on [Actor[akka://FlowAskSpec/user/dontReply#")
    }

    "signal ask failure" in {
      val c = TestSubscriber.manualProbe[Reply]()
      val ref = failsOn1
      Source(1 to 5).ask[Reply](4)(ref).to(Sink.fromSubscriber(c)).run()
      val sub = c.expectSubscription()
      sub.request(10)
      c.expectError().getMessage should be("Booming for 1!")
    }

    "signal failure when target actor is terminated" in {
      val r = system.actorOf(Props(classOf[Replier]).withDispatcher("akka.test.stream-dispatcher"), "wanna-fail")
      val done = Source.maybe[Int].ask[Reply](4)(r).runWith(Sink.ignore)

      intercept[RuntimeException] {
        r ! PoisonPill
        Await.result(done, remainingOrDefault)
      }.getMessage should startWith(
        "Actor watched by [ask()] has terminated! Was: Actor[akka://FlowAskSpec/user/wanna-fail#")
    }

    "a failure mid-stream must skip element with resume strategy" in {
      val p = TestProbe()

      val input = "a" :: "b" :: "c" :: "d" :: "e" :: "f" :: Nil

      val elements = Source
        .fromIterator(() => input.iterator)
        .ask[String](5)(p.ref)
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
        .runWith(Sink.seq)

      // the problematic ordering:
      p.expectMsg("a")
      p.lastSender ! "a"

      p.expectMsg("b")
      p.lastSender ! "b"

      p.expectMsg("c")
      val cSender = p.lastSender

      p.expectMsg("d")
      p.lastSender ! "d"

      p.expectMsg("e")
      p.lastSender ! "e"

      p.expectMsg("f")
      p.lastSender ! "f"

      cSender ! akka.actor.Status.Failure(new Exception("Booom!"))

      elements.futureValue should ===(List("a", "b", /* no c */ "d", "e", "f"))
    }

    "resume after ask failure" in {
      val c = TestSubscriber.manualProbe[Reply]()
      val ref = failsOn3
      Source(1 to 5)
        .ask[Reply](4)(ref)
        .withAttributes(supervisionStrategy(resumingDecider))
        .to(Sink.fromSubscriber(c))
        .run()
      val sub = c.expectSubscription()
      sub.request(10)
      for (n <- List(1, 2, 4, 5)) c.expectNext(Reply(n))
      c.expectComplete()
    }

    "resume after multiple failures" in {
      Await.result(
        Source(1 to 6)
          .ask[Reply](2)(failAllExcept6)
          .withAttributes(supervisionStrategy(resumingDecider))
          .runWith(Sink.head),
        3.seconds) should ===(Reply(6))
    }

    "should handle cancel properly" in {
      val pub = TestPublisher.manualProbe[Int]()
      val sub = TestSubscriber.manualProbe[Reply]()

      Source.fromPublisher(pub).ask[Reply](4)(dontReply).runWith(Sink.fromSubscriber(sub))

      val upstream = pub.expectSubscription()
      upstream.expectRequest()

      sub.expectSubscription().cancel()

      upstream.expectCancellation()

    }

  }
}
