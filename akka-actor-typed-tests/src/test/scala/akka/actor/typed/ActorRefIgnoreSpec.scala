/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import java.util.concurrent.TimeoutException

import scala.concurrent.duration._
import scala.util.{ Failure, Success }

import org.scalatest.concurrent.PatienceConfiguration.{ Timeout => PatienceTimeout }
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.{ ScalaTestWithActorTestKit, TestProbe }
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.util.Timeout

class ActorRefIgnoreSpec extends ScalaTestWithActorTestKit() with AnyWordSpecLike {

  case class Request(replyTo: ActorRef[Int])

  // this Actor behavior receives simple request and answers back total number of
  // messages it received so far
  val askMeActorBehavior: Behavior[Request] = {
    def internalBehavior(counter: Int): Behavior[Request] =
      Behaviors.receiveMessage[Request] {
        case Request(replyTo) =>
          val newCounter = counter + 1
          replyTo ! newCounter
          internalBehavior(newCounter)
      }

    internalBehavior(0)
  }

  /**
   * This actor sends a ask to 'askMeRef' at bootstrap and forward the answer to the probe.
   * We will use it through out this test.
   */
  def behavior(askMeRef: ActorRef[Request], probe: TestProbe[Int]) = Behaviors.setup[Int] { context =>
    implicit val timeout: Timeout = 1.second

    // send a message to interactWithRef
    context.ask(askMeRef, Request.apply) {
      case Success(res) => res
      case Failure(ex)  => throw ex
    }

    Behaviors.receiveMessage { num =>
      // receive response from interactWithRef and sent to prob
      probe.ref ! num
      Behaviors.same
    }
  }

  "IgnoreActorRef instance" should {

    "ignore all incoming messages" in {

      val askMeRef = testKit.spawn(askMeActorBehavior)

      val probe = testKit.createTestProbe[Int]("response-probe")
      askMeRef ! Request(probe.ref)
      probe.expectMessage(1)

      // this is more a compile-time proof
      // since the reply is ignored, we can't check that a message was sent to it
      askMeRef ! Request(testKit.system.ignoreRef)
      probe.expectNoMessage()

      // but we do check that the counter has increased when we used the ActorRef.ignore
      askMeRef ! Request(probe.ref)
      probe.expectMessage(3)
    }

    // this is kind of obvious, the Future won't complete because the ignoreRef is used
    "make a Future timeout when used in a 'ask'" in {

      implicit val timeout: Timeout = 500.millis
      val askMeRef = testKit.spawn(askMeActorBehavior)

      val failedAsk =
        askMeRef
          .ask { (_: ActorRef[Request]) =>
            Request(testKit.system.ignoreRef) // <- pass the ignoreRef instead, so Future never completes
          }
          .failed
          .futureValue(PatienceTimeout(1.second))

      failedAsk shouldBe a[TimeoutException]
    }

    // similar to above, but using actor-to-actor interaction
    "ignore messages when used in actor-to-actor interaction ('ask')" in {

      val probe = testKit.createTestProbe[Int]("probe-response")

      // this prove that the machinery works, probe will receive a response
      val askMeRef = testKit.spawn(askMeActorBehavior)
      testKit.spawn(behavior(askMeRef, probe))
      probe.expectMessage(1)

      // new interaction using ignoreRef, probe won't receive anything
      val ignoreRef = testKit.system.ignoreRef[Request]
      testKit.spawn(behavior(ignoreRef, probe))
      probe.expectNoMessage()

    }

    "be watchable from another actor without throwing an exception" in {

      val probe = testKit.createTestProbe[String]("probe-response")

      val forwardMessageRef =
        Behaviors.setup[String] { ctx =>
          ctx.watch(testKit.system.ignoreRef[String])

          Behaviors.receiveMessage { str =>
            probe.ref ! str
            Behaviors.same
          }
        }

      // this proves that the actor started and is operational and 'watch' didn't impact it
      val ref = testKit.spawn(forwardMessageRef)
      ref ! "abc"
      probe.expectMessage("abc")
    }

    "be a singleton" in {
      withClue("using the same type") {
        testKit.system.ignoreRef[String] shouldBe theSameInstanceAs(testKit.system.ignoreRef[String])
      }

      withClue("using different types") {
        testKit.system.ignoreRef[String] shouldBe theSameInstanceAs(testKit.system.ignoreRef[Int])
      }
    }

    "be adaptable back and forth to classic" in {
      testKit.system.ignoreRef[String].toClassic.toTyped[String]
    }

  }
}
