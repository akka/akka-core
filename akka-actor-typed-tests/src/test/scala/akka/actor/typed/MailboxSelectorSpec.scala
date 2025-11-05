/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ActorCell
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.internal.adapter.ActorContextAdapter
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.dispatch.BoundedMessageQueueSemantics
import akka.dispatch.BoundedNodeMessageQueue
import akka.dispatch.MessageQueue
import akka.dispatch.UnboundedMessageQueueSemantics

class MailboxSelectorSpec extends ScalaTestWithActorTestKit("""
    specific-mailbox {
      mailbox-type = "akka.dispatch.NonBlockingBoundedMailbox"
      mailbox-capacity = 4 
    }
  """) with AnyWordSpecLike with LogCapturing {

  case class WhatsYourMailbox(replyTo: ActorRef[MessageQueue])
  private def behavior: Behavior[WhatsYourMailbox] =
    Behaviors.setup { context =>
      Behaviors.receiveMessage[WhatsYourMailbox] {
        case WhatsYourMailbox(replyTo) =>
          val mailbox = context match {
            case adapter: ActorContextAdapter[_] =>
              adapter.classicActorContext match {
                case cell: ActorCell =>
                  cell.mailbox.messageQueue
                case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
              }
            case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
          }

          replyTo ! mailbox
          Behaviors.stopped
      }
    }

  "The Mailbox selectors" must {
    "default to unbounded" in {
      val actor = spawn(behavior)
      val mailbox = actor.ask(WhatsYourMailbox.apply).futureValue
      mailbox shouldBe a[UnboundedMessageQueueSemantics]
    }

    "select a bounded mailbox" in {
      val actor = spawn(behavior, MailboxSelector.bounded(3))
      val mailbox = actor.ask(WhatsYourMailbox.apply).futureValue
      mailbox shouldBe a[BoundedMessageQueueSemantics]
      // capacity is private so only way to test is to fill mailbox
    }

    "set capacity on a bounded mailbox" in {
      val latch = new CountDownLatch(1)
      val probe = testKit.createTestProbe[String]()
      val actor = spawn(Behaviors.receiveMessage[String] {
        case "one" =>
          // block here so we can fill mailbox up
          probe ! "blocking-on-one"
          latch.await(10, TimeUnit.SECONDS)
          Behaviors.same
        case _ =>
          Behaviors.same
      }, MailboxSelector.bounded(2))
      actor ! "one" // actor will block here
      probe.expectMessage("blocking-on-one")
      actor ! "two"
      LoggingTestKit.deadLetters().expect {
        actor ! "three"
        actor ! "four" // mailbox full, will be dropped
      }
      latch.countDown()
    }

    "select an arbitrary mailbox from config" in {
      val actor = spawn(behavior, MailboxSelector.fromConfig("specific-mailbox"))
      val mailbox = actor.ask(WhatsYourMailbox.apply).futureValue
      mailbox shouldBe a[BoundedMessageQueueSemantics]
      mailbox.asInstanceOf[BoundedNodeMessageQueue].capacity should ===(4)

    }
  }

}
