/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ActorCell
import akka.actor.BootstrapSetup
import akka.actor.setup.ActorSystemSetup
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.DispatcherSelector
import akka.actor.typed.SpawnProtocol
import akka.actor.typed.internal.adapter.ActorContextAdapter
import akka.actor.typed.scaladsl.AskPattern._
import akka.dispatch.BoundedMessageQueueSemantics
import akka.dispatch.BoundedNodeMessageQueue
import akka.dispatch.Dispatchers
import akka.dispatch.MessageQueue
import akka.dispatch.NodeMessageQueue

object DispatcherSelectorSpec {
  val config = ConfigFactory.parseString("""
      specific-dispatcher {
        executor = thread-pool-executor
        type = PinnedDispatcher
      }
      specific-mailbox {
        mailbox-type = "akka.dispatch.NonBlockingBoundedMailbox"
        mailbox-capacity = 4
      }
      specific-dispatcher-with-mailbox {
        type = Dispatcher
        executor = "thread-pool-executor"
        mailbox-type = "akka.dispatch.NonBlockingBoundedMailbox"
        mailbox-capacity = 4
      }
    """)
}

class DispatcherSelectorSpec(config: Config)
    extends ScalaTestWithActorTestKit(config)
    with AnyWordSpecLike
    with LogCapturing {

  def this() = this(DispatcherSelectorSpec.config)

  case class WhatsYourDispatcherAndMailbox(replyTo: ActorRef[(String, MessageQueue)])

  private def behavior: Behavior[WhatsYourDispatcherAndMailbox] =
    Behaviors.setup { context =>
      Behaviors.receiveMessage[WhatsYourDispatcherAndMailbox] {
        case WhatsYourDispatcherAndMailbox(replyTo) =>
          val result = context match {
            case adapter: ActorContextAdapter[_] =>
              adapter.classicActorContext match {
                case cell: ActorCell =>
                  (cell.dispatcher.id, cell.mailbox.messageQueue)
                case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
              }
            case unexpected => throw new RuntimeException(s"Unexpected: $unexpected")
          }

          replyTo ! result
          Behaviors.stopped
      }
    }

  "DispatcherSelector" must {

    "default to unbounded" in {
      val actor = spawn(behavior)
      val (_, mailbox) = actor.ask(this.WhatsYourDispatcherAndMailbox.apply).futureValue
      mailbox shouldBe a[NodeMessageQueue]
    }

    "select a blocking dispatcher" in {
      val actor = spawn(behavior, DispatcherSelector.blocking())
      val (dispatcherId, _) = actor.ask(this.WhatsYourDispatcherAndMailbox.apply).futureValue
      dispatcherId shouldBe Dispatchers.DefaultBlockingDispatcherId
    }

    "select dispatcher from config" in {
      val actor = spawn(behavior, DispatcherSelector.fromConfig("specific-dispatcher"))
      val (dispatcherId, _) = actor.ask(this.WhatsYourDispatcherAndMailbox.apply).futureValue
      dispatcherId shouldBe "specific-dispatcher"
    }

    "detect unknown dispatcher from config" in {
      val probe = createTestProbe[(String, MessageQueue)]()
      LoggingTestKit.error("Spawn failed").expect {
        val ref = spawn(behavior, DispatcherSelector.fromConfig("unknown"))
        probe.expectTerminated(ref)
      }
    }

    "select same dispatcher as parent" in {
      val parent = spawn(SpawnProtocol(), DispatcherSelector.fromConfig("specific-dispatcher"))

      val child = parent.ask { (replyTo: ActorRef[ActorRef[WhatsYourDispatcherAndMailbox]]) =>
        SpawnProtocol.Spawn(behavior, "child", DispatcherSelector.sameAsParent(), replyTo)
      }.futureValue

      val (dispatcherId, _) = child.ask(this.WhatsYourDispatcherAndMailbox.apply).futureValue
      dispatcherId shouldBe "specific-dispatcher"
    }

    "select same dispatcher as parent, several levels" in {
      val guardian = spawn(SpawnProtocol(), DispatcherSelector.fromConfig("specific-dispatcher"))

      val parent = guardian.ask { (replyTo: ActorRef[ActorRef[SpawnProtocol.Command]]) =>
        SpawnProtocol.Spawn(SpawnProtocol(), "parent", DispatcherSelector.sameAsParent(), replyTo)
      }.futureValue

      val child = parent.ask { (replyTo: ActorRef[ActorRef[WhatsYourDispatcherAndMailbox]]) =>
        SpawnProtocol.Spawn(behavior, "child", DispatcherSelector.sameAsParent(), replyTo)
      }.futureValue

      val (dispatcherId, _) = child.ask(this.WhatsYourDispatcherAndMailbox.apply).futureValue
      dispatcherId shouldBe "specific-dispatcher"
    }

    "use default dispatcher if selecting parent dispatcher for user guardian" in {
      val sys = ActorSystem(
        behavior,
        "DispatcherSelectorSpec2",
        ActorSystemSetup.create(BootstrapSetup()),
        DispatcherSelector.sameAsParent())
      try {
        val (dispatcherId, _) = sys.ask(this.WhatsYourDispatcherAndMailbox.apply).futureValue
        dispatcherId shouldBe "akka.actor.default-dispatcher"
      } finally {
        ActorTestKit.shutdown(sys)
      }
    }

    "select an specific dispatcher from config with mailbox selector" in {
      val actor =
        spawn(behavior, DispatcherSelector.fromConfig("specific-dispatcher").withMailboxFromConfig("specific-mailbox"))
      val (dispatcherId, mailbox) = actor.ask(this.WhatsYourDispatcherAndMailbox.apply).futureValue
      mailbox shouldBe a[BoundedMessageQueueSemantics]
      mailbox.asInstanceOf[BoundedNodeMessageQueue].capacity should ===(4)
      dispatcherId shouldBe "specific-dispatcher"
    }

    "select an mailbox via dispatcher config would not work" in {
      val actor = spawn(behavior, DispatcherSelector.fromConfig("specific-dispatcher-with-mailbox"))
      val (dispatcherId, mailbox) = actor.ask(this.WhatsYourDispatcherAndMailbox.apply).futureValue
      mailbox shouldBe a[NodeMessageQueue]
      dispatcherId shouldBe "specific-dispatcher-with-mailbox"
    }
  }
}
