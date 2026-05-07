/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import akka.actor.DeadLetter
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.FishingOutcomes
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.Props
import akka.actor.typed.internal.AdaptMessage

object MessageAdapterSpec {
  val config = ConfigFactory.parseString("""
      akka.log-dead-letters = on
      ping-pong-dispatcher {
        executor = thread-pool-executor
        type = PinnedDispatcher
      }
      snitch-dispatcher {
        executor = thread-pool-executor
        type = PinnedDispatcher
      }
    """)
}

class MessageAdapterSpec
    extends ScalaTestWithActorTestKit(MessageAdapterSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  private val log = LoggerFactory.getLogger(getClass)

  private def assertDeadLetter(deadLetterProbe: TestProbe[DeadLetter], expectedMessage: Any): Unit = {
    deadLetterProbe.fishForMessage(deadLetterProbe.remainingOrDefault, s"looking for DeadLetter $expectedMessage") {
      deadLetter =>
        deadLetter.message match {
          case AdaptMessage(msg, _) if msg.getClass == expectedMessage.getClass =>
            msg shouldBe expectedMessage
            FishingOutcomes.complete
          case msg if msg.getClass == expectedMessage.getClass =>
            msg shouldBe expectedMessage
            FishingOutcomes.complete
          case other =>
            // something else, not from this test
            log.debug(s"Ignoring other DeadLetter: $other")
            FishingOutcomes.continueAndIgnore
        }
    }
  }

  "Message adapters" must {

    "map messages inside the actor" in {
      case class Ping(sender: ActorRef[Response])
      trait Response
      case class Pong(selfName: String, threadName: String) extends Response

      case class AnotherPong(selfName: String, threadName: String)

      val pingPong = spawn(Behaviors.receive[Ping] { (context, message) =>
        message.sender ! Pong(context.self.path.name, Thread.currentThread().getName)
        Behaviors.same
      }, "ping-pong", Props.empty.withDispatcherFromConfig("ping-pong-dispatcher"))

      val probe = TestProbe[AnotherPong]()

      val snitch = Behaviors.setup[AnotherPong] { context =>
        val replyTo =
          context.messageAdapter[Response](_ => AnotherPong(context.self.path.name, Thread.currentThread().getName))
        pingPong ! Ping(replyTo)

        // also verify the internal spawnMessageAdapter
        val replyTo2: ActorRef[Response] =
          context.spawnMessageAdapter(_ => AnotherPong(context.self.path.name, Thread.currentThread().getName))
        pingPong ! Ping(replyTo2)

        Behaviors.receiveMessage { anotherPong =>
          probe.ref ! anotherPong
          Behaviors.same
        }
      }

      spawn(snitch, "snitch", Props.empty.withDispatcherFromConfig("snitch-dispatcher"))

      val response1 = probe.receiveMessage()
      response1.selfName should ===("snitch")
      response1.threadName should startWith("MessageAdapterSpec-snitch-dispatcher")

      // and from the spawnMessageAdapter
      val response2 = probe.receiveMessage()
      response2.selfName should ===("snitch")
      response2.threadName should startWith("MessageAdapterSpec-snitch-dispatcher")
    }

    "use the right adapter" in {
      trait Ping
      case class Ping1(sender: ActorRef[Pong1]) extends Ping
      case class Ping2(sender: ActorRef[Pong2]) extends Ping
      case class Ping3(sender: ActorRef[Int]) extends Ping
      trait Response
      case class Pong1(greeting: String) extends Response
      case class Pong2(greeting: String) extends Response
      case class Pong3(greeting: Int) extends Response

      case class Wrapped(qualifier: String, response: Response)

      val pingPong = spawn(Behaviors.receiveMessagePartial[Ping] {
        case Ping1(sender) =>
          sender ! Pong1("hello-1")
          Behaviors.same
        case Ping2(sender) =>
          sender ! Pong2("hello-2")
          Behaviors.same
        case Ping3(sender) =>
          sender ! 3
          Behaviors.same
      })

      val probe = TestProbe[Wrapped]()

      val snitch = Behaviors.setup[Wrapped] { context =>
        context.messageAdapter[Response](pong => Wrapped(qualifier = "wrong", pong)) // this is replaced
        val replyTo1: ActorRef[Response] = context.messageAdapter(pong => Wrapped(qualifier = "1", pong))
        val replyTo2 = context.messageAdapter[Pong2](pong => Wrapped(qualifier = "2", pong))
        val replyTo3 =
          context.messageAdapter[Int](intValue => Wrapped(qualifier = intValue.toString, response = Pong3(intValue)))
        pingPong ! Ping1(replyTo1)
        pingPong ! Ping2(replyTo2)
        pingPong ! Ping3(replyTo3)

        Behaviors.receiveMessage { wrapped =>
          probe.ref ! wrapped
          Behaviors.same
        }
      }

      spawn(snitch)

      probe.expectMessage(Wrapped("1", Pong1("hello-1")))
      probe.expectMessage(Wrapped("2", Pong2("hello-2")))
      probe.expectMessage(Wrapped("3", Pong3(3)))
    }

    "not break if wrong/unknown response type" in {
      sealed trait Ping
      case class Ping1(sender: ActorRef[Pong1]) extends Ping
      case class Ping2(sender: ActorRef[Pong2]) extends Ping
      sealed trait Response
      case class Pong1(greeting: String) extends Response
      case class Pong2(greeting: String) extends Response

      case class Wrapped(qualifier: String, response: Response)

      val pingPong = spawn(Behaviors.receiveMessage[Ping] {
        case Ping1(sender) =>
          sender ! Pong1("hello-1")
          Behaviors.same
        case Ping2(sender) =>
          // doing something terribly wrong
          sender ! Pong2("hello-2")
          Behaviors.same
      })

      val unhandledProbe = createUnhandledMessageProbe()
      val probe = TestProbe[Wrapped]()

      val snitch = Behaviors.setup[Wrapped] { context =>
        val replyTo1 = context.messageAdapter[Pong1](pong => Wrapped(qualifier = "1", pong))
        pingPong ! Ping1(replyTo1)
        // doing something terribly wrong
        // Pong2 message adapter not registered
        pingPong ! Ping2(replyTo1.asInstanceOf[ActorRef[Pong2]])
        pingPong ! Ping1(replyTo1)

        Behaviors.receiveMessage { wrapped =>
          probe.ref ! wrapped
          Behaviors.same
        }
      }

      spawn(snitch)
      unhandledProbe.receiveMessage()

      probe.expectMessage(Wrapped("1", Pong1("hello-1")))
      // hello-2 discarded because it was wrong type
      probe.expectMessage(Wrapped("1", Pong1("hello-1")))
    }

    "stop when exception from adapter" in {
      case class Ping(sender: ActorRef[Pong])
      case class Pong(greeting: String)
      case class Wrapped(count: Int, response: Pong)

      val pingPong = spawn(Behaviors.receiveMessage[Ping] { ping =>
        ping.sender ! Pong("hello")
        Behaviors.same
      })

      val probe = TestProbe[Any]()

      val snitch = Behaviors.setup[Wrapped] { context =>
        var count = 0
        val replyTo = context.messageAdapter[Pong] { pong =>
          count += 1
          if (count == 3) throw new TestException("boom")
          else Wrapped(count, pong)
        }
        (1 to 4).foreach { _ =>
          pingPong ! Ping(replyTo)
        }

        Behaviors
          .receiveMessage[Wrapped] { wrapped =>
            probe.ref ! wrapped
            Behaviors.same
          }
          .receiveSignal {
            case (_, PostStop) =>
              probe.ref ! "stopped"
              Behaviors.same
          }
      }

      val ref = spawn(snitch)

      probe.expectMessage(Wrapped(1, Pong("hello")))
      probe.expectMessage(Wrapped(2, Pong("hello")))
      // exception was thrown for  3

      probe.expectMessage("stopped")
      probe.expectTerminated(ref)
    }

    "not catch exception thrown after adapter, when processing the message" in {
      case class Ping(n: Int, sender: ActorRef[Pong])
      case class Pong(greeting: String)
      case class Wrapped(response: Pong)

      val pingPong = spawn(Behaviors.receiveMessage[Ping] { ping =>
        ping.sender ! Pong(s"hello-${ping.n}")
        Behaviors.same
      })

      val probe = TestProbe[Any]()

      val snitch = Behaviors.setup[Wrapped] { context =>
        val replyTo = context.messageAdapter[Pong] { pong =>
          Wrapped(pong)
        }
        (1 to 5).foreach { n =>
          pingPong ! Ping(n, replyTo)
        }

        def behv(count: Int): Behavior[Wrapped] =
          Behaviors
            .receiveMessage[Wrapped] { _ =>
              probe.ref ! count
              if (count == 3) {
                throw TestException("boom")
              }
              behv(count + 1)
            }
            .receiveSignal {
              case (_, PostStop) =>
                probe.ref ! "stopped"
                Behaviors.same
            }

        behv(count = 1)
      }

      val deadLetterProbe = testKit.createDeadLetterProbe()

      // Not expecting "Exception thrown out of adapter. Stopping myself"
      val ref = LoggingTestKit.error[TestException].withMessageContains("boom").expect {
        spawn(snitch)
      }

      probe.expectMessage(1)
      probe.expectMessage(2)
      probe.expectMessage(3)
      // exception was thrown for 3
      probe.expectMessage("stopped")
      probe.expectTerminated(ref)

      assertDeadLetter(deadLetterProbe, Pong("hello-4"))
      assertDeadLetter(deadLetterProbe, Pong("hello-5"))
    }

  }

  "redirect to DeadLetter after termination" in {
    case class Ping(sender: ActorRef[Pong])
    case class Pong(greeting: String)
    case class PingReply(response: Pong)

    val pingProbe = createTestProbe[Ping]()

    val deadLetterProbe = testKit.createDeadLetterProbe()

    val snitch = Behaviors.setup[PingReply] { context =>
      val replyTo = context.messageAdapter[Pong](PingReply.apply)
      pingProbe.ref ! Ping(replyTo)
      Behaviors.stopped
    }
    val ref = spawn(snitch)

    createTestProbe().expectTerminated(ref)

    pingProbe.receiveMessage().sender ! Pong("hi")
    assertDeadLetter(deadLetterProbe, Pong("hi"))
  }

}
