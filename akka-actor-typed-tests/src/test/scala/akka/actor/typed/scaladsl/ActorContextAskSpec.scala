/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl

import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.{ ActorRef, PostStop, Props }
import akka.pattern.StatusReply

object ActorContextAskSpec {
  val config = ConfigFactory.parseString("""
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

class ActorContextAskSpec
    extends ScalaTestWithActorTestKit(ActorContextAskSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  "The Scala DSL ActorContext" must {

    "provide a safe ask" in {
      case class Ping(sender: ActorRef[Pong])
      case class Pong(selfName: String, threadName: String)

      val pingPong = spawn(Behaviors.receive[Ping] { (context, message) =>
        message.sender ! Pong(context.self.path.name, Thread.currentThread().getName)
        Behaviors.same
      }, "ping-pong", Props.empty.withDispatcherFromConfig("ping-pong-dispatcher"))

      val probe = TestProbe[Pong]()

      val snitch = Behaviors.setup[Pong] { context =>
        // Timeout comes from TypedAkkaSpec

        context.ask(pingPong, Ping.apply) {
          case Success(_)  => Pong(context.self.path.name + "1", Thread.currentThread().getName)
          case Failure(ex) => throw ex
        }

        Behaviors.receiveMessage { pong =>
          probe.ref ! pong
          Behaviors.same
        }
      }

      spawn(snitch, "snitch", Props.empty.withDispatcherFromConfig("snitch-dispatcher"))

      val pong = probe.receiveMessage()

      pong.selfName should ===("snitch1")
      pong.threadName should startWith("ActorContextAskSpec-snitch-dispatcher")
    }

    "fail actor when mapping does not match response" in {
      val probe = TestProbe[AnyRef]()

      trait Protocol
      case class Ping(respondTo: ActorRef[Pong.type]) extends Protocol
      case object Pong extends Protocol

      val pingPong = spawn(Behaviors.receiveMessagePartial[Protocol] {
        case Ping(respondTo) =>
          respondTo ! Pong
          Behaviors.same
      })

      val snitch = Behaviors.setup[AnyRef] { context =>
        context.ask(pingPong, Ping.apply) {
          case Success(message) => throw new NotImplementedError(message.toString)
          case Failure(x)       => x
        }

        Behaviors
          .receivePartial[AnyRef] {
            case (_, message) =>
              probe.ref ! message
              Behaviors.same
          }
          .receiveSignal {

            case (_, PostStop) =>
              probe.ref ! "stopped"
              Behaviors.same
          }
      }

      LoggingTestKit.error[NotImplementedError].withMessageContains("Pong").expect {
        spawn(snitch)
      }

      // the exception should cause a failure which should stop the actor
      probe.expectMessage("stopped")
    }

    "deal with timeouts in ask" in {
      val probe = TestProbe[AnyRef]()
      val snitch = Behaviors.setup[AnyRef] { context =>
        context.ask[String, String](system.deadLetters, _ => "boo") {
          case Success(m) => m
          case Failure(x) => x
        }(10.millis, implicitly[ClassTag[String]])

        Behaviors.receiveMessage { message =>
          probe.ref ! message
          Behaviors.same
        }
      }

      spawn(snitch)

      val exc = probe.expectMessageType[TimeoutException]
      exc.getMessage should include("had already been terminated")
    }

    "must timeout if recipient doesn't reply in time" in {
      val target = spawn(Behaviors.ignore[String])
      val probe = TestProbe[AnyRef]()
      val snitch = Behaviors.setup[AnyRef] { context =>
        context.ask[String, String](target, _ => "bar") {
          case Success(m) => m
          case Failure(x) => x
        }(10.millis, implicitly[ClassTag[String]])

        Behaviors.receiveMessage { message =>
          probe.ref ! message
          Behaviors.same
        }
      }

      spawn(snitch)

      val exc = probe.expectMessageType[TimeoutException]
      exc.getMessage should startWith("Ask timed out on")
      exc.getMessage should include(target.path.toString)
      exc.getMessage should include("[java.lang.String]") // message class
      exc.getMessage should include("[10 ms]") // timeout
    }

    "receive replies in same order as sent" in {
      case class Ping(n: Int, replyTo: ActorRef[Pong])
      case class Pong(n: Int)

      val N = 100
      val probe = TestProbe[Pong]()

      val pingPong = spawn(Behaviors.receiveMessage[Ping] { message =>
        message.replyTo ! Pong(message.n)
        Behaviors.same
      })

      val snitch = Behaviors.setup[Pong] { context =>
        (1 to N).foreach { n =>
          context.ask[Ping, Pong](pingPong, Ping(n, _)) {
            case Success(pong) => pong
            case Failure(ex)   => throw ex
          }
        }

        Behaviors.receiveMessage { pong =>
          probe.ref ! pong
          Behaviors.same
        }
      }

      spawn(snitch)

      probe.receiveMessages(N).map(_.n) should ===(1 to N)
    }

    "unwrap successful StatusReply messages using askWithStatus" in {
      case class Ping(ref: ActorRef[StatusReply[Pong.type]])
      case object Pong

      val probe = createTestProbe[Any]()
      spawn(Behaviors.setup[Pong.type] { ctx =>
        ctx.askWithStatus(probe.ref, Ping.apply) {
          case Success(Pong) => Pong
          case Failure(ex)   => throw ex
        }

        Behaviors.receiveMessage {
          case Pong =>
            probe.ref ! "got pong"
            Behaviors.same
        }
      })

      val replyTo = probe.expectMessageType[Ping].ref
      replyTo ! StatusReply.Success(Pong)
      probe.expectMessage("got pong")
    }

    "unwrap error message StatusReply messages using askWithStatus" in {
      case class Ping(ref: ActorRef[StatusReply[Pong.type]])
      case object Pong

      val probe = createTestProbe[Any]()
      spawn(Behaviors.setup[Throwable] { ctx =>
        ctx.askWithStatus(probe.ref, Ping.apply) {
          case Failure(ex) => ex
          case wat         => throw new IllegalArgumentException(s"Unexpected response $wat")
        }

        Behaviors.receiveMessage {
          case ex: Throwable =>
            probe.ref ! s"got error: ${ex.getClass.getName}, ${ex.getMessage}"
            Behaviors.same
        }
      })

      val replyTo = probe.expectMessageType[Ping].ref
      replyTo ! StatusReply.Error("boho")
      probe.expectMessage("got error: akka.pattern.StatusReply$ErrorMessage, boho")
    }

    "unwrap error with custom exception StatusReply messages using askWithStatus" in {
      case class Ping(ref: ActorRef[StatusReply[Pong.type]])
      case object Pong

      val probe = createTestProbe[Any]()
      case class Message(any: Any)
      spawn(Behaviors.setup[Throwable] { ctx =>
        ctx.askWithStatus(probe.ref, Ping.apply) {
          case Failure(ex) => ex
          case wat         => throw new IllegalArgumentException(s"Unexpected response $wat")
        }

        Behaviors.receiveMessage {
          case ex: Throwable =>
            probe.ref ! s"got error: ${ex.getClass.getName}, ${ex.getMessage}"
            Behaviors.same
        }
      })

      val replyTo = probe.expectMessageType[Ping].ref
      replyTo ! StatusReply.Error(TestException("boho"))
      probe.expectMessage("got error: akka.actor.testkit.typed.TestException, boho")
    }

  }

}
