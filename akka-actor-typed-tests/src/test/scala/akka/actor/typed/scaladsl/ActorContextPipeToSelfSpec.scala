/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.{ Failure, Success }
import scala.util.control.NoStackTrace

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.{ ScalaTestWithActorTestKit, TestProbe }
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.Props

object ActorContextPipeToSelfSpec {
  val config = ConfigFactory.parseString("""
      |pipe-to-self-spec-dispatcher {
      |  executor = thread-pool-executor
      |  type = PinnedDispatcher
      |}
    """.stripMargin)
}

final class ActorContextPipeToSelfSpec
    extends ScalaTestWithActorTestKit(ActorContextPipeToSelfSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  "The Scala DSL ActorContext pipeToSelf" must {
    "handle success" in { responseFrom(Future.successful("hi"), "success") should ===("ok: hi") }
    "handle failure" in { responseFrom(Future.failed(Fail), "failure") should ===(s"ko: $Fail") }
    "handle adapted null" in {
      val probe = testKit.createTestProbe[String]()
      val promise = Promise[String]()
      testKit.spawn(Behaviors.setup[String] { ctx =>
        ctx.pipeToSelf(promise.future) {
          case Success(value) =>
            probe.ref ! "adapting"
            value // we're passing on null here
          case Failure(ex) => throw ex
        }

        Behaviors.receiveMessage {
          case msg =>
            probe.ref ! msg
            Behaviors.same

        }
      })

      LoggingTestKit.warn("Adapter function returned null which is not valid as an actor message, ignoring").expect {
        // (probably more likely to happen in Java)
        promise.success(null.asInstanceOf[String])
      }

    }
  }

  object Fail extends NoStackTrace

  private def responseFrom(future: Future[String], postfix: String) = {
    final case class Msg(response: String, selfName: String, threadName: String)

    val probe = TestProbe[Msg]()
    val behavior = Behaviors.setup[Msg] { context =>
      context.pipeToSelf(future) {
        case Success(s) => Msg(s"ok: $s", context.self.path.name, Thread.currentThread().getName)
        case Failure(e) => Msg(s"ko: $e", context.self.path.name, Thread.currentThread().getName)
      }
      Behaviors.receiveMessage { msg =>
        probe.ref ! msg
        Behaviors.stopped
      }
    }
    val name = s"pipe-to-self-spec-$postfix"
    val props = Props.empty.withDispatcherFromConfig("pipe-to-self-spec-dispatcher")

    spawn(behavior, name, props)

    val msg = probe.expectMessageType[Msg]

    msg.selfName should ===(name)
    msg.threadName should startWith("ActorContextPipeToSelfSpec-pipe-to-self-spec-dispatcher")
    msg.response
  }

}
