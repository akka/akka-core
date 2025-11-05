/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorThreadSpec.Echo

object ActorThreadSpec {
  object Echo {
    final case class Msg(i: Int, replyTo: ActorRef[Int])

    def apply(): Behavior[Msg] =
      Behaviors.receiveMessage {
        case Msg(i, replyTo) =>
          replyTo ! i
          Behaviors.same
      }
  }

}

class ActorThreadSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  "Actor thread-safety checks" must {

    "detect illegal access to ActorContext from outside" in {
      @volatile var context: ActorContext[String] = null
      val probe = createTestProbe[String]()

      spawn(Behaviors.setup[String] { ctx =>
        // here it's ok
        ctx.children
        context = ctx
        probe.ref ! "initialized"
        Behaviors.empty
      })

      probe.expectMessage("initialized")
      intercept[UnsupportedOperationException] {
        context.children
      }.getMessage should include("Unsupported access to ActorContext")

    }

    "detect illegal access to ActorContext from other thread when processing message" in {
      val probe = createTestProbe[UnsupportedOperationException]()

      val ref = spawn(Behaviors.receive[CountDownLatch] {
        case (context, latch) =>
          Future {
            try {
              context.children
            } catch {
              case e: UnsupportedOperationException =>
                probe.ref ! e
            }
          }(context.executionContext)
          latch.await(5, TimeUnit.SECONDS)
          Behaviors.same
      })

      val l = new CountDownLatch(1)
      try {
        ref ! l
        probe.receiveMessage().getMessage should include("Unsupported access to ActorContext")
      } finally {
        l.countDown()
      }
    }

    "detect illegal access to ActorContext from other thread after processing message" in {
      val probe = createTestProbe[UnsupportedOperationException]()

      val ref = spawn(Behaviors.receive[CountDownLatch] {
        case (context, latch) =>
          Future {
            try {
              latch.await(5, TimeUnit.SECONDS)
              context.children
            } catch {
              case e: UnsupportedOperationException =>
                probe.ref ! e
            }
          }(context.executionContext)

          Behaviors.stopped
      })

      val l = new CountDownLatch(1)
      try {
        ref ! l
        probe.expectTerminated(ref)
      } finally {
        l.countDown()
      }
      probe.receiveMessage().getMessage should include("Unsupported access to ActorContext")
    }

    "detect illegal access from child" in {
      val probe = createTestProbe[UnsupportedOperationException]()

      val ref = spawn(Behaviors.receive[String] {
        case (context, _) =>
          // really bad idea to define a child actor like this
          context.spawnAnonymous(Behaviors.setup[String] { _ =>
            try {
              context.children
            } catch {
              case e: UnsupportedOperationException =>
                probe.ref ! e
            }
            Behaviors.empty
          })
          Behaviors.same
      })

      ref ! "hello"
      probe.receiveMessage().getMessage should include("Unsupported access to ActorContext")
    }

    "allow access from message adapter" in {
      val probe = createTestProbe[String]()
      val echo = spawn(Echo())

      spawn(Behaviors.setup[String] { context =>
        val replyAdapter = context.messageAdapter[Int] { i =>
          // this is allowed because the mapping function is running in the target actor
          context.children
          i.toString
        }
        echo ! Echo.Msg(17, replyAdapter)

        Behaviors.receiveMessage { msg =>
          probe.ref ! msg
          Behaviors.same
        }
      })

      probe.expectMessage("17")
    }

    "allow access from ask response mapper" in {
      val probe = createTestProbe[String]()
      val echo = spawn(Echo())

      spawn(Behaviors.setup[String] { context =>
        context.ask[Echo.Msg, Int](echo, Echo.Msg(18, _)) {
          case Success(i) =>
            // this is allowed because the mapping function is running in the target actor
            context.children
            i.toString
          case Failure(e) => throw e
        }

        Behaviors.receiveMessage { msg =>
          probe.ref ! msg
          Behaviors.same
        }
      })

      probe.expectMessage("18")
    }

    "detect wrong context in construction of AbstractBehavior" in {
      val probe = createTestProbe[String]()
      val ref = spawn(Behaviors.setup[String] { context =>
        // missing setup new AbstractBehavior and passing in parent's context
        val child = context.spawnAnonymous(new AbstractBehavior[String](context) {
          override def onMessage(msg: String): Behavior[String] = {
            probe.ref ! msg
            Behaviors.same
          }
        })

        Behaviors.receiveMessage { msg =>
          child ! msg
          Behaviors.same
        }
      })

      // 2 occurrences because one from PostStop also
      LoggingTestKit
        .error[IllegalStateException]
        .withMessageContains("was created with wrong ActorContext")
        .withOccurrences(2)
        .expect {
          // it's not detected when spawned, but when processing message
          ref ! "hello"
          probe.expectNoMessage()
        }
    }

    "detect illegal access from AbstractBehavior constructor" in {
      val probe = createTestProbe[UnsupportedOperationException]()

      spawn(Behaviors.setup[String] { context =>
        context.spawnAnonymous(
          Behaviors.setup[String](_ =>
            // wrongly using parent's context
            new AbstractBehavior[String](context) {
              try {
                this.context.children
              } catch {
                case e: UnsupportedOperationException =>
                  probe.ref ! e
              }

              override def onMessage(msg: String): Behavior[String] = {
                Behaviors.same
              }
            }))

        Behaviors.empty
      })

      probe.receiveMessage().getMessage should include("Unsupported access to ActorContext")
    }

    "detect sharing of same AbstractBehavior instance" in {
      // extremely contrived example, but the creativity among users can be great
      @volatile var behv: Behavior[CountDownLatch] = null

      val ref1 = spawn(Behaviors.setup[CountDownLatch] { context =>
        behv = new AbstractBehavior[CountDownLatch](context) {
          override def onMessage(latch: CountDownLatch): Behavior[CountDownLatch] = {
            latch.await(5, TimeUnit.SECONDS)
            Behaviors.same
          }
        }
        behv
      })

      eventually(behv shouldNot equal(null))

      // spawning same instance again
      val ref2 = spawn(behv)

      val latch1 = new CountDownLatch(1)
      try {
        ref1 ! latch1

        // 2 occurrences because one from PostStop also
        LoggingTestKit
          .error[IllegalStateException]
          .withMessageContains("was created with wrong ActorContext")
          .withOccurrences(2)
          .expect {
            ref2 ! new CountDownLatch(0)
          }
      } finally {
        latch1.countDown()
      }
    }

  }

}
