/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.annotation.tailrec

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.scaladsl.Behaviors

/**
 * Background: Originally there was an `OrElseBehavior` that could compose two `Behavior`, but that
 * wasn't safe when used together with narrow so `OrElseBehavior` was removed. Kept this
 * test to illustrate how functions can be used for composition.
 */
object OrElseSpec {

  sealed trait Ping
  final case class Ping1(replyTo: ActorRef[Pong]) extends Ping
  final case class Ping2(replyTo: ActorRef[Pong]) extends Ping
  final case class Ping3(replyTo: ActorRef[Pong]) extends Ping
  final case class PingInfinite(replyTo: ActorRef[Pong]) extends Ping

  case class Pong(counter: Int)

  object CompositionWithFunction {

    def ping(counters: Map[String, Int]): Behavior[Ping] = {

      val ping1: Ping => Behavior[Ping] = {
        case Ping1(replyTo: ActorRef[Pong]) =>
          val newCounters = counters.updated("ping1", counters.getOrElse("ping1", 0) + 1)
          replyTo ! Pong(newCounters("ping1"))
          ping(newCounters)
        case _ => Behaviors.unhandled
      }

      val ping2: Ping => Behavior[Ping] = {
        case Ping2(replyTo: ActorRef[Pong]) =>
          val newCounters = counters.updated("ping2", counters.getOrElse("ping2", 0) + 1)
          replyTo ! Pong(newCounters("ping2"))
          ping(newCounters)
        case _ => Behaviors.unhandled
      }

      val ping3: Ping => Behavior[Ping] = {
        case Ping3(replyTo: ActorRef[Pong]) =>
          val newCounters = counters.updated("ping3", counters.getOrElse("ping3", 0) + 1)
          replyTo ! Pong(newCounters("ping3"))
          ping(newCounters)
        case _ => Behaviors.unhandled
      }

      val pingHandlers: List[Ping => Behavior[Ping]] = ping1 :: ping2 :: ping3 :: Nil

      // this could be provided as a general purpose utility
      @tailrec def handle(command: Ping, handlers: List[Ping => Behavior[Ping]]): Behavior[Ping] = {
        handlers match {
          case Nil => Behaviors.unhandled
          case head :: tail =>
            val next = head(command)
            if (Behavior.isUnhandled(next)) handle(command, tail)
            else next
        }
      }

      Behaviors.receiveMessage(command => handle(command, pingHandlers))
    }
  }

  object CompositionWithPartialFunction {

    def ping(counters: Map[String, Int]): Behavior[Ping] = {

      val ping1: PartialFunction[Ping, Behavior[Ping]] = {
        case Ping1(replyTo: ActorRef[Pong]) =>
          val newCounters = counters.updated("ping1", counters.getOrElse("ping1", 0) + 1)
          replyTo ! Pong(newCounters("ping1"))
          ping(newCounters)
      }

      val ping2: PartialFunction[Ping, Behavior[Ping]] = {
        case Ping2(replyTo: ActorRef[Pong]) =>
          val newCounters = counters.updated("ping2", counters.getOrElse("ping2", 0) + 1)
          replyTo ! Pong(newCounters("ping2"))
          ping(newCounters)
      }

      val ping3: PartialFunction[Ping, Behavior[Ping]] = {
        case Ping3(replyTo: ActorRef[Pong]) =>
          val newCounters = counters.updated("ping3", counters.getOrElse("ping3", 0) + 1)
          replyTo ! Pong(newCounters("ping3"))
          ping(newCounters)
      }

      val pingHandlers: List[PartialFunction[Ping, Behavior[Ping]]] = ping1 :: ping2 :: ping3 :: Nil

      // this could be provided as a general purpose utility
      def handle(command: Ping, handlers: List[PartialFunction[Ping, Behavior[Ping]]]): Behavior[Ping] = {
        handlers match {
          case Nil          => Behaviors.unhandled
          case head :: tail => head.applyOrElse(command, handle(_, tail))
        }
      }

      Behaviors.receiveMessage(command => handle(command, pingHandlers))
    }
  }

  object CompositionWithInterceptor {
    // This is much more useful for independent composition than the original OrElseBehavior,
    // but it has the same type safety problem when combined with narrow
    class OrElseInterceptor(initialHandlers: Vector[Behavior[Ping]]) extends BehaviorInterceptor[Ping, Ping] {

      private var handlers: Vector[Behavior[Ping]] = initialHandlers

      override def aroundReceive(
          ctx: TypedActorContext[Ping],
          msg: Ping,
          target: BehaviorInterceptor.ReceiveTarget[Ping]): Behavior[Ping] = {

        @tailrec def handle(i: Int): Behavior[Ping] = {
          if (i == handlers.size)
            target(ctx, msg)
          else {
            val next = Behavior.interpretMessage(handlers(i), ctx, msg)
            if (Behavior.isUnhandled(next))
              handle(i + 1)
            else if (!Behavior.isAlive(next))
              next
            else {
              handlers = handlers.updated(i, Behavior.canonicalize(next, handlers(i), ctx))
              Behaviors.same
            }
          }
        }

        handle(0)
      }

      // could do same for signals

      override def isSame(other: BehaviorInterceptor[Any, Any]): Boolean = {
        other.isInstanceOf[OrElseInterceptor]

      }
    }

    def ping1(count: Int): Behavior[Ping] = Behaviors.receiveMessagePartial {
      case Ping1(replyTo: ActorRef[Pong]) =>
        val newCount = count + 1
        replyTo ! Pong(newCount)
        // note that this is nice since it doesn't have to know anything about the shared
        // state (counters Map) as in the other examples, and it can switch to it's own
        // new behavior
        ping1(newCount)
    }

    def ping2(count: Int): Behavior[Ping] = Behaviors.receiveMessage {
      case Ping2(replyTo: ActorRef[Pong]) =>
        val newCount = count + 1
        replyTo ! Pong(newCount)
        ping2(newCount)
      case _ => Behaviors.unhandled
    }

    def ping3(count: Int): Behavior[Ping] = Behaviors.receiveMessagePartial {
      case Ping3(replyTo: ActorRef[Pong]) =>
        val newCount = count + 1
        replyTo ! Pong(newCount)
        ping3(newCount)
    }

    def ping(): Behavior[Ping] = {
      val handlers = Vector(ping1(0), ping2(0), ping3(0))
      Behaviors.intercept(() => new OrElseInterceptor(handlers))(Behaviors.empty)
    }

  }

}

class OrElseSpec extends AnyWordSpec with Matchers with LogCapturing {

  import OrElseSpec._

  "Behavior that is composed" must {

    def testFirstMatching(behavior: Behavior[Ping]): Unit = {
      val inbox = TestInbox[Pong]("reply")
      val testkit = BehaviorTestKit(behavior)
      testkit.run(Ping1(inbox.ref))
      inbox.receiveMessage() should ===(Pong(1))
      testkit.run(Ping1(inbox.ref))
      inbox.receiveMessage() should ===(Pong(2))

      testkit.run(Ping2(inbox.ref))
      inbox.receiveMessage() should ===(Pong(1))
      testkit.run(Ping3(inbox.ref))
      inbox.receiveMessage() should ===(Pong(1))
      testkit.run(Ping2(inbox.ref))
      inbox.receiveMessage() should ===(Pong(2))
      testkit.run(Ping3(inbox.ref))
      inbox.receiveMessage() should ===(Pong(2))

      testkit.run(Ping1(inbox.ref))
      inbox.receiveMessage() should ===(Pong(3))
    }

    "use first matching function" in {
      testFirstMatching(CompositionWithFunction.ping(Map.empty))
    }

    "use first matching partial function" in {
      testFirstMatching(CompositionWithPartialFunction.ping(Map.empty))
    }

    "use first matching behavior via delegating interceptor" in {
      testFirstMatching(CompositionWithInterceptor.ping())
    }
  }

}
