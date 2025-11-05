/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor
import akka.actor.ActorInitializationException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._

object TransformMessagesSpec {

  // this is the sample from the Scaladoc
  val b: Behavior[Number] =
    Behaviors
      .receive[String] { (_, msg) =>
        println(msg)
        Behaviors.same
      }
      .transformMessages[Number] {
        case _: BigDecimal => s"BigDecimal(&dollar;b)"
        case _: BigInt     => s"BigInteger(&dollar;i)"
        // all other kinds of Number will be `unhandled`
      }
}

class TransformMessagesSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  implicit val classicSystem: actor.ActorSystem = system.toClassic

  def intToString(probe: ActorRef[String]): Behavior[Int] = {
    Behaviors
      .receiveMessage[String] { message =>
        probe ! message
        Behaviors.same
      }
      .transformMessages[Int] {
        case n if n != 13 => n.toString
      }
  }

  "transformMessages" should {

    "transform from an outer type to an inner type" in {
      val probe = TestProbe[String]()
      val ref = spawn(intToString(probe.ref))

      ref ! 42
      probe.expectMessage("42")
    }

    "filter messages" in {
      val probe = TestProbe[String]()
      val ref = spawn(intToString(probe.ref))

      ref ! 42
      ref ! 13
      ref ! 43
      probe.expectMessage("42")
      probe.expectMessage("43")
    }

    "not build up when the same transformMessages is used many times (initially)" in {
      val probe = TestProbe[String]()
      val transformCount = new AtomicInteger(0)

      // sadly the only "same" we can know is if it is the same PF
      val transformPF: PartialFunction[String, String] = {
        case s =>
          transformCount.incrementAndGet()
          s
      }
      def transform(behavior: Behavior[String]): Behavior[String] =
        behavior.transformMessages(transformPF)

      val beh =
        transform(transform(Behaviors.receiveMessage[String] { message =>
          probe.ref ! message
          Behaviors.same
        }))
      val ref = spawn(beh)

      ref ! "42"

      probe.expectMessage("42")
      transformCount.get should ===(1)
    }

    "not build up when the same transformMessages is used many times (recursively)" in {
      val probe = TestProbe[String]()
      val transformCount = new AtomicInteger(0)

      // sadly the only "same" we can know is if it is the same PF
      val transformPF: PartialFunction[String, String] = {
        case s =>
          transformCount.incrementAndGet()
          s
      }
      def transform(behavior: Behavior[String]): Behavior[String] =
        behavior.transformMessages(transformPF)

      def next: Behavior[String] =
        transform(Behaviors.receiveMessage[String] { message =>
          probe.ref ! message
          next
        })

      val ref = spawn(next)

      ref ! "42"
      probe.expectMessage("42")
      transformCount.get should ===(1)

      ref ! "43"
      probe.expectMessage("43")
      transformCount.get should ===(2)

    }

    "not allow mixing different transformMessages in the same behavior stack" in {
      val probe = TestProbe[String]()

      def transform(behavior: Behavior[String]): Behavior[String] =
        behavior.transformMessages[String] {
          case s => s.toLowerCase
        }

      LoggingTestKit.error[ActorInitializationException].expect {
        val ref = spawn(transform(transform(Behaviors.receiveMessage[String] { _ =>
          Behaviors.same
        })))

        probe.expectTerminated(ref, 3.seconds)
      }

    }

    "be possible to combine with inner timers" in {
      val probe = TestProbe[String]()
      val behv = Behaviors
        .withTimers[String] { timers =>
          timers.startSingleTimer("a", 10.millis)
          Behaviors.receiveMessage { msg =>
            probe.ref ! msg
            Behaviors.same
          }
        }
        .transformMessages[String] {
          case msg => msg.toUpperCase()
        }

      val ref = spawn(behv)

      probe.expectMessage("A")

      ref ! "b"
      probe.expectMessage("B")
    }

    "be possible to combine with outer timers" in {
      val probe = TestProbe[String]()
      val behv = Behaviors.withTimers[String] { timers =>
        timers.startSingleTimer("a", 10.millis)
        Behaviors
          .receiveMessage[String] { msg =>
            probe.ref ! msg
            Behaviors.same
          }
          .transformMessages[String] {
            case msg => msg.toUpperCase()
          }
      }

      val ref = spawn(behv)

      probe.expectMessage("A")

      ref ! "b"
      probe.expectMessage("B")
    }
  }

}
