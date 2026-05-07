/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.actor.testkit.typed.scaladsl

//#manual-scheduling-simple
import scala.concurrent.duration._
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.ManualTime
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

class ManualTimerExampleSpec
    extends ScalaTestWithActorTestKit(ManualTime.config)
    with AnyWordSpecLike
    with LogCapturing {

  val manualTime: ManualTime = ManualTime()

  "A timer" must {
    "schedule non-repeated ticks" in {
      case object Tick
      case object Tock

      val probe = TestProbe[Tock.type]()
      val behavior = Behaviors.withTimers[Tick.type] { timer =>
        timer.startSingleTimer(Tick, 10.millis)
        Behaviors.receiveMessage { _ =>
          probe.ref ! Tock
          Behaviors.same
        }
      }

      spawn(behavior)

      manualTime.expectNoMessageFor(9.millis, probe)

      manualTime.timePasses(2.millis)
      probe.expectMessage(Tock)

      manualTime.expectNoMessageFor(10.seconds, probe)
    }
    //#manual-scheduling-simple

    "schedule repeated ticks" in {
      case object Tick
      case object Tock

      val probe = TestProbe[Tock.type]()
      val behavior = Behaviors.withTimers[Tick.type] { timer =>
        timer.startTimerWithFixedDelay(Tick, 10.millis)
        Behaviors.receiveMessage { _ =>
          probe.ref ! Tock
          Behaviors.same
        }
      }

      spawn(behavior)

      for (_ <- 0 until 5) {
        manualTime.expectNoMessageFor(9.millis, probe)

        manualTime.timePasses(1.milli)
        probe.expectMessage(Tock)
      }
    }

    "replace timer" in {
      sealed trait Command
      case class Tick(n: Int) extends Command
      case class SlowThenBump(nextCount: Int) extends Command
      sealed trait Event
      case object Started extends Event
      case class Tock(n: Int) extends Event
      case object SlowThenBumpAck extends Event

      val probe = TestProbe[Event]("evt")
      val interval = 10.millis

      val behavior = Behaviors.withTimers[Command] { timer =>
        timer.startTimerWithFixedDelay("T", Tick(1), interval)

        probe.ref ! Started
        Behaviors.receiveMessage {
          case Tick(n) =>
            probe.ref ! Tock(n)
            Behaviors.same
          case SlowThenBump(nextCount) =>
            manualTime.timePasses(interval)
            timer.startTimerWithFixedDelay("T", Tick(nextCount), interval)
            probe.ref ! SlowThenBumpAck
            Behaviors.same
        }
      }

      val ref = spawn(behavior)

      // make sure we actually started the timer before we change the time
      probe.expectMessage(Started)

      manualTime.timePasses(11.millis)
      probe.expectMessage(Tock(1))

      // next Tock(1) enqueued in mailbox, but should be discarded because of new timer
      ref ! SlowThenBump(2)
      probe.expectMessage(SlowThenBumpAck)
      manualTime.expectNoMessageFor(9.millis, probe)

      manualTime.timePasses(2.millis)
      probe.expectMessage(Tock(2))
    }

    //#manual-scheduling-simple
  }
}
//#manual-scheduling-simple
