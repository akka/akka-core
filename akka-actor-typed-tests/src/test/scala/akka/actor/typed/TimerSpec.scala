/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.DeadLetter
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl._
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.testkit.TimingTest

class TimerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  sealed trait Command
  case class Tick(n: Int) extends Command
  case object Bump extends Command
  case class SlowThenBump(latch: CountDownLatch) extends Command
  case object End extends Command
  case class Throw(e: Throwable) extends Command
  case object Cancel extends Command
  case class SlowThenThrow(latch: CountDownLatch, e: Throwable) extends Command

  sealed trait Event
  case class Tock(n: Int) extends Event
  case class GotPostStop(timerActive: Boolean) extends Event
  case class GotPreRestart(timerActive: Boolean) extends Event
  case object Cancelled extends Event

  class Exc extends RuntimeException("simulated exc") with NoStackTrace

  val interval = 1.second

  def target(monitor: ActorRef[Event], timer: TimerScheduler[Command], bumpCount: Int): Behavior[Command] = {
    def bump(): Behavior[Command] = {
      val nextCount = bumpCount + 1
      timer.startTimerWithFixedDelay("T", Tick(nextCount), interval)
      target(monitor, timer, nextCount)
    }

    Behaviors
      .receive[Command] { (_, cmd) =>
        cmd match {
          case Tick(n) =>
            monitor ! Tock(n)
            Behaviors.same
          case Bump =>
            bump()
          case SlowThenBump(latch) =>
            latch.await(10, TimeUnit.SECONDS)
            bump()
          case End =>
            Behaviors.stopped
          case Cancel =>
            timer.cancel("T")
            monitor ! Cancelled
            Behaviors.same
          case Throw(e) =>
            throw e
          case SlowThenThrow(latch, e) =>
            latch.await(10, TimeUnit.SECONDS)
            throw e
        }
      }
      .receiveSignal {
        case (_, PreRestart) =>
          monitor ! GotPreRestart(timer.isTimerActive("T"))
          Behaviors.same
        case (_, PostStop) =>
          monitor ! GotPostStop(timer.isTimerActive("T"))
          Behaviors.same
      }
  }

  "A timer" must {

    "schedule non-repeated ticks" taggedAs TimingTest in {
      val probe = TestProbe[Event]("evt")
      val behv = Behaviors.withTimers[Command] { timer =>
        timer.startSingleTimer(Tick(1), 10.millis)
        target(probe.ref, timer, 1)
      }

      val ref = spawn(behv)
      probe.expectMessage(Tock(1))
      probe.expectNoMessage()

      ref ! End
      probe.expectMessage(GotPostStop(false))
    }

    "schedule repeated ticks" taggedAs TimingTest in {
      val probe = TestProbe[Event]("evt")
      val behv = Behaviors.withTimers[Command] { timer =>
        timer.startTimerWithFixedDelay(Tick(1), interval)
        target(probe.ref, timer, 1)
      }

      val ref = spawn(behv)
      probe.within((interval * 4) - 100.millis) {
        probe.expectMessage(Tock(1))
        probe.expectMessage(Tock(1))
        probe.expectMessage(Tock(1))
      }

      ref ! End
      probe.expectMessage(GotPostStop(false))
    }

    "replace timer" taggedAs TimingTest in {
      val probe = TestProbe[Event]("evt")
      val behv = Behaviors.withTimers[Command] { timer =>
        timer.startTimerWithFixedDelay("T", Tick(1), interval)
        target(probe.ref, timer, 1)
      }

      val ref = spawn(behv)
      probe.expectMessage(Tock(1))
      val latch = new CountDownLatch(1)
      // next Tock(1) enqueued in mailboxed, but should be discarded because of new timer
      ref ! SlowThenBump(latch)
      probe.expectNoMessage(interval + 100.millis.dilated)
      latch.countDown()
      probe.expectMessage(Tock(2))

      ref ! End
      probe.expectMessage(GotPostStop(false))
    }

    "cancel timer" taggedAs TimingTest in {
      val probe = TestProbe[Event]("evt")
      val behv = Behaviors.withTimers[Command] { timer =>
        timer.startTimerWithFixedDelay("T", Tick(1), interval)
        target(probe.ref, timer, 1)
      }

      val ref = spawn(behv)
      probe.expectMessage(Tock(1))
      ref ! Cancel
      probe.fishForMessage(3.seconds) {
        // we don't know that we will see exactly one tock
        case _: Tock => FishingOutcomes.continue
        // but we know that after we saw Cancelled we won't see any more
        case Cancelled => FishingOutcomes.complete
        case message   => FishingOutcomes.fail(s"unexpected message: $message")
      }
      probe.expectNoMessage(interval + 100.millis.dilated)

      ref ! End
      probe.expectMessage(GotPostStop(false))
    }

    "discard timers from old incarnation after restart, alt 1" taggedAs TimingTest in {
      val probe = TestProbe[Event]("evt")
      val startCounter = new AtomicInteger(0)
      val behv = Behaviors
        .supervise(Behaviors.withTimers[Command] { timer =>
          timer.startTimerWithFixedDelay("T", Tick(startCounter.incrementAndGet()), interval)
          target(probe.ref, timer, 1)
        })
        .onFailure[Exception](SupervisorStrategy.restart)

      val ref = spawn(behv)
      probe.expectMessage(Tock(1))

      val latch = new CountDownLatch(1)
      LoggingTestKit.error[Exc].expect {
        // next Tock(1) is enqueued in mailbox, but should be discarded by new incarnation
        ref ! SlowThenThrow(latch, new Exc)

        probe.expectNoMessage(interval + 100.millis.dilated)
        latch.countDown()
        probe.expectMessage(GotPreRestart(false))
        probe.expectNoMessage(interval / 2)
        probe.expectMessage(Tock(2))
      }
      ref ! End
      probe.expectMessage(GotPostStop(false))
    }

    "discard timers from old incarnation after restart, alt 2" taggedAs TimingTest in {
      val probe = TestProbe[Event]("evt")
      val behv = Behaviors
        .supervise(Behaviors.withTimers[Command] { timer =>
          timer.startTimerWithFixedDelay("T", Tick(1), interval)
          target(probe.ref, timer, 1)
        })
        .onFailure[Exception](SupervisorStrategy.restart)

      val ref = spawn(behv)
      probe.expectMessage(Tock(1))
      // change state so that we see that the restart starts over again
      ref ! Bump

      probe.expectMessage(Tock(2))

      LoggingTestKit.error[Exc].expect {
        val latch = new CountDownLatch(1)
        // next Tock(2) is enqueued in mailbox, but should be discarded by new incarnation
        ref ! SlowThenThrow(latch, new Exc)
        probe.expectNoMessage(interval + 100.millis.dilated)
        latch.countDown()
        probe.expectMessage(GotPreRestart(false))
        probe.expectMessage(Tock(1))
      }

      ref ! End
      probe.expectMessage(GotPostStop(false))
    }

    "cancel timers when stopped from exception" taggedAs TimingTest in {
      val probe = TestProbe[Event]()
      val behv = Behaviors.withTimers[Command] { timer =>
        timer.startTimerWithFixedDelay(Tick(1), interval)
        target(probe.ref, timer, 1)
      }
      val ref = spawn(behv)
      LoggingTestKit.error[Exc].expect {
        ref ! Throw(new Exc)
        probe.expectMessage(GotPostStop(false))
      }
    }

    "cancel timers when stopped voluntarily" taggedAs TimingTest in {
      val probe = TestProbe[Event]()
      val behv = Behaviors.withTimers[Command] { timer =>
        timer.startTimerWithFixedDelay(Tick(1), interval)
        target(probe.ref, timer, 1)
      }
      val ref = spawn(behv)
      ref ! End
      probe.expectMessage(GotPostStop(false))
    }

    "allow for nested timers" in {
      val probe = TestProbe[String]()
      val ref = spawn(Behaviors.withTimers[String] { outerTimer =>
        outerTimer.startTimerWithFixedDelay("outer-message", 50.millis)
        Behaviors.withTimers { innerTimer =>
          innerTimer.startTimerWithFixedDelay("inner-message", 50.millis)
          Behaviors.receiveMessage { message =>
            if (message == "stop") Behaviors.stopped
            else {
              probe.ref ! message
              Behaviors.same
            }
          }
        }
      })

      var seen = Set.empty[String]
      probe.fishForMessage(500.millis) {
        case message =>
          seen += message
          if (seen.size == 2) FishingOutcomes.complete
          else FishingOutcomes.continue
      }

      ref ! "stop"
    }

    "keep timers when behavior changes" in {
      val probe = TestProbe[String]()
      def newBehavior(n: Int): Behavior[String] = Behaviors.withTimers[String] { timers =>
        timers.startTimerWithFixedDelay(s"message${n}", 50.milli)
        Behaviors.receiveMessage { message =>
          if (message == "stop") Behaviors.stopped
          else {
            probe.ref ! message
            newBehavior(n + 1)
          }
        }
      }

      val ref = spawn(newBehavior(1))
      var seen = Set.empty[String]
      probe.fishForMessage(500.millis) {
        case message =>
          seen += message
          if (seen.size == 2) FishingOutcomes.complete
          else FishingOutcomes.continue
      }

      ref ! "stop"
    }

    "not leak timers when PostStop is used" in {
      val deadLetterProbe = createDeadLetterProbe()
      val probe = TestProbe[DeadLetter]()
      val ref = spawn(Behaviors.withTimers[String] { timers =>
        Behaviors.setup { _ =>
          timers.startTimerWithFixedDelay("test", 250.millis)
          Behaviors.receive { (context, _) =>
            Behaviors.stopped(() => context.log.info(s"stopping"))
          }
        }
      })
      LoggingTestKit.info("stopping").expect {
        ref ! "stop"
      }
      probe.expectTerminated(ref)
      deadLetterProbe.expectNoMessage(1.second)
    }
  }

  "discard TimerMsg on restart" in {
    // reproducer of similar issue as #26556, ClassCastException TimerMsg
    val probe = TestProbe[Event]("evt")

    def behv: Behavior[Command] =
      Behaviors.receiveMessage[Command] {
        case Tick(-1) =>
          probe.ref ! Tock(-1)
          Behaviors.withTimers[Command] { timer =>
            timer.startSingleTimer(Tick(0), 5.millis)
            Behaviors.receiveMessage[Command] {
              case Tick(0) =>
                probe.ref ! Tock(0)
                timer.startSingleTimer(Tick(1), 5.millis)
                // let Tick(0) arrive in mailbox, test will not fail if it arrives later
                Thread.sleep(100)
                throw TestException("boom")
              case _ =>
                Behaviors.unhandled
            }
          }
        case Tick(n) =>
          probe.ref ! Tock(n)
          Behaviors.same
        case End =>
          Behaviors.stopped
        case _ =>
          Behaviors.unhandled
      }

    LoggingTestKit.error[TestException].expect {
      val ref = spawn(Behaviors.supervise(behv).onFailure[TestException](SupervisorStrategy.restart))
      ref ! Tick(-1)
      probe.expectMessage(Tock(-1))
      probe.expectMessage(Tock(0))
      probe.expectNoMessage()

      // confirm that it was restarted, and not stopped due to ClassCastException of TimerMsg
      ref ! Tick(100)
      probe.expectMessage(Tock(100))

      ref ! End
    }
  }

  "discard TimerMsg when exception from withTimers block" in {
    val probe = TestProbe[Event]("evt")
    val behv = Behaviors.receiveMessage[Command] {
      case Tick(-1) =>
        probe.ref ! Tock(-1)
        Behaviors.withTimers[Command] { timer =>
          timer.startSingleTimer(Tick(0), 5.millis)
          // let Tick(0) arrive in mailbox, test will not fail if it arrives later
          Thread.sleep(100)
          throw TestException("boom")
        }
      case Tick(n) =>
        probe.ref ! Tock(n)
        Behaviors.same
      case End =>
        Behaviors.stopped
      case _ =>
        Behaviors.unhandled
    }

    LoggingTestKit.error[TestException].expect {
      val ref = spawn(Behaviors.supervise(behv).onFailure[TestException](SupervisorStrategy.restart))
      ref ! Tick(-1)
      probe.expectMessage(Tock(-1))
      probe.expectNoMessage()

      // confirm that it was restarted, and not stopped due to ClassCastException of TimerMsg
      ref ! Tick(100)
      probe.expectMessage(Tock(100))

      ref ! End
    }
  }
}
