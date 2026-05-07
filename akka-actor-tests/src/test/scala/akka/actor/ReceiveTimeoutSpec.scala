/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._

import language.postfixOps

import akka.testkit._

object ReceiveTimeoutSpec {
  case object Tick
  case object TransparentTick extends NotInfluenceReceiveTimeout

  class RestartingParent(probe: ActorRef) extends Actor {
    val restarting = new AtomicBoolean(false)
    val child = context.actorOf(Props(new RestartingChild(probe, restarting)))
    def receive = {
      case msg =>
        child.forward(msg)
    }
  }
  class RestartingChild(probe: ActorRef, restarting: AtomicBoolean) extends Actor {

    override def preStart(): Unit = {
      if (restarting.get) {
        probe ! "restarting"
        context.setReceiveTimeout(500.millis)
      } else {
        probe ! "starting"
      }
    }

    override def postStop(): Unit = {
      probe ! "stopping"
    }

    def receive = {
      case "crash" =>
        restarting.set(true)
        probe ! "crashing"
        throw TestException("boom bang")
      case ReceiveTimeout =>
        probe ! ReceiveTimeout
      case other =>
        probe ! other
    }
  }

  class StoppingSelfActor(probe: ActorRef) extends Actor {
    override def receive: Receive = {
      case "Stop" =>
        context.setReceiveTimeout(200.millis)
        context.stop(self)
    }

    override def postStop(): Unit = {
      probe ! "stopping"
    }
  }
}

class ReceiveTimeoutSpec extends AkkaSpec() {
  import ReceiveTimeoutSpec._

  "An actor with receive timeout" must {

    "get timeout" taggedAs TimingTest in {
      val timeoutLatch = TestLatch()

      val timeoutActor = system.actorOf(Props(new Actor {
        context.setReceiveTimeout(500 milliseconds)

        def receive = {
          case ReceiveTimeout => timeoutLatch.open()
        }
      }))

      Await.ready(timeoutLatch, TestLatch.DefaultTimeout)
      system.stop(timeoutActor)
    }

    "reschedule timeout after regular receive" taggedAs TimingTest in {
      val timeoutLatch = TestLatch()

      val timeoutActor = system.actorOf(Props(new Actor {
        context.setReceiveTimeout(500 milliseconds)

        def receive = {
          case Tick           => ()
          case ReceiveTimeout => timeoutLatch.open()
        }
      }))

      timeoutActor ! Tick

      Await.ready(timeoutLatch, TestLatch.DefaultTimeout)
      system.stop(timeoutActor)
    }

    "be able to turn off timeout if desired" taggedAs TimingTest in {
      val count = new AtomicInteger(0)
      val timeoutLatch = TestLatch()

      val timeoutActor = system.actorOf(Props(new Actor {
        context.setReceiveTimeout(500 milliseconds)

        def receive = {
          case Tick => ()
          case ReceiveTimeout =>
            count.incrementAndGet
            timeoutLatch.open()
            context.setReceiveTimeout(Duration.Undefined)
        }
      }))

      timeoutActor ! Tick

      Await.ready(timeoutLatch, TestLatch.DefaultTimeout)
      count.get should ===(1)
      system.stop(timeoutActor)
    }

    "not receive timeout message when not specified" taggedAs TimingTest in {
      val timeoutLatch = TestLatch()

      val timeoutActor = system.actorOf(Props(new Actor {
        def receive = {
          case ReceiveTimeout => timeoutLatch.open()
        }
      }))

      intercept[TimeoutException] { Await.ready(timeoutLatch, 1 second) }
      system.stop(timeoutActor)
    }

    "get timeout while receiving NotInfluenceReceiveTimeout messages" taggedAs TimingTest in {
      val timeoutLatch = TestLatch()

      val timeoutActor = system.actorOf(Props(new Actor {
        context.setReceiveTimeout(1 second)

        def receive = {
          case ReceiveTimeout  => timeoutLatch.open()
          case TransparentTick =>
        }
      }))

      val ticks = system.scheduler.scheduleWithFixedDelay(100.millis, 100.millis) { () =>
        timeoutActor ! TransparentTick
        timeoutActor ! Identify(None)
      }(system.dispatcher)

      Await.ready(timeoutLatch, TestLatch.DefaultTimeout)
      ticks.cancel()
      system.stop(timeoutActor)
    }

    "get timeout while receiving only NotInfluenceReceiveTimeout messages" taggedAs TimingTest in {
      val timeoutLatch = TestLatch(2)

      val timeoutActor = system.actorOf(Props(new Actor {
        context.setReceiveTimeout(1 second)

        def receive = {
          case ReceiveTimeout =>
            self ! TransparentTick
            timeoutLatch.countDown()
          case TransparentTick =>
        }
      }))

      Await.ready(timeoutLatch, TestLatch.DefaultTimeout)
      system.stop(timeoutActor)
    }

    "get timeout while receiving NotInfluenceReceiveTimeout messages scheduled with Timers" taggedAs TimingTest in {
      val timeoutLatch = TestLatch()
      val count = new AtomicInteger(0)

      class ActorWithTimer() extends Actor with Timers {
        timers.startTimerWithFixedDelay("transparentTick", TransparentTick, 100.millis)
        timers.startTimerWithFixedDelay("identifyTick", Identify(None), 100.millis)

        context.setReceiveTimeout(1 second)
        def receive: Receive = {
          case ReceiveTimeout =>
            timeoutLatch.open()
          case TransparentTick =>
            count.incrementAndGet()
        }
      }

      val timeoutActor = system.actorOf(Props(new ActorWithTimer()))

      Await.ready(timeoutLatch, TestLatch.DefaultTimeout)
      count.get() should be > 0
      system.stop(timeoutActor)
    }

    "be able to turn on timeout in NotInfluenceReceiveTimeout message handler" taggedAs TimingTest in {
      val timeoutLatch = TestLatch()

      val timeoutActor = system.actorOf(Props(new Actor {
        def receive = {
          case TransparentTick => context.setReceiveTimeout(500 milliseconds)
          case ReceiveTimeout  => timeoutLatch.open()
        }
      }))

      timeoutActor ! TransparentTick

      Await.ready(timeoutLatch, TestLatch.DefaultTimeout)
      system.stop(timeoutActor)
    }

    "be able to turn off timeout in NotInfluenceReceiveTimeout message handler" taggedAs TimingTest in {
      val timeoutLatch = TestLatch()

      val timeoutActor = system.actorOf(Props(new Actor {
        context.setReceiveTimeout(500 milliseconds)

        def receive = {
          case TransparentTick => context.setReceiveTimeout(Duration.Inf)
          case ReceiveTimeout  => timeoutLatch.open()
        }
      }))

      timeoutActor ! TransparentTick

      intercept[TimeoutException] { Await.ready(timeoutLatch, 1 second) }
      system.stop(timeoutActor)
    }

    "remove / change existing timeout while handling a NotInfluenceReceiveTimeout message" taggedAs TimingTest in {
      val timeoutLatch = TestLatch()
      val initialTimeout = 500.millis

      val timeoutActor = system.actorOf(Props(new Actor {
        context.setReceiveTimeout(initialTimeout)

        def receive: Receive = {
          case TransparentTick => context.setReceiveTimeout(Duration.Undefined)
          case ReceiveTimeout  => timeoutLatch.open()
        }
      }))

      timeoutActor ! TransparentTick

      intercept[TimeoutException] { Await.ready(timeoutLatch, initialTimeout * 2) }
      system.stop(timeoutActor)
    }

    "work correctly if the timeout is changed multiple times while handling a NotInfluenceReceiveTimeout message" taggedAs TimingTest in {
      val probe = TestProbe()
      val initialTimeout = 500.millis

      val timeoutActor = system.actorOf(Props(new Actor {
        var count = 0
        context.setReceiveTimeout(initialTimeout)

        def receive: Receive = {
          case TransparentTick =>
            context.setReceiveTimeout(initialTimeout * 2)
            count += 1 // do some work then
            context.setReceiveTimeout(initialTimeout)

          case ReceiveTimeout => probe.ref ! ReceiveTimeout
        }
      }))

      probe.expectNoMessage(initialTimeout / 2)

      timeoutActor ! TransparentTick
      timeoutActor ! TransparentTick

      // not triggered by initialTimeout because it was changed by the ticks
      // wait shorter than last set value, but longer than initialTimeout
      probe.expectNoMessage(initialTimeout / 2 + 50.millis)
      // now should happen
      probe.expectMsgType[ReceiveTimeout]
      system.stop(timeoutActor)
    }

    // #28266 reproducer
    "get the timeout when scheduled immediately on restart" in {
      val probe = TestProbe()
      val ref = system.actorOf(Props(new RestartingParent(probe.ref)))
      probe.expectMsg("starting")
      EventFilter.error("boom bang", occurrences = 1).intercept {
        ref ! "crash"
      }
      probe.expectMsg("crashing")
      probe.expectMsg("stopping")
      probe.expectMsg("restarting")
      probe.expectMsg(ReceiveTimeout)
    }

    "Will cancel receive timeout task if stopped" in {
      val probe = TestProbe()
      system.eventStream.subscribe(probe.ref, classOf[DeadLetter])
      val ref = system.actorOf(Props(new StoppingSelfActor(probe.ref)))
      ref ! "Stop"
      probe.expectMsg("stopping")
      probe.expectNoMessage(200.millis * 2)
    }
  }
}
