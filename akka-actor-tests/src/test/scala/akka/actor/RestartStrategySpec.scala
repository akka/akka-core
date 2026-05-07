/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.lang.Thread.sleep

import scala.concurrent.Await
import scala.concurrent.duration._

import language.postfixOps

import akka.pattern.ask
import akka.testkit.AkkaSpec
import akka.testkit.DefaultTimeout
import akka.testkit.EventFilter
import akka.testkit.TestEvent._
import akka.testkit.TestLatch

class RestartStrategySpec extends AkkaSpec with DefaultTimeout {

  override def atStartup(): Unit = {
    system.eventStream.publish(Mute(EventFilter[Exception]("Crashing...")))
  }

  object Ping
  object Crash

  "A RestartStrategy" must {

    "ensure that employee stays dead after max restarts within time range" in {
      val boss = system.actorOf(
        Props(
          new Supervisor(OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = 1 second)(List(classOf[Throwable])))))

      val restartLatch = new TestLatch
      val secondRestartLatch = new TestLatch
      val countDownLatch = new TestLatch(3)
      val stopLatch = new TestLatch

      val employeeProps = Props(new Actor {

        def receive = {
          case Ping  => countDownLatch.countDown()
          case Crash => throw new Exception("Crashing...")
        }

        override def postRestart(reason: Throwable) = {
          if (!restartLatch.isOpen)
            restartLatch.open()
          else
            secondRestartLatch.open()
        }

        override def postStop() = {
          stopLatch.open()
        }
      })
      val employee = Await.result((boss ? employeeProps).mapTo[ActorRef], timeout.duration)

      employee ! Ping
      employee ! Crash
      employee ! Ping

      // test restart and post restart ping
      Await.ready(restartLatch, 10 seconds)

      // now crash again... should not restart
      employee ! Crash
      employee ! Ping

      Await.ready(secondRestartLatch, 10 seconds)
      Await.ready(countDownLatch, 10 seconds)

      employee ! Crash
      Await.ready(stopLatch, 10 seconds)
    }

    "ensure that employee is immortal without max restarts and time range" in {
      val boss = system.actorOf(Props(new Supervisor(OneForOneStrategy()(List(classOf[Throwable])))))

      val countDownLatch = new TestLatch(100)

      val employeeProps = Props(new Actor {

        def receive = {
          case Crash => throw new Exception("Crashing...")
        }

        override def postRestart(reason: Throwable) = {
          countDownLatch.countDown()
        }
      })
      val employee = Await.result((boss ? employeeProps).mapTo[ActorRef], timeout.duration)

      (1 to 100).foreach { _ =>
        employee ! Crash
      }
      Await.ready(countDownLatch, 2 minutes)
      assert(!employee.isTerminated)
    }

    "ensure that employee restarts after number of crashes not within time range" in {
      val boss = system.actorOf(Props(
        new Supervisor(OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = 500 millis)(List(classOf[Throwable])))))

      val restartLatch = new TestLatch
      val secondRestartLatch = new TestLatch
      val thirdRestartLatch = new TestLatch
      val pingLatch = new TestLatch
      val secondPingLatch = new TestLatch

      val employeeProps = Props(new Actor {

        def receive = {
          case Ping =>
            if (!pingLatch.isOpen) pingLatch.open() else secondPingLatch.open()
          case Crash => throw new Exception("Crashing...")
        }
        override def postRestart(reason: Throwable) = {
          if (!restartLatch.isOpen)
            restartLatch.open()
          else if (!secondRestartLatch.isOpen)
            secondRestartLatch.open()
          else
            thirdRestartLatch.open()
        }

        override def postStop() = {
          if (restartLatch.isOpen) {
            secondRestartLatch.open()
          }
        }
      })
      val employee = Await.result((boss ? employeeProps).mapTo[ActorRef], timeout.duration)

      employee ! Ping
      employee ! Crash

      Await.ready(restartLatch, 10 seconds)
      Await.ready(pingLatch, 10 seconds)

      employee ! Ping
      employee ! Crash

      Await.ready(secondRestartLatch, 10 seconds)
      Await.ready(secondPingLatch, 10 seconds)

      // sleep to go out of the restart strategy's time range
      sleep(700L)

      // now crash again... should and post restart ping
      employee ! Crash
      employee ! Ping

      Await.ready(thirdRestartLatch, 1 second)

      assert(!employee.isTerminated)
    }

    "ensure that employee is not restarted after max retries" in {
      val boss = system.actorOf(Props(new Supervisor(OneForOneStrategy(maxNrOfRetries = 2)(List(classOf[Throwable])))))

      val restartLatch = new TestLatch
      val secondRestartLatch = new TestLatch
      val countDownLatch = new TestLatch(3)
      val stopLatch = new TestLatch

      val employeeProps = Props(new Actor {

        def receive = {
          case Ping  => countDownLatch.countDown()
          case Crash => throw new Exception("Crashing...")
        }
        override def postRestart(reason: Throwable) = {
          if (!restartLatch.isOpen)
            restartLatch.open()
          else
            secondRestartLatch.open()
        }

        override def postStop() = {
          stopLatch.open()
        }
      })
      val employee = Await.result((boss ? employeeProps).mapTo[ActorRef], timeout.duration)

      employee ! Ping
      employee ! Crash
      employee ! Ping

      // test restart and post restart ping
      Await.ready(restartLatch, 10 seconds)

      assert(!employee.isTerminated)

      // now crash again... should not restart
      employee ! Crash
      employee ! Ping

      Await.ready(secondRestartLatch, 10 seconds)
      Await.ready(countDownLatch, 10 seconds)

      sleep(700L)

      employee ! Crash
      Await.ready(stopLatch, 10 seconds)
      sleep(500L)
      assert(employee.isTerminated)
    }

    "ensure that employee is not restarted within time range" in {
      val restartLatch, stopLatch, maxNoOfRestartsLatch = new TestLatch
      val countDownLatch = new TestLatch(2)

      val boss = system.actorOf(Props(new Actor {
        override val supervisorStrategy = OneForOneStrategy(withinTimeRange = 1 second)(List(classOf[Throwable]))
        def receive = {
          case p: Props      => sender() ! context.watch(context.actorOf(p))
          case _: Terminated => maxNoOfRestartsLatch.open()
        }
      }))

      val employeeProps = Props(new Actor {

        def receive = {
          case Ping  => countDownLatch.countDown()
          case Crash => throw new Exception("Crashing...")
        }

        override def postRestart(reason: Throwable) = {
          restartLatch.open()
        }

        override def postStop() = {
          stopLatch.open()
        }
      })
      val employee = Await.result((boss ? employeeProps).mapTo[ActorRef], timeout.duration)

      employee ! Ping
      employee ! Crash
      employee ! Ping

      // test restart and post restart ping
      Await.ready(restartLatch, 10 seconds)

      assert(!employee.isTerminated)

      // now crash again... should not restart
      employee ! Crash

      // may not be running
      employee ! Ping
      Await.ready(countDownLatch, 10 seconds)

      // may not be running
      employee ! Crash

      Await.ready(stopLatch, 10 seconds)

      Await.ready(maxNoOfRestartsLatch, 10 seconds)
      sleep(500L)
      assert(employee.isTerminated)
    }
  }
}
