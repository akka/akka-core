/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import org.scalatest.concurrent.Eventually
import org.scalatest.prop.TableDrivenPropertyChecks._

import akka.actor._
import akka.testkit._

object BackoffSupervisorSpec {

  class TestException extends RuntimeException with NoStackTrace

  object Child {
    def props(probe: ActorRef): Props =
      Props(new Child(probe))
  }

  class Child(probe: ActorRef) extends Actor {
    def receive: Receive = {
      case "boom" => throw new TestException
      case msg    => probe ! msg
    }
  }

  object ManualChild {
    def props(probe: ActorRef): Props =
      Props(new ManualChild(probe))
  }

  class ManualChild(probe: ActorRef) extends Actor {
    def receive: Receive = {
      case "boom" => throw new TestException
      case msg =>
        probe ! msg
        context.parent ! BackoffSupervisor.Reset
    }
  }
}

class BackoffSupervisorSpec extends AkkaSpec with ImplicitSender with Eventually {
  import BackoffSupervisorSpec._

  def onStopOptions(props: Props = Child.props(testActor), maxNrOfRetries: Int = -1): BackoffOnStopOptions =
    BackoffOpts.onStop(props, "c1", 100.millis, 3.seconds, 0.2).withMaxNrOfRetries(maxNrOfRetries)
  def onFailureOptions(props: Props = Child.props(testActor), maxNrOfRetries: Int = -1): BackoffOnFailureOptions =
    BackoffOpts.onFailure(props, "c1", 100.millis, 3.seconds, 0.2).withMaxNrOfRetries(maxNrOfRetries)

  def create(options: BackoffOnStopOptions): ActorRef = system.actorOf(BackoffSupervisor.props(options))
  def create(options: BackoffOnFailureOptions): ActorRef = system.actorOf(BackoffSupervisor.props(options))

  "BackoffSupervisor" must {
    "start child again when it stops when using `Backoff.onStop`" in {
      val supervisor = create(onStopOptions())
      supervisor ! BackoffSupervisor.GetCurrentChild
      val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
      watch(c1)
      c1 ! PoisonPill
      expectTerminated(c1)
      awaitAssert {
        supervisor ! BackoffSupervisor.GetCurrentChild
        // new instance
        expectMsgType[BackoffSupervisor.CurrentChild].ref.get should !==(c1)
      }
    }

    "forward messages to the child" in {
      def assertForward(supervisor: ActorRef) = {
        supervisor ! "hello"
        expectMsg("hello")
      }
      assertForward(create(onStopOptions()))
      assertForward(create(onFailureOptions()))
    }

    "support custom supervision strategy" in {
      def assertCustomStrategy(supervisor: ActorRef) = {
        supervisor ! BackoffSupervisor.GetCurrentChild
        val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
        watch(c1)
        c1 ! "boom"
        expectTerminated(c1)
        awaitAssert {
          supervisor ! BackoffSupervisor.GetCurrentChild
          // new instance
          expectMsgType[BackoffSupervisor.CurrentChild].ref.get should !==(c1)
        }
      }
      filterException[TestException] {
        val stoppingStrategy = OneForOneStrategy() {
          case _: TestException => SupervisorStrategy.Stop
        }
        val restartingStrategy = OneForOneStrategy() {
          case _: TestException => SupervisorStrategy.Restart
        }

        assertCustomStrategy(create(onStopOptions().withSupervisorStrategy(stoppingStrategy)))

        assertCustomStrategy(create(onFailureOptions().withSupervisorStrategy(restartingStrategy)))
      }
    }

    "support default stopping strategy when using `Backoff.onStop`" in {
      filterException[TestException] {
        val supervisor = create(onStopOptions().withDefaultStoppingStrategy)
        supervisor ! BackoffSupervisor.GetCurrentChild
        val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
        watch(c1)
        supervisor ! BackoffSupervisor.GetRestartCount
        expectMsg(BackoffSupervisor.RestartCount(0))

        c1 ! "boom"
        expectTerminated(c1)
        awaitAssert {
          supervisor ! BackoffSupervisor.GetCurrentChild
          // new instance
          expectMsgType[BackoffSupervisor.CurrentChild].ref.get should !==(c1)
        }
        supervisor ! BackoffSupervisor.GetRestartCount
        expectMsg(BackoffSupervisor.RestartCount(1))

      }
    }

    "support manual reset" in {
      filterException[TestException] {
        def assertManualReset(supervisor: ActorRef) = {
          supervisor ! BackoffSupervisor.GetCurrentChild
          val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
          watch(c1)
          c1 ! "boom"
          expectTerminated(c1)

          awaitAssert {
            supervisor ! BackoffSupervisor.GetRestartCount
            expectMsg(BackoffSupervisor.RestartCount(1))
          }

          awaitAssert {
            supervisor ! BackoffSupervisor.GetCurrentChild
            // new instance
            expectMsgType[BackoffSupervisor.CurrentChild].ref.get should !==(c1)
          }

          supervisor ! "hello"
          expectMsg("hello")

          // making sure the Reset is handled by supervisor
          supervisor ! "hello"
          expectMsg("hello")

          supervisor ! BackoffSupervisor.GetRestartCount
          expectMsg(BackoffSupervisor.RestartCount(0))
        }

        val stoppingStrategy = OneForOneStrategy() {
          case _: TestException => SupervisorStrategy.Stop
        }
        val restartingStrategy = OneForOneStrategy() {
          case _: TestException => SupervisorStrategy.Restart
        }

        assertManualReset(
          create(onStopOptions(ManualChild.props(testActor)).withManualReset.withSupervisorStrategy(stoppingStrategy)))

        assertManualReset(
          create(
            onFailureOptions(ManualChild.props(testActor)).withManualReset.withSupervisorStrategy(restartingStrategy)))
      }
    }

    "reply to sender if replyWhileStopped is specified" in {
      filterException[TestException] {
        val supervisor = create(
          BackoffOpts
            .onFailure(Child.props(testActor), "c1", 100.seconds, 300.seconds, 0.2)
            .withMaxNrOfRetries(-1)
            .withReplyWhileStopped("child was stopped"))
        supervisor ! BackoffSupervisor.GetCurrentChild
        val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
        watch(c1)
        supervisor ! BackoffSupervisor.GetRestartCount
        expectMsg(BackoffSupervisor.RestartCount(0))

        c1 ! "boom"
        expectTerminated(c1)

        awaitAssert {
          supervisor ! BackoffSupervisor.GetRestartCount
          expectMsg(BackoffSupervisor.RestartCount(1))
        }

        supervisor ! "boom"
        expectMsg("child was stopped")
      }
    }

    "use provided actor while stopped and withHandlerWhileStopped is specified" in {
      val handler = system.actorOf(Props(new Actor {
        override def receive: Receive = {
          case "still there?" =>
            sender() ! "not here!"
        }
      }))
      filterException[TestException] {
        val supervisor = create(
          BackoffOpts
            .onFailure(Child.props(testActor), "c1", 100.seconds, 300.seconds, 0.2)
            .withMaxNrOfRetries(-1)
            .withHandlerWhileStopped(handler))
        supervisor ! BackoffSupervisor.GetCurrentChild
        val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
        watch(c1)
        supervisor ! BackoffSupervisor.GetRestartCount
        expectMsg(BackoffSupervisor.RestartCount(0))

        c1 ! "boom"
        expectTerminated(c1)

        awaitAssert {
          supervisor ! BackoffSupervisor.GetRestartCount
          expectMsg(BackoffSupervisor.RestartCount(1))
        }

        supervisor ! "still there?"
        expectMsg("not here!")
      }
    }

    "not reply to sender if replyWhileStopped is NOT specified" in {
      filterException[TestException] {
        val supervisor =
          create(
            BackoffOpts.onFailure(Child.props(testActor), "c1", 100.seconds, 300.seconds, 0.2).withMaxNrOfRetries(-1))
        supervisor ! BackoffSupervisor.GetCurrentChild
        val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
        watch(c1)
        supervisor ! BackoffSupervisor.GetRestartCount
        expectMsg(BackoffSupervisor.RestartCount(0))

        c1 ! "boom"
        expectTerminated(c1)

        awaitAssert {
          supervisor ! BackoffSupervisor.GetRestartCount
          expectMsg(BackoffSupervisor.RestartCount(1))
        }

        EventFilter.warning(pattern = ".*boom.*", occurrences = 1).intercept {
          supervisor ! "boom" //this will be sent to deadLetters
          expectNoMessage(500.milliseconds)
        }
      }
    }

    "correctly calculate the delay" in {
      val delayTable =
        Table(
          ("restartCount", "minBackoff", "maxBackoff", "randomFactor", "expectedResult"),
          (0, 0.minutes, 0.minutes, 0d, 0.minutes),
          (0, 5.minutes, 7.minutes, 0d, 5.minutes),
          (2, 5.seconds, 7.seconds, 0d, 7.seconds),
          (2, 5.seconds, 7.days, 0d, 20.seconds),
          (29, 5.minutes, 10.minutes, 0d, 10.minutes),
          (29, 10000.days, 10000.days, 0d, 10000.days),
          (Int.MaxValue, 10000.days, 10000.days, 0d, 10000.days))
      forAll(delayTable) {
        (
            restartCount: Int,
            minBackoff: FiniteDuration,
            maxBackoff: FiniteDuration,
            randomFactor: Double,
            expectedResult: FiniteDuration) =>
          val calculatedValue = BackoffSupervisor.calculateDelay(restartCount, minBackoff, maxBackoff, randomFactor)
          assert(calculatedValue === expectedResult)
          // and that is the same as
          val calculatedValue2 =
            RetrySupport.calculateExponentialBackoffDelay(restartCount, minBackoff, maxBackoff, randomFactor)
          assert(calculatedValue2 === expectedResult)
      }
    }

    "stop restarting the child after reaching maxNrOfRetries limit (Backoff.onStop)" in {
      val supervisorWatcher = TestProbe()
      val supervisor = create(onStopOptions(maxNrOfRetries = 2))
      def waitForChild: Option[ActorRef] = {
        eventually(timeout(1.second), interval(50.millis)) {
          supervisor ! BackoffSupervisor.GetCurrentChild
          val c = expectMsgType[BackoffSupervisor.CurrentChild].ref
          c.isDefined shouldBe true
        }

        supervisor ! BackoffSupervisor.GetCurrentChild
        expectMsgType[BackoffSupervisor.CurrentChild].ref
      }

      supervisorWatcher.watch(supervisor)

      supervisor ! BackoffSupervisor.GetRestartCount
      expectMsg(BackoffSupervisor.RestartCount(0))

      supervisor ! BackoffSupervisor.GetCurrentChild
      val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
      watch(c1)
      c1 ! PoisonPill
      expectTerminated(c1)

      // that we saw c1 terminated does not mean the supervisor did yet
      awaitAssert {
        supervisor ! BackoffSupervisor.GetRestartCount
        expectMsg(BackoffSupervisor.RestartCount(1))
      }
      val c2 = waitForChild.get
      awaitAssert(c2 should !==(c1))
      watch(c2)
      c2 ! PoisonPill
      expectTerminated(c2)

      // that we saw c2 terminated does not mean the supervisor did yet
      awaitAssert {
        supervisor ! BackoffSupervisor.GetRestartCount
        expectMsg(BackoffSupervisor.RestartCount(2))
      }
      val c3 = waitForChild.get
      awaitAssert(c3 should !==(c2))
      watch(c3)
      c3 ! PoisonPill
      expectTerminated(c3)
      supervisorWatcher.expectTerminated(supervisor)
    }

    "stop restarting the child after reaching maxNrOfRetries limit (Backoff.onFailure)" in {
      filterException[TestException] {
        val supervisorWatcher = TestProbe()
        val supervisor = create(onFailureOptions(maxNrOfRetries = 2))

        def waitForChild: Option[ActorRef] = {
          eventually(timeout(1.second), interval(50.millis)) {
            supervisor ! BackoffSupervisor.GetCurrentChild
            val c = expectMsgType[BackoffSupervisor.CurrentChild].ref
            c.isDefined shouldBe true
          }

          supervisor ! BackoffSupervisor.GetCurrentChild
          expectMsgType[BackoffSupervisor.CurrentChild].ref
        }

        supervisorWatcher.watch(supervisor)

        supervisor ! BackoffSupervisor.GetRestartCount
        expectMsg(BackoffSupervisor.RestartCount(0))

        supervisor ! BackoffSupervisor.GetCurrentChild
        val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
        watch(c1)
        c1 ! "boom"
        expectTerminated(c1)

        // that we saw c1 terminated does not mean the supervisor did yet
        awaitAssert {
          supervisor ! BackoffSupervisor.GetRestartCount
          expectMsg(BackoffSupervisor.RestartCount(1))
        }
        val c2 = waitForChild.get
        awaitAssert(c2 should !==(c1))
        watch(c2)
        c2 ! "boom"
        expectTerminated(c2)

        // that we saw c2 terminated does not mean the supervisor did yet
        awaitAssert {
          supervisor ! BackoffSupervisor.GetRestartCount
          expectMsg(BackoffSupervisor.RestartCount(2))
        }

        val c3 = waitForChild.get
        awaitAssert(c3 should !==(c2))
        watch(c3)
        c3 ! "boom"
        expectTerminated(c3)
        supervisorWatcher.expectTerminated(supervisor)
      }
    }

    "stop restarting the child if final stop message received (Backoff.onStop)" in {
      val stopMessage = "stop"
      val supervisorWatcher = TestProbe()
      val supervisor: ActorRef = create(onStopOptions(maxNrOfRetries = 100).withFinalStopMessage(_ == stopMessage))
      supervisor ! BackoffSupervisor.GetCurrentChild
      val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
      watch(c1)
      supervisorWatcher.watch(supervisor)

      supervisor ! stopMessage
      expectMsg("stop")
      c1 ! PoisonPill
      expectTerminated(c1)
      supervisorWatcher.expectTerminated(supervisor)
    }

    "supervisor must not stop when final stop message has not been received" in {
      val stopMessage = "stop"
      val supervisorWatcher = TestProbe()
      val supervisor: ActorRef = create(onStopOptions(maxNrOfRetries = 100).withFinalStopMessage(_ == stopMessage))
      supervisor ! BackoffSupervisor.GetCurrentChild
      val c1 = expectMsgType[BackoffSupervisor.CurrentChild].ref.get
      watch(c1)
      supervisorWatcher.watch(supervisor)

      c1 ! PoisonPill
      expectTerminated(c1)
      // since actor stopped we can expect the two messages to end up in dead letters
      EventFilter.warning(pattern = ".*(ping|stop).*", occurrences = 1).intercept {
        supervisor ! "ping"
        supervisorWatcher.expectNoMessage(20.millis) // supervisor must not terminate

        supervisor ! stopMessage
        supervisorWatcher.expectTerminated(supervisor)
      }
    }
  }
}
