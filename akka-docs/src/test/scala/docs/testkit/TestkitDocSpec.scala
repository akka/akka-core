/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.testkit

import language.postfixOps
import scala.util.Success
import akka.testkit._

import scala.annotation.nowarn

//#imports-test-probe
import scala.concurrent.duration._
import akka.actor._
import akka.testkit.TestProbe

//#imports-test-probe

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.control.NonFatal

@nowarn("msg=never used") // sample snippets
object TestKitDocSpec {
  case object Say42
  case object Unknown

  class MyActor extends Actor {
    def receive = {
      case Say42       => sender() ! 42
      case "some work" => sender() ! "some result"
    }
  }

  class TestFsmActor extends Actor with FSM[Int, String] {
    startWith(1, "")
    when(1) {
      case Event("go", _) => goto(2).using("go")
    }
    when(2) {
      case Event("back", _) => goto(1).using("back")
    }
  }

  //#my-double-echo
  class MyDoubleEcho extends Actor {
    var dest1: ActorRef = _
    var dest2: ActorRef = _
    def receive = {
      case (d1: ActorRef, d2: ActorRef) =>
        dest1 = d1
        dest2 = d2
      case x =>
        dest1 ! x
        dest2 ! x
    }
  }

  //#my-double-echo

  //#test-probe-forward-actors
  class Source(target: ActorRef) extends Actor {
    def receive = {
      case "start" => target ! "work"
    }
  }

  class Destination extends Actor {
    def receive = {
      case x => // Do something..
    }
  }

  //#test-probe-forward-actors

  //#timer
  case class TriggerScheduling(foo: String)

  object SchedKey
  case class ScheduledMessage(foo: String)

  class TestTimerActor extends Actor with Timers {
    override def receive = {
      case TriggerScheduling(foo) => triggerScheduling(ScheduledMessage(foo))
    }

    def triggerScheduling(msg: ScheduledMessage) =
      timers.startSingleTimer(SchedKey, msg, 500.millis)
  }
  //#timer

  class LoggingActor extends Actor {
    //#logging-receive
    import akka.event.LoggingReceive
    def receive = LoggingReceive {
      case msg => // Do something ...
    }
    def otherState: Receive = LoggingReceive.withLabel("other") {
      case msg => // Do something else ...
    }
    //#logging-receive
  }
}

@nowarn("msg=never used") // sample snippets
class TestKitDocSpec extends AkkaSpec with DefaultTimeout with ImplicitSender {
  import TestKitDocSpec._

  "demonstrate usage of TestActorRef" in {
    //#test-actor-ref
    import akka.testkit.TestActorRef

    val actorRef = TestActorRef[MyActor]
    val actor = actorRef.underlyingActor
    //#test-actor-ref
  }

  "demonstrate built-in expect methods" in {

    testActor.tell("hello", ActorRef.noSender)
    testActor.tell("hello", ActorRef.noSender)
    testActor.tell("hello", ActorRef.noSender)
    testActor.tell("world", ActorRef.noSender)
    testActor.tell(42, ActorRef.noSender)
    //#test-expect
    val hello: String = expectMsg("hello")
    val any: String = expectMsgAnyOf("hello", "world")
    val all: immutable.Seq[String] = expectMsgAllOf("hello", "world")
    val i: Int = expectMsgType[Int]
    expectNoMessage(200.millis)
    //#test-expect
    testActor.tell("receveN-1", ActorRef.noSender)
    testActor.tell("receveN-2", ActorRef.noSender)
    //#test-expect
    val two: immutable.Seq[AnyRef] = receiveN(2)
    //#test-expect
    assert("hello" == hello)
    assert("hello" == any)
    assert(42 == i)
  }

  "demonstrate usage of TestFSMRef" in {
    //#test-fsm-ref
    import akka.testkit.TestFSMRef
    import scala.concurrent.duration._

    val fsm = TestFSMRef(new TestFsmActor)

    val mustBeTypedProperly: TestActorRef[TestFsmActor] = fsm

    assert(fsm.stateName == 1)
    assert(fsm.stateData == "")
    fsm ! "go" // being a TestActorRef, this runs also on the CallingThreadDispatcher
    assert(fsm.stateName == 2)
    assert(fsm.stateData == "go")

    fsm.setState(stateName = 1)
    assert(fsm.stateName == 1)

    assert(fsm.isTimerActive("test") == false)
    fsm.startTimerWithFixedDelay("test", 12, 10 millis)
    assert(fsm.isTimerActive("test") == true)
    fsm.cancelTimer("test")
    assert(fsm.isTimerActive("test") == false)
    //#test-fsm-ref
  }

  "demonstrate testing of behavior" in {

    //#test-behavior
    import akka.testkit.TestActorRef
    import akka.pattern.ask

    val actorRef = TestActorRef(new MyActor)
    // hypothetical message stimulating a '42' answer
    val future: Future[Any] = actorRef ? Say42
    future.futureValue should be(42)
    //#test-behavior
  }

  "demonstrate unhandled message" in {
    //#test-unhandled
    import akka.testkit.TestActorRef
    system.eventStream.subscribe(testActor, classOf[UnhandledMessage])
    val ref = TestActorRef[MyActor]
    ref.receive(Unknown)
    expectMsg(1 second, UnhandledMessage(Unknown, system.deadLetters, ref))
    //#test-unhandled
  }

  "demonstrate expecting exceptions" in {
    //#test-expecting-exceptions
    import akka.testkit.TestActorRef

    val actorRef = TestActorRef(new Actor {
      def receive = {
        case "hello" => throw new IllegalArgumentException("boom")
      }
    })
    intercept[IllegalArgumentException] { actorRef.receive("hello") }
    //#test-expecting-exceptions
  }

  "demonstrate within" in {
    type Worker = MyActor
    //#test-within
    import akka.actor.Props
    import scala.concurrent.duration._

    val worker = system.actorOf(Props[Worker]())
    within(200 millis) {
      worker ! "some work"
      expectMsg("some result")
      expectNoMessage() // will block for the rest of the 200ms
      Thread.sleep(300) // will NOT make this block fail
    }
    //#test-within
  }

  "demonstrate dilated duration" in {
    //#duration-dilation
    import scala.concurrent.duration._
    import akka.testkit._
    10.milliseconds.dilated
    //#duration-dilation
  }

  "demonstrate usage of probe" in {
    //#test-probe
    val probe1 = TestProbe()
    val probe2 = TestProbe()
    val actor = system.actorOf(Props[MyDoubleEcho]())
    actor ! ((probe1.ref, probe2.ref))
    actor ! "hello"
    probe1.expectMsg(500 millis, "hello")
    probe2.expectMsg(500 millis, "hello")
    //#test-probe

    //#test-special-probe
    final case class Update(id: Int, value: String)

    val probe = new TestProbe(system) {
      def expectUpdate(x: Int) = {
        expectMsgPF() {
          case Update(id, _) if id == x => ()
        }
        sender() ! "ACK"
      }
    }
    //#test-special-probe
  }

  "demonstrate usage of test probe with custom name" in {
    //#test-probe-with-custom-name
    val worker = TestProbe("worker")
    val aggregator = TestProbe("aggregator")

    worker.ref.path.name should startWith("worker")
    aggregator.ref.path.name should startWith("aggregator")
    //#test-probe-with-custom-name
  }

  "demonstrate probe watch" in {
    import akka.testkit.TestProbe
    val target = system.actorOf(Props.empty)
    //#test-probe-watch
    val probe = TestProbe()
    probe.watch(target)
    target ! PoisonPill
    probe.expectTerminated(target)
    //#test-probe-watch
  }

  "demonstrate probe reply" in {
    import akka.testkit.TestProbe
    import scala.concurrent.duration._
    import akka.pattern.ask
    //#test-probe-reply
    val probe = TestProbe()
    val future = probe.ref ? "hello"
    probe.expectMsg(0 millis, "hello") // TestActor runs on CallingThreadDispatcher
    probe.reply("world")
    assert(future.isCompleted && future.value.contains(Success("world")))
    //#test-probe-reply
  }

  "demonstrate probe forward" in {
    import akka.testkit.TestProbe
    import akka.actor.Props
    //#test-probe-forward
    val probe = TestProbe()
    val source = system.actorOf(Props(classOf[Source], probe.ref))
    val dest = system.actorOf(Props[Destination]())
    source ! "start"
    probe.expectMsg("work")
    probe.forward(dest)
    //#test-probe-forward
  }

  "demonstrate using inheritance to test timers" in {
    //#timer-test
    import akka.testkit.TestProbe
    import akka.actor.Props

    val probe = TestProbe()
    val actor = system.actorOf(Props(new TestTimerActor() {
      override def triggerScheduling(msg: ScheduledMessage) =
        probe.ref ! msg
    }))

    actor ! TriggerScheduling("abc")
    probe.expectMsg(ScheduledMessage("abc"))
    //#timer-test
  }

  "demonstrate calling thread dispatcher" in {
    //#calling-thread-dispatcher
    import akka.testkit.CallingThreadDispatcher
    val ref = system.actorOf(Props[MyActor]().withDispatcher(CallingThreadDispatcher.Id))
    //#calling-thread-dispatcher
  }

  "demonstrate EventFilter" in {
    //#event-filter
    import akka.testkit.EventFilter
    import com.typesafe.config.ConfigFactory

    implicit val system: ActorSystem = ActorSystem(
      "testsystem",
      ConfigFactory.parseString("""
      akka.loggers = ["akka.testkit.TestEventListener"]
      """))
    try {
      val actor = system.actorOf(Props.empty)
      EventFilter[ActorKilledException](occurrences = 1).intercept {
        actor ! Kill
      }
    } finally {
      shutdown(system)
    }
    //#event-filter
  }

  "demonstrate TestKitBase" in {
    //#test-kit-base
    import akka.testkit.TestKitBase

    class MyTest extends TestKitBase {
      implicit lazy val system: ActorSystem = ActorSystem()

      //#put-your-test-code-here
      val probe = TestProbe()
      probe.send(testActor, "hello")
      try expectMsg("hello")
      catch { case NonFatal(e) => system.terminate(); throw e }
      //#put-your-test-code-here

      shutdown(system)
    }
    //#test-kit-base
  }

  "demonstrate within() nesting" in {
    intercept[AssertionError] {
      //#test-within-probe
      val probe = TestProbe()
      within(1 second) {
        probe.expectMsg("hello")
      }
      //#test-within-probe
    }
  }

}
