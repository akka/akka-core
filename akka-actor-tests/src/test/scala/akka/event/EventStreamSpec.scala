/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.testkit.{ AkkaSpec, TestProbe }

object EventStreamSpec {

  val config = ConfigFactory.parseString("""
      akka {
        stdout-loglevel = WARNING
        loglevel = INFO
        loggers = ["akka.event.EventStreamSpec$MyLog", "%s"]
      }
      """.format(Logging.StandardOutLogger.getClass.getName))

  val configUnhandled = ConfigFactory.parseString("""
      akka {
        stdout-loglevel = WARNING
        loglevel = WARNING
        actor.debug.unhandled = on
        log-dead-letters = off
      }
      """)

  val configUnhandledWithDebug =
    ConfigFactory.parseString("akka.actor.debug.event-stream = on").withFallback(configUnhandled)

  final case class M(i: Int)

  final case class SetTarget(ref: ActorRef)

  class MyLog extends Actor {
    var dst: ActorRef = context.system.deadLetters
    def receive = {
      case Logging.InitializeLogger(bus) =>
        bus.subscribe(context.self, classOf[SetTarget])
        bus.subscribe(context.self, classOf[UnhandledMessage])
        sender() ! Logging.LoggerInitialized
      case SetTarget(ref)      => { dst = ref; dst ! "OK" }
      case e: Logging.LogEvent => dst ! e
      case u: UnhandledMessage => dst ! u
    }
  }

  // class hierarchy for subchannel test
  class A
  class B2 extends A
  class B3 extends A
  class C extends B2

  trait T
  trait AT extends T
  trait ATT extends AT
  trait BT extends T
  trait BTT extends BT
  class CC
  class CCATBT extends CC with ATT with BTT
}

class EventStreamSpec extends AkkaSpec(EventStreamSpec.config) {

  import EventStreamSpec._

  val impl = system.asInstanceOf[ActorSystemImpl]

  "An EventStream" must {

    "manage subscriptions" in {
      //#event-bus-start-unsubscriber-scala
      val bus = new EventStream(system, true)
      bus.startUnsubscriber()
      //#event-bus-start-unsubscriber-scala

      bus.subscribe(testActor, classOf[M])
      bus.publish(M(42))
      within(3.seconds) {
        expectMsg(M(42))
        bus.unsubscribe(testActor)
        bus.publish(M(13))
        expectNoMessage()
      }
    }

    "not allow null as subscriber" in {
      val bus = new EventStream(system, true)
      intercept[IllegalArgumentException] { bus.subscribe(null, classOf[M]) }.getMessage should ===(
        "subscriber is null")
    }

    "not allow null as unsubscriber" in {
      val bus = new EventStream(system, true)
      intercept[IllegalArgumentException] { bus.unsubscribe(null, classOf[M]) }.getMessage should ===(
        "subscriber is null")
      intercept[IllegalArgumentException] { bus.unsubscribe(null) }.getMessage should ===("subscriber is null")
    }

    "be able to log unhandled messages" in {
      val sys = ActorSystem("EventStreamSpecUnhandled", configUnhandled)
      try {
        sys.eventStream.subscribe(testActor, classOf[AnyRef])
        val m = UnhandledMessage(42, sys.deadLetters, sys.deadLetters)
        sys.eventStream.publish(m)
        expectMsgAllOf(
          m,
          Logging.Debug(
            sys.deadLetters.path.toString,
            sys.deadLetters.getClass,
            "unhandled message from " + sys.deadLetters + ": 42"))
        sys.eventStream.unsubscribe(testActor)
      } finally {
        shutdown(sys)
      }
    }

    "manage log levels" in {
      val bus = new EventStream(system, false)
      bus.startDefaultLoggers(impl)
      bus.publish(SetTarget(testActor))
      expectMsg("OK")
      within(3.seconds) {
        import Logging._
        verifyLevel(bus, InfoLevel)
        bus.setLogLevel(WarningLevel)
        verifyLevel(bus, WarningLevel)
        bus.setLogLevel(DebugLevel)
        verifyLevel(bus, DebugLevel)
        bus.setLogLevel(ErrorLevel)
        verifyLevel(bus, ErrorLevel)
      }
    }

    "manage sub-channels using classes" in {
      val a = new A
      val b2 = new B2
      val b3 = new B3
      val c = new C
      val bus = new EventStream(system, false)
      within(3.seconds) {
        bus.subscribe(testActor, classOf[B3]) should ===(true)
        bus.publish(c)
        bus.publish(b3)
        expectMsg(b3)
        bus.subscribe(testActor, classOf[A]) should ===(true)
        bus.publish(c)
        expectMsg(c)
        bus.publish(b2)
        expectMsg(b2)
        bus.unsubscribe(testActor, classOf[B2]) should ===(true)
        bus.publish(c)
        bus.publish(b3)
        bus.publish(a)
        expectMsg(b3)
        expectMsg(a)
        expectNoMessage()
      }
    }

    "manage sub-channels using classes and traits (update on subscribe)" in {
      val es = new EventStream(system, false)
      val tm1 = new CC
      val tm2 = new CCATBT
      val a1, a2, a3, a4 = TestProbe()

      es.subscribe(a1.ref, classOf[AT]) should ===(true)
      es.subscribe(a2.ref, classOf[BT]) should ===(true)
      es.subscribe(a3.ref, classOf[CC]) should ===(true)
      es.subscribe(a4.ref, classOf[CCATBT]) should ===(true)
      es.publish(tm1)
      es.publish(tm2)
      (a1.expectMsgType[AT]: AT) should ===(tm2)
      (a2.expectMsgType[BT]: BT) should ===(tm2)
      (a3.expectMsgType[CC]: CC) should ===(tm1)
      (a3.expectMsgType[CC]: CC) should ===(tm2)
      (a4.expectMsgType[CCATBT]: CCATBT) should ===(tm2)
      es.unsubscribe(a1.ref, classOf[AT]) should ===(true)
      es.unsubscribe(a2.ref, classOf[BT]) should ===(true)
      es.unsubscribe(a3.ref, classOf[CC]) should ===(true)
      es.unsubscribe(a4.ref, classOf[CCATBT]) should ===(true)
    }

    "manage sub-channels using classes and traits (update on unsubscribe)" in {
      val es = new EventStream(system, false)
      val tm1 = new CC
      val tm2 = new CCATBT
      val a1, a2, a3, a4 = TestProbe()

      es.subscribe(a1.ref, classOf[AT]) should ===(true)
      es.subscribe(a2.ref, classOf[BT]) should ===(true)
      es.subscribe(a3.ref, classOf[CC]) should ===(true)
      es.subscribe(a4.ref, classOf[CCATBT]) should ===(true)
      es.unsubscribe(a3.ref, classOf[CC]) should ===(true)
      es.publish(tm1)
      es.publish(tm2)
      (a1.expectMsgType[AT]: AT) should ===(tm2)
      (a2.expectMsgType[BT]: BT) should ===(tm2)
      a3.expectNoMessage(1.second)
      (a4.expectMsgType[CCATBT]: CCATBT) should ===(tm2)
      es.unsubscribe(a1.ref, classOf[AT]) should ===(true)
      es.unsubscribe(a2.ref, classOf[BT]) should ===(true)
      es.unsubscribe(a4.ref, classOf[CCATBT]) should ===(true)
    }

    "manage sub-channels using classes and traits (update on unsubscribe all)" in {
      val es = new EventStream(system, false)
      val tm1 = new CC
      val tm2 = new CCATBT
      val a1, a2, a3, a4 = TestProbe()

      es.subscribe(a1.ref, classOf[AT]) should ===(true)
      es.subscribe(a2.ref, classOf[BT]) should ===(true)
      es.subscribe(a3.ref, classOf[CC]) should ===(true)
      es.subscribe(a4.ref, classOf[CCATBT]) should ===(true)
      es.unsubscribe(a3.ref)
      es.publish(tm1)
      es.publish(tm2)
      (a1.expectMsgType[AT]: AT) should ===(tm2)
      (a2.expectMsgType[BT]: BT) should ===(tm2)
      a3.expectNoMessage(1.second)
      (a4.expectMsgType[CCATBT]: CCATBT) should ===(tm2)
      es.unsubscribe(a1.ref, classOf[AT]) should ===(true)
      es.unsubscribe(a2.ref, classOf[BT]) should ===(true)
      es.unsubscribe(a4.ref, classOf[CCATBT]) should ===(true)
    }

    "manage sub-channels using classes and traits (update on publish)" in {
      val es = new EventStream(system, false)
      val tm1 = new CC
      val tm2 = new CCATBT
      val a1, a2 = TestProbe()

      es.subscribe(a1.ref, classOf[AT]) should ===(true)
      es.subscribe(a2.ref, classOf[BT]) should ===(true)
      es.publish(tm1)
      es.publish(tm2)
      (a1.expectMsgType[AT]: AT) should ===(tm2)
      (a2.expectMsgType[BT]: BT) should ===(tm2)
      es.unsubscribe(a1.ref, classOf[AT]) should ===(true)
      es.unsubscribe(a2.ref, classOf[BT]) should ===(true)
    }

    "manage sub-channels using classes and traits (unsubscribe classes used with trait)" in {
      val es = new EventStream(system, false)
      val tm1 = new CC
      val tm2 = new CCATBT
      val a1, a2, a3 = TestProbe()

      es.subscribe(a1.ref, classOf[AT]) should ===(true)
      es.subscribe(a2.ref, classOf[BT]) should ===(true)
      es.subscribe(a2.ref, classOf[CC]) should ===(true)
      es.subscribe(a3.ref, classOf[CC]) should ===(true)
      es.unsubscribe(a2.ref, classOf[CC]) should ===(true)
      es.unsubscribe(a3.ref, classOf[CCATBT]) should ===(true)
      es.publish(tm1)
      es.publish(tm2)
      (a1.expectMsgType[AT]: AT) should ===(tm2)
      (a2.expectMsgType[BT]: BT) should ===(tm2)
      (a3.expectMsgType[CC]: CC) should ===(tm1)
      es.unsubscribe(a1.ref, classOf[AT]) should ===(true)
      es.unsubscribe(a2.ref, classOf[BT]) should ===(true)
      es.unsubscribe(a3.ref, classOf[CC]) should ===(true)
    }

    "manage sub-channels using classes and traits (subscribe after publish)" in {
      val es = new EventStream(system, false)
      val tm1 = new CCATBT
      val a1, a2 = TestProbe()

      es.subscribe(a1.ref, classOf[AT]) should ===(true)
      es.publish(tm1)
      (a1.expectMsgType[AT]: AT) should ===(tm1)
      a2.expectNoMessage(1.second)
      es.subscribe(a2.ref, classOf[BTT]) should ===(true)
      es.publish(tm1)
      (a1.expectMsgType[AT]: AT) should ===(tm1)
      (a2.expectMsgType[BTT]: BTT) should ===(tm1)
      es.unsubscribe(a1.ref, classOf[AT]) should ===(true)
      es.unsubscribe(a2.ref, classOf[BTT]) should ===(true)
    }

    "unsubscribe an actor on its termination" in {
      val sys = ActorSystem("EventStreamSpecUnsubscribeOnTerminated", configUnhandledWithDebug)

      try {
        val es = sys.eventStream
        val a1, a2 = TestProbe()
        val tm = new A

        val target = sys.actorOf(Props(new Actor {
          def receive = { case in => a1.ref.forward(in) }
        }), "to-be-killed")

        es.subscribe(a2.ref, classOf[Any])
        es.subscribe(target, classOf[A]) should ===(true)
        es.subscribe(target, classOf[A]) should ===(false)

        target ! PoisonPill
        fishForDebugMessage(a2, s"unsubscribing $target from all channels")
        fishForDebugMessage(a2, s"unwatching $target")

        es.publish(tm)

        a1.expectNoMessage(1.second)
        a2.expectMsg(tm)
      } finally {
        shutdown(sys)
      }
    }

    "unsubscribe the actor, when it subscribes already in terminated state" in {
      val sys = ActorSystem("EventStreamSpecUnsubscribeTerminated", configUnhandledWithDebug)

      try {
        val es = sys.eventStream
        val probe = TestProbe()

        val terminated = system.actorOf(Props(new Actor {
          def receive = { case _ => }
        }), "to-be-killed")

        watch(terminated)
        terminated ! PoisonPill
        expectTerminated(terminated)

        es.subscribe(probe.ref, classOf[Any])

        // target1 is Terminated; When subscribing, it will be unsubscribed by the Unsubscriber right away
        es.subscribe(terminated, classOf[A]) should ===(true)
        fishForDebugMessage(probe, s"unsubscribing $terminated from all channels")

        awaitAssert {
          // when we try to re-subscribe it, that should be ok, since not among the subscribed
          es.subscribe(terminated, classOf[A]) should ===(true)
        }
        fishForDebugMessage(probe, s"unsubscribing $terminated from all channels")
      } finally {
        shutdown(sys)
      }
    }

    "not allow initializing a TerminatedUnsubscriber twice" in {
      val sys = ActorSystem("MustNotAllowDoubleInitOfTerminatedUnsubscriber")
      // initializes an TerminatedUnsubscriber during start

      try {
        val es = sys.eventStream
        val p = TestProbe()

        val refWillBeUsedAsUnsubscriber = es.initUnsubscriber(p.ref)

        refWillBeUsedAsUnsubscriber should equal(false)

      } finally {
        shutdown(sys)
      }
    }

    "unwatch an actor from unsubscriber when that actor unsubscribes from the stream" in {
      val sys = ActorSystem("MustUnregisterDuringUnsubscribe", configUnhandledWithDebug)

      try {
        val es = sys.eventStream
        val a1, a2 = TestProbe()

        es.subscribe(a1.ref, classOf[Logging.Debug])

        es.subscribe(a2.ref, classOf[A])
        fishForDebugMessage(a1, s"watching ${a2.ref}")

        es.unsubscribe(a2.ref)
        fishForDebugMessage(a1, s"unwatching ${a2.ref}")

      } finally {
        shutdown(sys)
      }
    }

    "unwatch an actor from unsubscriber when that actor unsubscribes from channels it subscribed" in {
      val sys = ActorSystem("MustUnregisterWhenNoMoreChannelSubscriptions", configUnhandledWithDebug)

      try {
        val es = sys.eventStream
        val a1, a2 = TestProbe()

        es.subscribe(a1.ref, classOf[Logging.Debug])

        es.subscribe(a2.ref, classOf[A])
        es.subscribe(a2.ref, classOf[T])
        fishForDebugMessage(a1, s"watching ${a2.ref}")
        fishForDebugMessage(a1, s"watching ${a2.ref}") // the unsubscriber "starts to watch" each time, as watching is idempotent

        es.unsubscribe(a2.ref, classOf[A]) should equal(true)
        fishForDebugMessage(a1, s"unsubscribing ${a2.ref} from channel class akka.event.EventStreamSpec$$A")
        a1.expectNoMessage(1.second)

        es.unsubscribe(a2.ref, classOf[T]) should equal(true)
        // order of log entries is not deterministic, one is on this thread one from actor, but should be both these
        val debugLogEntries = Set(a1.expectMsgType[Logging.Debug], a1.expectMsgType[Logging.Debug]).collect {
          case Logging.Debug(_, _, msg: String) => msg
        }
        debugLogEntries.exists(
          _.startsWith(s"unsubscribing ${a2.ref} from channel interface akka.event.EventStreamSpec$$T")) should ===(
          true)
        debugLogEntries.exists(_.startsWith(s"unwatching ${a2.ref}, since has no subscriptions")) should ===(true)
        a1.expectNoMessage(1.second)

        es.unsubscribe(a2.ref, classOf[T]) should equal(false)

      } finally {
        shutdown(sys)
      }
    }

  }

  private def verifyLevel(bus: LoggingBus, level: Logging.LogLevel): Unit = {
    import Logging._
    val allmsg =
      Seq(Debug("", null, "debug"), Info("", null, "info"), Warning("", null, "warning"), Error("", null, "error"))
    val msg = allmsg.filter(_.level <= level)
    allmsg.foreach(bus.publish)
    msg.foreach(expectMsg(_))
  }

  private def fishForDebugMessage(a: TestProbe, messagePrefix: String): Unit = {
    a.fishForMessage(hint = "expected debug message prefix: " + messagePrefix) {
      case Logging.Debug(_, _, msg: String) if msg.startsWith(messagePrefix) => true
      case _                                                                 => false
    }
  }

}
