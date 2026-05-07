/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.{ ConcurrentLinkedQueue, RejectedExecutionException }
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.language.postfixOps

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor.setup.ActorSystemSetup
import akka.dispatch._
import akka.japi.Util.immutableSeq
import akka.pattern.ask
import akka.testkit.{ TestKit, _ }
import akka.util.{ Switch, Timeout }
import akka.util.Helpers.ConfigOps

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

object ActorSystemSpec {

  class Waves extends Actor {
    var master: ActorRef = _
    var terminaters = Set[ActorRef]()

    def receive = {
      case n: Int =>
        master = sender()
        terminaters = Set() ++ (for (_ <- 1 to n) yield {
            val man = context.watch(context.system.actorOf(Props[Terminater]()))
            man ! "run"
            man
          })
      case Terminated(child) if terminaters contains child =>
        terminaters -= child
        if (terminaters.isEmpty) {
          master ! "done"
          context.stop(self)
        }
    }

    override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {
      if (master ne null) {
        master ! "failed with " + cause + " while processing " + msg
      }
      context.stop(self)
    }
  }

  class Terminater extends Actor {
    def receive = {
      case "run" => context.stop(self)
    }
  }

  class Strategy extends SupervisorStrategyConfigurator {
    def create() = OneForOneStrategy() {
      case _ => SupervisorStrategy.Escalate
    }
  }

  final case class FastActor(latch: TestLatch, testActor: ActorRef) extends Actor {
    val ref1 = context.actorOf(Props.empty)
    context.actorSelection(ref1.path.toString).tell(Identify(ref1), testActor)
    latch.countDown()

    def receive = {
      case _ =>
    }
  }

  class SlowDispatcher(_config: Config, _prerequisites: DispatcherPrerequisites)
      extends MessageDispatcherConfigurator(_config, _prerequisites) {
    private val instance: Dispatcher = new Dispatcher(
      this,
      this.config.getString("id"),
      this.config.getInt("throughput"),
      this.config.getNanosDuration("throughput-deadline-time"),
      configureExecutor(),
      this.config.getMillisDuration("shutdown-timeout")) {
      val doneIt = new Switch
      override protected[akka] def registerForExecution(
          mbox: Mailbox,
          hasMessageHint: Boolean,
          hasSystemMessageHint: Boolean): Boolean = {
        val ret = super.registerForExecution(mbox, hasMessageHint, hasSystemMessageHint)
        doneIt.switchOn {
          TestKit.awaitCond(mbox.actor.actor != null, 1.second)
          mbox.actor.actor match {
            case FastActor(latch, _) => Await.ready(latch, 1.second)
            case _                   => throw new IllegalStateException()
          }
        }
        ret
      }
    }

    /**
     * Returns the same dispatcher instance for each invocation
     */
    override def dispatcher(): MessageDispatcher = instance
  }

  val config = s"""
      slow {
        type="${classOf[SlowDispatcher].getName}"
      }"""

}

class ActorSystemSpec extends AkkaSpec(ActorSystemSpec.config) with ImplicitSender with ScalaCheckPropertyChecks {

  import ActorSystemSpec.FastActor

  "An ActorSystem" must {

    "reject common invalid names" in {
      for (n <- Seq(
             "-hallowelt",
             "_hallowelt",
             "hallo welt",
             "hallo*welt",
             "hallo@welt",
             "hallo#welt",
             "hallo$welt",
             "hallo%welt",
             "hallo/welt")) intercept[IllegalArgumentException] {
        ActorSystem(n)
      }
    }

    "reject all invalid names" in {
      forAll { (name: String) =>
        whenever(!name.matches("""^[a-zA-Z0-9][a-zA-Z0-9-_]*$""")) {
          an[IllegalArgumentException] should be thrownBy ActorSystem(name)
        }
      }
    }

    "allow valid names" in {
      shutdown(ActorSystem("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"))
    }

    "log dead letters" in {
      val sys =
        ActorSystem("LogDeadLetters", ConfigFactory.parseString("akka.loglevel=INFO").withFallback(AkkaSpec.testConf))
      try {
        val probe = TestProbe()(sys)
        val a = sys.actorOf(Props[ActorSystemSpec.Terminater]())
        probe.watch(a)
        a.tell("run", probe.ref)
        probe.expectTerminated(a)
        EventFilter
          .info(pattern = """from Actor\[akka://LogDeadLetters/system/testProbe.*not delivered""", occurrences = 1)
          .intercept {
            EventFilter
              .warning(
                pattern = """received dead letter from Actor\[akka://LogDeadLetters/system/testProbe""",
                occurrences = 1)
              .intercept {
                a.tell("boom", probe.ref)
              }(sys)
          }(sys)

      } finally shutdown(sys)
    }

    "log dead letters sent without sender reference" in {
      val sys =
        ActorSystem("LogDeadLetters", ConfigFactory.parseString("akka.loglevel=INFO").withFallback(AkkaSpec.testConf))
      try {
        val probe = TestProbe()(sys)
        val a = sys.actorOf(Props[ActorSystemSpec.Terminater]())
        probe.watch(a)
        a.tell("run", probe.ref)
        probe.expectTerminated(a)

        // Expecting two log entries: one from the actor system at info level and one at warning level from the logging testkit
        EventFilter
          .info(pattern = ".*not delivered", occurrences = 1)
          .intercept {
            EventFilter
              .warning(pattern = "received dead letter: boom", occurrences = 1)
              .intercept {
                a.tell("boom", ActorRef.noSender)
              }(sys)
          }(sys)

      } finally shutdown(sys)
    }

    "run termination callbacks in order" in {
      val system2 = ActorSystem("TerminationCallbacks", AkkaSpec.testConf)
      val result = new ConcurrentLinkedQueue[Int]
      val count = 10
      val latch = TestLatch(count)

      for (i <- 1 to count) {
        system2.registerOnTermination {
          Thread.sleep((i % 3).millis.dilated.toMillis)
          result.add(i)
          latch.countDown()
        }
      }

      system2.terminate()
      Await.ready(latch, 5 seconds)

      val expected = (for (i <- 1 to count) yield i).reverse

      immutableSeq(result) should ===(expected)
    }

    "awaitTermination after termination callbacks" in {
      val system2 = ActorSystem("AwaitTermination", AkkaSpec.testConf)
      @volatile
      var callbackWasRun = false

      system2.registerOnTermination {
        Thread.sleep(50.millis.dilated.toMillis)
        callbackWasRun = true
      }
      import system.dispatcher
      system2.scheduler.scheduleOnce(200.millis.dilated) { system2.terminate() }

      Await.ready(system2.whenTerminated, 5 seconds)
      callbackWasRun should ===(true)
    }

    "return isTerminated status correctly" in {
      val system = ActorSystem().asInstanceOf[ActorSystemImpl]
      val wt = system.whenTerminated
      wt.isCompleted should ===(false)
      val f = system.terminate()
      val terminated = Await.result(wt, 10 seconds)
      system.whenTerminated.isCompleted should ===(true)
      terminated.actor should ===(system.provider.rootGuardian)
      terminated.addressTerminated should ===(true)
      terminated.existenceConfirmed should ===(true)
      (terminated should be).theSameInstanceAs(Await.result(f, 10 seconds))
    }

    "throw RejectedExecutionException when shutdown" in {
      val system2 = ActorSystem("RejectedExecution-1", AkkaSpec.testConf)
      Await.ready(system2.terminate(), 10 seconds)

      intercept[RejectedExecutionException] {
        system2.registerOnTermination { println("IF YOU SEE THIS THEN THERE'S A BUG HERE") }
      }.getMessage should ===("ActorSystem already terminated.")
    }

    "throw RejectedExecutionException or run termination callback when shutting down" in {
      val system2 = ActorSystem("RejectedExecution-2", AkkaSpec.testConf)
      // using counter to detect double calls
      val count = new AtomicInteger

      try {
        system2.terminate()
        system2.registerOnTermination { count.incrementAndGet() }
      } catch {
        case _: RejectedExecutionException => count.incrementAndGet()
      }

      Await.ready(system2.whenTerminated, 10.seconds)
      count.get() should ===(1)
    }

    "reliably create waves of actors" in {
      import system.dispatcher
      implicit val timeout: Timeout = Timeout((20 seconds).dilated)
      val waves = for (_ <- 1 to 3) yield system.actorOf(Props[ActorSystemSpec.Waves]()) ? 50000
      Await.result(Future.sequence(waves), timeout.duration + 5.seconds) should ===(Vector("done", "done", "done"))
    }

    "find actors that just have been created" in {
      system.actorOf(Props(new FastActor(TestLatch(), testActor)).withDispatcher("slow"))
      expectMsgType[ActorIdentity].ref should !==(None)
    }

    "reliable deny creation of actors while shutting down" in {
      val system = ActorSystem()
      import system.dispatcher
      system.scheduler.scheduleOnce(100 millis) { system.terminate() }
      var failing = false
      var created = Vector.empty[ActorRef]
      while (!system.whenTerminated.isCompleted) {
        try {
          val t = system.actorOf(Props[ActorSystemSpec.Terminater]())
          failing should not be true // because once failing => always failing (it’s due to shutdown)
          created :+= t
          if (created.size % 1000 == 0) Thread.sleep(50) // in case of unfair thread scheduling
        } catch {
          case _: IllegalStateException => failing = true
        }

        if (!failing && system.uptime >= 10) {
          println(created.last)
          println(system.asInstanceOf[ExtendedActorSystem].printTree)
          fail("System didn't terminate within 5 seconds")
        }
      }

      created.filter(ref =>
        !ref.isTerminated && !ref.asInstanceOf[ActorRefWithCell].underlying.isInstanceOf[UnstartedCell]) should ===(
        Seq.empty[ActorRef])
    }

    "shut down when /user fails" in {
      implicit val system = ActorSystem("Stop", AkkaSpec.testConf)
      EventFilter[ActorKilledException]().intercept {
        system.actorSelection("/user") ! Kill
        Await.ready(system.whenTerminated, Duration.Inf)
      }
    }

    "allow configuration of guardian supervisor strategy" in {
      implicit val system =
        ActorSystem(
          "Stop",
          ConfigFactory
            .parseString("akka.actor.guardian-supervisor-strategy=akka.actor.StoppingSupervisorStrategy")
            .withFallback(AkkaSpec.testConf))
      val a = system.actorOf(Props(new Actor {
        def receive = {
          case "die" => throw new Exception("hello")
        }
      }))
      val probe = TestProbe()
      probe.watch(a)
      EventFilter[Exception]("hello", occurrences = 1).intercept {
        a ! "die"
      }
      val t = probe.expectMsg(Terminated(a)(existenceConfirmed = true, addressTerminated = false))
      t.existenceConfirmed should ===(true)
      t.addressTerminated should ===(false)
      shutdown(system)
    }

    "shut down when /user escalates" in {
      implicit val system =
        ActorSystem(
          "Stop",
          ConfigFactory
            .parseString("akka.actor.guardian-supervisor-strategy=\"akka.actor.ActorSystemSpec$Strategy\"")
            .withFallback(AkkaSpec.testConf))
      val a = system.actorOf(Props(new Actor {
        def receive = {
          case "die" => throw new Exception("hello")
        }
      }))
      EventFilter[Exception]("hello").intercept {
        a ! "die"
        Await.ready(system.whenTerminated, Duration.Inf)
      }
    }

    "not allow top-level actor creation with custom guardian" in {
      val sys = new ActorSystemImpl(
        "custom",
        ConfigFactory.defaultReference(),
        getClass.getClassLoader,
        None,
        Some(Props.empty),
        ActorSystemSetup.empty)
      sys.start()
      try {
        intercept[UnsupportedOperationException] {
          sys.actorOf(Props.empty)
        }
        intercept[UnsupportedOperationException] {
          sys.actorOf(Props.empty, "empty")
        }
      } finally shutdown(sys)
    }
  }

}
