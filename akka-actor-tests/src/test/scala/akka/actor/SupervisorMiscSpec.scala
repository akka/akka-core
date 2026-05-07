/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal
import language.postfixOps
import akka.pattern.ask
import akka.testkit.{ filterEvents, EventFilter }
import akka.testkit.AkkaSpec
import akka.testkit.DefaultTimeout
import akka.testkit.TestException

import java.util.concurrent.atomic.AtomicInteger

object SupervisorMiscSpec {
  val config = """
    pinned-dispatcher {
      executor = thread-pool-executor
      type = PinnedDispatcher
    }
    test-dispatcher {
    }
    """
}

class SupervisorMiscSpec extends AkkaSpec(SupervisorMiscSpec.config) with DefaultTimeout {

  "A Supervisor" must {

    "restart a crashing actor and its dispatcher for any dispatcher" in {
      filterEvents(EventFilter[Exception]("Kill")) {
        val countDownLatch = new CountDownLatch(4)

        val supervisor = system.actorOf(Props(
          new Supervisor(OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 5 seconds)(List(classOf[Exception])))))

        val workerProps = Props(new Actor {
          override def postRestart(cause: Throwable): Unit = { countDownLatch.countDown() }
          def receive = {
            case "status" => this.sender() ! "OK"
            case _        => this.context.stop(self)
          }
        })

        val actor1, actor2 =
          Await.result((supervisor ? workerProps.withDispatcher("pinned-dispatcher")).mapTo[ActorRef], timeout.duration)

        val actor3 =
          Await.result((supervisor ? workerProps.withDispatcher("test-dispatcher")).mapTo[ActorRef], timeout.duration)

        val actor4 =
          Await.result((supervisor ? workerProps.withDispatcher("pinned-dispatcher")).mapTo[ActorRef], timeout.duration)

        actor1 ! Kill
        actor2 ! Kill
        actor3 ! Kill
        actor4 ! Kill

        countDownLatch.await(10, TimeUnit.SECONDS)

        Seq("actor1" -> actor1, "actor2" -> actor2, "actor3" -> actor3, "actor4" -> actor4)
          .map {
            case (id, ref) => (id, ref ? "status")
          }
          .foreach {
            case (id, f) => (id, Await.result(f, timeout.duration)) should ===((id, "OK"))
          }
      }
    }

    "be able to create named children in its constructor" in {
      val a = system.actorOf(Props(new Actor {
        context.actorOf(Props.empty, "bob")
        def receive = { case x: Exception => throw x }
        override def preStart(): Unit = testActor ! "preStart"
      }))
      val m = "weird message"
      EventFilter[Exception](m, occurrences = 1).intercept {
        a ! new Exception(m)
      }
      expectMsg("preStart")
      expectMsg("preStart")
      a.isTerminated should ===(false)
    }

    "be able to recreate child when old child is Terminated" in {
      val parent = system.actorOf(Props(new Actor {
        val kid = context.watch(context.actorOf(Props.empty, "foo"))
        def receive = {
          case Terminated(`kid`) =>
            try {
              val newKid = context.actorOf(Props.empty, "foo")
              val result =
                if (newKid eq kid) "Failure: context.actorOf returned the same instance!"
                else if (!kid.isTerminated) "Kid is zombie"
                else if (newKid.isTerminated) "newKid was stillborn"
                else if (kid.path != newKid.path) "The kids do not share the same path"
                else "green"
              testActor ! result
            } catch {
              case NonFatal(e) => testActor ! e
            }
          case "engage" => context.stop(kid)
        }
      }))
      parent ! "engage"
      expectMsg("green")
    }

    "not be able to recreate child when old child is alive" in {
      val parent = system.actorOf(Props(new Actor {
        def receive = {
          case "engage" =>
            try {
              val kid = context.actorOf(Props.empty, "foo")
              context.stop(kid)
              context.actorOf(Props.empty, "foo")
              testActor ! "red"
            } catch {
              case _: InvalidActorNameException => testActor ! "green"
            }
        }
      }))
      parent ! "engage"
      expectMsg("green")
    }

    "be able to create a similar kid in the fault handling strategy" in {
      val parent = system.actorOf(Props(new Actor {
        override val supervisorStrategy = new OneForOneStrategy()(SupervisorStrategy.defaultStrategy.decider) {
          override def handleChildTerminated(
              context: ActorContext,
              child: ActorRef,
              children: Iterable[ActorRef]): Unit = {
            val newKid = context.actorOf(Props.empty, child.path.name)
            testActor ! { if ((newKid ne child) && newKid.path == child.path) "green" else "red" }
          }
        }

        def receive = { case "engage" => context.stop(context.actorOf(Props.empty, "Robert")) }
      }))
      parent ! "engage"
      expectMsg("green")
      EventFilter[IllegalStateException]("handleChildTerminated failed", occurrences = 1).intercept {
        system.stop(parent)
      }
    }

    "have access to the failing child’s reference in supervisorStrategy" in {
      val parent = system.actorOf(Props(new Actor {
        override val supervisorStrategy = OneForOneStrategy() {
          case _: Exception => testActor ! sender(); SupervisorStrategy.Stop
        }
        def receive = {
          case "doit" => context.actorOf(Props.empty, "child") ! Kill
        }
      }))
      EventFilter[ActorKilledException](occurrences = 1).intercept {
        parent ! "doit"
      }
      val p = expectMsgType[ActorRef].path
      p.parent should ===(parent.path)
      p.name should ===("child")
    }

    "allow supervision of ActorInitializationException" in {
      val counter = new AtomicInteger(2)

      val supervisor =
        system.actorOf(Props(new Supervisor(OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 50 seconds) {
          case _: ActorInitializationException => SupervisorStrategy.Restart
          case _                               => SupervisorStrategy.Escalate
        })))

      val workerProps = Props(new Actor {
        override def preStart(): Unit = {
          if (counter.decrementAndGet() > 0)
            throw TestException("BOOM")
        }

        def receive = {
          case "status" => this.sender() ! "OK"
          case _        => this.context.stop(self)
        }
      })

      val actor1 =
        Await.result((supervisor ? workerProps).mapTo[ActorRef], timeout.duration)

      Await.result((actor1 ? "status").mapTo[String], timeout.duration) should ===("OK")
    }
  }
}
