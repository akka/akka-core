/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.ConfigurationException
import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.testkit._

object RoutingSpec {

  val config = """
    akka.actor.deployment {
      /router1 {
        router = round-robin-pool
        nr-of-instances = 3
      }
      /router2 {
        router = round-robin-pool
        nr-of-instances = 3
      }
      /router3 {
        router = round-robin-pool
        nr-of-instances = 0
      }
    }
    """

  class TestActor extends Actor {
    def receive = { case _ => }
  }

  class Echo extends Actor {
    def receive = {
      case _ => sender() ! self
    }
  }

}

class RoutingSpec extends AkkaSpec(RoutingSpec.config) with DefaultTimeout with ImplicitSender {
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  import RoutingSpec._

  muteDeadLetters(classOf[akka.dispatch.sysmsg.DeathWatchNotification])(system)

  "routers in general" must {

    "evict terminated routees" in {
      val router = system.actorOf(RoundRobinPool(2).props(routeeProps = Props[Echo]()))
      router ! ""
      router ! ""
      val c1, c2 = expectMsgType[ActorRef]
      watch(router)
      watch(c2)
      system.stop(c2)
      expectTerminated(c2).existenceConfirmed should ===(true)
      // it might take a while until the Router has actually processed the Terminated message
      awaitCond {
        router ! ""
        router ! ""
        val res = receiveWhile(100 millis, messages = 2) {
          case x: ActorRef => x
        }
        res == Seq(c1, c1)
      }
      system.stop(c1)
      expectTerminated(router).existenceConfirmed should ===(true)
    }

    "not terminate when resizer is used" in {
      val latch = TestLatch(1)
      val resizer = new Resizer {
        def isTimeForResize(messageCounter: Long): Boolean = messageCounter == 0
        def resize(currentRoutees: immutable.IndexedSeq[Routee]): Int = {
          latch.countDown()
          2
        }
      }
      val router =
        system.actorOf(
          RoundRobinPool(nrOfInstances = 0, resizer = Some(resizer)).props(routeeProps = Props[TestActor]()))
      watch(router)
      Await.ready(latch, remainingOrDefault)
      router ! GetRoutees
      val routees = expectMsgType[Routees].routees
      routees.size should ===(2)
      routees.foreach { _.send(PoisonPill, testActor) }
      // expect no Terminated
      expectNoMessage(2.seconds)
    }

    "use configured nr-of-instances when FromConfig" in {
      val router = system.actorOf(FromConfig.props(routeeProps = Props[TestActor]()), "router1")
      router ! GetRoutees
      expectMsgType[Routees].routees.size should ===(3)
      watch(router)
      system.stop(router)
      expectTerminated(router)
    }

    "use configured nr-of-instances when router is specified" in {
      val router = system.actorOf(RoundRobinPool(nrOfInstances = 2).props(routeeProps = Props[TestActor]()), "router2")
      router ! GetRoutees
      expectMsgType[Routees].routees.size should ===(3)
      system.stop(router)
    }

    "use specified resizer when resizer not configured" in {
      val latch = TestLatch(1)
      val resizer = new Resizer {
        def isTimeForResize(messageCounter: Long): Boolean = messageCounter == 0
        def resize(currentRoutees: immutable.IndexedSeq[Routee]): Int = {
          latch.countDown()
          3
        }
      }
      val router =
        system.actorOf(
          RoundRobinPool(nrOfInstances = 0, resizer = Some(resizer)).props(routeeProps = Props[TestActor]()),
          "router3")
      Await.ready(latch, remainingOrDefault)
      router ! GetRoutees
      expectMsgType[Routees].routees.size should ===(3)
      system.stop(router)
    }

    "set supplied supervisorStrategy" in {
      //#supervision
      val escalator = OneForOneStrategy() {
        //#custom-strategy
        case e => testActor ! e; SupervisorStrategy.Escalate
        //#custom-strategy
      }
      val router =
        system.actorOf(RoundRobinPool(1, supervisorStrategy = escalator).props(routeeProps = Props[TestActor]()))
      //#supervision
      router ! GetRoutees
      EventFilter[ActorKilledException](occurrences = 1).intercept {
        expectMsgType[Routees].routees.head.send(Kill, testActor)
      }
      expectMsgType[ActorKilledException]

      val router2 =
        system.actorOf(RoundRobinPool(1).withSupervisorStrategy(escalator).props(routeeProps = Props[TestActor]()))
      router2 ! GetRoutees
      EventFilter[ActorKilledException](occurrences = 1).intercept {
        expectMsgType[Routees].routees.head.send(Kill, testActor)
      }
      expectMsgType[ActorKilledException]
    }

    "set supplied supervisorStrategy for FromConfig" in {
      val escalator = OneForOneStrategy() {
        case e => testActor ! e; SupervisorStrategy.Escalate
      }
      val router =
        system.actorOf(FromConfig.withSupervisorStrategy(escalator).props(routeeProps = Props[TestActor]()), "router1")
      router ! GetRoutees
      EventFilter[ActorKilledException](occurrences = 1).intercept {
        expectMsgType[Routees].routees.head.send(Kill, testActor)
      }
      expectMsgType[ActorKilledException]
    }

    "default to all-for-one-always-escalate strategy" in {
      val restarter = OneForOneStrategy() {
        case e => testActor ! e; SupervisorStrategy.Restart
      }
      val supervisor = system.actorOf(Props(new Supervisor(restarter)))
      supervisor ! RoundRobinPool(3).props(routeeProps = Props(new Actor {
        def receive = {
          case x: String => throw new Exception(x)
        }
        override def postRestart(reason: Throwable): Unit = testActor ! "restarted"
      }))
      val router = expectMsgType[ActorRef]
      EventFilter[Exception]("die", occurrences = 1).intercept {
        router ! "die"
      }
      expectMsgType[Exception].getMessage should ===("die")
      expectMsg("restarted")
      expectMsg("restarted")
      expectMsg("restarted")
    }

    "start in-line for context.actorOf()" in {
      system.actorOf(Props(new Actor {
        def receive = {
          case "start" =>
            (context.actorOf(RoundRobinPool(2).props(routeeProps = Props(new Actor {
              def receive = { case x => sender() ! x }
            }))) ? "hello").pipeTo(sender())
        }
      })) ! "start"
      expectMsg("hello")
    }

  }

  "no router" must {

    "send message to connection" in {
      class Actor1 extends Actor {
        def receive = {
          case msg => testActor.forward(msg)
        }
      }

      val routedActor = system.actorOf(NoRouter.props(routeeProps = Props(new Actor1)))
      routedActor ! "hello"
      routedActor ! "end"

      expectMsg("hello")
      expectMsg("end")
    }
  }

  "router FromConfig" must {
    "throw suitable exception when not configured" in {
      val e = intercept[ConfigurationException] {
        system.actorOf(FromConfig.props(routeeProps = Props[TestActor]()), "routerNotDefined")
      }
      e.getMessage should include("routerNotDefined")
    }

    "allow external configuration" in {
      val sys = ActorSystem(
        "FromConfig",
        ConfigFactory
          .parseString("akka.actor.deployment./routed.router=round-robin-pool")
          .withFallback(system.settings.config))
      try {
        sys.actorOf(FromConfig.props(routeeProps = Props[TestActor]()), "routed")
      } finally {
        shutdown(sys)
      }
    }

  }

}
