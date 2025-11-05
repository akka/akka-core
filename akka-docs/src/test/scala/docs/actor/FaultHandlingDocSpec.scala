/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor

import language.postfixOps
import akka.actor.{ ActorRef, ActorSystem, Props, Terminated }
import FaultHandlingDocSpec._

//#testkit
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import akka.testkit.{ EventFilter, ImplicitSender, TestKit }

//#testkit
object FaultHandlingDocSpec {
  //#supervisor
  //#child
  import akka.actor.Actor

  //#child
  class Supervisor extends Actor {
    //#strategy
    import akka.actor.OneForOneStrategy
    import akka.actor.SupervisorStrategy._
    import scala.concurrent.duration._

    override val supervisorStrategy =
      OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
        case _: ArithmeticException      => Resume
        case _: NullPointerException     => Restart
        case _: IllegalArgumentException => Stop
        case _: Exception                => Escalate
      }
    //#strategy

    def receive = {
      case p: Props => sender() ! context.actorOf(p)
    }
  }
  //#supervisor

  //#supervisor2
  class Supervisor2 extends Actor {
    //#strategy2
    import akka.actor.OneForOneStrategy
    import akka.actor.SupervisorStrategy._
    import scala.concurrent.duration._

    override val supervisorStrategy =
      OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
        case _: ArithmeticException      => Resume
        case _: NullPointerException     => Restart
        case _: IllegalArgumentException => Stop
        case _: Exception                => Escalate
      }
    //#strategy2

    def receive = {
      case p: Props => sender() ! context.actorOf(p)
    }
    // override default to kill all children during restart
    override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {}
  }
  //#supervisor2

  class Supervisor3 extends Actor {
    //#default-strategy-fallback
    import akka.actor.OneForOneStrategy
    import akka.actor.SupervisorStrategy._
    import scala.concurrent.duration._

    override val supervisorStrategy =
      OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
        case _: ArithmeticException => Resume
        case t =>
          super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
      }
    //#default-strategy-fallback

    def receive = Actor.emptyBehavior
  }

  //#child
  class Child extends Actor {
    var state = 0
    def receive = {
      case ex: Exception => throw ex
      case x: Int        => state = x
      case "get"         => sender() ! state
    }
  }
  //#child

  val testConf: Config = ConfigFactory.parseString("""
      akka {
        loggers = ["akka.testkit.TestEventListener"]
      }
  """)
}
//#testkit
class FaultHandlingDocSpec(_system: ActorSystem)
    extends TestKit(_system)
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  def this() =
    this(
      ActorSystem(
        "FaultHandlingDocSpec",
        ConfigFactory.parseString("""
      akka {
        loggers = ["akka.testkit.TestEventListener"]
        loglevel = "WARNING"
      }
      """)))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A supervisor" must {
    "apply the chosen strategy for its child" in {
      //#testkit

      //#create
      val supervisor = system.actorOf(Props[Supervisor](), "supervisor")

      supervisor ! Props[Child]()
      val child = expectMsgType[ActorRef] // retrieve answer from TestKit’s testActor
      //#create
      EventFilter.warning(occurrences = 1).intercept {
        //#resume
        child ! 42 // set state to 42
        child ! "get"
        expectMsg(42)

        child ! new ArithmeticException // crash it
        child ! "get"
        expectMsg(42)
        //#resume
      }
      EventFilter[NullPointerException](occurrences = 1).intercept {
        //#restart
        child ! new NullPointerException // crash it harder
        child ! "get"
        expectMsg(0)
        //#restart
      }
      EventFilter[IllegalArgumentException](occurrences = 1).intercept {
        //#stop
        watch(child) // have testActor watch “child”
        child ! new IllegalArgumentException // break it
        expectMsgPF() { case Terminated(`child`) => () }
        //#stop
      }
      EventFilter[Exception]("CRASH", occurrences = 2).intercept {
        //#escalate-kill
        supervisor ! Props[Child]() // create new child
        val child2 = expectMsgType[ActorRef]
        watch(child2)
        child2 ! "get" // verify it is alive
        expectMsg(0)

        child2 ! new Exception("CRASH") // escalate failure
        expectMsgPF() {
          case t @ Terminated(`child2`) if t.existenceConfirmed => ()
        }
        //#escalate-kill
        //#escalate-restart
        val supervisor2 = system.actorOf(Props[Supervisor2](), "supervisor2")

        supervisor2 ! Props[Child]()
        val child3 = expectMsgType[ActorRef]

        child3 ! 23
        child3 ! "get"
        expectMsg(23)

        child3 ! new Exception("CRASH")
        child3 ! "get"
        expectMsg(0)
        //#escalate-restart
      }
      //#testkit
      // code here
    }
  }
}
//#testkit
