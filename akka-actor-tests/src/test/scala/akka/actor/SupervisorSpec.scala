/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import language.postfixOps
import org.scalatest.BeforeAndAfterEach

import akka.{ Die, Ping }
import akka.ConfigurationException
import akka.dispatch.MailboxType
import akka.dispatch.MessageQueue
import akka.pattern.ask
import akka.routing.RoundRobinPool
import akka.testkit._
import akka.testkit.TestEvent._

object SupervisorSpec {
  val Timeout = 5.seconds

  case object DieReply

  // =====================================================
  // Message logs
  // =====================================================

  val PingMessage = "ping"
  val PongMessage = "pong"
  val ExceptionMessage = "Expected exception; to test fault-tolerance"

  // =====================================================
  // Actors
  // =====================================================

  class PingPongActor(sendTo: ActorRef) extends Actor {
    def receive = {
      case Ping =>
        sendTo ! PingMessage
        if (sender() != sendTo)
          sender() ! PongMessage
      case Die =>
        throw new RuntimeException(ExceptionMessage)
      case DieReply =>
        val e = new RuntimeException(ExceptionMessage)
        sender() ! Status.Failure(e)
        throw e
    }

    override def postRestart(reason: Throwable): Unit = {
      sendTo ! reason.getMessage
    }
  }

  class Master(sendTo: ActorRef) extends Actor {
    val temp = context.watch(context.actorOf(Props(new PingPongActor(sendTo))))

    var s: ActorRef = _

    override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 0)(List(classOf[Exception]))

    def receive = {
      case Die                => temp.forward(Die)
      case Terminated(`temp`) => sendTo ! "terminated"
      case Status.Failure(_)  => /*Ignore*/
    }
  }

  class Creator(target: ActorRef) extends Actor {
    override val supervisorStrategy = OneForOneStrategy() {
      case ex =>
        target ! ((self, sender(), ex))
        SupervisorStrategy.Stop
    }
    def receive = {
      case p: Props => sender() ! context.actorOf(p)
    }
  }

  def creator(target: ActorRef, fail: Boolean = false) = {
    val p = Props(new Creator(target))
    if (fail) p.withMailbox("error-mailbox") else p
  }

  val failure = new AssertionError("deliberate test failure")

  class Mailbox(@nowarn("msg=never used") settings: ActorSystem.Settings, @nowarn("msg=never used") config: Config)
      extends MailboxType {
    override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue =
      throw failure
  }

  val config = ConfigFactory.parseString("""
    error-mailbox {
      mailbox-type = "akka.actor.SupervisorSpec$Mailbox"
    }
    """)
}

class SupervisorSpec
    extends AkkaSpec(SupervisorSpec.config)
    with BeforeAndAfterEach
    with ImplicitSender
    with DefaultTimeout {

  import SupervisorSpec._

  val DilatedTimeout = Timeout.dilated

  // =====================================================
  // Creating actors and supervisors
  // =====================================================

  private def child(supervisor: ActorRef, props: Props): ActorRef =
    Await.result((supervisor ? props).mapTo[ActorRef], timeout.duration)

  def temporaryActorAllForOne = {
    val supervisor =
      system.actorOf(Props(new Supervisor(AllForOneStrategy(maxNrOfRetries = 0)(List(classOf[Exception])))))
    val temporaryActor = child(supervisor, Props(new PingPongActor(testActor)))

    (temporaryActor, supervisor)
  }

  def singleActorAllForOne = {
    val supervisor = system.actorOf(
      Props(
        new Supervisor(
          AllForOneStrategy(maxNrOfRetries = 3, withinTimeRange = DilatedTimeout)(List(classOf[Exception])))))
    val pingpong = child(supervisor, Props(new PingPongActor(testActor)))

    (pingpong, supervisor)
  }

  def singleActorOneForOne = {
    val supervisor = system.actorOf(
      Props(
        new Supervisor(
          OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = DilatedTimeout)(List(classOf[Exception])))))
    val pingpong = child(supervisor, Props(new PingPongActor(testActor)))

    (pingpong, supervisor)
  }

  def multipleActorsAllForOne = {
    val supervisor = system.actorOf(
      Props(
        new Supervisor(
          AllForOneStrategy(maxNrOfRetries = 3, withinTimeRange = DilatedTimeout)(List(classOf[Exception])))))
    val pingpong1, pingpong2, pingpong3 = child(supervisor, Props(new PingPongActor(testActor)))

    (pingpong1, pingpong2, pingpong3, supervisor)
  }

  def multipleActorsOneForOne = {
    val supervisor = system.actorOf(
      Props(
        new Supervisor(
          OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = DilatedTimeout)(List(classOf[Exception])))))
    val pingpong1, pingpong2, pingpong3 = child(supervisor, Props(new PingPongActor(testActor)))

    (pingpong1, pingpong2, pingpong3, supervisor)
  }

  def nestedSupervisorsAllForOne = {
    val topSupervisor = system.actorOf(
      Props(
        new Supervisor(
          AllForOneStrategy(maxNrOfRetries = 3, withinTimeRange = DilatedTimeout)(List(classOf[Exception])))))
    val pingpong1 = child(topSupervisor, Props(new PingPongActor(testActor)))

    val middleSupervisor = child(
      topSupervisor,
      Props(new Supervisor(AllForOneStrategy(maxNrOfRetries = 3, withinTimeRange = DilatedTimeout)(Nil))))
    val pingpong2, pingpong3 = child(middleSupervisor, Props(new PingPongActor(testActor)))

    (pingpong1, pingpong2, pingpong3, topSupervisor)
  }

  override def atStartup(): Unit = {
    system.eventStream.publish(Mute(EventFilter[RuntimeException](ExceptionMessage)))
  }

  override def beforeEach() = {}

  def ping(pingPongActor: ActorRef) = {
    Await.result(pingPongActor.?(Ping)(DilatedTimeout), DilatedTimeout) should ===(PongMessage)
    expectMsg(Timeout, PingMessage)
  }

  def kill(pingPongActor: ActorRef) = {
    val result = pingPongActor.?(DieReply)(DilatedTimeout)
    expectMsg(Timeout, ExceptionMessage) //this is sent from PingPongActor's postRestart()
    intercept[RuntimeException] { Await.result(result, DilatedTimeout) }
  }

  def killExpectNoRestart(pingPongActor: ActorRef) = {
    val result = pingPongActor.?(DieReply)(DilatedTimeout)
    expectNoMessage(500 milliseconds)
    intercept[RuntimeException] { Await.result(result, DilatedTimeout) }
  }

  "A supervisor" must {

    "not restart child more times than permitted" in {
      val master = system.actorOf(Props(new Master(testActor)))

      master ! Die
      expectMsg(3 seconds, "terminated")
      expectNoMessage(1 second)
    }

    "restart properly when same instance is returned" in {
      val restarts = 3 //max number of restarts

      // can't be anonymous class due to https://github.com/akka/akka-core/issues/32128
      class ChildActor extends Actor {
        var preRestarts = 0
        var postRestarts = 0
        var preStarts = 0
        var postStops = 0

        override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
          preRestarts += 1;
          testActor ! ("preRestart" + preRestarts)
        }

        override def postRestart(reason: Throwable): Unit = {
          postRestarts += 1;
          testActor ! ("postRestart" + postRestarts)
        }

        override def preStart(): Unit = {
          preStarts += 1; testActor ! ("preStart" + preStarts)
        }

        override def postStop(): Unit = {
          postStops += 1; testActor ! ("postStop" + postStops)
        }

        def receive = {
          case "crash" => {
            testActor ! "crashed"; throw new RuntimeException("Expected")
          }
          case "ping" => sender() ! "pong"
        }
      }

      lazy val childInstance = new ChildActor

      val master = system.actorOf(Props(new Actor {
        override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = restarts)(List(classOf[Exception]))
        val child = context.actorOf(Props(childInstance))

        def receive = {
          case msg => child.forward(msg)
        }
      }))

      expectMsg("preStart1")

      master ! "ping"
      expectMsg("pong")

      filterEvents(EventFilter[RuntimeException]("Expected", occurrences = restarts + 1)) {
        (1 to restarts).foreach { i =>
          master ! "crash"
          expectMsg("crashed")

          expectMsg("preRestart" + i)
          expectMsg("postRestart" + i)

          master ! "ping"
          expectMsg("pong")
        }
        master ! "crash"
        expectMsg("crashed")
        expectMsg("postStop1")
      }

      expectNoMessage(1 second)
    }

    "not restart temporary actor" in {
      val (temporaryActor, _) = temporaryActorAllForOne

      intercept[RuntimeException] { Await.result(temporaryActor.?(DieReply)(DilatedTimeout), DilatedTimeout) }

      expectNoMessage(1 second)
    }

    "start server for nested supervisor hierarchy" in {
      val (actor1, _, _, _) = nestedSupervisorsAllForOne
      ping(actor1)
      expectNoMessage(1 second)
    }

    "kill single actor OneForOne" in {
      val (actor, _) = singleActorOneForOne
      kill(actor)
    }

    "call-kill-call single actor OneForOne" in {
      val (actor, _) = singleActorOneForOne
      ping(actor)
      kill(actor)
      ping(actor)
    }

    "kill single actor AllForOne" in {
      val (actor, _) = singleActorAllForOne
      kill(actor)
    }

    "call-kill-call single actor AllForOne" in {
      val (actor, _) = singleActorAllForOne
      ping(actor)
      kill(actor)
      ping(actor)
    }

    "kill multiple actors OneForOne 1" in {
      val (actor1, _, _, _) = multipleActorsOneForOne
      kill(actor1)
    }

    "kill multiple actors OneForOne 2" in {
      val (_, _, actor3, _) = multipleActorsOneForOne
      kill(actor3)
    }

    "call-kill-call multiple actors OneForOne" in {
      val (actor1, actor2, actor3, _) = multipleActorsOneForOne

      ping(actor1)
      ping(actor2)
      ping(actor3)

      kill(actor2)

      ping(actor1)
      ping(actor2)
      ping(actor3)
    }

    "kill multiple actors AllForOne" in {
      val (_, actor2, _, _) = multipleActorsAllForOne

      kill(actor2)

      // and two more exception messages
      expectMsg(Timeout, ExceptionMessage)
      expectMsg(Timeout, ExceptionMessage)
    }

    "call-kill-call multiple actors AllForOne" in {
      val (actor1, actor2, actor3, _) = multipleActorsAllForOne

      ping(actor1)
      ping(actor2)
      ping(actor3)

      kill(actor2)

      // and two more exception messages
      expectMsg(Timeout, ExceptionMessage)
      expectMsg(Timeout, ExceptionMessage)

      ping(actor1)
      ping(actor2)
      ping(actor3)
    }

    "one-way kill single actor OneForOne" in {
      val (actor, _) = singleActorOneForOne

      actor ! Die
      expectMsg(Timeout, ExceptionMessage)
    }

    "one-way call-kill-call single actor OneForOne" in {
      val (actor, _) = singleActorOneForOne

      actor ! Ping
      actor ! Die
      actor ! Ping

      expectMsg(Timeout, PingMessage)
      expectMsg(Timeout, ExceptionMessage)
      expectMsg(Timeout, PingMessage)
    }

    "restart killed actors in nested supervisor hierarchy" in {
      val (actor1, actor2, actor3, _) = nestedSupervisorsAllForOne

      ping(actor1)
      ping(actor2)
      ping(actor3)

      kill(actor2)

      // and two more exception messages
      expectMsg(Timeout, ExceptionMessage)
      expectMsg(Timeout, ExceptionMessage)

      ping(actor1)
      ping(actor2)
      ping(actor3)
    }

    "attempt restart when exception during restart" in {
      val inits = new AtomicInteger(0)
      val supervisor = system.actorOf(Props(
        new Supervisor(OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 10 seconds)(classOf[Exception] :: Nil))))

      val dyingProps = Props(new Actor {
        val init = inits.getAndIncrement()
        if (init % 3 == 1) throw new IllegalStateException("Don't wanna!")

        override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {
          if (init % 3 == 0) throw new IllegalStateException("Don't wanna!")
        }

        def receive = {
          case Ping => sender() ! PongMessage
          case DieReply =>
            val e = new RuntimeException("Expected")
            sender() ! Status.Failure(e)
            throw e
        }
      })
      supervisor ! dyingProps
      val dyingActor = expectMsgType[ActorRef]

      filterEvents(
        EventFilter[RuntimeException]("Expected", occurrences = 1),
        EventFilter[PreRestartException]("Don't wanna!", occurrences = 1),
        EventFilter[PostRestartException]("Don't wanna!", occurrences = 1)) {
        intercept[RuntimeException] {
          Await.result(dyingActor.?(DieReply)(DilatedTimeout), DilatedTimeout)
        }
      }

      dyingActor ! Ping
      expectMsg(PongMessage)

      inits.get should ===(3)

      system.stop(supervisor)
    }

    "not lose system messages when a NonFatal exception occurs when processing a system message" in {
      val parent = system.actorOf(Props(new Actor {
        override val supervisorStrategy = OneForOneStrategy()({
          case e: IllegalStateException if e.getMessage == "OHNOES" => throw e
          case _                                                    => SupervisorStrategy.Restart
        })
        val child = context.watch(context.actorOf(Props(new Actor {
          override def postRestart(reason: Throwable): Unit = testActor ! "child restarted"
          def receive = {
            case l: TestLatch => { Await.ready(l, 5 seconds); throw new IllegalStateException("OHNOES") }
            case "test"       => sender() ! "child green"
          }
        }), "child"))

        override def postRestart(reason: Throwable): Unit = testActor ! "parent restarted"

        // Overriding to disable auto-unwatch
        override def preRestart(reason: Throwable, msg: Option[Any]): Unit = {
          context.children.foreach(context.stop)
          postStop()
        }

        def receive = {
          case Terminated(t) if t.path == child.path => testActor ! "child terminated"
          case l: TestLatch                          => child ! l
          case "test"                                => sender() ! "green"
          case "testchild"                           => child.forward("test")
          case "testchildAndAck"                     => child.forward("test"); sender() ! "ack"
        }
      }))

      val latch = TestLatch()
      parent ! latch
      parent ! "testchildAndAck"
      expectMsg("ack")
      filterEvents(
        EventFilter[IllegalStateException]("OHNOES", occurrences = 1),
        EventFilter.warning(pattern = "dead.*test", occurrences = 1)) {
        latch.countDown()
      }
      expectMsg("parent restarted")
      expectMsg("child terminated")
      parent ! "test"
      expectMsg("green")
      parent ! "testchild"
      expectMsg("child green")
    }

    "log pre-creation check failures" when {

      "creating a top-level actor" in EventFilter[ActorInitializationException](occurrences = 1).intercept {
        val ref = system.actorOf(creator(testActor, fail = true))
        watch(ref)
        expectTerminated(ref)
      }

      "creating a normal child actor" in EventFilter[ConfigurationException](occurrences = 1).intercept {
        val top = system.actorOf(creator(testActor))
        top ! creator(testActor)
        val middle = expectMsgType[ActorRef]
        middle ! creator(testActor, fail = true)
        expectMsgPF(hint = "ConfigurationException") {
          case (_, _, ex: ConfigurationException) =>
            ex.getCause should ===(failure)
        }
      }

      "creating a top-level router" in EventFilter[ActorInitializationException](occurrences = 1).intercept {
        val ref = system.actorOf(creator(testActor, fail = true).withRouter(RoundRobinPool(1)))
        watch(ref)
        expectTerminated(ref)
      }

      "creating a router" in EventFilter[ConfigurationException](occurrences = 1).intercept {
        val top = system.actorOf(creator(testActor))
        top ! creator(testActor)
        val middle = expectMsgType[ActorRef]
        middle ! creator(testActor, fail = true).withRouter(RoundRobinPool(1))
        expectMsgPF(hint = "ConfigurationException") {
          case (_, _, ex: ConfigurationException) =>
            ex.getCause should ===(failure)
        }
      }

    }
  }

  "restarts a child infinitely if maxNrOfRetries = -1 and withinTimeRange = Duration.Inf" in {
    val supervisor = system.actorOf(
      Props(new Supervisor(
        OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf)(classOf[Exception] :: Nil))))

    val pingpong = child(supervisor, Props(new PingPongActor(testActor)))

    kill(pingpong)
    kill(pingpong)
    kill(pingpong)
    kill(pingpong)
    kill(pingpong)
    kill(pingpong)
    kill(pingpong)
    ping(pingpong)
  }

  "treats maxNrOfRetries = -1 as maxNrOfRetries = 1 if withinTimeRange is non-infinite Duration" in {
    val supervisor = system.actorOf(Props(
      new Supervisor(OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = 10 seconds)(classOf[Exception] :: Nil))))

    val pingpong = child(supervisor, Props(new PingPongActor(testActor)))

    ping(pingpong)
    kill(pingpong)
    ping(pingpong)
    killExpectNoRestart(pingpong)
  }

  "treats withinTimeRange = Duration.Inf as a single infinite restart window" in {
    val supervisor = system.actorOf(Props(
      new Supervisor(OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = Duration.Inf)(classOf[Exception] :: Nil))))

    val pingpong = child(supervisor, Props(new PingPongActor(testActor)))

    //impossible to confirm if the restart window is infinite, so making sure maxNrOfRetries is respected correctly
    kill(pingpong)
    kill(pingpong)
    kill(pingpong)
    killExpectNoRestart(pingpong)
  }

}
