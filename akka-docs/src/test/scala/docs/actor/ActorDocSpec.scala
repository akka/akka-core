/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.actor

import akka.actor.Kill
import jdocs.actor.ImmutableMessage

import language.postfixOps
import scala.annotation.nowarn

//#imports1
import akka.actor.Actor
import akka.actor.Props
import akka.event.Logging

//#imports1

import scala.concurrent.Future
import akka.actor.{ ActorLogging, ActorRef, ActorSystem, PoisonPill, Terminated }
import akka.testkit._
import akka.util._
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.CoordinatedShutdown

//#my-actor
class MyActor extends Actor {
  val log = Logging(context.system, this)

  def receive = {
    case "test" => log.info("received test")
    case _      => log.info("received unknown message")
  }
}
//#my-actor

final case class DoIt(msg: ImmutableMessage)
final case class Message(s: String)

//#context-actorOf
class FirstActor extends Actor {
  val child = context.actorOf(Props[MyActor](), name = "myChild")
  //#plus-some-behavior
  def receive = {
    case x => sender() ! x
  }
  //#plus-some-behavior
}
//#context-actorOf

@nowarn("msg=never used") // sample snippets
class ActorWithArgs(arg: String) extends Actor {
  def receive = { case _ => () }
}

//#actor-with-value-class-argument
class Argument(val value: String) extends AnyVal
//#actor-with-value-class-argument
@nowarn("msg=never used") // sample snippets
//#actor-with-value-class-argument
class ValueClassActor(arg: Argument) extends Actor {
  def receive = { case _ => () }
}

object ValueClassActor {
  def props1(arg: Argument) = Props(classOf[ValueClassActor], arg) // fails at runtime
  def props2(arg: Argument) = Props(classOf[ValueClassActor], arg.value) // ok
  def props3(arg: Argument) = Props(new ValueClassActor(arg)) // ok
}
//#actor-with-value-class-argument

class DemoActorWrapper extends Actor {
  //#props-factory
  object DemoActor {

    /**
     * Create Props for an actor of this type.
     *
     * @param magicNumber The magic number to be passed to this actor’s constructor.
     * @return a Props for creating this actor, which can then be further configured
     *         (e.g. calling `.withDispatcher()` on it)
     */
    def props(magicNumber: Int): Props = Props(new DemoActor(magicNumber))
  }

  class DemoActor(magicNumber: Int) extends Actor {
    def receive = {
      case x: Int => sender() ! (x + magicNumber)
    }
  }

  @nowarn("msg=never used") // sample snippets
  class SomeOtherActor extends Actor {
    // Props(new DemoActor(42)) would not be safe
    context.actorOf(DemoActor.props(42), "demo")
    // ...
    //#props-factory
    def receive = {
      case msg =>
    }
    //#props-factory
  }
  //#props-factory

  def receive = Actor.emptyBehavior
}

class ActorWithMessagesWrapper {
  //#messages-in-companion
  object MyActor {
    case class Greeting(from: String)
    case object Goodbye
  }
  class MyActor extends Actor with ActorLogging {
    import MyActor._
    def receive = {
      case Greeting(greeter) => log.info(s"I was greeted by $greeter.")
      case Goodbye           => log.info("Someone said goodbye to me.")
    }
  }
  //#messages-in-companion

  def receive = Actor.emptyBehavior
}

class Hook extends Actor {
  var child: ActorRef = _
  //#preStart
  override def preStart(): Unit = {
    child = context.actorOf(Props[MyActor](), "child")
  }
  //#preStart
  def receive = Actor.emptyBehavior
  //#postStop
  override def postStop(): Unit = {
    //#clean-up-some-resources
    ()
    //#clean-up-some-resources
  }
  //#postStop
}

class ReplyException extends Actor {
  def receive = {
    case _ =>
      //#reply-exception
      try {
        val result = operation()
        sender() ! result
      } catch {
        case e: Exception =>
          sender() ! akka.actor.Status.Failure(e)
          throw e
      }
    //#reply-exception
  }

  def operation(): String = { "Hi" }

}

class StoppingActorsWrapper {
  //#stoppingActors-actor
  class MyActor extends Actor {

    val child: ActorRef = ???

    def receive = {
      case "interrupt-child" =>
        context.stop(child)

      case "done" =>
        context.stop(self)
    }

  }

  //#stoppingActors-actor
}

//#gracefulStop-actor
object Manager {
  case object Shutdown
}

class Manager extends Actor {
  import Manager._
  val worker = context.watch(context.actorOf(Props[Cruncher](), "worker"))

  def receive = {
    case "job" => worker ! "crunch"
    case Shutdown =>
      worker ! PoisonPill
      context.become(shuttingDown)
  }

  def shuttingDown: Receive = {
    case "job" => sender() ! "service unavailable, shutting down"
    case Terminated(`worker`) =>
      context.stop(self)
  }
}
//#gracefulStop-actor

class Cruncher extends Actor {
  def receive = {
    case "crunch" => // crunch...
  }
}

//#swapper
case object Swap
class Swapper extends Actor {
  import context._
  val log = Logging(system, this)

  def receive = {
    case Swap =>
      log.info("Hi")
      become({
        case Swap =>
          log.info("Ho")
          unbecome() // resets the latest 'become' (just for fun)
      }, discardOld = false) // push on top instead of replace
  }
}

object SwapperApp extends App {
  val system = ActorSystem("SwapperSystem")
  val swap = system.actorOf(Props[Swapper](), name = "swapper")
  swap ! Swap // logs Hi
  swap ! Swap // logs Ho
  swap ! Swap // logs Hi
  swap ! Swap // logs Ho
  swap ! Swap // logs Hi
  swap ! Swap // logs Ho
}
//#swapper

//#receive-orElse

trait ProducerBehavior {
  this: Actor =>

  val producerBehavior: Receive = {
    case GiveMeThings =>
      sender() ! Give("thing")
  }
}

trait ConsumerBehavior {
  this: Actor with ActorLogging =>

  val consumerBehavior: Receive = {
    case ref: ActorRef =>
      ref ! GiveMeThings

    case Give(thing) =>
      log.info("Got a thing! It's {}", thing)
  }
}

class Producer extends Actor with ProducerBehavior {
  def receive = producerBehavior
}

class Consumer extends Actor with ActorLogging with ConsumerBehavior {
  def receive = consumerBehavior
}

class ProducerConsumer extends Actor with ActorLogging with ProducerBehavior with ConsumerBehavior {

  def receive = producerBehavior.orElse[Any, Unit](consumerBehavior)
}

// protocol
case object GiveMeThings
final case class Give(thing: Any)

//#receive-orElse

//#immutable-message-definition
case class User(name: String)

// define the case class
case class Register(user: User)
//#immutable-message-definition

@nowarn("msg=never used") // sample snippets
class ActorDocSpec extends AkkaSpec("""
  akka.loglevel = INFO
  akka.loggers = []
  """) {

  "import context" in {
    new AnyRef {
      //#import-context
      class FirstActor extends Actor {
        import context._
        val myActor = actorOf(Props[MyActor](), name = "myactor")
        def receive = {
          case x => myActor ! x
        }
      }
      //#import-context

      val first = system.actorOf(Props(classOf[FirstActor], this), name = "first")
      system.stop(first)
    }
  }

  "creating actor with system.actorOf" in {
    val myActor = system.actorOf(Props[MyActor]())

    // testing the actor

    // TODO: convert docs to AkkaSpec(Map(...))
    val filter = EventFilter.custom {
      case e: Logging.Info => true
      case _               => false
    }
    system.eventStream.publish(TestEvent.Mute(filter))
    system.eventStream.subscribe(testActor, classOf[Logging.Info])

    myActor ! "test"
    expectMsgPF(1 second) { case Logging.Info(_, _, "received test") => true }

    myActor ! "unknown"
    expectMsgPF(1 second) { case Logging.Info(_, _, "received unknown message") => true }

    system.eventStream.unsubscribe(testActor)
    system.eventStream.publish(TestEvent.UnMute(filter))

    system.stop(myActor)
  }

  "instantiates a case class" in {
    //#immutable-message-instantiation
    val user = User("Mike")
    // create a new case class message
    val message = Register(user)
    //#immutable-message-instantiation
  }

  "use poison pill" in {
    val victim = system.actorOf(Props[MyActor]())
    //#poison-pill
    watch(victim)
    victim ! PoisonPill
    //#poison-pill
    expectTerminated(victim)
  }

  "creating a Props config" in {
    //#creating-props
    import akka.actor.Props

    val props1 = Props[MyActor]()
    val props2 = Props(new ActorWithArgs("arg")) // careful, see below
    val props3 = Props(classOf[ActorWithArgs], "arg") // no support for value class arguments
    //#creating-props

    //#creating-props-deprecated
    // NOT RECOMMENDED within another actor:
    // encourages to close over enclosing class
    val props7 = Props(new MyActor)
    //#creating-props-deprecated
  }

  "creating actor with Props" in {
    //#system-actorOf
    import akka.actor.ActorSystem

    // ActorSystem is a heavy object: create only one per application
    val system = ActorSystem("mySystem")
    val myActor = system.actorOf(Props[MyActor](), "myactor2")
    //#system-actorOf
    shutdown(system)
  }
  private abstract class DummyActorProxy {
    def actorRef: ActorRef
  }

  "creating actor with IndirectActorProducer" in {
    class Echo(name: String) extends Actor {
      def receive = {
        case n: Int => sender() ! name
        case message =>
          val target = testActor
          //#forward
          target.forward(message)
        //#forward
      }
    }

    val a: DummyActorProxy = new DummyActorProxy() {
      val applicationContext = this

      //#creating-indirectly
      import akka.actor.IndirectActorProducer

      class DependencyInjector(applicationContext: AnyRef, beanName: String) extends IndirectActorProducer {

        override def actorClass = classOf[Actor]
        override def produce() =
          //#obtain-fresh-Actor-instance-from-DI-framework
          new Echo(beanName)

        def this(beanName: String) = this("", beanName)
        //#obtain-fresh-Actor-instance-from-DI-framework
      }

      val actorRef: ActorRef =
        system.actorOf(Props(classOf[DependencyInjector], applicationContext, "hello"), "helloBean")
      //#creating-indirectly
    }

    val actorRef = a.actorRef

    val message = 42
    implicit val self = testActor
    //#tell
    actorRef ! message
    //#tell
    expectMsg("hello")
    actorRef ! "huhu"
    expectMsg("huhu")
  }

  "using implicit timeout" in {
    val myActor = system.actorOf(Props[FirstActor]())
    //#using-implicit-timeout
    import scala.concurrent.duration._
    import akka.util.Timeout
    import akka.pattern.ask
    implicit val timeout: Timeout = 5.seconds
    val future = myActor ? "hello"
    //#using-implicit-timeout
    Await.result(future, timeout.duration) should be("hello")

  }

  "using explicit timeout" in {
    val myActor = system.actorOf(Props[FirstActor]())
    //#using-explicit-timeout
    import scala.concurrent.duration._
    import akka.pattern.ask
    val future = myActor.ask("hello")(5 seconds)
    //#using-explicit-timeout
    Await.result(future, 5 seconds) should be("hello")
  }

  "using receiveTimeout" in {
    //#receive-timeout
    import akka.actor.ReceiveTimeout
    import scala.concurrent.duration._
    class MyActor extends Actor {
      // To set an initial delay
      context.setReceiveTimeout(30 milliseconds)
      def receive = {
        case "Hello" =>
          // To set in a response to a message
          context.setReceiveTimeout(100 milliseconds)
        case ReceiveTimeout =>
          // To turn it off
          context.setReceiveTimeout(Duration.Undefined)
          throw new RuntimeException("Receive timed out")
      }
    }
    //#receive-timeout
  }

  //#hot-swap-actor
  class HotSwapActor extends Actor {
    import context._
    def angry: Receive = {
      case "foo" => sender() ! "I am already angry?"
      case "bar" => become(happy)
    }

    def happy: Receive = {
      case "bar" => sender() ! "I am already happy :-)"
      case "foo" => become(angry)
    }

    def receive = {
      case "foo" => become(angry)
      case "bar" => become(happy)
    }
  }
  //#hot-swap-actor

  "using hot-swap" in {
    val actor = system.actorOf(Props(classOf[HotSwapActor], this), name = "hot")
  }

  "using Stash" in {
    //#stash
    import akka.actor.Stash
    class ActorWithProtocol extends Actor with Stash {
      def receive = {
        case "open" =>
          unstashAll()
          context.become({
            case "write" => // do writing...
            case "close" =>
              unstashAll()
              context.unbecome()
            case msg => stash()
          }, discardOld = false) // stack on top instead of replacing
        case msg => stash()
      }
    }
    //#stash
  }

  "using watch" in {
    new AnyRef {
      //#watch
      import akka.actor.{ Actor, Props, Terminated }

      class WatchActor extends Actor {
        val child = context.actorOf(Props.empty, "child")
        context.watch(child) // <-- this is the only call needed for registration
        var lastSender = context.system.deadLetters

        def receive = {
          case "kill" =>
            context.stop(child)
            lastSender = sender()
          case Terminated(`child`) =>
            lastSender ! "finished"
        }
      }
      //#watch

      val victim = system.actorOf(Props(classOf[WatchActor], this))
      victim.tell("kill", testActor)
      expectMsg("finished")
    }
  }

  "using Kill" in {
    val victim = system.actorOf(TestActors.echoActorProps)
    implicit val sender = testActor
    val context = this

    //#kill
    context.watch(victim) // watch the Actor to receive Terminated message once it dies

    victim ! Kill

    expectMsgPF(hint = "expecting victim to terminate") {
      case Terminated(v) if v == victim => v // the Actor has indeed terminated
    }
    //#kill
  }

  "demonstrate ActorSelection" in {
    val context = system
    //#selection-local
    // will look up this absolute path
    context.actorSelection("/user/serviceA/aggregator")
    // will look up sibling beneath same supervisor
    context.actorSelection("../joe")
    //#selection-local
    //#selection-wildcard
    // will look all children to serviceB with names starting with worker
    context.actorSelection("/user/serviceB/worker*")
    // will look up all siblings beneath same supervisor
    context.actorSelection("../*")
    //#selection-wildcard
    //#selection-remote
    context.actorSelection("akka://app@otherhost:1234/user/serviceB")
    //#selection-remote
  }

  "using Identify" in {
    new AnyRef {
      //#identify
      import akka.actor.{ Actor, ActorIdentity, Identify, Props, Terminated }

      class Follower extends Actor {
        val identifyId = 1
        context.actorSelection("/user/another") ! Identify(identifyId)

        def receive = {
          case ActorIdentity(`identifyId`, Some(ref)) =>
            context.watch(ref)
            context.become(active(ref))
          case ActorIdentity(`identifyId`, None) => context.stop(self)

        }

        def active(another: ActorRef): Actor.Receive = {
          case Terminated(`another`) => context.stop(self)
        }
      }
      //#identify

      val a = system.actorOf(Props.empty)
      val b = system.actorOf(Props(classOf[Follower], this))
      watch(b)
      system.stop(a)
      expectMsgType[akka.actor.Terminated].actor should be(b)
    }
  }

  "using pattern gracefulStop" in {
    val actorRef = system.actorOf(Props[Manager]())
    //#gracefulStop
    import akka.pattern.gracefulStop
    import scala.concurrent.Await

    try {
      val stopped: Future[Boolean] = gracefulStop(actorRef, 5 seconds, Manager.Shutdown)
      Await.result(stopped, 6 seconds)
      // the actor has been stopped
    } catch {
      // the actor wasn't stopped within 5 seconds
      case e: akka.pattern.AskTimeoutException =>
    }
    //#gracefulStop
  }

  "using pattern ask / pipeTo" in {
    val actorA, actorB, actorC, actorD = system.actorOf(Props.empty)
    //#ask-pipeTo
    import akka.pattern.{ ask, pipe }
    import system.dispatcher // The ExecutionContext that will be used
    final case class Result(x: Int, s: String, d: Double)
    case object Request

    implicit val timeout: Timeout = 5.seconds // needed for `?` below

    val f: Future[Result] =
      for {
        x <- ask(actorA, Request).mapTo[Int] // call pattern directly
        s <- actorB.ask(Request).mapTo[String] // call by implicit conversion
        d <- (actorC ? Request).mapTo[Double] // call by symbolic name
      } yield Result(x, s, d)

    f.pipeTo(actorD) // .. or ..
    pipe(f) to actorD
    //#ask-pipeTo
  }

  class Replier extends Actor {
    def receive = {
      case ref: ActorRef =>
        //#reply-with-sender
        sender().tell("reply", context.parent) // replies will go back to parent
        sender().!("reply")(context.parent) // alternative syntax
      //#reply-with-sender
      case x =>
        //#reply-without-sender
        sender() ! x // replies will go to this actor
      //#reply-without-sender
    }
  }

  "replying with own or other sender" in {
    val actor = system.actorOf(Props(classOf[Replier], this))
    implicit val me = testActor
    actor ! 42
    expectMsg(42)
    lastSender should be(actor)
    actor ! me
    expectMsg("reply")
    lastSender.path.toStringWithoutAddress should be("/user")
    expectMsg("reply")
    lastSender.path.toStringWithoutAddress should be("/user")
  }

  "using CoordinatedShutdown" in {
    // other snippets moved to docs.actor.typed.CoordinatedActorShutdownSpec
    { // https://github.com/akka/akka-core/issues/29056
      val someActor = system.actorOf(Props(classOf[Replier], this))
      someActor ! PoisonPill
      //#coordinated-shutdown-addActorTerminationTask
      CoordinatedShutdown(system).addActorTerminationTask(
        CoordinatedShutdown.PhaseBeforeServiceUnbind,
        "someTaskName",
        someActor,
        Some("stop"))
      //#coordinated-shutdown-addActorTerminationTask
    }
  }

}
