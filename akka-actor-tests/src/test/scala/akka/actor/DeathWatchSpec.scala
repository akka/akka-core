/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.concurrent.Await
import scala.concurrent.duration._

import language.postfixOps

import akka.actor.Props.EmptyActor
import akka.dispatch.sysmsg.{ DeathWatchNotification, Failed }
import akka.pattern.ask
import akka.testkit._

class LocalDeathWatchSpec extends AkkaSpec with ImplicitSender with DefaultTimeout with DeathWatchSpec

object DeathWatchSpec {
  object Watcher {
    def props(target: ActorRef, testActor: ActorRef) =
      Props(classOf[Watcher], target, testActor)
  }
  class Watcher(target: ActorRef, testActor: ActorRef) extends Actor {
    context.watch(target)
    def receive = {
      case t: Terminated => testActor.forward(WrappedTerminated(t))
      case x             => testActor.forward(x)
    }
  }

  class EmptyWatcher(target: ActorRef) extends Actor {
    context.watch(target)
    def receive = Actor.emptyBehavior
  }

  class NKOTBWatcher(testActor: ActorRef) extends Actor {
    def receive = {
      case "NKOTB" =>
        val currentKid = context.watch(context.actorOf(Props(new Actor {
          def receive = { case "NKOTB" => context.stop(self) }
        }), "kid"))
        currentKid.forward("NKOTB")
        context.become {
          case Terminated(`currentKid`) =>
            testActor ! "GREEN"
            context.unbecome()
        }
    }
  }

  class WUWatcher extends Actor {
    def receive = {
      case W(ref) => context.watch(ref)
      case U(ref) => context.unwatch(ref)
      case Latches(t1: TestLatch, t2: TestLatch) =>
        t1.countDown()
        Await.ready(t2, 3.seconds)
    }
  }

  /**
   * Forwarding `Terminated` to non-watching testActor is not possible,
   * and therefore the `Terminated` message is wrapped.
   */
  final case class WrappedTerminated(t: Terminated)

  final case class W(ref: ActorRef)
  final case class U(ref: ActorRef)
  final case class FF(fail: Failed)

  final case class Latches(t1: TestLatch, t2: TestLatch) extends NoSerializationVerificationNeeded

  object WatchWithVerifier {
    case class WatchThis(ref: ActorRef)
    case object Watching
    case class CustomWatchMsg(ref: ActorRef)
    case class StartStashing(numberOfMessagesToStash: Int)
    case object StashingStarted

    def props(probe: ActorRef) = Props(new WatchWithVerifier(probe))
  }
  class WatchWithVerifier(probe: ActorRef) extends Actor with Stash {
    import WatchWithVerifier._
    private var stashing = false
    private var stashNMessages = 0

    override def receive: Receive = {
      case StartStashing(messagesToStash) =>
        stashing = true
        stashNMessages = messagesToStash
        sender() ! StashingStarted
      case WatchThis(ref) =>
        context.watchWith(ref, CustomWatchMsg(ref))
        sender() ! Watching
      case _ if stashing =>
        stash()
        stashNMessages -= 1
        if (stashNMessages == 0) {
          stashing = false
          unstashAll()
        }
      case msg: CustomWatchMsg =>
        probe ! msg
    }
  }
}

trait DeathWatchSpec { this: AkkaSpec with ImplicitSender with DefaultTimeout =>

  import DeathWatchSpec._

  lazy val supervisor = system.actorOf(Props(classOf[Supervisor], SupervisorStrategy.defaultStrategy), "watchers")

  def startWatching(target: ActorRef) =
    Await.result((supervisor ? Watcher.props(target, testActor)).mapTo[ActorRef], 3 seconds)

  "The Death Watch" must {
    def expectTerminationOf(actorRef: ActorRef) =
      expectMsgPF(5 seconds, "" + actorRef + ": Stopped or Already terminated when linking") {
        case WrappedTerminated(Terminated(`actorRef`)) => true
      }

    "notify with one Terminated message when an Actor is stopped" in {
      val terminal = system.actorOf(Props.empty)
      startWatching(terminal) ! "hallo"
      expectMsg("hallo")

      terminal ! PoisonPill

      expectTerminationOf(terminal)
    }

    "notify with one Terminated message when an Actor is already dead" in {
      val terminal = system.actorOf(Props.empty)

      terminal ! PoisonPill

      startWatching(terminal)
      expectTerminationOf(terminal)
    }

    "notify with all monitors with one Terminated message when an Actor is stopped" in {
      val terminal = system.actorOf(Props.empty)
      val monitor1, monitor2, monitor3 = startWatching(terminal)

      terminal ! PoisonPill

      expectTerminationOf(terminal)
      expectTerminationOf(terminal)
      expectTerminationOf(terminal)

      system.stop(monitor1)
      system.stop(monitor2)
      system.stop(monitor3)
    }

    "notify with _current_ monitors with one Terminated message when an Actor is stopped" in {
      val terminal = system.actorOf(Props.empty)
      val monitor1, monitor3 = startWatching(terminal)
      val monitor2 = system.actorOf(Props(new Actor {
        context.watch(terminal)
        context.unwatch(terminal)
        def receive = {
          case "ping"        => sender() ! "pong"
          case t: Terminated => testActor ! WrappedTerminated(t)
        }
      }).withDeploy(Deploy.local))

      monitor2 ! "ping"

      expectMsg("pong") //Needs to be here since watch and unwatch are asynchronous

      terminal ! PoisonPill

      expectTerminationOf(terminal)
      expectTerminationOf(terminal)

      system.stop(monitor1)
      system.stop(monitor2)
      system.stop(monitor3)
    }

    "notify with a Terminated message once when an Actor is stopped but not when restarted" in {
      filterException[ActorKilledException] {
        val supervisor =
          system.actorOf(Props(new Supervisor(OneForOneStrategy(maxNrOfRetries = 2)(List(classOf[Exception])))))
        val terminalProps = TestActors.echoActorProps
        val terminal = Await.result((supervisor ? terminalProps).mapTo[ActorRef], timeout.duration)

        startWatching(terminal)

        terminal ! Kill
        terminal ! Kill
        Await.result(terminal ? "foo", timeout.duration) should ===("foo")
        terminal ! Kill

        expectTerminationOf(terminal)
        terminal.isTerminated should ===(true)

        system.stop(supervisor)
      }
    }

    "fail a monitor which does not handle Terminated()" in {
      filterEvents(EventFilter[ActorKilledException](), EventFilter[DeathPactException]()) {
        val strategy = new OneForOneStrategy()(SupervisorStrategy.defaultStrategy.decider) {
          override def handleFailure(
              context: ActorContext,
              child: ActorRef,
              cause: Throwable,
              stats: ChildRestartStats,
              children: Iterable[ChildRestartStats]) = {
            testActor.tell(FF(Failed(child, cause, 0)), child)
            super.handleFailure(context, child, cause, stats, children)
          }
        }
        val supervisor = system.actorOf(Props(new Supervisor(strategy)).withDeploy(Deploy.local))

        val failed = Await.result((supervisor ? Props.empty).mapTo[ActorRef], timeout.duration)
        val brother =
          Await.result((supervisor ? Props(classOf[EmptyWatcher], failed)).mapTo[ActorRef], timeout.duration)

        startWatching(brother)

        failed ! Kill
        val result = receiveWhile(3 seconds, messages = 3) {
          case FF(Failed(_, _: ActorKilledException, _)) if lastSender eq failed       => 1
          case FF(Failed(_, DeathPactException(`failed`), _)) if lastSender eq brother => 2
          case WrappedTerminated(Terminated(`brother`))                                => 3
        }
        testActor.isTerminated should not be true
        result should ===(Seq(1, 2, 3))
      }
    }

    "be able to watch a child with the same name after the old died" in {
      val parent = system.actorOf(Props(classOf[NKOTBWatcher], testActor).withDeploy(Deploy.local))

      parent ! "NKOTB"
      expectMsg("GREEN")
      parent ! "NKOTB"
      expectMsg("GREEN")
    }

    "only notify when watching" in {
      val subject = system.actorOf(Props[EmptyActor]())

      testActor
        .asInstanceOf[InternalActorRef]
        .sendSystemMessage(DeathWatchNotification(subject, existenceConfirmed = true, addressTerminated = false))

      // the testActor is not watching subject and will not receive a Terminated msg
      expectNoMessage()
    }

    "discard Terminated when unwatched between sysmsg and processing" in {
      val t1, t2 = TestLatch()
      val w = system.actorOf(Props[WUWatcher]().withDeploy(Deploy.local), "myDearWatcher")
      val p = TestProbe()
      w ! W(p.ref)
      w ! Latches(t1, t2)
      Await.ready(t1, 3.seconds)
      watch(p.ref)
      system.stop(p.ref)
      expectTerminated(p.ref)
      w ! U(p.ref)
      t2.countDown()
      /*
       * now the WUWatcher will
       * - process the DeathWatchNotification and enqueue Terminated
       * - process the unwatch command
       * - process the Terminated
       * If it receives the Terminated it will die, which in fact it should not
       */
      w ! Identify(())
      expectMsg(ActorIdentity((), Some(w)))
      w ! Identify(())
      expectMsg(ActorIdentity((), Some(w)))
    }

    "watch with custom message" in {
      val verifierProbe = TestProbe()
      val verifier = system.actorOf(WatchWithVerifier.props(verifierProbe.ref))
      val subject = system.actorOf(Props[EmptyActor]())
      verifier ! WatchWithVerifier.WatchThis(subject)
      expectMsg(WatchWithVerifier.Watching)

      subject ! PoisonPill
      verifierProbe.expectMsg(WatchWithVerifier.CustomWatchMsg(subject))
    }

    // Coverage for #29101
    "stash watchWith termination message correctly" in {
      val verifierProbe = TestProbe()
      val verifier = system.actorOf(WatchWithVerifier.props(verifierProbe.ref))
      val subject = system.actorOf(Props[EmptyActor]())
      verifier ! WatchWithVerifier.WatchThis(subject)
      expectMsg(WatchWithVerifier.Watching)
      verifier ! WatchWithVerifier.StartStashing(numberOfMessagesToStash = 1)
      expectMsg(WatchWithVerifier.StashingStarted)

      subject ! PoisonPill
      verifierProbe.expectMsg(WatchWithVerifier.CustomWatchMsg(subject))
    }
  }

}
