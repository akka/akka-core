/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl.adapter

import scala.util.control.NoStackTrace

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.{ actor => classic }
import akka.Done
import akka.NotUsed
import akka.actor.ActorInitializationException
import akka.actor.InvalidMessageException
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Terminated
import akka.actor.typed.internal.adapter.SchedulerAdapter
import akka.actor.typed.scaladsl.Behaviors
import akka.serialization.SerializationExtension
import akka.testkit._

object AdapterSpec {
  val classic1: classic.Props = classic.Props(new Classic1)

  class Classic1 extends classic.Actor {
    def receive = {
      case "ping"     => sender() ! "pong"
      case t: ThrowIt => throw t
    }
  }

  val classicFailInConstructor: classic.Props = classic.Props(new ClassicFailInConstructor)

  class ClassicFailInConstructor extends classic.Actor {
    throw new TestException("Exception in constructor")
    def receive = {
      case "ping" => sender() ! "pong"
    }
  }

  def classicForwarder(ref: classic.ActorRef): classic.Props = classic.Props(new ClassicForwarder(ref))

  class ClassicForwarder(ref: classic.ActorRef) extends classic.Actor {
    def receive = {
      case a: String => ref ! a
    }
  }

  def typed1(ref: classic.ActorRef, probe: ActorRef[String]): Behavior[String] =
    Behaviors
      .receive[String] { (context, message) =>
        message match {
          case "send" =>
            val replyTo = context.self.toClassic
            ref.tell("ping", replyTo)
            Behaviors.same
          case "pong" =>
            probe ! "ok"
            Behaviors.same
          case "actorOf" =>
            val child = context.actorOf(classic1)
            child.tell("ping", context.self.toClassic)
            Behaviors.same
          case "watch" =>
            context.watch(ref)
            Behaviors.same
          case "supervise-restart" =>
            // restart is the default
            val child = context.actorOf(classic1)
            context.watch(child)
            child ! ThrowIt3
            child.tell("ping", context.self.toClassic)
            Behaviors.same
          case "supervise-start-fail" =>
            val child = context.actorOf(classicFailInConstructor)
            context.watch(child)
            Behaviors.same
          case "stop-child" =>
            val child = context.actorOf(classic1)
            context.watch(child)
            context.stop(child)
            Behaviors.same
        }
      }
      .receiveSignal {
        case (_, Terminated(_)) =>
          probe ! "terminated"
          Behaviors.same
      }

  def unhappyTyped(msg: String): Behavior[String] = Behaviors.setup[String] { ctx =>
    val child = ctx.spawnAnonymous(Behaviors.receiveMessage[String] { _ =>
      throw TestException(msg)
    })
    child ! "throw please"
    Behaviors.empty
  }

  sealed trait Typed2Msg
  final case class Ping(replyTo: ActorRef[String]) extends Typed2Msg
  case object StopIt extends Typed2Msg
  sealed trait ThrowIt extends RuntimeException with Typed2Msg with NoStackTrace
  case object ThrowIt1 extends ThrowIt
  case object ThrowIt2 extends ThrowIt
  case object ThrowIt3 extends ThrowIt

  def classic2(ref: ActorRef[Ping], probe: ActorRef[String]): classic.Props =
    classic.Props(new Classic2(ref, probe))

  class Classic2(ref: ActorRef[Ping], probe: ActorRef[String]) extends classic.Actor {

    override val supervisorStrategy = classic.OneForOneStrategy() {
      ({
        case ThrowIt1 =>
          probe ! "thrown-stop"
          classic.SupervisorStrategy.Stop
        case ThrowIt2 =>
          probe ! "thrown-resume"
          classic.SupervisorStrategy.Resume
        case ThrowIt3 =>
          probe ! "thrown-restart"
          // TODO Restart will not really restart the behavior
          classic.SupervisorStrategy.Restart
      }: classic.SupervisorStrategy.Decider).orElse(classic.SupervisorStrategy.defaultDecider)
    }

    def receive = {
      case "send" => ref ! Ping(self) // implicit conversion
      case "pong" => probe ! "ok"
      case "spawn" =>
        val child = context.spawnAnonymous(typed2)
        child ! Ping(self)
      case "actorOf-props" =>
        // this is how Cluster Sharding can be used
        val child = context.actorOf(typed2Props)
        child ! Ping(self)
      case "watch" =>
        context.watch(ref)
      case classic.Terminated(_) =>
        probe ! "terminated"
      case "supervise-stop" =>
        testSupervise(ThrowIt1)
      case "supervise-resume" =>
        testSupervise(ThrowIt2)
      case "supervise-restart" =>
        testSupervise(ThrowIt3)
      case "stop-child" =>
        val child = context.spawnAnonymous(typed2)
        context.watch(child)
        context.stop(child)
    }

    private def testSupervise(t: ThrowIt): Unit = {
      val child = context.spawnAnonymous(typed2)
      context.watch(child)
      child ! t
      child ! Ping(self)
    }
  }

  def typed2: Behavior[Typed2Msg] =
    Behaviors.receive { (_, message) =>
      message match {
        case Ping(replyTo) =>
          replyTo ! "pong"
          Behaviors.same
        case StopIt =>
          Behaviors.stopped
        case t: ThrowIt =>
          throw t
      }
    }

  def typed2Props: classic.Props = PropsAdapter(typed2)

}

class AdapterSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with LogCapturing {
  import AdapterSpec._

  implicit val system: classic.ActorSystem = akka.actor.ActorSystem("AdapterSpec")
  def typedSystem: ActorSystem[Nothing] = system.toTyped

  "ActorSystem adaption" must {
    "only happen once for a given actor system" in {
      val typed1 = system.toTyped
      val typed2 = system.toTyped

      (typed1 should be).theSameInstanceAs(typed2)
    }

    "not crash if guardian is stopped" in {
      for { _ <- 0 to 10 } {
        var systemN: akka.actor.typed.ActorSystem[NotUsed] = null
        try {
          systemN = ActorSystem.create(
            Behaviors.setup[NotUsed](_ => Behaviors.stopped[NotUsed]),
            "AdapterSpec-stopping-guardian")
        } finally if (system != null) TestKit.shutdownActorSystem(systemN.toClassic)
      }
    }

    "not crash if guardian is stopped very quickly" in {
      for { _ <- 0 to 10 } {
        var systemN: akka.actor.typed.ActorSystem[Done] = null
        try {
          systemN = ActorSystem.create(Behaviors.receive[Done] { (context, message) =>
            context.self ! Done
            message match {
              case Done => Behaviors.stopped
            }

          }, "AdapterSpec-stopping-guardian-2")

        } finally if (system != null) TestKit.shutdownActorSystem(systemN.toClassic)
      }
    }

    "convert Scheduler" in {
      val typedScheduler = system.scheduler.toTyped
      typedScheduler.getClass should ===(classOf[SchedulerAdapter])
      (typedScheduler.toClassic should be).theSameInstanceAs(system.scheduler)
    }

    "allow seamless access to untyped extensions" in {
      SerializationExtension(typedSystem) should not be (null)
    }
  }

  "Adapted actors" must {

    "send message from typed to classic" in {
      val probe = TestProbe()
      val classicRef = system.actorOf(classic1)
      val typedRef = system.spawnAnonymous(typed1(classicRef, probe.ref))
      typedRef ! "send"
      probe.expectMsg("ok")
    }

    "not send null message from typed to classic" in {
      val probe = TestProbe()
      val classicRef = system.actorOf(classic1)
      val typedRef = system.spawnAnonymous(typed1(classicRef, probe.ref))
      intercept[InvalidMessageException] {
        typedRef ! null
      }
    }

    "send message from classic to typed" in {
      val probe = TestProbe()
      val typedRef = system.spawnAnonymous(typed2)
      val classicRef = system.actorOf(classic2(typedRef, probe.ref))
      classicRef ! "send"
      probe.expectMsg("ok")
    }

    "spawn typed child from classic parent" in {
      val probe = TestProbe()
      val ign = system.spawnAnonymous(Behaviors.ignore[Ping])
      val classicRef = system.actorOf(classic2(ign, probe.ref))
      classicRef ! "spawn"
      probe.expectMsg("ok")
    }

    "actorOf typed child via Props from classic parent" in {
      val probe = TestProbe()
      val ign = system.spawnAnonymous(Behaviors.ignore[Ping])
      val classicRef = system.actorOf(classic2(ign, probe.ref))
      classicRef ! "actorOf-props"
      probe.expectMsg("ok")
    }

    "actorOf classic child from typed parent" in {
      val probe = TestProbe()
      val ignore = system.actorOf(classic.Props.empty)
      val typedRef = system.spawnAnonymous(typed1(ignore, probe.ref))
      typedRef ! "actorOf"
      probe.expectMsg("ok")
    }

    "watch typed from classic" in {
      val probe = TestProbe()
      val typedRef = system.spawnAnonymous(typed2)
      val classicRef = system.actorOf(classic2(typedRef, probe.ref))
      classicRef ! "watch"
      typedRef ! StopIt
      probe.expectMsg("terminated")
    }

    "watch classic from typed" in {
      val probe = TestProbe()
      val classicRef = system.actorOf(classic1)
      val typedRef = system.spawnAnonymous(typed1(classicRef, probe.ref))
      typedRef ! "watch"
      classicRef ! classic.PoisonPill
      probe.expectMsg("terminated")
    }

    "supervise classic child from typed parent" in {
      // FIXME there's a warning with null logged from the classic empty child here, where does that come from?
      val probe = TestProbe()
      val ignore = system.actorOf(classic.Props.empty)
      val typedRef = system.spawnAnonymous(typed1(ignore, probe.ref))

      // only stop supervisorStrategy
      LoggingTestKit
        .error[AdapterSpec.ThrowIt3.type]
        .expect {
          typedRef ! "supervise-restart"
          probe.expectMsg("ok")
        }(system.toTyped)
    }

    "supervise classic child that throws in constructor from typed parent" in {
      val probe = TestProbe()
      val ignore = system.actorOf(classic.Props.empty)
      val typedRef = system.spawnAnonymous(typed1(ignore, probe.ref))

      LoggingTestKit
        .error[ActorInitializationException]
        .expect {
          typedRef ! "supervise-start-fail"
          probe.expectMsg("terminated")
        }(system.toTyped)
    }

    "stop typed child from classic parent" in {
      val probe = TestProbe()
      val ignore = system.spawnAnonymous(Behaviors.ignore[Ping])
      val classicRef = system.actorOf(classic2(ignore, probe.ref))
      classicRef ! "stop-child"
      probe.expectMsg("terminated")
    }

    "stop classic child from typed parent" in {
      val probe = TestProbe()
      val ignore = system.actorOf(classic.Props.empty)
      val typedRef = system.spawnAnonymous(typed1(ignore, probe.ref))
      typedRef ! "stop-child"
      probe.expectMsg("terminated")
    }

    "log exception if not by handled typed supervisor" in {
      val throwMsg = "sad panda"
      LoggingTestKit
        .error("sad panda")
        .expect {
          system.spawnAnonymous(unhappyTyped(throwMsg))
          Thread.sleep(1000)
        }(system.toTyped)
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
