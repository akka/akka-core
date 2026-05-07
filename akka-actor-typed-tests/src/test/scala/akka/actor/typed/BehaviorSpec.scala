/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import java.util.function.{ Function => F1 }

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.scaladsl.{ BehaviorTestKit, TestInbox }
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.javadsl.{ Behaviors => JBehaviors }
import akka.actor.typed.scaladsl.{ AbstractBehavior => SAbstractBehavior }
import akka.actor.typed.scaladsl.{ Behaviors => SBehaviors }
import akka.japi.pf.{ FI, PFBuilder }

object BehaviorSpec {
  sealed trait Command {
    def expectedResponse(context: TypedActorContext[Command]): Seq[Event] = Nil
  }
  case object GetSelf extends Command {
    override def expectedResponse(context: TypedActorContext[Command]): Seq[Event] = Self(context.asScala.self) :: Nil
  }
  // Behavior under test must return Unhandled
  case object Miss extends Command {
    override def expectedResponse(context: TypedActorContext[Command]): Seq[Event] = Missed :: Nil
  }
  // Behavior under test must return same
  case object Ignore extends Command {
    override def expectedResponse(context: TypedActorContext[Command]): Seq[Event] = Ignored :: Nil
  }
  case object Ping extends Command {
    override def expectedResponse(context: TypedActorContext[Command]): Seq[Event] = Pong :: Nil
  }
  case object Swap extends Command {
    override def expectedResponse(context: TypedActorContext[Command]): Seq[Event] = Swapped :: Nil
  }
  case class GetState()(s: State) extends Command {
    override def expectedResponse(context: TypedActorContext[Command]): Seq[Event] = s :: Nil
  }
  case class AuxPing(id: Int) extends Command {
    override def expectedResponse(context: TypedActorContext[Command]): Seq[Event] = Pong :: Nil
  }
  case object Stop extends Command

  sealed trait Event
  case class ReceivedSignal(signal: Signal) extends Event
  case class Self(self: ActorRef[Command]) extends Event
  case object Missed extends Event
  case object Ignored extends Event
  case object Pong extends Event
  case object Swapped extends Event

  sealed trait State extends Event {
    def next: State
  }
  val StateA: State = new State {
    override def toString = "StateA"
    override def next = StateB
  }
  val StateB: State = new State {
    override def toString = "StateB"
    override def next = StateA
  }

  trait Common extends AnyWordSpecLike with Matchers with TypeCheckedTripleEquals with LogCapturing {
    type Aux >: Null <: AnyRef
    def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux)
    def checkAux(signal: Signal, aux: Aux): Unit = ()
    def checkAux(command: Command, aux: Aux): Unit = ()

    case class Init(behv: Behavior[Command], inbox: TestInbox[Event], aux: Aux) {
      def mkCtx(): Setup = {
        val testkit = BehaviorTestKit(behv)
        inbox.receiveAll()
        Setup(testkit, inbox, aux)
      }
    }
    case class Setup(testKit: BehaviorTestKit[Command], inbox: TestInbox[Event], aux: Aux)

    def init(): Init = {
      val inbox = TestInbox[Event]("evt")
      val (behv, aux) = behavior(inbox.ref)
      Init(behv, inbox, aux)
    }

    def init(factory: ActorRef[Event] => (Behavior[Command], Aux)): Init = {
      val inbox = TestInbox[Event]("evt")
      val (behv, aux) = factory(inbox.ref)
      Init(behv, inbox, aux)
    }

    def mkCtx(): Setup =
      init().mkCtx()

    implicit class Check(val setup: Setup) {
      def check(signal: Signal): Setup = {
        setup.testKit.signal(signal)
        setup.inbox.receiveAll() should ===(ReceivedSignal(signal) :: Nil)
        checkAux(signal, setup.aux)
        setup
      }
      def check(command: Command): Setup = {
        setup.testKit.run(command)
        setup.inbox.receiveAll() should ===(command.expectedResponse(setup.testKit.context))
        checkAux(command, setup.aux)
        setup
      }
      def check2(command: Command): Setup = {
        setup.testKit.run(command)
        val expected = command.expectedResponse(setup.testKit.context)
        setup.inbox.receiveAll() should ===(expected ++ expected)
        checkAux(command, setup.aux)
        setup
      }
    }

    val ex = new Exception("mine!")
  }

  trait Siphon extends Common {
    override type Aux = TestInbox[Command]

    override def checkAux(command: Command, aux: Aux): Unit = {
      aux.receiveAll() should ===(command :: Nil)
    }
  }

  trait SignalSiphon extends Common {
    override type Aux = TestInbox[Either[Signal, Command]]

    override def checkAux(command: Command, aux: Aux): Unit = {
      aux.receiveAll() should ===(Right(command) :: Nil)
    }

    override def checkAux(signal: Signal, aux: Aux): Unit = {
      aux.receiveAll() should ===(Left(signal) :: Nil)
    }
  }

  def mkFull(monitor: ActorRef[Event], state: State = StateA): Behavior[Command] = {
    SBehaviors
      .receive[Command] {
        case (context, GetSelf) =>
          monitor ! Self(context.self)
          SBehaviors.same
        case (_, Miss) =>
          monitor ! Missed
          SBehaviors.unhandled
        case (_, Ignore) =>
          monitor ! Ignored
          SBehaviors.same
        case (_, Ping) =>
          monitor ! Pong
          mkFull(monitor, state)
        case (_, Swap) =>
          monitor ! Swapped
          mkFull(monitor, state.next)
        case (_, GetState()) =>
          monitor ! state
          SBehaviors.same
        case (_, Stop) => SBehaviors.stopped
        case (_, _)    => SBehaviors.unhandled
      }
      .receiveSignal {
        case (_, signal) =>
          monitor ! ReceivedSignal(signal)
          SBehaviors.same
      }
  }
  /*
   * function converters for Java, to ease the pain on Scala 2.11
   */
  def pf(f: PFBuilder[Command, Command] => PFBuilder[Command, Command]) =
    new F1[PFBuilder[Command, Command], PFBuilder[Command, Command]] {
      override def apply(in: PFBuilder[Command, Command]) = f(in)
    }
  def fi(f: Command => Command) =
    new FI.Apply[Command, Command] {
      override def apply(in: Command) = f(in)
    }

  trait Lifecycle extends Common {
    "Lifecycle" must {
      "must react to PreStart" in {
        pending
      }

      "must react to PostStop" in {
        mkCtx().check(PostStop)
      }

      "must react to PostStop after a message" in {
        mkCtx().check(GetSelf).check(PostStop)
      }

      "must react to PreRestart" in {
        mkCtx().check(PreRestart)
      }

      "must react to PreRestart after a message" in {
        mkCtx().check(GetSelf).check(PreRestart)
      }

      "must react to Terminated" in {
        mkCtx().check(Terminated(TestInbox("x").ref))
      }

      "must react to Terminated after a message" in {
        mkCtx().check(GetSelf).check(Terminated(TestInbox("x").ref))
      }

      "must react to a message after Terminated" in {
        mkCtx().check(Terminated(TestInbox("x").ref)).check(GetSelf)
      }
    }
  }

  trait Messages extends Common {
    "Messages" must {
      "react to two messages" in {
        mkCtx().check(Ping).check(Ping)
      }

      "react to a message after missing one" in {
        mkCtx().check(Miss).check(Ping)
      }

      "must react to a message after ignoring one" in {
        mkCtx().check(Ignore).check(Ping)
      }
    }
  }

  trait Unhandled extends Common {
    "Unhandled" must {
      "must return Unhandled" in {
        val Setup(testKit, inbox, aux) = mkCtx()
        val next = Behavior.interpretMessage(testKit.currentBehavior, testKit.context, Miss)
        Behavior.isUnhandled(next) should ===(true)
        inbox.receiveAll() should ===(Missed :: Nil)
        checkAux(Miss, aux)
      }
    }
  }

  trait Stoppable extends Common {
    "Stopping" must {
      "must stop" in {
        val Setup(testkit, _, aux) = mkCtx()
        testkit.run(Stop)
        Behavior.isAlive(testkit.currentBehavior) should be(false)
        checkAux(Stop, aux)
      }
    }
  }

  trait Become extends Common with Unhandled {
    "Becoming" must {
      "must be in state A" in {
        mkCtx().check(GetState()(StateA))
      }

      "must switch to state B" in {
        mkCtx().check(Swap).check(GetState()(StateB))
      }

      "must switch back to state A" in {
        mkCtx().check(Swap).check(Swap).check(GetState()(StateA))
      }
    }
  }

  trait BecomeWithLifecycle extends Become with Lifecycle {
    "Become with lifecycle" must {
      "react to PostStop after swap" in {
        mkCtx().check(Swap).check(PostStop)
      }

      "react to PostStop after a message after swap" in {
        mkCtx().check(Swap).check(GetSelf).check(PostStop)
      }

      "react to PreRestart after swap" in {
        mkCtx().check(Swap).check(PreRestart)
      }

      "react to PreRestart after a message after swap" in {
        mkCtx().check(Swap).check(GetSelf).check(PreRestart)
      }

      "react to Terminated after swap" in {
        mkCtx().check(Swap).check(Terminated(TestInbox("x").ref))
      }

      "react to Terminated after a message after swap" in {
        mkCtx().check(Swap).check(GetSelf).check(Terminated(TestInbox("x").ref))
      }

      "react to a message after Terminated after swap" in {
        mkCtx().check(Swap).check(Terminated(TestInbox("x").ref)).check(GetSelf)
      }
    }
  }

  /**
   * This targets behavior wrappers to ensure that the wrapper does not
   * hold on to the changed behavior. Wrappers must be immutable.
   */
  trait Reuse extends Common {
    "Reuse" must {
      "must be reusable" in {
        val i = init()
        i.mkCtx().check(GetState()(StateA)).check(Swap).check(GetState()(StateB))
        i.mkCtx().check(GetState()(StateA)).check(Swap).check(GetState()(StateB))
      }
    }
  }

}

import BehaviorSpec._

class FullBehaviorSpec extends ScalaTestWithActorTestKit with Messages with BecomeWithLifecycle with Stoppable {
  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = mkFull(monitor) -> null
}

class ReceiveBehaviorSpec extends Messages with BecomeWithLifecycle with Stoppable {
  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = behv(monitor, StateA) -> null
  private def behv(monitor: ActorRef[Event], state: State): Behavior[Command] = {
    SBehaviors
      .receive[Command] {
        case (context, GetSelf) =>
          monitor ! Self(context.self)
          SBehaviors.same
        case (_, Miss) =>
          monitor ! Missed
          SBehaviors.unhandled
        case (_, Ignore) =>
          monitor ! Ignored
          SBehaviors.same
        case (_, Ping) =>
          monitor ! Pong
          behv(monitor, state)
        case (_, Swap) =>
          monitor ! Swapped
          behv(monitor, state.next)
        case (_, GetState()) =>
          monitor ! state
          SBehaviors.same
        case (_, Stop)       => SBehaviors.stopped
        case (_, _: AuxPing) => SBehaviors.unhandled
      }
      .receiveSignal {
        case (_, signal) =>
          monitor ! ReceivedSignal(signal)
          SBehaviors.same
      }
  }
}

class ImmutableWithSignalScalaBehaviorSpec extends Messages with BecomeWithLifecycle with Stoppable {

  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = behv(monitor) -> null

  def behv(monitor: ActorRef[Event], state: State = StateA): Behavior[Command] =
    SBehaviors
      .receive[Command] { (context, message) =>
        message match {
          case GetSelf =>
            monitor ! Self(context.self)
            SBehaviors.same
          case Miss =>
            monitor ! Missed
            SBehaviors.unhandled
          case Ignore =>
            monitor ! Ignored
            SBehaviors.same
          case Ping =>
            monitor ! Pong
            behv(monitor, state)
          case Swap =>
            monitor ! Swapped
            behv(monitor, state.next)
          case GetState() =>
            monitor ! state
            SBehaviors.same
          case Stop       => SBehaviors.stopped
          case _: AuxPing => SBehaviors.unhandled
        }
      }
      .receiveSignal {
        case (_, sig) =>
          monitor ! ReceivedSignal(sig)
          SBehaviors.same
      }
}

class ImmutableScalaBehaviorSpec extends Messages with Become with Stoppable {

  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = behv(monitor, StateA) -> null

  def behv(monitor: ActorRef[Event], state: State): Behavior[Command] =
    SBehaviors.receive[Command] { (context, message) =>
      message match {
        case GetSelf =>
          monitor ! Self(context.self)
          SBehaviors.same
        case Miss =>
          monitor ! Missed
          SBehaviors.unhandled
        case Ignore =>
          monitor ! Ignored
          SBehaviors.same
        case Ping =>
          monitor ! Pong
          behv(monitor, state)
        case Swap =>
          monitor ! Swapped
          behv(monitor, state.next)
        case GetState() =>
          monitor ! state
          SBehaviors.same
        case Stop       => SBehaviors.stopped
        case _: AuxPing => SBehaviors.unhandled
      }
    }
}

class MutableScalaBehaviorSpec extends Messages with Become with Stoppable {

  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = behv(monitor) -> null

  def behv(monitor: ActorRef[Event]): Behavior[Command] =
    SBehaviors.setup[Command] { context =>
      new SAbstractBehavior[Command](context) {
        private var state: State = StateA

        override def onMessage(message: Command): Behavior[Command] = {
          message match {
            case GetSelf =>
              monitor ! Self(this.context.self)
              this
            case Miss =>
              monitor ! Missed
              SBehaviors.unhandled
            case Ignore =>
              monitor ! Ignored
              SBehaviors.same // this or same works the same way
            case Ping =>
              monitor ! Pong
              this
            case Swap =>
              monitor ! Swapped
              state = state.next
              this
            case GetState() =>
              monitor ! state
              this
            case Stop       => SBehaviors.stopped
            case _: AuxPing => SBehaviors.unhandled
          }
        }
      }
    }
}

class TransformMessagesScalaBehaviorSpec extends ImmutableWithSignalScalaBehaviorSpec with Reuse with Siphon {

  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = {
    val inbox = TestInbox[Command]("transformMessagesListener")
    super.behavior(monitor)._1.transformMessages[Command] { case c => inbox.ref ! c; c } -> inbox
  }
}

class DeferredScalaBehaviorSpec extends ImmutableWithSignalScalaBehaviorSpec {
  override type Aux = TestInbox[Done]

  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = {
    val inbox = TestInbox[Done]("deferredListener")
    (SBehaviors.setup(_ => {
      inbox.ref ! Done
      super.behavior(monitor)._1
    }), inbox)
  }

  override def checkAux(signal: Signal, aux: Aux): Unit =
    aux.receiveAll() should ===(Done :: Nil)
}

class InterceptScalaBehaviorSpec extends ImmutableWithSignalScalaBehaviorSpec with Reuse with SignalSiphon {
  import BehaviorInterceptor._

  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = {
    val inbox = TestInbox[Either[Signal, Command]]("tapListener")
    val tap = new BehaviorInterceptor[Command, Command] {
      override def aroundReceive(
          context: TypedActorContext[Command],
          message: Command,
          target: ReceiveTarget[Command]): Behavior[Command] = {
        inbox.ref ! Right(message)
        target(context, message)
      }

      override def aroundSignal(
          context: TypedActorContext[Command],
          signal: Signal,
          target: SignalTarget[Command]): Behavior[Command] = {
        inbox.ref ! Left(signal)
        target(context, signal)
      }
    }
    (SBehaviors.intercept(() => tap)(super.behavior(monitor)._1), inbox)
  }
}

class RestarterScalaBehaviorSpec extends ImmutableWithSignalScalaBehaviorSpec with Reuse {
  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = {
    SBehaviors.supervise(super.behavior(monitor)._1).onFailure(SupervisorStrategy.restart) -> null
  }
}

class ImmutableWithSignalJavaBehaviorSpec extends Messages with BecomeWithLifecycle with Stoppable {
  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = behv(monitor) -> null
  def behv(monitor: ActorRef[Event], state: State = StateA): Behavior[Command] =
    JBehaviors.receive(
      (context, message) =>
        message match {
          case GetSelf =>
            monitor ! Self(context.getSelf)
            SBehaviors.same
          case Miss =>
            monitor ! Missed
            SBehaviors.unhandled
          case Ignore =>
            monitor ! Ignored
            SBehaviors.same
          case Ping =>
            monitor ! Pong
            behv(monitor, state)
          case Swap =>
            monitor ! Swapped
            behv(monitor, state.next)
          case GetState() =>
            monitor ! state
            SBehaviors.same
          case Stop       => SBehaviors.stopped
          case _: AuxPing => SBehaviors.unhandled
        },
      (_, sig) => {
        monitor ! ReceivedSignal(sig)
        SBehaviors.same
      })
}

class ImmutableJavaBehaviorSpec extends Messages with Become with Stoppable {
  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = behv(monitor, StateA) -> null
  def behv(monitor: ActorRef[Event], state: State): Behavior[Command] =
    JBehaviors.receive { (context, message) =>
      message match {
        case GetSelf =>
          monitor ! Self(context.getSelf)
          SBehaviors.same
        case Miss =>
          monitor ! Missed
          SBehaviors.unhandled
        case Ignore =>
          monitor ! Ignored
          SBehaviors.same
        case Ping =>
          monitor ! Pong
          behv(monitor, state)
        case Swap =>
          monitor ! Swapped
          behv(monitor, state.next)
        case GetState() =>
          monitor ! state
          SBehaviors.same
        case Stop       => SBehaviors.stopped
        case _: AuxPing => SBehaviors.unhandled
      }
    }
}

class TransformMessagesJavaBehaviorSpec extends ImmutableWithSignalJavaBehaviorSpec with Reuse with Siphon {
  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = {
    val inbox = TestInbox[Command]("transformMessagesListener")
    JBehaviors.transformMessages(classOf[Command], super.behavior(monitor)._1, pf(_.`match`(classOf[Command], fi(x => {
      inbox.ref ! x
      x
    })))) -> inbox
  }
}

class DeferredJavaBehaviorSpec extends ImmutableWithSignalJavaBehaviorSpec {
  override type Aux = TestInbox[Done]

  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = {
    val inbox = TestInbox[Done]("deferredListener")
    (JBehaviors.setup(_ => {
      inbox.ref ! Done
      super.behavior(monitor)._1
    }), inbox)
  }

  override def checkAux(signal: Signal, aux: Aux): Unit =
    aux.receiveAll() should ===(Done :: Nil)
}

class TapJavaBehaviorSpec extends ImmutableWithSignalJavaBehaviorSpec with Reuse with SignalSiphon {
  import BehaviorInterceptor._

  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = {
    val inbox = TestInbox[Either[Signal, Command]]("tapListener")
    val tap = new BehaviorInterceptor[Command, Command] {
      override def aroundReceive(
          context: TypedActorContext[Command],
          message: Command,
          target: ReceiveTarget[Command]): Behavior[Command] = {
        inbox.ref ! Right(message)
        target(context, message)
      }

      override def aroundSignal(
          context: TypedActorContext[Command],
          signal: Signal,
          target: SignalTarget[Command]): Behavior[Command] = {
        inbox.ref ! Left(signal)
        target(context, signal)
      }
    }
    (JBehaviors.intercept(() => tap, super.behavior(monitor)._1), inbox)
  }
}

class RestarterJavaBehaviorSpec extends ImmutableWithSignalJavaBehaviorSpec with Reuse {
  override def behavior(monitor: ActorRef[Event]): (Behavior[Command], Aux) = {
    JBehaviors.supervise(super.behavior(monitor)._1).onFailure(classOf[Exception], SupervisorStrategy.restart) -> null
  }
}
