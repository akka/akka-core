/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ActorInitializationException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.internal.PoisonPillInterceptor
import akka.actor.typed.scaladsl.Behaviors

object InterceptSpec {
  final case class Msg(hello: String, replyTo: ActorRef[String])
  case object MyPoisonPill

  class SameTypeInterceptor extends BehaviorInterceptor[String, String] {
    import BehaviorInterceptor._
    override def aroundReceive(
        context: TypedActorContext[String],
        message: String,
        target: ReceiveTarget[String]): Behavior[String] = {
      target(context, message)
    }

    override def isSame(other: BehaviorInterceptor[Any, Any]): Boolean =
      other.isInstanceOf[SameTypeInterceptor]
  }

  // This is similar to how EventSourcedBehavior is implemented
  object MultiProtocol {
    final case class ExternalResponse(s: String)
    final case class Command(s: String)

    sealed trait InternalProtocol
    object InternalProtocol {
      final case class WrappedCommand(c: Command) extends InternalProtocol
      final case class WrappedExternalResponse(r: ExternalResponse) extends InternalProtocol
    }

    private class ProtocolTransformer extends BehaviorInterceptor[Any, InternalProtocol] {
      override def aroundReceive(
          ctx: TypedActorContext[Any],
          msg: Any,
          target: BehaviorInterceptor.ReceiveTarget[InternalProtocol]): Behavior[InternalProtocol] = {
        val wrapped = msg match {
          case c: Command          => InternalProtocol.WrappedCommand(c)
          case r: ExternalResponse => InternalProtocol.WrappedExternalResponse(r)
          case unexpected          => throw new RuntimeException(s"Unexpected: $unexpected")
        }
        target(ctx, wrapped)
      }

    }

    def apply(probe: ActorRef[String]): Behavior[Command] = {
      Behaviors
        .intercept(() => new ProtocolTransformer)(Behaviors.receiveMessagePartial[InternalProtocol] {
          case InternalProtocol.WrappedCommand(cmd) =>
            probe ! cmd.s
            Behaviors.same
          case InternalProtocol.WrappedExternalResponse(rsp) =>
            probe ! rsp.s
            Behaviors.same
        })
        .narrow
    }
  }
}

class InterceptSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  import BehaviorInterceptor._
  import InterceptSpec._

  private def snitchingInterceptor(probe: ActorRef[String]) = new BehaviorInterceptor[String, String] {
    override def aroundReceive(
        context: TypedActorContext[String],
        message: String,
        target: ReceiveTarget[String]): Behavior[String] = {
      probe ! ("before " + message)
      val b = target(context, message)
      probe ! ("after " + message)
      b
    }
    // keeping the instance equality as "isSame" for these
  }

  "Intercept" must {

    "intercept messages" in {
      val probe = TestProbe[String]()
      val interceptor = snitchingInterceptor(probe.ref)

      val ref: ActorRef[String] = spawn(Behaviors.intercept(() => interceptor)(Behaviors.receiveMessage[String] { m =>
        probe.ref ! s"actual behavior $m"
        Behaviors.same
      }))

      ref ! "message"

      probe.expectMessage("before message")
      probe.expectMessage("actual behavior message")
      probe.expectMessage("after message")

    }

    "intercept messages only using the outermost of the same interceptor (initially)" in {
      val probe = TestProbe[String]()

      val interceptor = snitchingInterceptor(probe.ref)
      def intercept(beh: Behavior[String]): Behavior[String] =
        Behaviors.intercept(() => interceptor)(beh)

      val beh: Behavior[String] =
        intercept(intercept(Behaviors.receiveMessage(_ => Behaviors.same)))

      val ref = spawn(beh)

      ref ! "message 1"
      // here'd we get duplicates if both intercepts stayed in effect
      probe.expectMessage("before message 1")
      probe.expectMessage("after message 1")
    }

    "intercept messages only using the latest of the same interceptor (recursively)" in {
      val probe = TestProbe[String]()

      val interceptor = snitchingInterceptor(probe.ref)
      def next(count: Int): Behavior[String] =
        Behaviors.intercept(() => interceptor)(Behaviors.receiveMessage(_ => next(count + 1)))

      val ref = spawn(next(1))

      ref ! "message 1"
      probe.expectMessage("before message 1")
      probe.expectMessage("after message 1")

      // here we'd get duplicates if it wasn't deduplicated
      // but instead of waiting for no-message we can confirm no duplicates by sending next
      ref ! "message 2"
      probe.expectMessage("before message 2")
      probe.expectMessage("after message 2")

      ref ! "message 3"
      probe.expectMessage("before message 3")
      probe.expectMessage("after message 3")
    }

    "intercept messages keeping all different interceptors (initially)" in {
      val probe = TestProbe[String]()

      def intercept(beh: Behavior[String]): Behavior[String] =
        // a new interceptor instance every call
        Behaviors.intercept(() => snitchingInterceptor(probe.ref))(beh)

      val beh: Behavior[String] =
        intercept(intercept(Behaviors.receiveMessage(_ => Behaviors.same)))

      val ref = spawn(beh)

      ref ! "message 1"
      probe.expectMessage("before message 1")
      probe.expectMessage("before message 1")
      probe.expectMessage("after message 1")
      probe.expectMessage("after message 1")

    }

    "intercept messages keeping all different interceptors (recursively)" in {
      val probe = TestProbe[String]()

      def next(count: Int): Behavior[String] =
        Behaviors.intercept(
          // a new instance every "recursion"
          () => snitchingInterceptor(probe.ref))(Behaviors.receiveMessage(_ => next(count + 1)))

      val ref = spawn(next(1))

      ref ! "message 1"
      probe.expectMessage("before message 1")
      probe.expectMessage("after message 1")

      ref ! "message 2"
      probe.expectMessage("before message 2")
      probe.expectMessage("before message 2")
      probe.expectMessage("after message 2")
      probe.expectMessage("after message 2")
    }

    "allow an interceptor to replace started behavior" in {
      val interceptor = new BehaviorInterceptor[String, String] {

        override def aroundStart(
            context: TypedActorContext[String],
            target: PreStartTarget[String]): Behavior[String] = {
          Behaviors.stopped
        }

        def aroundReceive(
            context: TypedActorContext[String],
            message: String,
            target: ReceiveTarget[String]): Behavior[String] =
          target(context, message)
      }

      val innerBehaviorStarted = new AtomicBoolean(false)
      val ref = spawn(Behaviors.intercept(() => interceptor)(Behaviors.setup { _ =>
        innerBehaviorStarted.set(true)
        Behaviors.unhandled[String]
      }))

      val probe = TestProbe()
      probe.expectTerminated(ref, 3.seconds)
      innerBehaviorStarted.get should ===(false)
    }

    "intercept with nested setup" in {
      val probe = TestProbe[String]()
      val interceptor = snitchingInterceptor(probe.ref)

      val ref: ActorRef[String] = spawn(Behaviors.intercept(() => interceptor)(Behaviors.setup { _ =>
        var count = 0
        Behaviors.receiveMessage[String] { m =>
          count += 1
          probe.ref ! s"actual behavior $m-$count"
          Behaviors.same
        }
      }))

      ref ! "a"
      probe.expectMessage("before a")
      probe.expectMessage("actual behavior a-1")
      probe.expectMessage("after a")

      ref ! "b"
      probe.expectMessage("before b")
      probe.expectMessage("actual behavior b-2")
      probe.expectMessage("after b")
    }

    "intercept with recursively setup" in {
      val probe = TestProbe[String]()
      val interceptor = snitchingInterceptor(probe.ref)

      def next(count1: Int): Behavior[String] = {
        Behaviors.intercept(() => interceptor)(Behaviors.setup { _ =>
          var count2 = 0
          Behaviors.receiveMessage[String] { m =>
            count2 += 1
            probe.ref ! s"actual behavior $m-$count1-$count2"
            next(count1 + 1)
          }
        })
      }

      val ref: ActorRef[String] = spawn(next(1))

      ref ! "a"
      probe.expectMessage("before a")
      probe.expectMessage("actual behavior a-1-1")
      probe.expectMessage("after a")

      ref ! "b"
      probe.expectMessage("before b")
      probe.expectMessage("actual behavior b-2-1")
      probe.expectMessage("after b")

      ref ! "c"
      probe.expectMessage("before c")
      probe.expectMessage("actual behavior c-3-1")
      probe.expectMessage("after c")
    }

    "not allow intercept setup(same)" in {
      val probe = TestProbe[String]()
      val interceptor = snitchingInterceptor(probe.ref)

      LoggingTestKit.error[ActorInitializationException].expect {
        val ref = spawn(Behaviors.intercept(() => interceptor)(Behaviors.setup[String] { _ =>
          Behaviors.same[String]
        }))
        probe.expectTerminated(ref, probe.remainingOrDefault)
      }

    }

    "be useful for implementing signal based PoisonPill" in {

      def inner(count: Int): Behavior[Msg] = Behaviors.receiveMessage {
        case Msg(hello, replyTo) =>
          replyTo ! s"$hello-$count"
          inner(count + 1)
      }

      val decorated: Behavior[Msg] =
        Behaviors.intercept(() => new PoisonPillInterceptor[Msg])(inner(0))

      val ref = spawn(decorated)
      val probe = TestProbe[String]()
      ref ! Msg("hello", probe.ref)
      probe.expectMessage("hello-0")
      ref ! Msg("hello", probe.ref)
      probe.expectMessage("hello-1")

      ref.unsafeUpcast[Any] ! PoisonPill

      probe.expectTerminated(ref, probe.remainingOrDefault)
    }

    "be useful for implementing custom message based PoisonPill" in {

      def inner(count: Int): Behavior[Msg] = Behaviors.receiveMessage {
        case Msg(hello, replyTo) =>
          replyTo ! s"$hello-$count"
          inner(count + 1)
      }

      val poisonInterceptor = new BehaviorInterceptor[Any, Msg] {
        override def aroundReceive(
            context: TypedActorContext[Any],
            message: Any,
            target: ReceiveTarget[Msg]): Behavior[Msg] =
          message match {
            case MyPoisonPill => Behaviors.stopped
            case m: Msg       => target(context, m)
            case _            => Behaviors.unhandled
          }

      }

      val decorated: Behavior[Msg] =
        Behaviors.intercept(() => poisonInterceptor)(inner(0)).narrow

      val ref = spawn(decorated)
      val probe = TestProbe[String]()
      ref ! Msg("hello", probe.ref)
      probe.expectMessage("hello-0")
      ref ! Msg("hello", probe.ref)
      probe.expectMessage("hello-1")

      ref.unsafeUpcast[Any] ! MyPoisonPill

      probe.expectTerminated(ref, probe.remainingOrDefault)
    }

    "be able to intercept message subclasses" in {
      trait Message
      class A extends Message
      class B extends Message

      val interceptProbe = TestProbe[Message]()

      val interceptor: BehaviorInterceptor[Message, Message] =
        new BehaviorInterceptor[Message, Message] {

          override def aroundReceive(
              ctx: TypedActorContext[Message],
              msg: Message,
              target: ReceiveTarget[Message]): Behavior[Message] = {
            interceptProbe.ref ! msg
            target(ctx, msg)
          }

        }

      val probe = TestProbe[Message]()
      val ref = spawn(Behaviors.intercept(() => interceptor)(Behaviors.receiveMessage { msg =>
        probe.ref ! msg
        Behaviors.same
      }))

      ref ! new A
      ref ! new B

      interceptProbe.expectMessageType[A]
      probe.expectMessageType[A]
      interceptProbe.expectMessageType[B]
      probe.expectMessageType[B]
    }

    "intercept PostStop" in {
      val probe = TestProbe[String]()
      val postStopInterceptor = new BehaviorSignalInterceptor[String] {
        override def aroundSignal(
            ctx: TypedActorContext[String],
            signal: Signal,
            target: SignalTarget[String]): Behavior[String] = {
          signal match {
            case PostStop =>
              probe.ref ! "interceptor-post-stop"
            case _ =>
          }
          target(ctx, signal)
        }
      }

      val ref = spawn(Behaviors.intercept(() => postStopInterceptor)(Behaviors.receiveMessage[String] { _ =>
        Behaviors.stopped { () =>
          probe.ref ! "callback-post-stop"
        }
      }))

      ref ! "stop"
      probe.awaitAssert {
        probe.expectMessage("interceptor-post-stop") // previous behavior when stopping get the signal
        probe.expectMessage("callback-post-stop")
      }
    }

    "not grow stack when nesting same interceptor" in {
      def next(n: Int, p: ActorRef[Array[StackTraceElement]]): Behavior[String] = {
        Behaviors.intercept(() => new SameTypeInterceptor) {

          Behaviors.receiveMessage { _ =>
            if (n == 20) {
              val e = new RuntimeException().fillInStackTrace()
              val trace = e.getStackTrace
              p ! trace
              Behaviors.stopped
            } else {
              next(n + 1, p)
            }
          }
        }
      }

      val probe = TestProbe[Array[StackTraceElement]]()
      val ref = spawn(next(0, probe.ref))
      (1 to 21).foreach { n =>
        ref ! n.toString
      }
      val elements = probe.receiveMessage()
      if (elements.count(_.getClassName == "SameTypeInterceptor") > 1)
        fail(s"Stack contains SameTypeInterceptor more than once: \n${elements.mkString("\n\t")}")
    }
  }

  "Protocol transformer interceptor" must {
    import MultiProtocol._

    "be possible to combine with another interceptor" in {
      val probe = createTestProbe[String]()

      val toUpper = new BehaviorInterceptor[Command, Command] {
        override def aroundReceive(
            ctx: TypedActorContext[Command],
            msg: Command,
            target: BehaviorInterceptor.ReceiveTarget[Command]): Behavior[Command] = {
          target(ctx, Command(msg.s.toUpperCase()))
        }

      }

      val ref = spawn(Behaviors.intercept(() => toUpper)(MultiProtocol(probe.ref)))

      ref ! Command("a")
      probe.expectMessage("A")
      ref.unsafeUpcast[Any] ! ExternalResponse("b")
      probe.expectMessage("b") // bypass toUpper interceptor
    }

    "be possible to combine with transformMessages" in {
      val probe = createTestProbe[String]()
      val ref = spawn(MultiProtocol(probe.ref).transformMessages[String] {
        case s => Command(s.toUpperCase())
      })

      ref ! "a"
      probe.expectMessage("A")
      ref.unsafeUpcast[Any] ! ExternalResponse("b")
      probe.expectMessage("b") // bypass transformMessages interceptor
    }

    "be possible to combine with MDC" in {
      val probe = createTestProbe[String]()
      val ref = spawn(Behaviors.setup[Command] { _ =>
        Behaviors.withMdc(staticMdc = Map("x" -> "y"), mdcForMessage = (msg: Command) => {
          probe.ref ! s"mdc:${msg.s.toUpperCase()}"
          Map("msg" -> msg.s.toUpperCase())
        }) {
          MultiProtocol(probe.ref)
        }
      })

      ref ! Command("a")
      probe.expectMessage("mdc:A")
      probe.expectMessage("a")
      ref.unsafeUpcast[Any] ! ExternalResponse("b")
      probe.expectMessage("b") // bypass mdc interceptor

    }

    "be possible to combine with PoisonPillInterceptor" in {
      val probe = createTestProbe[String]()
      val ref =
        spawn(Behaviors.intercept(() => new PoisonPillInterceptor[MultiProtocol.Command])(MultiProtocol(probe.ref)))

      ref ! Command("a")
      probe.expectMessage("a")
      ref.unsafeUpcast[Any] ! PoisonPill
      probe.expectTerminated(ref, probe.remainingOrDefault)
    }
  }

}
