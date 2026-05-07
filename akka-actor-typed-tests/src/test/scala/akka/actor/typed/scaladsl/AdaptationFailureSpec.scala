/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.scaladsl

import scala.concurrent.Promise

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.Behavior
import akka.actor.typed.MessageAdaptionFailure
import akka.actor.typed.PreRestart
import akka.actor.typed.Signal
import akka.actor.typed.Terminated

object AdaptationFailureSpec {

  def emptyAbstractBehavior: Behavior[Any] = Behaviors.setup(new EmptyAbstractBehavior(_))
  class EmptyAbstractBehavior(ctx: ActorContext[Any]) extends AbstractBehavior[Any](ctx) {
    def onMessage(msg: Any): Behavior[Any] = this
  }

  def abstractBehaviorHandlingOtherSignals: Behavior[Any] = Behaviors.setup(new AbstractBehaviorHandlingOtherSignals(_))
  class AbstractBehaviorHandlingOtherSignals(ctx: ActorContext[Any]) extends AbstractBehavior[Any](ctx) {

    def onMessage(msg: Any): Behavior[Any] = this

    override def onSignal: PartialFunction[Signal, Behavior[Any]] = {
      case PreRestart => Behaviors.same
    }
  }

  def abstractBehaviorHandlingMessageAdaptionFailure: Behavior[Any] =
    Behaviors.setup(new AbstractBehaviorHandlingMessageAdaptionFailure(_))
  class AbstractBehaviorHandlingMessageAdaptionFailure(ctx: ActorContext[Any]) extends AbstractBehavior[Any](ctx) {

    def onMessage(msg: Any): Behavior[Any] = this

    override def onSignal: PartialFunction[Signal, Behavior[Any]] = {
      case MessageAdaptionFailure(_) => Behaviors.same
    }
  }

}
class AdaptationFailureSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  import AdaptationFailureSpec._

  val crashingBehaviors: List[(String, Behavior[Any])] =
    "receive" -> Behaviors.receive[Any]((_, _) => Behaviors.same) ::
    "receiveMessage" -> Behaviors.receiveMessage[Any](_ => Behaviors.same) ::
    "receivePartial" -> Behaviors.receivePartial[Any](PartialFunction.empty) ::
    "receiveSignal" -> Behaviors.receiveSignal[Any](PartialFunction.empty) ::
    "receiveSignal not catching adaption failure" ->
    Behaviors.receiveSignal[Any] {
      case (_, PreRestart) => Behaviors.same
    } ::
    "AbstractBehavior" -> emptyAbstractBehavior ::
    "AbstractBehavior handling other signals" -> abstractBehaviorHandlingOtherSignals ::
    Nil

  val nonCrashingBehaviors: List[(String, Behavior[Any])] =
    "empty" -> Behaviors.empty[Any] ::
    "ignore" -> Behaviors.ignore[Any] ::
    "receiveSignal catching adaption failure" ->
    Behaviors.receiveSignal[Any] {
      case (_, MessageAdaptionFailure(_)) => Behaviors.same
    } ::
    "AbstractBehavior handling MessageAdaptionFailure" -> abstractBehaviorHandlingMessageAdaptionFailure ::
    Nil

  "Failure in an adapter" must {

    crashingBehaviors.foreach {
      case (name, behavior) =>
        s"default to crash the actor or $name" in {
          val probe = createTestProbe()
          val ref = spawn(Behaviors.setup[Any] { ctx =>
            val adapter = ctx.messageAdapter[Any](_ => throw TestException("boom"))
            adapter ! "go boom"

            behavior
          })
          probe.expectTerminated(ref)
        }
    }

    nonCrashingBehaviors.foreach {
      case (name, behavior) =>
        s"ignore the failure for $name" in {
          val probe = createTestProbe[Any]()
          val threw = Promise[Done]()
          val ref = spawn(Behaviors.setup[Any] { ctx =>
            val adapter = ctx.messageAdapter[Any] { _ =>
              threw.success(Done)
              throw TestException("boom")
            }
            adapter ! "go boom"
            behavior
          })
          spawn(Behaviors.setup[Any] { ctx =>
            ctx.watch(ref)

            Behaviors.receiveSignal {
              case (_, Terminated(`ref`)) =>
                probe.ref ! "actor-stopped"
                Behaviors.same
            }
          })

          probe.expectNoMessage()
        }
    }

  }
}
