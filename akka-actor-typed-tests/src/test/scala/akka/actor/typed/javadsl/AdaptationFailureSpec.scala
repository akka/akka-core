/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl

import scala.concurrent.Promise

import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.Behavior
import akka.actor.typed.MessageAdaptionFailure
import akka.actor.typed.PreRestart
import akka.actor.typed.Terminated

object AdaptationFailureSpec {
  def emptyAbstractBehavior: Behavior[Any] = Behaviors.setup(new EmptyAbstractBehavior(_))
  class EmptyAbstractBehavior(ctx: ActorContext[Any]) extends AbstractBehavior[Any](ctx) {
    protected def createReceive: Receive[Any] = newReceiveBuilder.build()
  }

  def abstractBehaviorHandlingOtherSignals: Behavior[Any] = Behaviors.setup(new AbstractBehaviorHandlingOtherSignals(_))
  class AbstractBehaviorHandlingOtherSignals(ctx: ActorContext[Any]) extends AbstractBehavior[Any](ctx) {
    protected def createReceive: Receive[Any] =
      newReceiveBuilder.onSignal(classOf[PreRestart], (_: PreRestart) => Behaviors.same).build()
  }

  def abstractBehaviorHandlingMessageAdaptationFailure: Behavior[Any] =
    Behaviors.setup(new AbstractBehaviorHandlingMessageAdaptationFailure(_))
  class AbstractBehaviorHandlingMessageAdaptationFailure(ctx: ActorContext[Any]) extends AbstractBehavior[Any](ctx) {
    protected def createReceive: Receive[Any] =
      newReceiveBuilder.onSignal(classOf[MessageAdaptionFailure], (_: MessageAdaptionFailure) => Behaviors.same).build()
  }
}

class AdaptationFailureSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  import AdaptationFailureSpec._

  val crashingBehaviors: List[(String, Behavior[Any])] =
    "AbstractBehavior" -> emptyAbstractBehavior ::
    "AbstractBehavior handling other signals" -> abstractBehaviorHandlingOtherSignals ::
    Nil

  val nonCrashingBehaviors: List[(String, Behavior[Any])] =
    "AbstractBehavior handling MessageAdaptationFailure" -> abstractBehaviorHandlingMessageAdaptationFailure ::
    Nil

  "Failure in an adapter" must {

    crashingBehaviors.foreach {
      case (name, behavior) =>
        s"default to crash the actor or $name" in {
          val probe = createTestProbe()
          val ref = spawn(Behaviors.setup[Any] { ctx =>
            val adapter = ctx.messageAdapter[Any](classOf[Any], _ => throw TestException("boom"))
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
            val adapter = ctx.messageAdapter[Any](classOf[Any], { _ =>
              threw.success(Done)
              throw TestException("boom")
            })
            adapter ! "go boom"
            behavior
          })
          spawn(Behaviors.setup[Any] { ctx =>
            ctx.watch(ref)

            Behaviors.receiveSignal {
              case (_, Terminated(`ref`)) =>
                probe.ref ! "actor-stopped"
                Behaviors.same
              case _ => Behaviors.unhandled
            }
          })

          probe.expectNoMessage()
        }
    }
  }

}
