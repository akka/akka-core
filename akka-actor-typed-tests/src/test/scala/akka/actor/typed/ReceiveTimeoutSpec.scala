/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import akka.actor.NotInfluenceReceiveTimeout
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.pattern.after
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

object ReceiveTimeoutSpec {

  object RegularTimeout {
    def apply(probe: ActorRef[String]): Behavior[String] = Behaviors.receivePartial {
      case (context, "start") =>
        context.log.debug("receive timeout set to 100ms")
        context.setReceiveTimeout(100.millis, "shutdown")
        Behaviors.same
      case (context, "shutdown") =>
        context.log.debug("saw receive timeout")
        probe ! "shutdown"
        Behaviors.stopped
      case (context, someMessage) =>
        context.log.debug("saw message [{}]", someMessage)
        Behaviors.same
    }
  }

  object AdaptedWithNotInfluenceTimeout {
    case class SomeMessage(text: String) extends NotInfluenceReceiveTimeout

    def apply(probe: ActorRef[String]): Behavior[String] = Behaviors.receivePartial {
      case (context, "start") =>
        context.log.debug("receive timeout set to 100ms")
        context.setReceiveTimeout(100.millis, "shutdown")
        context.self ! "keep-adapting"
        Behaviors.same
      case (context, "keep-adapting") =>
        // keep cycling these NotInfluenceReceiveTimeout adapted futures
        implicit val ec: ExecutionContext = context.executionContext
        context.log.debug("adapting")
        context.pipeToSelf(
          after(20.millis, context.system.classicSystem.scheduler)(Future.successful(SomeMessage("keep-adapting")))) {
          case Success(SomeMessage(text)) => text
          case Failure(error)             => throw error
        }
        Behaviors.same
      case (context, "shutdown") =>
        context.log.debug("saw receive timeout")
        probe ! "shutdown"
        Behaviors.stopped
    }
  }

}

class ReceiveTimeoutSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with Matchers with LogCapturing {

  "Typed actor receive timeouts" should {
    "trigger regular receive timeout" in {
      val probe = testKit.createTestProbe[String]()
      val actor = testKit.spawn(ReceiveTimeoutSpec.RegularTimeout(probe.ref))
      actor ! "start"
      actor ! "some message"
      probe.expectMessage("shutdown")
    }
    "not be reset by adapted replies marked with NotInfluenceReceiveTimeout" in {
      val probe = testKit.createTestProbe[String]()
      val actor = testKit.spawn(ReceiveTimeoutSpec.AdaptedWithNotInfluenceTimeout(probe.ref))
      actor ! "start"
      probe.expectMessage("shutdown")
    }
  }
}
