/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.routing

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.{ LogCapturing, ScalaTestWithActorTestKit }
import akka.actor.typed.{ ActorRef, Behavior, DispatcherSelector }
import akka.actor.typed.scaladsl.{ Behaviors, Routers }

object PoolRouterSpec {

  object RouteeBehavior {

    final case class WhichDispatcher(replyTo: ActorRef[String])

    def apply(): Behavior[WhichDispatcher] = Behaviors.receiveMessage {
      case WhichDispatcher(replyTo) =>
        replyTo ! Thread.currentThread.getName
        Behaviors.same
    }
  }
}

class PoolRouterSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  import PoolRouterSpec.RouteeBehavior
  import RouteeBehavior.WhichDispatcher

  "PoolRouter" must {

    "use the default dispatcher per default for its routees" in {
      val probe = createTestProbe[String]()
      val pool = spawn(Routers.pool(1)(RouteeBehavior()), "default-pool")
      pool ! WhichDispatcher(probe.ref)

      val response = probe.receiveMessage()
      response should startWith("PoolRouterSpec-akka.actor.default-dispatcher")
    }

    "use the specified dispatcher for its routees" in {
      val probe = createTestProbe[String]()
      val pool = spawn(
        Routers.pool(1)(RouteeBehavior()).withRouteeProps(DispatcherSelector.blocking()),
        "pool-with-blocking-routees")
      pool ! WhichDispatcher(probe.ref)

      val response = probe.receiveMessage()
      response should startWith("PoolRouterSpec-akka.actor.default-blocking-io-dispatcher")
    }
  }
}
