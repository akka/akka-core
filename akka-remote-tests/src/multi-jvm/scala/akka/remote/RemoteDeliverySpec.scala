/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.Identify
import akka.actor.Props
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.serialization.jackson.CborSerializable

object RemoteDeliveryConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(debugConfig(on = false).withFallback(RemotingMultiNodeSpec.commonConfig))
}

class RemoteDeliveryMultiJvmNode1 extends RemoteDeliverySpec
class RemoteDeliveryMultiJvmNode2 extends RemoteDeliverySpec
class RemoteDeliveryMultiJvmNode3 extends RemoteDeliverySpec

object RemoteDeliverySpec {
  final case class Letter(n: Int, route: List[ActorRef]) extends CborSerializable

  class Postman extends Actor {
    def receive = {
      case Letter(n, route) => route.head ! Letter(n, route.tail)
    }
  }
}

abstract class RemoteDeliverySpec extends RemotingMultiNodeSpec(RemoteDeliveryConfig) {
  import RemoteDeliveryConfig._
  import RemoteDeliverySpec._

  override def initialParticipants = roles.size

  def identify(role: RoleName, actorName: String): ActorRef = within(10 seconds) {
    system.actorSelection(node(role) / "user" / actorName) ! Identify(actorName)
    expectMsgType[ActorIdentity].ref.get
  }

  "Remote message delivery" must {

    "not drop messages under normal circumstances" in {
      system.actorOf(Props[Postman](), "postman-" + myself.name)
      enterBarrier("actors-started")

      runOn(first) {
        val p1 = identify(first, "postman-first")
        val p2 = identify(second, "postman-second")
        val p3 = identify(third, "postman-third")
        val route = p2 :: p3 :: p2 :: p3 :: testActor :: Nil

        for (n <- 1 to 500) {
          p1 ! Letter(n, route)
          expectMsg(5.seconds, Letter(n, Nil))
          // in case the loop count is increased it is good with some progress feedback
          if (n % 10000 == 0) log.info("Passed [{}]", n)
        }
      }

      enterBarrier("after-1")
    }

  }
}
