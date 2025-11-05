/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import scala.concurrent.duration._

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl.{ ScalaTestWithActorTestKit, TestProbe }
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.typed.ClusterSingletonPoisonPillSpec.GetSelf

object ClusterSingletonPoisonPillSpec {

  final case class GetSelf(replyTo: ActorRef[ActorRef[Any]])
  val sneakyBehavior: Behavior[GetSelf] = Behaviors.receive {
    case (ctx, GetSelf(replyTo)) =>
      replyTo ! ctx.self.unsafeUpcast[Any]
      Behaviors.same
  }
}

class ClusterSingletonPoisonPillSpec
    extends ScalaTestWithActorTestKit(ClusterSingletonApiSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  implicit val testSettings: TestKitSettings = TestKitSettings(system)
  val clusterNode1 = Cluster(system)
  clusterNode1.manager ! Join(clusterNode1.selfMember.address)
  val classicSystem1 = system.toClassic
  "A typed cluster singleton" must {

    "support using PoisonPill to stop" in {
      val probe = TestProbe[ActorRef[Any]]()
      val singleton =
        ClusterSingleton(system).init(SingletonActor(ClusterSingletonPoisonPillSpec.sneakyBehavior, "sneaky"))
      singleton ! GetSelf(probe.ref)
      val singletonRef = probe.receiveMessage()
      singletonRef ! PoisonPill
      probe.expectTerminated(singletonRef, 1.second)
    }

  }

}
