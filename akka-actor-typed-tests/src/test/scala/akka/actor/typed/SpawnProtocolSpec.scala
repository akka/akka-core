/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import scala.concurrent.Future
import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{ AnyWordSpec, AnyWordSpecLike }

import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout

object SpawnProtocolSpec {
  sealed trait Message
  final case class Ping(replyTo: ActorRef[Pong.type]) extends Message
  case object Pong

  val target: Behavior[Message] =
    Behaviors.receiveMessage {
      case Ping(replyTo) =>
        replyTo ! Pong
        Behaviors.same
    }
}

class SpawnProtocolSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  import SpawnProtocolSpec._
  implicit val testSettings: TestKitSettings = TestKitSettings(system)

  "Spawn behavior" must {
    "spawn child actor" in {
      val parentReply = TestProbe[ActorRef[Message]]()
      val parent = spawn(SpawnProtocol(), "parent")
      parent ! SpawnProtocol.Spawn(target, "child", Props.empty, parentReply.ref)
      val child = parentReply.receiveMessage()
      child.path.name should ===("child")
      child.path.parent.name should ===("parent")

      val childReply = TestProbe[Pong.type]()
      child ! Ping(childReply.ref)
    }

    "have nice API for ask" in {
      val parent = spawn(SpawnProtocol(), "parent2")
      import akka.actor.typed.scaladsl.AskPattern._
      implicit val timeout: Timeout = Timeout(5.seconds)
      val parentReply: Future[ActorRef[Ping]] =
        parent.ask(SpawnProtocol.Spawn(target, "child", Props.empty, _))
      val child = parentReply.futureValue
      val childReply = TestProbe[Pong.type]()
      child ! Ping(childReply.ref)
    }

    "be possible to use as guardian behavior" in {
      val sys = ActorSystem(SpawnProtocol(), "SpawnProtocolSpec2")
      try {
        val guardianReply = TestProbe[ActorRef[Message]]()(sys)
        sys ! SpawnProtocol.Spawn(target, "child1", Props.empty, guardianReply.ref)
        val child1 = guardianReply.receiveMessage()
        child1.path.elements.mkString("/", "/", "") should ===("/user/child1")

        sys ! SpawnProtocol.Spawn(target, "child2", Props.empty, guardianReply.ref)
        val child2 = guardianReply.receiveMessage()
        child2.path.elements.mkString("/", "/", "") should ===("/user/child2")
      } finally {
        ActorTestKit.shutdown(sys)
      }
    }

    "spawn with unique name when given name is taken" in {
      val parentReply = TestProbe[ActorRef[Message]]()
      val parent = spawn(SpawnProtocol(), "parent3")

      parent ! SpawnProtocol.Spawn(target, "child", Props.empty, parentReply.ref)
      val child0 = parentReply.receiveMessage()
      child0.path.name should ===("child")

      parent ! SpawnProtocol.Spawn(target, "child", Props.empty, parentReply.ref)
      val child1 = parentReply.receiveMessage()
      child1.path.name should ===("child-1")

      // take the generated name
      parent ! SpawnProtocol.Spawn(target, "child-2", Props.empty, parentReply.ref)
      val child2 = parentReply.receiveMessage()
      child2.path.name should ===("child-2")

      // "child" is taken, and also "child-1" and "child-2"
      parent ! SpawnProtocol.Spawn(target, "child", Props.empty, parentReply.ref)
      val child3 = parentReply.receiveMessage()
      child3.path.name should ===("child-3")
    }
  }
}

class StubbedSpawnProtocolSpec extends AnyWordSpec with Matchers with LogCapturing {

  import SpawnProtocolSpec._

  "Stubbed Spawn behavior" must {

    "spawn with given name" in {
      val parentReply = TestInbox[ActorRef[Message]]()
      val testkit = BehaviorTestKit(SpawnProtocol())
      testkit.run(SpawnProtocol.Spawn(target, "child", Props.empty, parentReply.ref))
      val child = parentReply.receiveMessage()
      child.path.name should ===("child")
      testkit.expectEffect(Effects.spawned(target, "child"))
    }

    "spawn anonymous when name undefined" in {
      val parentReply = TestInbox[ActorRef[Message]]()
      val testkit = BehaviorTestKit(SpawnProtocol())
      testkit.run(SpawnProtocol.Spawn(target, "", Props.empty, parentReply.ref))
      val child = parentReply.receiveMessage()
      child.path.name should startWith("$")
      testkit.expectEffect(Effects.spawnedAnonymous(target))
    }
  }

}
