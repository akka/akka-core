/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.coexistence
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import akka.{ actor => u }
import akka.actor.Actor
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.testkit.TestProbe

object ProbedBehavior {
  def behavior(probe: u.ActorRef): Behavior[String] = {
    Behaviors
      .receiveMessagePartial[String] {
        case "throw" => throw TestException("oh dear")
      }
      .receiveSignal {
        case (_, s) =>
          probe ! s
          Behaviors.same
      }
  }
}

object ClassicSupervisingTypedSpec {

  case class SpawnFromClassic(behav: Behavior[String], name: String)
  case class SpawnAnonFromClassic(behav: Behavior[String])
  case class TypedSpawnedFromClassicContext(actorRef: ActorRef[String])

  class ClassicToTyped extends Actor {

    override def receive: Receive = {
      case SpawnFromClassic(behav, name) =>
        sender() ! TypedSpawnedFromClassicContext(context.spawn(behav, name))
      case SpawnAnonFromClassic(behav) =>
        sender() ! TypedSpawnedFromClassicContext(context.spawnAnonymous(behav))
    }
  }
}

class ClassicSupervisingTypedSpec extends AnyWordSpecLike with LogCapturing with BeforeAndAfterAll {

  import ClassicSupervisingTypedSpec._

  val classicSystem = akka.actor.ActorSystem(
    "ClassicSupervisingTypedSpec",
    ConfigFactory.parseString("""
      akka.actor.testkit.typed.expect-no-message-default = 50 ms
      """))
  val classicTestKit = new akka.testkit.TestKit(classicSystem)
  implicit val classicSender: u.ActorRef = classicTestKit.testActor
  import classicTestKit._

  "A classic actor system that spawns typed actors" should {
    "default to stop for supervision" in {
      val probe = TestProbe()
      val underTest = classicSystem.spawn(ProbedBehavior.behavior(probe.ref), "a1")
      watch(underTest.toClassic)
      underTest ! "throw"
      probe.expectMsg(PostStop)
      probe.expectNoMessage()
      expectTerminated(underTest.toClassic)
    }

    "default to stop for supervision for spawn anonymous" in {
      val probe = TestProbe()
      val underTest = classicSystem.spawnAnonymous(ProbedBehavior.behavior(probe.ref))
      watch(underTest.toClassic)
      underTest ! "throw"
      probe.expectMsg(PostStop)
      probe.expectNoMessage()
      expectTerminated(underTest.toClassic)
    }

    "allows overriding the default" in {
      val probe = TestProbe()
      val value = Behaviors.supervise(ProbedBehavior.behavior(probe.ref)).onFailure(SupervisorStrategy.restart)
      val underTest = classicSystem.spawn(value, "a2")
      watch(underTest.toClassic)
      underTest ! "throw"
      probe.expectMsg(PreRestart)
      probe.expectNoMessage()
      expectNoMessage()
    }

    "default to stop supervision (from context)" in {
      val classic = classicSystem.actorOf(u.Props(new ClassicToTyped()))
      val probe = TestProbe()
      classic ! SpawnFromClassic(ProbedBehavior.behavior(probe.ref), "a3")
      val underTest = expectMsgType[TypedSpawnedFromClassicContext].actorRef
      watch(underTest.toClassic)
      underTest ! "throw"
      probe.expectMsg(PostStop)
      probe.expectNoMessage()
      expectTerminated(underTest.toClassic)
    }

    "allow overriding the default (from context)" in {
      val classic = classicSystem.actorOf(u.Props(new ClassicToTyped()))
      val probe = TestProbe()
      val behavior = Behaviors.supervise(ProbedBehavior.behavior(probe.ref)).onFailure(SupervisorStrategy.restart)
      classic ! SpawnFromClassic(behavior, "a4")
      val underTest = expectMsgType[TypedSpawnedFromClassicContext].actorRef
      watch(underTest.toClassic)
      underTest ! "throw"
      probe.expectMsg(PreRestart)
      probe.expectNoMessage()
      expectNoMessage()
    }

    "default to stop supervision for spawn anonymous (from context)" in {
      val classic = classicSystem.actorOf(u.Props(new ClassicToTyped()))
      val probe = TestProbe()
      classic ! SpawnAnonFromClassic(ProbedBehavior.behavior(probe.ref))
      val underTest = expectMsgType[TypedSpawnedFromClassicContext].actorRef
      watch(underTest.toClassic)
      underTest ! "throw"
      probe.expectMsg(PostStop)
      probe.expectNoMessage()
      expectTerminated(underTest.toClassic)
    }

    "default to stop for supervision of systemActorOf" in {
      val probe = TestProbe()
      val underTest = classicSystem.toTyped.systemActorOf(ProbedBehavior.behavior(probe.ref), "s1")
      watch(underTest.toClassic)
      underTest ! "throw"
      probe.expectMsg(PostStop)
      probe.expectNoMessage()
      expectTerminated(underTest.toClassic)
    }

    "default to stop for PropsAdapter" in {
      val probe = TestProbe()
      val underTest = classicSystem.actorOf(PropsAdapter(ProbedBehavior.behavior(probe.ref)))
      watch(underTest)
      underTest ! "throw"
      probe.expectMsg(PostStop)
      probe.expectNoMessage()
      expectTerminated(underTest)
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    classicTestKit.shutdown(classicSystem)
  }

}
