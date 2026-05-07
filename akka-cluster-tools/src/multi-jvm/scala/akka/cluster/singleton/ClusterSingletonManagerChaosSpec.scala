/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.RootActorPath
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._
import akka.testkit.TestEvent._

object ClusterSingletonManagerChaosSpec extends MultiNodeConfig {
  val controller = role("controller")
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")
  val sixth = role("sixth")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = 0s
    """))

  case object EchoStarted

  /**
   * The singleton actor
   */
  class Echo(testActor: ActorRef) extends Actor {
    testActor ! EchoStarted

    def receive = {
      case _ => sender() ! self
    }
  }
}

class ClusterSingletonManagerChaosMultiJvmNode1 extends ClusterSingletonManagerChaosSpec
class ClusterSingletonManagerChaosMultiJvmNode2 extends ClusterSingletonManagerChaosSpec
class ClusterSingletonManagerChaosMultiJvmNode3 extends ClusterSingletonManagerChaosSpec
class ClusterSingletonManagerChaosMultiJvmNode4 extends ClusterSingletonManagerChaosSpec
class ClusterSingletonManagerChaosMultiJvmNode5 extends ClusterSingletonManagerChaosSpec
class ClusterSingletonManagerChaosMultiJvmNode6 extends ClusterSingletonManagerChaosSpec
class ClusterSingletonManagerChaosMultiJvmNode7 extends ClusterSingletonManagerChaosSpec

class ClusterSingletonManagerChaosSpec
    extends MultiNodeSpec(ClusterSingletonManagerChaosSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import ClusterSingletonManagerChaosSpec._

  override def initialParticipants = roles.size

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system).join(node(to).address)
      createSingleton()
    }
  }

  def createSingleton(): ActorRef = {
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props(classOf[Echo], testActor),
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system)),
      name = "echo")
  }

  def crash(roles: RoleName*): Unit = {
    runOn(controller) {
      roles.foreach { r =>
        log.info("Shutdown [{}]", node(r).address)
        testConductor.exit(r, 0).await
      }
    }
  }

  def echo(oldest: RoleName): ActorSelection =
    system.actorSelection(RootActorPath(node(oldest).address) / "user" / "echo" / "singleton")

  def awaitMemberUp(memberProbe: TestProbe, nodes: RoleName*): Unit = {
    runOn(nodes.filterNot(_ == nodes.head): _*) {
      memberProbe.expectMsgType[MemberUp](15.seconds).member.address should ===(node(nodes.head).address)
    }
    runOn(nodes.head) {
      memberProbe.receiveN(nodes.size, 15.seconds).collect { case MemberUp(m) => m.address }.toSet should ===(
        nodes.map(node(_).address).toSet)
    }
    enterBarrier(nodes.head.name + "-up")
  }

  "A ClusterSingletonManager in chaotic cluster" must {

    "startup 6 node cluster" in within(60 seconds) {
      val memberProbe = TestProbe()
      Cluster(system).subscribe(memberProbe.ref, classOf[MemberUp])
      memberProbe.expectMsgClass(classOf[CurrentClusterState])

      join(first, first)
      awaitMemberUp(memberProbe, first)
      runOn(first) {
        expectMsg(EchoStarted)
      }
      enterBarrier("first-started")

      join(second, first)
      awaitMemberUp(memberProbe, second, first)

      join(third, first)
      awaitMemberUp(memberProbe, third, second, first)

      join(fourth, first)
      awaitMemberUp(memberProbe, fourth, third, second, first)

      join(fifth, first)
      awaitMemberUp(memberProbe, fifth, fourth, third, second, first)

      join(sixth, first)
      awaitMemberUp(memberProbe, sixth, fifth, fourth, third, second, first)

      runOn(controller) {
        echo(first) ! "hello"
        expectMsgType[ActorRef](3.seconds).path.address should ===(node(first).address)
      }
      enterBarrier("first-verified")

    }

    "take over when three oldest nodes crash in 6 nodes cluster" in within(90 seconds) {
      // mute logging of deadLetters during shutdown of systems
      if (!log.isDebugEnabled)
        system.eventStream.publish(Mute(DeadLettersFilter[Any]))
      enterBarrier("logs-muted")

      crash(first, second, third)
      enterBarrier("after-crash")
      runOn(fourth) {
        expectMsg(EchoStarted)
      }
      enterBarrier("fourth-active")

      runOn(controller) {
        echo(fourth) ! "hello"
        expectMsgType[ActorRef](3.seconds).path.address should ===(node(fourth).address)
      }
      enterBarrier("fourth-verified")

    }
  }
}
