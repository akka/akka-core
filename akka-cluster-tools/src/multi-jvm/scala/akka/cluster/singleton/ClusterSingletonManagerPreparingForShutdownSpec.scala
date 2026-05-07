/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.MemberStatus.Removed
import akka.cluster.MultiNodeClusterSpec
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._

object ClusterSingletonManagerPreparingForShutdownSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO 
    akka.actor.provider = "cluster"
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = off
    akka.cluster.leader-actions-interval = 100ms
    """))

  case object EchoStarted

  /**
   * The singleton actor
   */
  class Echo(testActor: ActorRef) extends Actor with ActorLogging {
    override def preStart(): Unit = {
      log.info("Singleton starting on {}", Cluster(context.system).selfUniqueAddress)
      testActor ! "preStart"
    }
    override def postStop(): Unit = {
      testActor ! "postStop"
    }

    def receive = {
      case "stop" =>
        testActor ! "stop"
        context.stop(self)
      case _ =>
        sender() ! self
    }
  }
}

class ClusterSingletonManagerPreparingForShutdownMultiJvmNode1 extends ClusterSingletonManagerPreparingForShutdownSpec
class ClusterSingletonManagerPreparingForShutdownMultiJvmNode2 extends ClusterSingletonManagerPreparingForShutdownSpec
class ClusterSingletonManagerPreparingForShutdownMultiJvmNode3 extends ClusterSingletonManagerPreparingForShutdownSpec

class ClusterSingletonManagerPreparingForShutdownSpec
    extends MultiNodeClusterSpec(ClusterSingletonManagerPreparingForShutdownSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import ClusterSingletonManagerPreparingForShutdownSpec._

  override def initialParticipants = roles.size

  def createSingleton(): ActorRef = {
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props(classOf[Echo], testActor),
        terminationMessage = "stop",
        settings = ClusterSingletonManagerSettings(system)),
      name = "echo")
  }

  val echoProxyTerminatedProbe = TestProbe()

  lazy val echoProxy: ActorRef = {
    echoProxyTerminatedProbe.watch(
      system.actorOf(
        ClusterSingletonProxy
          .props(singletonManagerPath = "/user/echo", settings = ClusterSingletonProxySettings(system)),
        name = "echoProxy"))
  }

  "Preparing for shut down ClusterSingletonManager" must {

    "form cluster" in {
      awaitClusterUp(first, second, third)
    }

    "not handover when ready for shutdown" in {

      createSingleton()
      runOn(first) {
        within(10.seconds) {
          expectMsg("preStart")
          echoProxy ! "hello"
          expectMsgType[ActorRef]
          expectNoMessage(2.seconds)
        }
      }
      enterBarrier("singleton-active")

      runOn(first) {
        Cluster(system).prepareForFullClusterShutdown()
      }
      awaitAssert({
        withClue("members: " + Cluster(system).readView.members) {
          Cluster(system).selfMember.status shouldEqual MemberStatus.ReadyForShutdown
        }
      }, 10.seconds)
      enterBarrier("preparation-complete")

      runOn(first) {
        Cluster(system).leave(address(first))
      }
      awaitAssert(
        {
          runOn(second, third) {
            withClue("members: " + Cluster(system).readView.members) {
              Cluster(system).readView.members.size shouldEqual 2
            }
          }
          runOn(first) {
            withClue("self member: " + Cluster(system).selfMember) {
              Cluster(system).selfMember.status shouldEqual MemberStatus.Removed
            }
          }
        },
        8.seconds) // this timeout must be lower than coordinated shutdown timeout otherwise it could pass due to the timeout continuing with the cluster exit
      // where as this is testing that shutdown happens right away when a cluster is in preparing to shutdown mode
      enterBarrier("initial-singleton-removed")

      // even tho the handover isn't completed the new oldest node will start it after a timeout
      // make sure this isn't the case
      runOn(second) {
        echoProxy ! "hello"
        expectNoMessage(5.seconds)
      }

      enterBarrier("no-singleton-running")
    }

    "last nodes should shut down" in {
      runOn(second) {
        Cluster(system).leave(address(third))
        Cluster(system).leave(address(second))
      }
      awaitAssert({
        withClue("self member: " + Cluster(system).selfMember) {
          Cluster(system).selfMember.status shouldEqual Removed
        }
      }, 10.seconds)
      enterBarrier("done")
    }

  }
}
