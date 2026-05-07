/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._

object ClusterSingletonManagerLeaveSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = off
    """))

  case object EchoStarted

  /**
   * The singleton actor
   */
  class Echo(testActor: ActorRef) extends Actor {
    override def preStart(): Unit = {
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

class ClusterSingletonManagerLeaveMultiJvmNode1 extends ClusterSingletonManagerLeaveSpec
class ClusterSingletonManagerLeaveMultiJvmNode2 extends ClusterSingletonManagerLeaveSpec
class ClusterSingletonManagerLeaveMultiJvmNode3 extends ClusterSingletonManagerLeaveSpec

class ClusterSingletonManagerLeaveSpec
    extends MultiNodeSpec(ClusterSingletonManagerLeaveSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import ClusterSingletonManagerLeaveSpec._

  override def initialParticipants = roles.size

  lazy val cluster = Cluster(system)

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
      createSingleton()
    }
  }

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

  "Leaving ClusterSingletonManager" must {

    "hand-over to new instance" in {
      join(first, first)

      runOn(first) {
        within(5.seconds) {
          expectMsg("preStart")
          echoProxy ! "hello"
          expectMsgType[ActorRef]
        }
      }
      enterBarrier("first-active")

      join(second, first)
      runOn(first, second) {
        within(10.seconds) {
          awaitAssert(cluster.state.members.count(m => m.status == MemberStatus.Up) should be(2))
        }
      }
      enterBarrier("second-up")

      join(third, first)
      within(10.seconds) {
        awaitAssert(cluster.state.members.count(m => m.status == MemberStatus.Up) should be(3))
      }
      enterBarrier("all-up")

      runOn(second) {
        cluster.leave(node(first).address)
      }

      runOn(first) {
        cluster.registerOnMemberRemoved(testActor ! "MemberRemoved")
        expectMsg(10.seconds, "stop")
        expectMsg("postStop")
        // CoordinatedShutdown makes sure that singleton actors are
        // stopped before Cluster shutdown
        expectMsg("MemberRemoved")
        echoProxyTerminatedProbe.expectTerminated(echoProxy, 10.seconds)
      }
      enterBarrier("first-stopped")

      runOn(second) {
        expectMsg("preStart")
      }
      enterBarrier("second-started")

      runOn(second, third) {
        val p = TestProbe()
        val firstAddress = node(first).address
        p.within(15.seconds) {
          p.awaitAssert {
            echoProxy.tell("hello2", p.ref)
            p.expectMsgType[ActorRef](1.seconds).path.address should not be (firstAddress)

          }
        }
      }
      enterBarrier("second-working")

      runOn(second) {
        cluster.registerOnMemberRemoved(testActor ! "MemberRemoved")
        cluster.leave(node(second).address)
        expectMsg(15.seconds, "stop")
        expectMsg("postStop")
        expectMsg("MemberRemoved")
        echoProxyTerminatedProbe.expectTerminated(echoProxy, 10.seconds)
      }
      enterBarrier("second-stopped")

      runOn(third) {
        expectMsg("preStart")
      }
      enterBarrier("third-started")

      runOn(third) {
        cluster.registerOnMemberRemoved(testActor ! "MemberRemoved")
        cluster.leave(node(third).address)
        expectMsg(5.seconds, "stop")
        expectMsg("postStop")
        expectMsg("MemberRemoved")
        echoProxyTerminatedProbe.expectTerminated(echoProxy, 10.seconds)
      }
      enterBarrier("third-stopped")

    }

  }
}
