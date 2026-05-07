/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Identify
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._

object ClusterSingletonManagerLeave2Spec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

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
  class Echo(testActor: ActorRef) extends Actor with ActorLogging {
    override def preStart(): Unit = {
      log.debug("Started singleton at [{}]", Cluster(context.system).selfAddress)
      testActor ! "preStart"
    }
    override def postStop(): Unit = {
      log.debug("Stopped singleton at [{}]", Cluster(context.system).selfAddress)
      testActor ! "postStop"
    }

    def receive = {
      case "stop" =>
        testActor ! "stop"
      // this is the stop message from singleton manager, but don't stop immediately
      // will be stopped via PoisonPill from the test to simulate delay
      case _ =>
        sender() ! self
    }
  }
}

class ClusterSingletonManagerLeave2MultiJvmNode1 extends ClusterSingletonManagerLeave2Spec
class ClusterSingletonManagerLeave2MultiJvmNode2 extends ClusterSingletonManagerLeave2Spec
class ClusterSingletonManagerLeave2MultiJvmNode3 extends ClusterSingletonManagerLeave2Spec
class ClusterSingletonManagerLeave2MultiJvmNode4 extends ClusterSingletonManagerLeave2Spec
class ClusterSingletonManagerLeave2MultiJvmNode5 extends ClusterSingletonManagerLeave2Spec

class ClusterSingletonManagerLeave2Spec
    extends MultiNodeSpec(ClusterSingletonManagerLeave2Spec)
    with STMultiNodeSpec
    with ImplicitSender {
  import ClusterSingletonManagerLeave2Spec._

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

  "Leaving ClusterSingletonManager with two nodes" must {

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
      runOn(first, second, third) {
        within(10.seconds) {
          awaitAssert(cluster.state.members.count(m => m.status == MemberStatus.Up) should be(3))
        }
      }
      enterBarrier("third-up")
      runOn(first, second, third, fourth) {
        join(fourth, first)
        within(10.seconds) {
          awaitAssert(cluster.state.members.count(m => m.status == MemberStatus.Up) should be(4))
        }
      }
      enterBarrier("fourth-up")
      join(fifth, first)
      within(10.seconds) {
        awaitAssert(cluster.state.members.count(m => m.status == MemberStatus.Up) should be(5))
      }
      enterBarrier("all-up")

      runOn(first) {
        cluster.registerOnMemberRemoved(testActor ! "MemberRemoved")
        cluster.leave(cluster.selfAddress)
        expectMsg(10.seconds, "stop") // from singleton manager, but will not stop immediately
      }
      runOn(second, fourth) {
        cluster.registerOnMemberRemoved(testActor ! "MemberRemoved")
        cluster.leave(cluster.selfAddress)
        expectMsg(10.seconds, "MemberRemoved")
      }

      runOn(second, third) {
        (1 to 3).foreach { n =>
          Thread.sleep(1000)
          // singleton should not be started before old has been stopped
          system.actorSelection("/user/echo/singleton") ! Identify(n)
          expectMsg(ActorIdentity(n, None)) // not started
        }
      }
      enterBarrier("still-running-at-first")

      runOn(first) {
        system.actorSelection("/user/echo/singleton") ! PoisonPill
        expectMsg("postStop")
        // CoordinatedShutdown makes sure that singleton actors are
        // stopped before Cluster shutdown
        expectMsg(10.seconds, "MemberRemoved")
        echoProxyTerminatedProbe.expectTerminated(echoProxy, 10.seconds)
      }
      enterBarrier("stopped")

      runOn(third) {
        expectMsg(10.seconds, "preStart")
      }
      enterBarrier("third-started")

      runOn(third, fifth) {
        val p = TestProbe()
        val firstAddress = node(first).address
        p.within(15.seconds) {
          p.awaitAssert {
            echoProxy.tell("hello2", p.ref)
            p.expectMsgType[ActorRef](1.seconds).path.address should not be (firstAddress)

          }
        }
      }
      enterBarrier("third-working")
    }

  }
}
