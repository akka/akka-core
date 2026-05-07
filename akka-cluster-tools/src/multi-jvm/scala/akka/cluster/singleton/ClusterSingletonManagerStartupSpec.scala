/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._

object ClusterSingletonManagerStartupSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

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
  class Echo extends Actor {
    def receive = {
      case _ =>
        sender() ! self
    }
  }
}

class ClusterSingletonManagerStartupMultiJvmNode1 extends ClusterSingletonManagerStartupSpec
class ClusterSingletonManagerStartupMultiJvmNode2 extends ClusterSingletonManagerStartupSpec
class ClusterSingletonManagerStartupMultiJvmNode3 extends ClusterSingletonManagerStartupSpec

class ClusterSingletonManagerStartupSpec
    extends MultiNodeSpec(ClusterSingletonManagerStartupSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import ClusterSingletonManagerStartupSpec._

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
        singletonProps = Props(classOf[Echo]),
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system)),
      name = "echo")
  }

  lazy val echoProxy: ActorRef = {
    system.actorOf(
      ClusterSingletonProxy
        .props(singletonManagerPath = "/user/echo", settings = ClusterSingletonProxySettings(system)),
      name = "echoProxy")
  }

  "Startup of Cluster Singleton" must {

    "be quick" in {
      join(first, first)
      join(second, first)
      join(third, first)

      within(7.seconds) {
        awaitAssert {
          val members = Cluster(system).state.members
          members.size should be(3)
          members.forall(_.status == MemberStatus.Up) should be(true)
        }
      }
      enterBarrier("all-up")

      // the singleton instance is expected to start "instantly"
      echoProxy ! "hello"
      expectMsgType[ActorRef](3.seconds)

      enterBarrier("done")
    }

  }
}
