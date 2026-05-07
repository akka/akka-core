/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.annotation.nowarn
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ Actor, ActorLogging, Address, PoisonPill, Props }
import akka.cluster.Cluster
import akka.cluster.ClusterSettings
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec, STMultiNodeSpec }
import akka.serialization.jackson.CborSerializable
import akka.testkit.ImplicitSender

object MultiDcSingletonManagerSpec extends MultiNodeConfig {
  val controller = role("controller")
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.actor.provider = "cluster"
    """))

  nodeConfig(controller) {
    ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = one
      akka.cluster.roles = []""")
  }

  nodeConfig(first) {
    ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = one
      akka.cluster.roles = [ worker ]""")
  }
  nodeConfig(second, third) {
    ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = two
      akka.cluster.roles = [ worker ]""")
  }
}

class MultiDcSingletonManagerMultiJvmNode1 extends MultiDcSingletonManagerSpec
class MultiDcSingletonManagerMultiJvmNode2 extends MultiDcSingletonManagerSpec
class MultiDcSingletonManagerMultiJvmNode3 extends MultiDcSingletonManagerSpec
class MultiDcSingletonManagerMultiJvmNode4 extends MultiDcSingletonManagerSpec

class MultiDcSingleton extends Actor with ActorLogging {
  import MultiDcSingleton._

  val cluster = Cluster(context.system)

  override def receive: Receive = {
    case Ping =>
      sender() ! Pong(cluster.settings.SelfDataCenter, cluster.selfAddress, cluster.selfRoles)
  }
}
object MultiDcSingleton {
  case object Ping extends CborSerializable
  case class Pong(fromDc: String, fromAddress: Address, roles: Set[String]) extends CborSerializable
}

@nowarn("msg=Use Akka Distributed Cluster")
abstract class MultiDcSingletonManagerSpec
    extends MultiNodeSpec(MultiDcSingletonManagerSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import MultiDcSingletonManagerSpec._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)
  cluster.join(node(controller).address)
  enterBarrier("nodes-joined")

  val worker = "worker"

  "A SingletonManager in a multi data center cluster" must {
    "start a singleton instance for each data center" in {

      runOn(first, second, third) {
        system.actorOf(
          ClusterSingletonManager
            .props(Props[MultiDcSingleton](), PoisonPill, ClusterSingletonManagerSettings(system).withRole(worker)),
          "singletonManager")
      }

      val proxy = system.actorOf(
        ClusterSingletonProxy.props("/user/singletonManager", ClusterSingletonProxySettings(system).withRole(worker)))

      enterBarrier("managers-started")

      proxy ! MultiDcSingleton.Ping
      val pong = expectMsgType[MultiDcSingleton.Pong](20.seconds)

      enterBarrier("pongs-received")

      pong.fromDc should equal(Cluster(system).settings.SelfDataCenter)
      pong.roles should contain(worker)
      runOn(controller, first) {
        pong.roles should contain(ClusterSettings.DcRolePrefix + "one")
      }
      runOn(second, third) {
        pong.roles should contain(ClusterSettings.DcRolePrefix + "two")
      }

      enterBarrier("after-1")
    }

    "be able to use proxy across different data centers" in {
      runOn(third) {
        val proxy = system.actorOf(
          ClusterSingletonProxy.props(
            "/user/singletonManager",
            ClusterSingletonProxySettings(system).withRole(worker).withDataCenter("one")))
        proxy ! MultiDcSingleton.Ping
        val pong = expectMsgType[MultiDcSingleton.Pong](10.seconds)
        pong.fromDc should ===("one")
        pong.roles should contain(worker)
        pong.roles should contain(ClusterSettings.DcRolePrefix + "one")
      }
      enterBarrier("after-1")
    }

  }
}
