/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.client

import scala.annotation.nowarn
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ ActorPath, ActorRef }
import akka.cluster.{ Cluster, MultiNodeClusterSpec }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, STMultiNodeSpec }
import akka.testkit.{ ImplicitSender, TestActors }

object ClusterClientHandoverSpec extends MultiNodeConfig {
  val client = role("client")
  val first = role("first")
  val second = role("second")
  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.client {
      heartbeat-interval = 1d
      acceptable-heartbeat-pause = 1d
      reconnect-timeout = 3s
      refresh-contacts-interval = 1d

    }
    akka.test.filter-leeway = 10s
  """).withFallback(MultiNodeClusterSpec.clusterConfig))
}

class ClusterClientHandoverSpecMultiJvmNode1 extends ClusterClientHandoverSpec
class ClusterClientHandoverSpecMultiJvmNode2 extends ClusterClientHandoverSpec
class ClusterClientHandoverSpecMultiJvmNode3 extends ClusterClientHandoverSpec

@nowarn("msg=deprecated")
class ClusterClientHandoverSpec
    extends MultiNodeClusterSpec(ClusterClientHandoverSpec)
    with STMultiNodeSpec
    with ImplicitSender {

  import ClusterClientHandoverSpec._

  override def initialParticipants: Int = 3

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system).join(node(to).address)
      ClusterClientReceptionist(system)
    }
    enterBarrier(from.name + "-joined")
  }

  def initialContacts: Set[ActorPath] = Set(first, second).map { r =>
    node(r) / "system" / "receptionist"
  }

  "A Cluster Client" must {

    "startup cluster with a single node" in within(30.seconds) {
      join(first, first)
      runOn(first) {
        val service = system.actorOf(TestActors.echoActorProps, "testService")
        ClusterClientReceptionist(system).registerService(service)
        awaitMembersUp(1)
      }
      enterBarrier("cluster-started")
    }

    var clusterClient: ActorRef = null

    "establish connection to first node" in {
      runOn(client) {
        clusterClient = system.actorOf(
          ClusterClient.props(ClusterClientSettings(system).withInitialContacts(initialContacts)),
          "client1")
        clusterClient ! ClusterClient.Send("/user/testService", "hello", localAffinity = true)
        expectMsgType[String](3.seconds) should be("hello")
      }
      enterBarrier("established")
    }

    "bring the second node into the cluster" in {
      join(second, first)
      runOn(second) {
        val service = system.actorOf(TestActors.echoActorProps, "testService")
        ClusterClientReceptionist(system).registerService(service)
        awaitMembersUp(2)
      }
      enterBarrier("second-up")
    }

    "remove first node from the cluster" in {
      runOn(first) {
        Cluster(system).leave(node(first).address)
      }

      runOn(second) {
        awaitMembersUp(1)
      }
      enterBarrier("handover-done")
    }

    "re-establish on receptionist shutdown" in {
      runOn(client) {
        clusterClient ! ClusterClient.Send("/user/testService", "hello", localAffinity = true)
        expectMsgType[String](3.seconds) should be("hello")
      }
      enterBarrier("handover-successful")
    }
  }
}
