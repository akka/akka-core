/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.cluster.client

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ Actor, Props }
import akka.cluster.Cluster
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec, STMultiNodeSpec }
import akka.testkit.{ EventFilter, ImplicitSender }

object ClusterClientStopSpec extends MultiNodeConfig {
  val client = role("client")
  val first = role("first")
  val second = role("second")
  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.client {
      heartbeat-interval = 1s
      acceptable-heartbeat-pause = 1s
      reconnect-timeout = 3s
      receptionist.number-of-contacts = 1

    }
    akka.test.filter-leeway = 10s
  """))

  class Service extends Actor {
    def receive = {
      case msg => sender() ! msg
    }
  }
}

class ClusterClientStopMultiJvmNode1 extends ClusterClientStopSpec
class ClusterClientStopMultiJvmNode2 extends ClusterClientStopSpec
class ClusterClientStopMultiJvmNode3 extends ClusterClientStopSpec

@nowarn("msg=deprecated")
class ClusterClientStopSpec extends MultiNodeSpec(ClusterClientStopSpec) with STMultiNodeSpec with ImplicitSender {

  import ClusterClientStopSpec._

  override def initialParticipants: Int = 3

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system).join(node(to).address)
      ClusterClientReceptionist(system)
    }
    enterBarrier(from.name + "-joined")
  }

  def awaitCount(expected: Int): Unit = {
    awaitAssert {
      DistributedPubSub(system).mediator ! DistributedPubSubMediator.Count
      val actual = expectMsgType[Int]
      actual should ===(expected)
    }
  }

  def initialContacts = Set(first, second).map { r =>
    node(r) / "system" / "receptionist"
  }

  "A Cluster Client" should {

    "startup cluster" in within(30.seconds) {
      join(first, first)
      join(second, first)
      runOn(first) {
        val service = system.actorOf(Props(classOf[Service]), "testService")
        ClusterClientReceptionist(system).registerService(service)
      }
      runOn(first, second) {
        awaitCount(1)
      }

      enterBarrier("cluster-started")
    }

    "stop if re-establish fails for too long time" in within(20.seconds) {
      runOn(client) {
        val c = system.actorOf(
          ClusterClient.props(ClusterClientSettings(system).withInitialContacts(initialContacts)),
          "client1")
        c ! ClusterClient.Send("/user/testService", "hello", localAffinity = true)
        expectMsgType[String](3.seconds) should be("hello")
        enterBarrier("was-in-contact")

        watch(c)

        expectTerminated(c, 10.seconds)
        EventFilter.warning(start = "Receptionist reconnect not successful within", occurrences = 1)

      }

      runOn(first, second) {
        enterBarrier("was-in-contact")
        Await.ready(system.terminate(), 10.seconds)

      }

    }

  }

}
