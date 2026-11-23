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
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._

object ClusterSingletonManagerStalePreviousOldestSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(
    ConfigFactory.parseString(
      """
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = off
    # A removed previous oldest is only dropped from the younger node's list after this margin.
    # It is deliberately much longer than the 10 seconds the test allows for the hand-over,
    # so a stale previous oldest shows up as a missed hand-over.
    akka.cluster.down-removal-margin = 30s
    """))

  /**
   * The singleton actor. Reports its lifecycle to the test actor of the node it runs on.
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
    }
  }
}

class ClusterSingletonManagerStalePreviousOldestMultiJvmNode1 extends ClusterSingletonManagerStalePreviousOldestSpec
class ClusterSingletonManagerStalePreviousOldestMultiJvmNode2 extends ClusterSingletonManagerStalePreviousOldestSpec
class ClusterSingletonManagerStalePreviousOldestMultiJvmNode3 extends ClusterSingletonManagerStalePreviousOldestSpec

/**
 * Reproduces a stale previous oldest in a younger node's hand-over bookkeeping.
 *
 * Nodes join in age order: first (oldest), second, third. The singleton is handed over cleanly
 * from first to second, and then from second to third. The third node was younger than both when
 * it joined, so its list of previous oldest nodes starts as [first, second]. Observing first's
 * replacement must not leave first at the head of that list, otherwise third waits for first's
 * removal (down-removal-margin) before it accepts second's TakeOver request when second leaves.
 */
class ClusterSingletonManagerStalePreviousOldestSpec
    extends MultiNodeSpec(ClusterSingletonManagerStalePreviousOldestSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import ClusterSingletonManagerStalePreviousOldestSpec._

  override def initialParticipants = roles.size

  lazy val cluster = Cluster(system)

  "ClusterSingletonManager" must {

    "hand over to the next oldest when the previous oldest left earlier" in {
      // every node runs a singleton manager, the singleton itself is created by whichever is oldest
      system.actorOf(
        ClusterSingletonManager.props(
          singletonProps = Props(classOf[Echo], testActor),
          terminationMessage = "stop",
          settings = ClusterSingletonManagerSettings(system)),
        name = "echo")

      // join in age order, one node at a time, so that ages are first < second < third
      runOn(first) {
        cluster.join(node(first).address)
        within(10.seconds) {
          awaitAssert(cluster.state.members.count(_.status == MemberStatus.Up) should be(1))
        }
      }
      enterBarrier("first-up")

      runOn(second) {
        cluster.join(node(first).address)
      }
      runOn(first, second) {
        within(10.seconds) {
          awaitAssert(cluster.state.members.count(_.status == MemberStatus.Up) should be(2))
        }
      }
      enterBarrier("second-up")

      runOn(third) {
        cluster.join(node(first).address)
      }
      within(10.seconds) {
        awaitAssert(cluster.state.members.count(_.status == MemberStatus.Up) should be(3))
      }
      enterBarrier("all-up")

      runOn(first) {
        expectMsg(10.seconds, "preStart")
      }
      enterBarrier("first-singleton-started")

      // first hands over to second
      runOn(first) {
        cluster.leave(node(first).address)
        expectMsg(10.seconds, "stop")
      }
      runOn(second) {
        expectMsg(15.seconds, "preStart")
      }
      enterBarrier("second-singleton-started")

      // second hands over to third, which must not wait for first's removal margin
      runOn(second) {
        cluster.leave(node(second).address)
      }
      runOn(third) {
        expectMsg(10.seconds, "preStart")
      }
      enterBarrier("third-singleton-started")
    }
  }
}
