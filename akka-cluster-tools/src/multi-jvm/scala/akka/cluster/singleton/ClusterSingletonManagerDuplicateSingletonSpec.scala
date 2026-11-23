/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.Props
import akka.actor.RootActorPath
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._

object ClusterSingletonManagerDuplicateSingletonSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(
    ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = off
    # Short margin: this spec checks that the singleton is never running on two nodes at once,
    # not hand-over latency. The latency of stale previous-oldest bookkeeping is covered by
    # ClusterSingletonManagerStalePreviousOldestSpec.
    akka.cluster.down-removal-margin = 2s
    """))

  case object Query
  final case class Running(roles: Set[String])

  // Started and Stopped are sent as plain strings: the spec runs with Java serialization disabled,
  // so custom message classes cannot cross the remote boundary.
  val StartedPrefix = "started:"
  val StoppedPrefix = "stopped:"

  /**
   * Keeps the set of roles that currently run the singleton, from the Started and Stopped
   * messages the singleton instances send. Runs on fourth.
   */
  class SingletonCollector extends Actor {
    private var running = Set.empty[String]

    def receive = {
      case s: String if s.startsWith(StartedPrefix) => running += s.stripPrefix(StartedPrefix)
      case s: String if s.startsWith(StoppedPrefix) => running -= s.stripPrefix(StoppedPrefix)
      case Query                                    => sender() ! Running(running)
    }
  }

  /**
   * The singleton actor. Reports its lifecycle, tagged with its role, to the collector on fourth,
   * identified by path.
   */
  class Echo(collectorPath: String, role: String) extends Actor {
    override def preStart(): Unit =
      context.actorSelection(collectorPath) ! (StartedPrefix + role)

    override def postStop(): Unit =
      context.actorSelection(collectorPath) ! (StoppedPrefix + role)

    def receive = {
      case "stop" => context.stop(self) // terminationMessage from the singleton manager
      case _      =>
    }
  }
}

class ClusterSingletonManagerDuplicateSingletonMultiJvmNode1 extends ClusterSingletonManagerDuplicateSingletonSpec
class ClusterSingletonManagerDuplicateSingletonMultiJvmNode2 extends ClusterSingletonManagerDuplicateSingletonSpec
class ClusterSingletonManagerDuplicateSingletonMultiJvmNode3 extends ClusterSingletonManagerDuplicateSingletonSpec
class ClusterSingletonManagerDuplicateSingletonMultiJvmNode4 extends ClusterSingletonManagerDuplicateSingletonSpec

/**
 * Checks that a clean hand-over chain never leaves the singleton running on two nodes.
 *
 * Nodes join in age order: first (oldest), second, third, fourth. The singleton is handed over
 * from first to second, then second to third, then third to fourth. After each hand-over the
 * singleton must run on exactly the new node.
 *
 * Each singleton instance reports Started and Stopped, with its own address, to a collector on
 * fourth, which is the youngest node and stays up for the whole test. The test queries the
 * collector's running set. Messages from different nodes are not ordered with respect to each
 * other, so a sample can briefly show both the old and the new holder during a hand-over. A
 * duplicate is therefore only reported when two consecutive samples, 200 ms apart, both show more
 * than one node running the singleton.
 */
class ClusterSingletonManagerDuplicateSingletonSpec
    extends MultiNodeSpec(ClusterSingletonManagerDuplicateSingletonSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import ClusterSingletonManagerDuplicateSingletonSpec._

  override def initialParticipants = roles.size

  lazy val cluster = Cluster(system)

  val collectorName = "singleton-collector"

  /** Full path of the collector on fourth, as seen from every node. */
  def collectorPath: String =
    (RootActorPath(node(fourth).address) / "user" / collectorName).toString

  /** Nodes that currently run the singleton, according to the collector. Call on fourth only. */
  def runningNodes(): Set[RoleName] = {
    system.actorSelection(RootActorPath(node(fourth).address) / "user" / collectorName) ! Query
    val running = expectMsgType[Running].roles
    roles.filter(r => running.contains(r.name)).toSet
  }

  /**
   * Waits until the singleton runs on exactly `to`, and returns the samples taken on the way.
   * Call on fourth only.
   */
  def awaitRunningOn(to: RoleName, timeout: FiniteDuration): Vector[Set[RoleName]] = {
    val deadline = Deadline.now + timeout
    var samples = Vector(runningNodes())
    while (samples.last != Set(to) && deadline.hasTimeLeft()) {
      Thread.sleep(200)
      samples :+= runningNodes()
    }
    samples
  }

  /** True if two consecutive samples both show more than one node running the singleton. */
  def hasPersistentDuplicate(samples: Vector[Set[RoleName]]): Boolean =
    samples.sliding(2).exists {
      case Seq(a, b) => a.size > 1 && b.size > 1
      case _         => false
    }

  def handOver(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.leave(node(from).address)
    }
    runOn(fourth) {
      val samples = awaitRunningOn(to, 15.seconds)
      withClue(s"samples during hand-over from [$from] to [$to]: [${samples.mkString(", ")}]") {
        hasPersistentDuplicate(samples) should be(false)
        samples.last should be(Set(to))
      }
    }
    enterBarrier(s"handed-over-from-${from.name}-to-${to.name}")
  }

  "ClusterSingletonManager" must {

    "never run the singleton on two nodes during a hand-over chain" in {
      // the collector must exist before any singleton instance starts
      runOn(fourth) {
        system.actorOf(Props[SingletonCollector](), collectorName)
      }
      enterBarrier("collector-ready")

      system.actorOf(
        ClusterSingletonManager.props(
          singletonProps = Props(classOf[Echo], collectorPath, myself.name),
          terminationMessage = "stop",
          settings = ClusterSingletonManagerSettings(system)),
        name = "echo")

      // join in age order, one node at a time, so that ages are first < second < third < fourth
      val joinOrder = List(first, second, third, fourth)
      joinOrder.zipWithIndex.foreach {
        case (r, i) =>
          runOn(r) {
            cluster.join(node(first).address)
          }
          runOn(joinOrder.take(i + 1): _*) {
            within(15.seconds) {
              awaitAssert(cluster.state.members.count(_.status == MemberStatus.Up) should be(i + 1))
            }
          }
          enterBarrier(s"up-${r.name}")
      }

      runOn(fourth) {
        awaitRunningOn(first, 10.seconds).last should be(Set(first))
      }
      enterBarrier("first-running")

      handOver(first, second)
      handOver(second, third)
      handOver(third, fourth)
    }
  }
}
