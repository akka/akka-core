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

object ClusterSingletonManagerConcurrentLeaveSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(
    ConfigFactory.parseString(
      """
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = off
    # Same margin as ClusterSingletonManagerStalePreviousOldestSpec, so a stale previous oldest
    # shows up as a hand-over that waits for this margin, longer than the wait below.
    akka.cluster.down-removal-margin = 30s
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

class ClusterSingletonManagerConcurrentLeaveMultiJvmNode1 extends ClusterSingletonManagerConcurrentLeaveSpec
class ClusterSingletonManagerConcurrentLeaveMultiJvmNode2 extends ClusterSingletonManagerConcurrentLeaveSpec
class ClusterSingletonManagerConcurrentLeaveMultiJvmNode3 extends ClusterSingletonManagerConcurrentLeaveSpec
class ClusterSingletonManagerConcurrentLeaveMultiJvmNode4 extends ClusterSingletonManagerConcurrentLeaveSpec

/**
 * Checks a hand-over when the two oldest nodes leave at about the same time.
 *
 * Nodes join in age order: first (oldest), second, third, fourth. First and second leave together.
 * Second becomes the oldest when first is exiting, but it is also exiting itself, so it must not take
 * over a singleton that it would lose when it exits. First is expected to hand over to third directly.
 * If second had already requested the hand-over when it started exiting it has to pass it on to third.
 * The singleton must end up on third, and never run on two nodes at once.
 *
 * Each singleton instance reports Started and Stopped, tagged with its role, to a collector on
 * fourth, which stays up for the whole test. Messages from different nodes are not ordered with
 * respect to each other, so a duplicate is only reported when two consecutive samples, 200 ms
 * apart, both show more than one node running the singleton.
 */
class ClusterSingletonManagerConcurrentLeaveSpec
    extends MultiNodeSpec(ClusterSingletonManagerConcurrentLeaveSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import ClusterSingletonManagerConcurrentLeaveSpec._

  override def initialParticipants = roles.size

  lazy val cluster = Cluster(system)

  val collectorName = "singleton-collector"

  /** Full path of the collector on fourth, as seen from every node. */
  def collectorPath: String =
    (RootActorPath(node(fourth).address) / "user" / collectorName).toString

  /** Roles that currently run the singleton, according to the collector. Call on fourth only. */
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

  "ClusterSingletonManager" must {

    "hand over past a leaving second-oldest when the two oldest leave together" in {
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

      // first and second leave together; the barrier keeps the two leave calls close in time
      runOn(first, second) {
        cluster.leave(node(myself).address)
      }
      enterBarrier("leaving-together")

      runOn(fourth) {
        val samples = awaitRunningOn(third, 10.seconds)
        withClue(s"samples during concurrent hand-over: [${samples.mkString(", ")}]") {
          hasPersistentDuplicate(samples) should be(false)
          samples.last should be(Set(third))
        }
      }
      enterBarrier("handed-over-to-third")
    }
  }
}
