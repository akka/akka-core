/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
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
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._

object ClusterSingletonManagerDownedSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    """))

  testTransport(on = true)

  case object EchoStarted
  case object EchoStopped

  /**
   * The singleton actor
   */
  class Echo(testActor: ActorRef) extends Actor {
    testActor ! EchoStarted

    override def postStop(): Unit = {
      testActor ! EchoStopped
    }

    def receive = {
      case _ => sender() ! self
    }
  }
}

class ClusterSingletonManagerDownedMultiJvmNode1 extends ClusterSingletonManagerDownedSpec
class ClusterSingletonManagerDownedMultiJvmNode2 extends ClusterSingletonManagerDownedSpec
class ClusterSingletonManagerDownedMultiJvmNode3 extends ClusterSingletonManagerDownedSpec

class ClusterSingletonManagerDownedSpec
    extends MultiNodeSpec(ClusterSingletonManagerDownedSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import ClusterSingletonManagerDownedSpec._

  override def initialParticipants = roles.size

  private val cluster = Cluster(system)

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
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system)),
      name = "echo")
  }

  "A ClusterSingletonManager downing" must {

    "startup 3 node" in {
      join(first, first)
      join(second, first)
      join(third, first)
      within(15.seconds) {
        awaitAssert {
          cluster.state.members.size should ===(3)
          cluster.state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
        }
      }
      runOn(first) {
        expectMsg(EchoStarted)
      }
      enterBarrier("started")
    }

    "stop instance when member is downed" in {
      runOn(first) {
        testConductor.blackhole(first, third, Direction.Both).await
        testConductor.blackhole(second, third, Direction.Both).await

        within(15.seconds) {
          awaitAssert {
            cluster.state.unreachable.size should ===(1)
          }
        }
      }
      enterBarrier("blackhole-1")
      runOn(first) {
        // another blackhole so that second can't mark gossip as seen and thereby deferring shutdown of first
        testConductor.blackhole(first, second, Direction.Both).await
        cluster.down(node(second).address)
        cluster.down(cluster.selfAddress)
        // singleton instance stopped, before failure detection of first-second
        expectMsg(3.seconds, EchoStopped)
      }

      enterBarrier("stopped")
    }
  }
}
