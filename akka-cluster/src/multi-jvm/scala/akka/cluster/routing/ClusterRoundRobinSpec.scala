/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.routing

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.Props
import akka.actor.Terminated
import akka.cluster.MultiNodeClusterSpec
import akka.pattern.ask
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.routing.ActorRefRoutee
import akka.routing.ActorSelectionRoutee
import akka.routing.FromConfig
import akka.routing.GetRoutees
import akka.routing.RoundRobinPool
import akka.routing.RoutedActorRef
import akka.routing.Routees
import akka.testkit._

object ClusterRoundRobinMultiJvmSpec extends MultiNodeConfig {

  class SomeActor(routeeType: RouteeType) extends Actor {
    def this() = this(PoolRoutee)

    def receive = {
      case "hit" => sender() ! Reply(routeeType, self)
    }
  }

  final case class Reply(routeeType: RouteeType, ref: ActorRef)

  sealed trait RouteeType extends Serializable
  object PoolRoutee extends RouteeType
  object GroupRoutee extends RouteeType

  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(debugConfig(on = false).withFallback(ConfigFactory.parseString(s"""
      akka.actor {
        serialization-bindings {
          "akka.cluster.routing.ClusterRoundRobinMultiJvmSpec$$Reply" = java-test
        }

        deployment {
          /router1 {
            router = round-robin-pool
            cluster {
              enabled = on
              max-nr-of-instances-per-node = 2
              max-total-nr-of-instances = 10
            }
          }
          /router3 {
            router = round-robin-pool
            cluster {
              enabled = on
              max-nr-of-instances-per-node = 1
              max-total-nr-of-instances = 10
              allow-local-routees = off
            }
          }
          /router4 {
            router = round-robin-group
            routees.paths = ["/user/myserviceA", "/user/myserviceB"]
            cluster.enabled = on
            cluster.max-total-nr-of-instances = 10
          }
          /router5 {
            router = round-robin-pool
            cluster {
              enabled = on
              use-roles = ["a"]
              max-total-nr-of-instances = 10
            }
          }
        }
      }
      """)).withFallback(MultiNodeClusterSpec.clusterConfig))

  nodeConfig(first, second)(ConfigFactory.parseString("""akka.cluster.roles =["a", "c"]"""))
  nodeConfig(third)(ConfigFactory.parseString("""akka.cluster.roles =["b", "c"]"""))

  testTransport(on = true)

}

class ClusterRoundRobinMultiJvmNode1 extends ClusterRoundRobinSpec
class ClusterRoundRobinMultiJvmNode2 extends ClusterRoundRobinSpec
class ClusterRoundRobinMultiJvmNode3 extends ClusterRoundRobinSpec
class ClusterRoundRobinMultiJvmNode4 extends ClusterRoundRobinSpec

abstract class ClusterRoundRobinSpec
    extends MultiNodeClusterSpec(ClusterRoundRobinMultiJvmSpec)
    with ImplicitSender
    with DefaultTimeout {
  import ClusterRoundRobinMultiJvmSpec._

  lazy val router1 = system.actorOf(FromConfig.props(Props[SomeActor]()), "router1")
  lazy val router2 = system.actorOf(
    ClusterRouterPool(
      RoundRobinPool(nrOfInstances = 0),
      ClusterRouterPoolSettings(totalInstances = 3, maxInstancesPerNode = 1, allowLocalRoutees = true))
      .props(Props[SomeActor]()),
    "router2")
  lazy val router3 = system.actorOf(FromConfig.props(Props[SomeActor]()), "router3")
  lazy val router4 = system.actorOf(FromConfig.props(), "router4")
  lazy val router5 = system.actorOf(RoundRobinPool(nrOfInstances = 0).props(Props[SomeActor]()), "router5")

  def receiveReplies(routeeType: RouteeType, expectedReplies: Int): Map[Address, Int] = {
    val zero = Map.empty[Address, Int] ++ roles.map(address(_) -> 0)
    receiveWhile(5 seconds, messages = expectedReplies) {
      case Reply(`routeeType`, ref) => fullAddress(ref)
    }.foldLeft(zero) {
      case (replyMap, address) => replyMap + (address -> (replyMap(address) + 1))
    }
  }

  /**
   * Fills in self address for local ActorRef
   */
  private def fullAddress(actorRef: ActorRef): Address = actorRef.path.address match {
    case Address(_, _, None, None) => cluster.selfAddress
    case fa                        => fa
  }

  def currentRoutees(router: ActorRef) =
    Await.result(router ? GetRoutees, timeout.duration).asInstanceOf[Routees].routees

  "A cluster router with a RoundRobin router" must {
    "start cluster with 2 nodes" in {
      awaitClusterUp(first, second)
      enterBarrier("after-1")
    }

    "deploy routees to the member nodes in the cluster" in {

      runOn(first) {
        router1.isInstanceOf[RoutedActorRef] should ===(true)

        // max-nr-of-instances-per-node=2 times 2 nodes
        awaitAssert(currentRoutees(router1).size should ===(4))

        val iterationCount = 10
        for (_ <- 0 until iterationCount) {
          router1 ! "hit"
        }

        val replies = receiveReplies(PoolRoutee, iterationCount)

        replies(first) should be > 0
        replies(second) should be > 0
        replies(third) should ===(0)
        replies(fourth) should ===(0)
        replies.values.sum should ===(iterationCount)
      }

      enterBarrier("after-2")
    }

    "lookup routees on the member nodes in the cluster" in {

      // cluster consists of first and second

      system.actorOf(Props(classOf[SomeActor], GroupRoutee), "myserviceA")
      system.actorOf(Props(classOf[SomeActor], GroupRoutee), "myserviceB")
      enterBarrier("myservice-started")

      runOn(first) {
        // 2 nodes, 2 routees on each node
        within(10.seconds) {
          awaitAssert(currentRoutees(router4).size should ===(4))
        }

        val iterationCount = 10
        for (_ <- 0 until iterationCount) {
          router4 ! "hit"
        }

        val replies = receiveReplies(GroupRoutee, iterationCount)

        replies(first) should be > 0
        replies(second) should be > 0
        replies(third) should ===(0)
        replies(fourth) should ===(0)
        replies.values.sum should ===(iterationCount)
      }

      enterBarrier("after-3")
    }

    "deploy routees to new nodes in the cluster" in {

      // add third and fourth
      awaitClusterUp(first, second, third, fourth)

      runOn(first) {
        // max-nr-of-instances-per-node=2 times 4 nodes
        awaitAssert(currentRoutees(router1).size should ===(8))

        val iterationCount = 10
        for (_ <- 0 until iterationCount) {
          router1 ! "hit"
        }

        val replies = receiveReplies(PoolRoutee, iterationCount)

        replies.values.foreach { _ should be > 0 }
        replies.values.sum should ===(iterationCount)
      }

      enterBarrier("after-4")
    }

    "lookup routees on new nodes in the cluster" in {

      // cluster consists of first, second, third and fourth

      runOn(first) {
        // 4 nodes, 2 routee on each node
        awaitAssert(currentRoutees(router4).size should ===(8))

        val iterationCount = 10
        for (_ <- 0 until iterationCount) {
          router4 ! "hit"
        }

        val replies = receiveReplies(GroupRoutee, iterationCount)

        replies.values.foreach { _ should be > 0 }
        replies.values.sum should ===(iterationCount)
      }

      enterBarrier("after-5")
    }

    "deploy routees to only remote nodes when allow-local-routees = off" in {

      runOn(first) {
        // max-nr-of-instances-per-node=1 times 3 nodes
        awaitAssert(currentRoutees(router3).size should ===(3))

        val iterationCount = 10
        for (_ <- 0 until iterationCount) {
          router3 ! "hit"
        }

        val replies = receiveReplies(PoolRoutee, iterationCount)

        replies(first) should ===(0)
        replies(second) should be > 0
        replies(third) should be > 0
        replies(fourth) should be > 0
        replies.values.sum should ===(iterationCount)
      }

      enterBarrier("after-6")
    }

    "deploy routees to specified node role" in {

      runOn(first) {
        awaitAssert(currentRoutees(router5).size should ===(2))

        val iterationCount = 10
        for (_ <- 0 until iterationCount) {
          router5 ! "hit"
        }

        val replies = receiveReplies(PoolRoutee, iterationCount)

        replies(first) should be > 0
        replies(second) should be > 0
        replies(third) should ===(0)
        replies(fourth) should ===(0)
        replies.values.sum should ===(iterationCount)
      }

      enterBarrier("after-7")
    }

    "deploy programatically defined routees to the member nodes in the cluster" in {

      runOn(first) {
        router2.isInstanceOf[RoutedActorRef] should ===(true)

        // totalInstances = 3, maxInstancesPerNode = 1
        awaitAssert(currentRoutees(router2).size should ===(3))

        val iterationCount = 10
        for (_ <- 0 until iterationCount) {
          router2 ! "hit"
        }

        val replies = receiveReplies(PoolRoutee, iterationCount)

        // note that router2 has totalInstances = 3, maxInstancesPerNode = 1
        val routees = currentRoutees(router2)
        val routeeAddresses = routees.collect { case ActorRefRoutee(ref) => fullAddress(ref) }

        routeeAddresses.size should ===(3)
        replies.values.sum should ===(iterationCount)
      }

      enterBarrier("after-8")
    }

    "remove routees for unreachable nodes, and add when reachable again" in within(30.seconds) {

      // myservice is already running

      def routees = currentRoutees(router4)
      def routeeAddresses = routees.collect { case ActorSelectionRoutee(sel) => fullAddress(sel.anchor) }.toSet

      runOn(first) {
        // 4 nodes, 2 routees on each node
        awaitAssert(currentRoutees(router4).size should ===(8))

        testConductor.blackhole(first, second, Direction.Both).await

        awaitAssert(routees.size should ===(6))
        routeeAddresses should not contain address(second)

        testConductor.passThrough(first, second, Direction.Both).await
        awaitAssert(routees.size should ===(8))
        routeeAddresses should contain(address(second))

      }

      enterBarrier("after-9")
    }

    "deploy programatically defined routees to other node when a node becomes down" in {
      muteMarkingAsUnreachable()

      runOn(first) {
        def routees = currentRoutees(router2)
        def routeeAddresses = routees.collect { case ActorRefRoutee(ref) => fullAddress(ref) }.toSet

        routees.foreach {
          case ActorRefRoutee(ref) => watch(ref)
          case _                   =>
        }
        val notUsedAddress = roles.map(address).toSet.diff(routeeAddresses).head
        val downAddress = routeeAddresses.find(_ != address(first)).get
        val downRouteeRef = routees.collectFirst {
          case ActorRefRoutee(ref) if ref.path.address == downAddress => ref
        }.get

        cluster.down(downAddress)
        expectMsgType[Terminated](15.seconds).actor should ===(downRouteeRef)
        awaitAssert {
          routeeAddresses should contain(notUsedAddress)
          routeeAddresses should not contain downAddress
        }

        val iterationCount = 10
        for (_ <- 0 until iterationCount) {
          router2 ! "hit"
        }

        val replies = receiveReplies(PoolRoutee, iterationCount)

        routeeAddresses.size should ===(3)
        replies.values.sum should ===(iterationCount)
      }

      enterBarrier("after-10")
    }

  }
}
