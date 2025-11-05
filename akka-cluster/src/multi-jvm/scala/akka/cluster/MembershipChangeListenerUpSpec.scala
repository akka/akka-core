/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import akka.actor.Actor
import akka.actor.Deploy
import akka.actor.Props
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object MembershipChangeListenerUpMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))
}

class MembershipChangeListenerUpMultiJvmNode1 extends MembershipChangeListenerUpSpec
class MembershipChangeListenerUpMultiJvmNode2 extends MembershipChangeListenerUpSpec
class MembershipChangeListenerUpMultiJvmNode3 extends MembershipChangeListenerUpSpec

abstract class MembershipChangeListenerUpSpec extends MultiNodeClusterSpec(MembershipChangeListenerUpMultiJvmSpec) {

  import ClusterEvent._
  import MembershipChangeListenerUpMultiJvmSpec._

  "A set of connected cluster systems" must {

    "(when two nodes) after cluster convergence updates the membership table then all MembershipChangeListeners should be triggered" taggedAs LongRunningTest in {

      awaitClusterUp(first)

      runOn(first, second) {
        val latch = TestLatch()
        val expectedAddresses = Set(first, second).map(address)
        cluster.subscribe(system.actorOf(Props(new Actor {
          var members = Set.empty[Member]
          def receive = {
            case state: CurrentClusterState => members = state.members
            case MemberUp(m) =>
              members = members - m + m
              if (members.map(_.address) == expectedAddresses)
                latch.countDown()
            case _ => // ignore
          }
        }).withDeploy(Deploy.local)), classOf[MemberEvent])
        enterBarrier("listener-1-registered")
        cluster.join(first)
        latch.await
      }

      runOn(third) {
        enterBarrier("listener-1-registered")
      }

      enterBarrier("after-1")
    }

    "(when three nodes) after cluster convergence updates the membership table then all MembershipChangeListeners should be triggered" taggedAs LongRunningTest in {

      val latch = TestLatch()
      val expectedAddresses = Set(first, second, third).map(address)
      cluster.subscribe(system.actorOf(Props(new Actor {
        var members = Set.empty[Member]
        def receive = {
          case state: CurrentClusterState => members = state.members
          case MemberUp(m) =>
            members = members - m + m
            if (members.map(_.address) == expectedAddresses)
              latch.countDown()
          case _ => // ignore
        }
      }).withDeploy(Deploy.local)), classOf[MemberEvent])
      enterBarrier("listener-2-registered")

      runOn(third) {
        cluster.join(first)
      }

      latch.await

      enterBarrier("after-2")
    }
  }
}
