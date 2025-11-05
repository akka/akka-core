/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.actor.Address
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

final case class ConvergenceMultiNodeConfig(failureDetectorPuppet: Boolean) extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(
    debugConfig(on = false).withFallback(ConfigFactory.parseString("""
      akka.cluster.failure-detector.threshold = 4
      akka.cluster.allow-weakly-up-members = off
      """)).withFallback(MultiNodeClusterSpec.clusterConfig(failureDetectorPuppet)))
}

class ConvergenceWithFailureDetectorPuppetMultiJvmNode1 extends ConvergenceSpec(failureDetectorPuppet = true)
class ConvergenceWithFailureDetectorPuppetMultiJvmNode2 extends ConvergenceSpec(failureDetectorPuppet = true)
class ConvergenceWithFailureDetectorPuppetMultiJvmNode3 extends ConvergenceSpec(failureDetectorPuppet = true)
class ConvergenceWithFailureDetectorPuppetMultiJvmNode4 extends ConvergenceSpec(failureDetectorPuppet = true)

class ConvergenceWithAccrualFailureDetectorMultiJvmNode1 extends ConvergenceSpec(failureDetectorPuppet = false)
class ConvergenceWithAccrualFailureDetectorMultiJvmNode2 extends ConvergenceSpec(failureDetectorPuppet = false)
class ConvergenceWithAccrualFailureDetectorMultiJvmNode3 extends ConvergenceSpec(failureDetectorPuppet = false)
class ConvergenceWithAccrualFailureDetectorMultiJvmNode4 extends ConvergenceSpec(failureDetectorPuppet = false)

abstract class ConvergenceSpec(multiNodeConfig: ConvergenceMultiNodeConfig)
    extends MultiNodeClusterSpec(multiNodeConfig) {

  def this(failureDetectorPuppet: Boolean) = this(ConvergenceMultiNodeConfig(failureDetectorPuppet))

  import multiNodeConfig._

  muteMarkingAsUnreachable()

  "A cluster of 3 members" must {

    "reach initial convergence" taggedAs LongRunningTest in {
      awaitClusterUp(first, second, third)

      runOn(fourth) {
        // doesn't join immediately
      }

      enterBarrier("after-1")
    }

    "not reach convergence while any nodes are unreachable" taggedAs LongRunningTest in {
      val thirdAddress = address(third)
      enterBarrier("before-shutdown")

      runOn(first) {
        // kill 'third' node
        testConductor.exit(third, 0).await
        markNodeAsUnavailable(thirdAddress)
      }

      runOn(first, second) {

        within(28 seconds) {
          // third becomes unreachable
          awaitAssert(clusterView.unreachableMembers.size should ===(1))
          awaitSeenSameState(first, second)
          // still one unreachable
          clusterView.unreachableMembers.size should ===(1)
          clusterView.unreachableMembers.head.address should ===(thirdAddress)
          clusterView.members.size should ===(3)

        }
      }

      enterBarrier("after-2")
    }

    "not move a new joining node to Up while there is no convergence" taggedAs LongRunningTest in {
      runOn(fourth) {
        // try to join
        cluster.join(first)
      }

      def memberStatus(address: Address): Option[MemberStatus] =
        clusterView.members.collectFirst { case m if m.address == address => m.status }

      enterBarrier("after-join")

      runOn(first, second, fourth) {
        for (_ <- 1 to 5) {
          awaitAssert(clusterView.members.size should ===(4))
          awaitSeenSameState(first, second, fourth)
          memberStatus(first) should ===(Some(MemberStatus.Up))
          memberStatus(second) should ===(Some(MemberStatus.Up))
          memberStatus(fourth) should ===(Some(MemberStatus.Joining))
          // wait and then check again
          Thread.sleep(1.second.dilated.toMillis)
        }
      }

      enterBarrier("after-3")
    }
  }
}
