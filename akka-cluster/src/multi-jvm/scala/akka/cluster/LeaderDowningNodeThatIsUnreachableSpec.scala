/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

final case class LeaderDowningNodeThatIsUnreachableMultiNodeConfig(failureDetectorPuppet: Boolean)
    extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
      akka.cluster.testkit.auto-down-unreachable-after = 2s"""))
      .withFallback(MultiNodeClusterSpec.clusterConfig(failureDetectorPuppet)))
}

class LeaderDowningNodeThatIsUnreachableWithFailureDetectorPuppetMultiJvmNode1
    extends LeaderDowningNodeThatIsUnreachableSpec(failureDetectorPuppet = true)
class LeaderDowningNodeThatIsUnreachableWithFailureDetectorPuppetMultiJvmNode2
    extends LeaderDowningNodeThatIsUnreachableSpec(failureDetectorPuppet = true)
class LeaderDowningNodeThatIsUnreachableWithFailureDetectorPuppetMultiJvmNode3
    extends LeaderDowningNodeThatIsUnreachableSpec(failureDetectorPuppet = true)
class LeaderDowningNodeThatIsUnreachableWithFailureDetectorPuppetMultiJvmNode4
    extends LeaderDowningNodeThatIsUnreachableSpec(failureDetectorPuppet = true)

class LeaderDowningNodeThatIsUnreachableWithAccrualFailureDetectorMultiJvmNode1
    extends LeaderDowningNodeThatIsUnreachableSpec(failureDetectorPuppet = false)
class LeaderDowningNodeThatIsUnreachableWithAccrualFailureDetectorMultiJvmNode2
    extends LeaderDowningNodeThatIsUnreachableSpec(failureDetectorPuppet = false)
class LeaderDowningNodeThatIsUnreachableWithAccrualFailureDetectorMultiJvmNode3
    extends LeaderDowningNodeThatIsUnreachableSpec(failureDetectorPuppet = false)
class LeaderDowningNodeThatIsUnreachableWithAccrualFailureDetectorMultiJvmNode4
    extends LeaderDowningNodeThatIsUnreachableSpec(failureDetectorPuppet = false)

abstract class LeaderDowningNodeThatIsUnreachableSpec(
    multiNodeConfig: LeaderDowningNodeThatIsUnreachableMultiNodeConfig)
    extends MultiNodeClusterSpec(multiNodeConfig) {

  def this(failureDetectorPuppet: Boolean) =
    this(LeaderDowningNodeThatIsUnreachableMultiNodeConfig(failureDetectorPuppet))

  import multiNodeConfig._

  muteMarkingAsUnreachable()

  "The Leader in a 4 node cluster" must {

    "be able to DOWN a 'last' node that is UNREACHABLE" taggedAs LongRunningTest in {
      awaitClusterUp(first, second, third, fourth)

      val fourthAddress = address(fourth)

      enterBarrier("before-exit-fourth-node")
      runOn(first) {
        // kill 'fourth' node
        testConductor.exit(fourth, 0).await
        enterBarrier("down-fourth-node")

        // mark the node as unreachable in the failure detector
        markNodeAsUnavailable(fourthAddress)

        // --- HERE THE LEADER SHOULD DETECT FAILURE AND AUTO-DOWN THE UNREACHABLE NODE ---

        awaitMembersUp(numberOfMembers = 3, canNotBePartOfMemberRing = Set(fourthAddress), 30.seconds)
      }

      runOn(fourth) {
        enterBarrier("down-fourth-node")
      }

      runOn(second, third) {
        enterBarrier("down-fourth-node")

        awaitMembersUp(numberOfMembers = 3, canNotBePartOfMemberRing = Set(fourthAddress), 30.seconds)
      }

      enterBarrier("await-completion-1")
    }

    "be able to DOWN a 'middle' node that is UNREACHABLE" taggedAs LongRunningTest in {
      val secondAddress = address(second)

      enterBarrier("before-down-second-node")
      runOn(first) {
        // kill 'second' node
        testConductor.exit(second, 0).await
        enterBarrier("down-second-node")

        // mark the node as unreachable in the failure detector
        markNodeAsUnavailable(secondAddress)

        // --- HERE THE LEADER SHOULD DETECT FAILURE AND AUTO-DOWN THE UNREACHABLE NODE ---

        awaitMembersUp(numberOfMembers = 2, canNotBePartOfMemberRing = Set(secondAddress), 30.seconds)
      }

      runOn(second) {
        enterBarrier("down-second-node")
      }

      runOn(third) {
        enterBarrier("down-second-node")

        awaitMembersUp(numberOfMembers = 2, canNotBePartOfMemberRing = Set(secondAddress), 30 seconds)
      }

      enterBarrier("await-completion-2")
    }
  }
}
