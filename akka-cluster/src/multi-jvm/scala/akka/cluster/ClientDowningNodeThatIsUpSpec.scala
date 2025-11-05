/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

final case class ClientDowningNodeThatIsUpMultiNodeConfig(failureDetectorPuppet: Boolean) extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfig(failureDetectorPuppet)))
}

class ClientDowningNodeThatIsUpWithFailureDetectorPuppetMultiJvmNode1
    extends ClientDowningNodeThatIsUpSpec(failureDetectorPuppet = true)
class ClientDowningNodeThatIsUpWithFailureDetectorPuppetMultiJvmNode2
    extends ClientDowningNodeThatIsUpSpec(failureDetectorPuppet = true)
class ClientDowningNodeThatIsUpWithFailureDetectorPuppetMultiJvmNode3
    extends ClientDowningNodeThatIsUpSpec(failureDetectorPuppet = true)
class ClientDowningNodeThatIsUpWithFailureDetectorPuppetMultiJvmNode4
    extends ClientDowningNodeThatIsUpSpec(failureDetectorPuppet = true)

class ClientDowningNodeThatIsUpWithAccrualFailureDetectorMultiJvmNode1
    extends ClientDowningNodeThatIsUpSpec(failureDetectorPuppet = false)
class ClientDowningNodeThatIsUpWithAccrualFailureDetectorMultiJvmNode2
    extends ClientDowningNodeThatIsUpSpec(failureDetectorPuppet = false)
class ClientDowningNodeThatIsUpWithAccrualFailureDetectorMultiJvmNode3
    extends ClientDowningNodeThatIsUpSpec(failureDetectorPuppet = false)
class ClientDowningNodeThatIsUpWithAccrualFailureDetectorMultiJvmNode4
    extends ClientDowningNodeThatIsUpSpec(failureDetectorPuppet = false)

abstract class ClientDowningNodeThatIsUpSpec(multiNodeConfig: ClientDowningNodeThatIsUpMultiNodeConfig)
    extends MultiNodeClusterSpec(multiNodeConfig) {

  def this(failureDetectorPuppet: Boolean) = this(ClientDowningNodeThatIsUpMultiNodeConfig(failureDetectorPuppet))

  import multiNodeConfig._

  "Client of a 4 node cluster" must {

    "be able to DOWN a node that is UP (healthy and available)" taggedAs LongRunningTest in {
      val thirdAddress = address(third)
      awaitClusterUp(first, second, third, fourth)

      runOn(first) {
        // mark 'third' node as DOWN
        cluster.down(thirdAddress)
        enterBarrier("down-third-node")

        markNodeAsUnavailable(thirdAddress)

        awaitMembersUp(numberOfMembers = 3, canNotBePartOfMemberRing = Set(thirdAddress))
        clusterView.members.exists(_.address == thirdAddress) should ===(false)
      }

      runOn(third) {
        enterBarrier("down-third-node")
      }

      runOn(second, fourth) {
        enterBarrier("down-third-node")

        awaitMembersUp(numberOfMembers = 3, canNotBePartOfMemberRing = Set(thirdAddress))
      }

      enterBarrier("await-completion")
    }
  }
}
