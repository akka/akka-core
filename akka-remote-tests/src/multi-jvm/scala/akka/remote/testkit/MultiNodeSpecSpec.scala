/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.testkit

import akka.remote.RemotingMultiNodeSpec
import akka.testkit.LongRunningTest

object MultiNodeSpecMultiJvmSpec extends MultiNodeConfig {
  commonConfig(debugConfig(on = false).withFallback(RemotingMultiNodeSpec.commonConfig))

  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
}

class MultiNodeSpecSpecMultiJvmNode1 extends MultiNodeSpecSpec
class MultiNodeSpecSpecMultiJvmNode2 extends MultiNodeSpecSpec
class MultiNodeSpecSpecMultiJvmNode3 extends MultiNodeSpecSpec
class MultiNodeSpecSpecMultiJvmNode4 extends MultiNodeSpecSpec

class MultiNodeSpecSpec extends RemotingMultiNodeSpec(MultiNodeSpecMultiJvmSpec) {

  def initialParticipants = 4

  "A MultiNodeSpec" must {

    "wait for all nodes to remove themselves before we shut the conductor down" taggedAs LongRunningTest in {
      enterBarrier("startup")
      // this test is empty here since it only exercises the shutdown code in the MultiNodeSpec
    }

  }
}
