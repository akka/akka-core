/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.Promise

import akka.remote.testkit.MultiNodeConfig
import akka.util.Version

object AppVersionMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))
}

class AppVersionMultiJvmNode1 extends AppVersionSpec
class AppVersionMultiJvmNode2 extends AppVersionSpec

abstract class AppVersionSpec extends MultiNodeClusterSpec(AppVersionMultiJvmSpec) {

  import AppVersionMultiJvmSpec._

  "Later appVersion" must {
    "be used when joining" in {
      val laterVersion = Promise[Version]()
      cluster.setAppVersionLater(laterVersion.future)
      // ok to try to join immediately
      runOn(first) {
        cluster.join(first)
        // not joining until laterVersion has been completed
        Thread.sleep(100)
        cluster.selfMember.status should ===(MemberStatus.Removed)
        laterVersion.trySuccess(Version("2"))
        awaitAssert {
          cluster.selfMember.status should ===(MemberStatus.Up)
          cluster.selfMember.appVersion should ===(Version("2"))
        }
      }
      enterBarrier("first-joined")

      runOn(second) {
        cluster.joinSeedNodes(List(address(first), address(second)))
        // not joining until laterVersion has been completed
        Thread.sleep(100)
        cluster.selfMember.status should ===(MemberStatus.Removed)
        laterVersion.trySuccess(Version("3"))
        awaitAssert {
          cluster.selfMember.status should ===(MemberStatus.Up)
          cluster.selfMember.appVersion should ===(Version("3"))
        }
      }
      enterBarrier("second-joined")

      cluster.state.members.find(_.address == address(first)).get.appVersion should ===(Version("2"))
      cluster.state.members.find(_.address == address(second)).get.appVersion should ===(Version("3"))

      enterBarrier("after-1")
    }
  }
}
