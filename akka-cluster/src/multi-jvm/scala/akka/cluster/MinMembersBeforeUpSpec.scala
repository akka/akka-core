/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import com.typesafe.config.ConfigFactory

import akka.cluster.MemberStatus._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object MinMembersBeforeUpMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("akka.cluster.min-nr-of-members = 3"))
      .withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))
}

object MinMembersBeforeUpWithWeaklyUpMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.cluster.min-nr-of-members = 3
      akka.cluster.allow-weakly-up-members = 3 s"""))
      .withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))
}

object MinMembersOfRoleBeforeUpMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("akka.cluster.role.backend.min-nr-of-members = 2"))
      .withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))

  nodeConfig(first)(ConfigFactory.parseString("akka.cluster.roles =[frontend]"))

  nodeConfig(second, third)(ConfigFactory.parseString("akka.cluster.roles =[backend]"))
}

class MinMembersBeforeUpMultiJvmNode1 extends MinMembersBeforeUpSpec
class MinMembersBeforeUpMultiJvmNode2 extends MinMembersBeforeUpSpec
class MinMembersBeforeUpMultiJvmNode3 extends MinMembersBeforeUpSpec

class MinMembersBeforeUpWithWeaklyUpMultiJvmNode1 extends MinMembersBeforeUpSpec
class MinMembersBeforeUpWithWeaklyUpMultiJvmNode2 extends MinMembersBeforeUpSpec
class MinMembersBeforeUpWithWeaklyUpMultiJvmNode3 extends MinMembersBeforeUpSpec

class MinMembersOfRoleBeforeUpMultiJvmNode1 extends MinMembersOfRoleBeforeUpSpec
class MinMembersOfRoleBeforeUpMultiJvmNode2 extends MinMembersOfRoleBeforeUpSpec
class MinMembersOfRoleBeforeUpMultiJvmNode3 extends MinMembersOfRoleBeforeUpSpec

abstract class MinMembersBeforeUpSpec extends MinMembersBeforeUpBase(MinMembersBeforeUpMultiJvmSpec) {

  override def first: RoleName = MinMembersBeforeUpMultiJvmSpec.first
  override def second: RoleName = MinMembersBeforeUpMultiJvmSpec.second
  override def third: RoleName = MinMembersBeforeUpMultiJvmSpec.third

  "Cluster leader" must {
    "wait with moving members to UP until minimum number of members have joined" taggedAs LongRunningTest in {
      testWaitMovingMembersToUp()
    }
  }
}

abstract class MinMembersBeforeUpWithWeaklyUpSpec extends MinMembersBeforeUpBase(MinMembersBeforeUpMultiJvmSpec) {

  override def first: RoleName = MinMembersBeforeUpWithWeaklyUpMultiJvmSpec.first
  override def second: RoleName = MinMembersBeforeUpWithWeaklyUpMultiJvmSpec.second
  override def third: RoleName = MinMembersBeforeUpWithWeaklyUpMultiJvmSpec.third

  "Cluster leader" must {
    "wait with moving members to UP until minimum number of members have joined with weakly up enabled" taggedAs LongRunningTest in {
      testWaitMovingMembersToUp()
    }
  }
}

abstract class MinMembersOfRoleBeforeUpSpec extends MinMembersBeforeUpBase(MinMembersOfRoleBeforeUpMultiJvmSpec) {

  override def first: RoleName = MinMembersOfRoleBeforeUpMultiJvmSpec.first
  override def second: RoleName = MinMembersOfRoleBeforeUpMultiJvmSpec.second
  override def third: RoleName = MinMembersOfRoleBeforeUpMultiJvmSpec.third

  "Cluster leader" must {
    "wait with moving members to UP until minimum number of members with specific role have joined" taggedAs LongRunningTest in {
      testWaitMovingMembersToUp()
    }
  }
}

abstract class MinMembersBeforeUpBase(multiNodeConfig: MultiNodeConfig) extends MultiNodeClusterSpec(multiNodeConfig) {

  def first: RoleName
  def second: RoleName
  def third: RoleName

  def testWaitMovingMembersToUp(): Unit = {
    val onUpLatch = TestLatch(1)
    cluster.registerOnMemberUp(onUpLatch.countDown())

    runOn(first) {
      cluster.join(myself)
      awaitAssert {
        clusterView.status should ===(Joining)
      }
    }
    enterBarrier("first-started")

    onUpLatch.isOpen should ===(false)

    runOn(second) {
      cluster.join(first)
    }
    runOn(first, second) {
      val expectedAddresses = Set(first, second).map(address)
      awaitAssert {
        clusterView.members.map(_.address) should ===(expectedAddresses)
      }
      clusterView.members.unsorted.map(_.status) should ===(Set(Joining))
      // and it should not change
      (1 to 5).foreach { _ =>
        Thread.sleep(1000)
        clusterView.members.map(_.address) should ===(expectedAddresses)
        clusterView.members.unsorted.map(_.status) should ===(Set(Joining))
      }
    }
    enterBarrier("second-joined")

    runOn(third) {
      cluster.join(first)
    }
    awaitClusterUp(first, second, third)

    onUpLatch.await

    enterBarrier("after-1")
  }

}
