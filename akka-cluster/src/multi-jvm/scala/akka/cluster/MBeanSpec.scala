/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.lang.management.ManagementFactory
import javax.management.InstanceNotFoundException
import javax.management.ObjectName

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object MBeanMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(debugConfig(on = false).withFallback(ConfigFactory.parseString("""
    akka.cluster.jmx.enabled = on
    akka.cluster.roles = [testNode]
    akka.cluster.app-version = "1.2.3"
    """)).withFallback(MultiNodeClusterSpec.clusterConfig))

}

class MBeanMultiJvmNode1 extends MBeanSpec
class MBeanMultiJvmNode2 extends MBeanSpec
class MBeanMultiJvmNode3 extends MBeanSpec
class MBeanMultiJvmNode4 extends MBeanSpec

abstract class MBeanSpec extends MultiNodeClusterSpec(MBeanMultiJvmSpec) {

  import MBeanMultiJvmSpec._

  val mbeanName = new ObjectName("akka:type=Cluster")
  lazy val mbeanServer = ManagementFactory.getPlatformMBeanServer

  "Cluster MBean" must {
    "expose attributes" taggedAs LongRunningTest in {
      val info = mbeanServer.getMBeanInfo(mbeanName)
      info.getAttributes.map(_.getName).toSet should ===(
        Set("ClusterStatus", "Members", "Unreachable", "MemberStatus", "Leader", "Singleton", "Available"))
      enterBarrier("after-1")
    }

    "expose operations" taggedAs LongRunningTest in {
      val info = mbeanServer.getMBeanInfo(mbeanName)
      info.getOperations.map(_.getName).toSet should ===(Set("join", "leave", "down"))
      enterBarrier("after-2")
    }

    "change attributes after startup" taggedAs LongRunningTest in {
      runOn(first) {
        mbeanServer.getAttribute(mbeanName, "Available").asInstanceOf[Boolean] should ===(false)
        mbeanServer.getAttribute(mbeanName, "Singleton").asInstanceOf[Boolean] should ===(false)
        mbeanServer.getAttribute(mbeanName, "Leader") should ===("")
        mbeanServer.getAttribute(mbeanName, "Members") should ===("")
        mbeanServer.getAttribute(mbeanName, "Unreachable") should ===("")
        mbeanServer.getAttribute(mbeanName, "MemberStatus") should ===("Removed")
      }
      awaitClusterUp(first)
      runOn(first) {
        awaitAssert(mbeanServer.getAttribute(mbeanName, "MemberStatus") should ===("Up"))
        awaitAssert(mbeanServer.getAttribute(mbeanName, "Leader") should ===(address(first).toString))
        mbeanServer.getAttribute(mbeanName, "Singleton").asInstanceOf[Boolean] should ===(true)
        mbeanServer.getAttribute(mbeanName, "Members") should ===(address(first).toString)
        mbeanServer.getAttribute(mbeanName, "Unreachable") should ===("")
        mbeanServer.getAttribute(mbeanName, "Available").asInstanceOf[Boolean] should ===(true)
      }
      enterBarrier("after-3")
    }

    "support join" taggedAs LongRunningTest in {
      runOn(second, third, fourth) {
        mbeanServer.invoke(mbeanName, "join", Array(address(first).toString), Array("java.lang.String"))
      }
      enterBarrier("joined")

      awaitMembersUp(4)
      assertMembers(clusterView.members, roles.map(address(_)): _*)
      awaitAssert(mbeanServer.getAttribute(mbeanName, "MemberStatus") should ===("Up"))
      val expectedMembers = roles.sorted.map(address(_)).mkString(",")
      awaitAssert(mbeanServer.getAttribute(mbeanName, "Members") should ===(expectedMembers))
      val expectedLeader = address(roleOfLeader())
      awaitAssert(mbeanServer.getAttribute(mbeanName, "Leader") should ===(expectedLeader.toString))
      mbeanServer.getAttribute(mbeanName, "Singleton").asInstanceOf[Boolean] should ===(false)

      enterBarrier("after-4")
    }

    val fourthAddress = address(fourth)

    "format cluster status as JSON with full reachability info" taggedAs LongRunningTest in within(30 seconds) {
      runOn(first) {
        testConductor.exit(fourth, 0).await
      }
      enterBarrier("fourth-shutdown")

      runOn(first, second, third) {
        awaitAssert(mbeanServer.getAttribute(mbeanName, "Unreachable") should ===(fourthAddress.toString))
        val expectedMembers = Seq(first, second, third, fourth).sorted.map(address(_)).mkString(",")
        awaitAssert(mbeanServer.getAttribute(mbeanName, "Members") should ===(expectedMembers))
      }
      enterBarrier("fourth-unreachable")

      runOn(first) {
        val sortedNodes = Vector(first, second, third, fourth).sorted.map(address(_))
        val unreachableObservedBy = Vector(first, second, third).sorted.map(address(_))
        val expectedJson =
          s"""{
             |  "members": [
             |    {
             |      "address": "${sortedNodes(0)}",
             |      "roles": [
             |        "dc-default",
             |        "testNode"
             |      ],
             |      "status": "Up",
             |      "app-version": "1.2.3"
             |    },
             |    {
             |      "address": "${sortedNodes(1)}",
             |      "roles": [
             |        "dc-default",
             |        "testNode"
             |      ],
             |      "status": "Up",
             |      "app-version": "1.2.3"
             |    },
             |    {
             |      "address": "${sortedNodes(2)}",
             |      "roles": [
             |        "dc-default",
             |        "testNode"
             |      ],
             |      "status": "Up",
             |      "app-version": "1.2.3"
             |    },
             |    {
             |      "address": "${sortedNodes(3)}",
             |      "roles": [
             |        "dc-default",
             |        "testNode"
             |      ],
             |      "status": "Up",
             |      "app-version": "1.2.3"
             |    }
             |  ],
             |  "self-address": "${address(first)}",
             |  "unreachable": [
             |    {
             |      "node": "${address(fourth)}",
             |      "observed-by": [
             |        "${unreachableObservedBy(0)}",
             |        "${unreachableObservedBy(1)}",
             |        "${unreachableObservedBy(2)}"
             |      ]
             |    }
             |  ]
             |}
             |""".stripMargin

        // awaitAssert to make sure that all nodes detects unreachable
        within(15.seconds) {
          awaitAssert(mbeanServer.getAttribute(mbeanName, "ClusterStatus") should ===(expectedJson))
        }
      }

      enterBarrier("after-5")

    }

    "support down" taggedAs LongRunningTest in within(20 seconds) {

      // fourth unreachable in previous step

      runOn(second) {
        mbeanServer.invoke(mbeanName, "down", Array(fourthAddress.toString), Array("java.lang.String"))
      }
      enterBarrier("fourth-down")

      runOn(first, second, third) {
        awaitMembersUp(3, canNotBePartOfMemberRing = Set(fourthAddress))
        assertMembers(clusterView.members, first, second, third)
        awaitAssert(mbeanServer.getAttribute(mbeanName, "Unreachable") should ===(""))
      }

      enterBarrier("after-6")
    }

    "support leave" taggedAs LongRunningTest in within(20 seconds) {
      runOn(second) {
        mbeanServer.invoke(mbeanName, "leave", Array(address(third).toString), Array("java.lang.String"))
      }
      enterBarrier("third-left")
      runOn(first, second) {
        awaitMembersUp(2)
        assertMembers(clusterView.members, first, second)
        val expectedMembers = Seq(first, second).sorted.map(address(_)).mkString(",")
        awaitAssert(mbeanServer.getAttribute(mbeanName, "Members") should ===(expectedMembers))
      }
      runOn(third) {
        awaitCond(cluster.isTerminated)
        // mbean should be unregistered, i.e. throw InstanceNotFoundException
        awaitAssert(intercept[InstanceNotFoundException] {
          mbeanServer.getMBeanInfo(mbeanName)
        })
      }

      enterBarrier("after-7")
    }

  }
}
