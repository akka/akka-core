/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.collection.immutable
import scala.collection.immutable.SortedSet
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.annotation.InternalApi
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object MultiDcHeartbeatTakingOverSpecMultiJvmSpec extends MultiNodeConfig {
  val first = role("first") //   alpha
  val second = role("second") // alpha
  val third = role("third") //   alpha

  val fourth = role("fourth") // beta
  val fifth = role("fifth") //   beta

  nodeConfig(first, second, third)(ConfigFactory.parseString("""
    akka {
      cluster.multi-data-center.self-data-center = alpha
    }
    """))

  nodeConfig(fourth, fifth)(ConfigFactory.parseString("""
    akka {
      cluster.multi-data-center.self-data-center = beta
    }
    """))

  commonConfig(ConfigFactory.parseString("""
    akka {
      actor.provider = cluster

      loggers = ["akka.testkit.TestEventListener"]
      loglevel = INFO

      cluster {
        debug.verbose-heartbeat-logging = off

        multi-data-center {
          cross-data-center-connections = 2
        }
      }
    }
    """))

}

class MultiDcHeartbeatTakingOverSpecMultiJvmNode1 extends MultiDcHeartbeatTakingOverSpec
class MultiDcHeartbeatTakingOverSpecMultiJvmNode2 extends MultiDcHeartbeatTakingOverSpec
class MultiDcHeartbeatTakingOverSpecMultiJvmNode3 extends MultiDcHeartbeatTakingOverSpec
class MultiDcHeartbeatTakingOverSpecMultiJvmNode4 extends MultiDcHeartbeatTakingOverSpec
class MultiDcHeartbeatTakingOverSpecMultiJvmNode5 extends MultiDcHeartbeatTakingOverSpec

@nowarn("msg=Use Akka Distributed Cluster")
abstract class MultiDcHeartbeatTakingOverSpec extends MultiNodeClusterSpec(MultiDcHeartbeatTakingOverSpecMultiJvmSpec) {

  "A 2-dc cluster" must {

    val observer: TestProbe = TestProbe("alpha-observer")

    val crossDcHeartbeatSenderPath = "/system/cluster/core/daemon/crossDcHeartbeatSender"
    val selectCrossDcHeartbeatSender: ActorSelection = system.actorSelection(crossDcHeartbeatSenderPath)

    // these will be filled in during the initial phase of the test -----------
    var expectedAlphaHeartbeaterNodes: SortedSet[Member] = SortedSet.empty
    var expectedAlphaHeartbeaterRoles: List[RoleName] = List.empty

    var expectedBetaHeartbeaterNodes: SortedSet[Member] = SortedSet.empty
    var expectedBetaHeartbeaterRoles: List[RoleName] = List.empty

    var expectedNoActiveHeartbeatSenderRoles: Set[RoleName] = Set.empty
    // end of these will be filled in during the initial phase of the test -----------

    def refreshOldestMemberHeartbeatStatuses() = {
      expectedAlphaHeartbeaterNodes = takeNOldestMembers(dataCenter = "alpha", 2)
      expectedAlphaHeartbeaterRoles = membersAsRoles(expectedAlphaHeartbeaterNodes)

      expectedBetaHeartbeaterNodes = takeNOldestMembers(dataCenter = "beta", 2)
      expectedBetaHeartbeaterRoles = membersAsRoles(expectedBetaHeartbeaterNodes)

      expectedNoActiveHeartbeatSenderRoles = roles.toSet -- (expectedAlphaHeartbeaterRoles ++ expectedBetaHeartbeaterRoles)
    }

    "collect information on oldest nodes" taggedAs LongRunningTest in {
      // allow all nodes to join:
      awaitClusterUp(roles: _*)

      refreshOldestMemberHeartbeatStatuses()
      info(s"expectedAlphaHeartbeaterNodes = ${expectedAlphaHeartbeaterNodes.map(_.address.port.get)}")
      info(s"expectedBetaHeartbeaterNodes = ${expectedBetaHeartbeaterNodes.map(_.address.port.get)}")
      info(s"expectedNoActiveHeartbeatSenderRoles = ${expectedNoActiveHeartbeatSenderRoles.map(_.port.get)}")

      expectedAlphaHeartbeaterRoles.size should ===(2)
      expectedBetaHeartbeaterRoles.size should ===(2)

      enterBarrier("found-expectations")
    }

    "be healthy" taggedAs LongRunningTest in within(5.seconds) {
      implicit val sender: ActorRef = observer.ref
      runOn(expectedAlphaHeartbeaterRoles.toList: _*) {
        awaitAssert {
          selectCrossDcHeartbeatSender ! CrossDcHeartbeatSender.ReportStatus()
          observer.expectMsgType[CrossDcHeartbeatSender.MonitoringActive]
        }
      }
      runOn(expectedBetaHeartbeaterRoles.toList: _*) {
        awaitAssert {
          selectCrossDcHeartbeatSender ! CrossDcHeartbeatSender.ReportStatus()
          observer.expectMsgType[CrossDcHeartbeatSender.MonitoringActive]
        }
      }
      runOn(expectedNoActiveHeartbeatSenderRoles.toList: _*) {
        awaitAssert {
          selectCrossDcHeartbeatSender ! CrossDcHeartbeatSender.ReportStatus()
          observer.expectMsgType[CrossDcHeartbeatSender.MonitoringDormant]
        }
      }

      enterBarrier("sunny-weather-done")
    }

    "other node must become oldest when current DC-oldest Leaves" taggedAs LongRunningTest in {
      val observer = TestProbe("alpha-observer-prime")

      // we leave one of the current oldest nodes of the `alpha` DC,
      // since it has 3 members the "not yet oldest" one becomes oldest and should start monitoring across datacenter
      val preLeaveOldestAlphaRole = expectedAlphaHeartbeaterRoles.head
      val preLeaveOldestAlphaAddress =
        expectedAlphaHeartbeaterNodes.find(_.address.port.get == preLeaveOldestAlphaRole.port.get).get.address
      runOn(preLeaveOldestAlphaRole) {
        info(s"Leaving: ${preLeaveOldestAlphaAddress}")
        cluster.leave(cluster.selfAddress)
      }

      awaitMemberRemoved(preLeaveOldestAlphaAddress)
      enterBarrier("wat")

      // refresh our view about who is currently monitoring things in alpha:
      refreshOldestMemberHeartbeatStatuses()

      enterBarrier("after-alpha-monitoring-node-left")

      implicit val sender: ActorRef = observer.ref
      val expectedAlphaMonitoringNodesAfterLeaving =
        (takeNOldestMembers(dataCenter = "alpha", 3).filterNot(_.status == MemberStatus.Exiting))
      runOn(membersAsRoles(expectedAlphaMonitoringNodesAfterLeaving).toList: _*) {
        awaitAssert(
          {

            selectCrossDcHeartbeatSender ! CrossDcHeartbeatSender.ReportStatus()

            try {
              observer.expectMsgType[CrossDcHeartbeatSender.MonitoringActive](5.seconds)
              info(s"Got confirmation from ${observer.lastSender} that it is actively monitoring now")
            } catch {
              case ex: Throwable =>
                throw new AssertionError(
                  s"Monitoring was Dormant on ${cluster.selfAddress}, where we expected it to be active!",
                  ex)
            }
          },
          20.seconds)
      }
      enterBarrier("confirmed-heartbeating-take-over")
    }
  }

  /**
   * INTERNAL API
   * Returns `Up` (or in "later" status, like Leaving etc, but never `Joining` or `WeaklyUp`) members,
   * sorted by Member.ageOrdering (from oldest to youngest). This restriction on status is needed to
   * strongly guarantee the order of "oldest" members, as they're linearized by the order in which they become Up
   * (since marking that transition is a Leader action).
   */
  private def membersByAge(dataCenter: ClusterSettings.DataCenter): immutable.SortedSet[Member] =
    SortedSet
      .empty(Member.ageOrdering)
      .union(
        cluster.state.members.filter(m =>
          m.dataCenter == dataCenter &&
          m.status != MemberStatus.Joining && m.status != MemberStatus.WeaklyUp))

  /** INTERNAL API */
  @InternalApi
  private[cluster] def takeNOldestMembers(dataCenter: ClusterSettings.DataCenter, n: Int): immutable.SortedSet[Member] =
    membersByAge(dataCenter).take(n)

  private def membersAsRoles(ms: SortedSet[Member]): List[RoleName] = {
    val res = ms.toList.flatMap(m => roleName(m.address))
    require(res.size == ms.size, s"Not all members were converted to roles! Got: ${ms}, found ${res}")
    res
  }
}
