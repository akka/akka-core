/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.cluster.ClusterEvent._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.testkit.TestProbe

object MultiDcSplitBrainMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  commonConfig(ConfigFactory.parseString("""
      akka.loglevel = DEBUG # issue #24955
      akka.cluster.debug.verbose-heartbeat-logging = on
      akka.cluster.debug.verbose-gossip-logging = on
      akka.cluster.multi-data-center {
        failure-detector {
          acceptable-heartbeat-pause = 4s
          heartbeat-interval = 1s
        }
      }
      akka.cluster {
        gossip-interval                     = 500ms
        leader-actions-interval             = 1s
        downing-provider-class = akka.cluster.testkit.AutoDowning
        testkit.auto-down-unreachable-after = 1s
      }
    """).withFallback(MultiNodeClusterSpec.clusterConfig))

  nodeConfig(first, second)(ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = "dc1"
    """))

  nodeConfig(third, fourth, fifth)(ConfigFactory.parseString("""
      akka.cluster.multi-data-center.self-data-center = "dc2"
    """))

  testTransport(on = true)
}

class MultiDcSplitBrainMultiJvmNode1 extends MultiDcSplitBrainSpec
class MultiDcSplitBrainMultiJvmNode2 extends MultiDcSplitBrainSpec
class MultiDcSplitBrainMultiJvmNode3 extends MultiDcSplitBrainSpec
class MultiDcSplitBrainMultiJvmNode4 extends MultiDcSplitBrainSpec
class MultiDcSplitBrainMultiJvmNode5 extends MultiDcSplitBrainSpec

@nowarn("msg=Use Akka Distributed Cluster")
abstract class MultiDcSplitBrainSpec extends MultiNodeClusterSpec(MultiDcSplitBrainMultiJvmSpec) {

  import MultiDcSplitBrainMultiJvmSpec._

  val dc1 = List(first, second)
  val dc2 = List(third, fourth, fifth)
  var splits = 0
  var unsplits = 0

  def splitDataCenters(doNotVerify: Set[RoleName]): Unit = {
    splits += 1
    val memberNodes = (dc1 ++ dc2).filterNot(doNotVerify)
    enterBarrier(s"split-$splits")

    runOn(first) {
      for (dc1Node <- dc1; dc2Node <- dc2) {
        testConductor.blackhole(dc1Node, dc2Node, Direction.Both).await
      }
    }

    enterBarrier(s"after-split-$splits")

    runOn(memberNodes: _*) {
      within(15.seconds) {
        runOn(dc1: _*) {
          awaitAssert {
            cluster.state.unreachableDataCenters should ===(Set("dc2"))
          }
        }
        runOn(dc2: _*) {
          awaitAssert {
            cluster.state.unreachableDataCenters should ===(Set("dc1"))
          }
        }
      }
      system.log.debug("Cluster state after split: {}", cluster.state)
      cluster.state.unreachable should ===(Set.empty)
    }
    enterBarrier(s"after-split-verified-$splits")
  }

  def unsplitDataCenters(notMembers: Set[RoleName]): Unit = {
    unsplits += 1
    val memberNodes = (dc1 ++ dc2).filterNot(notMembers)
    enterBarrier(s"unsplit-$unsplits")

    runOn(first) {
      for (dc1Node <- dc1; dc2Node <- dc2) {
        testConductor.passThrough(dc1Node, dc2Node, Direction.Both).await
      }
    }

    enterBarrier(s"after-unsplit-$unsplits")

    runOn(memberNodes: _*) {
      within(15.seconds) {
        awaitAssert {
          cluster.state.unreachableDataCenters should ===(Set.empty)
        }
        system.log.debug("Cluster state after unsplit: {}", cluster.state)
      }
    }
    enterBarrier(s"after-unsplit-verified-$unsplits")
  }

  "A cluster with multiple data centers" must {
    "be able to form two data centers" in {
      awaitClusterUp(first, second, third)
    }

    "publish DataCenterReachabilityEvent" in {
      val dcReachabilityProbe = TestProbe()
      Cluster(system).subscribe(
        dcReachabilityProbe.ref,
        ClusterEvent.InitialStateAsSnapshot,
        classOf[DataCenterReachabilityEvent])
      dcReachabilityProbe.expectMsgType[CurrentClusterState].unreachableDataCenters should ===(Set.empty)

      splitDataCenters(doNotVerify = Set(fourth, fifth))
      runOn(first, second, third) {
        dcReachabilityProbe.expectMsgType[UnreachableDataCenter]
      }
      enterBarrier("unreachable-verified")

      unsplitDataCenters(notMembers = Set(fourth, fifth))
      runOn(first, second, third) {
        dcReachabilityProbe.expectMsgType[ReachableDataCenter]
      }

      enterBarrier("publish-event-verified")
    }

    "be able to have a data center member join while there is inter data center split" in within(20.seconds) {
      // introduce a split between data centers
      splitDataCenters(doNotVerify = Set(fourth, fifth))

      runOn(fourth) {
        cluster.join(third)
      }
      enterBarrier("inter-data-center unreachability")

      // should be able to join and become up since the
      // split is between dc1 and dc2
      runOn(third, fourth) {
        awaitAssert(clusterView.members.collect {
          case m if m.dataCenter == "dc2" && m.status == MemberStatus.Up => m.address
        } should ===(Set(address(third), address(fourth))))
      }
      enterBarrier("dc2-join-completed")

      unsplitDataCenters(notMembers = Set(fifth))

      runOn(dc1: _*) {
        awaitAssert(clusterView.members.collect {
          case m if m.dataCenter == "dc2" && m.status == MemberStatus.Up => m.address
        } should ===(Set(address(third), address(fourth))))
      }

      enterBarrier("inter-data-center-split-1-done")
    }

    // fifth is still not a member of the cluster

    "be able to have data center member leave while there is inter data center split" in within(20.seconds) {
      splitDataCenters(doNotVerify = Set(fifth))

      runOn(fourth) {
        cluster.leave(fourth)
      }

      runOn(third) {
        awaitAssert(clusterView.members.filter(_.address == address(fourth)) should ===(Set.empty))
      }
      enterBarrier("node-4-left")

      unsplitDataCenters(notMembers = Set(fourth, fifth))

      runOn(first, second) {
        awaitAssert(clusterView.members.filter(_.address == address(fourth)) should ===(Set.empty))
      }

      enterBarrier("inter-data-center-split-2-done")
    }

    // forth has left the cluster, fifth is still not a member

    "be able to have data center member restart (same host:port) while there is inter data center split" in within(
      60.seconds) {
      val subscribeProbe = TestProbe()
      runOn(first, second, third, fifth) {
        Cluster(system).subscribe(subscribeProbe.ref, InitialStateAsSnapshot, classOf[MemberUp], classOf[MemberRemoved])
        subscribeProbe.expectMsgType[CurrentClusterState]
      }
      enterBarrier("subscribed")

      runOn(fifth) {
        Cluster(system).join(third)
      }
      var fifthOriginalUniqueAddress: Option[UniqueAddress] = None
      runOn(first, second, third, fifth) {
        awaitAssert(clusterView.members.collect {
          case m if m.dataCenter == "dc2" && m.status == MemberStatus.Up => m.address
        } should ===(Set(address(third), address(fifth))))
        fifthOriginalUniqueAddress = clusterView.members.collectFirst {
          case m if m.address == address(fifth) => m.uniqueAddress
        }
      }
      enterBarrier("fifth-joined")

      splitDataCenters(doNotVerify = Set(fourth))

      runOn(fifth) {
        Cluster(system).shutdown()
      }

      runOn(third) {
        awaitAssert(clusterView.members.collect {
          case m if m.dataCenter == "dc2" => m.address
        } should ===(Set(address(third))))
      }

      enterBarrier("fifth-removed")

      runOn(fifth) {
        // we can't use any multi-jvm test facilities on fifth after this, because have to shutdown
        // actor system to be able to start new with same port
        val thirdAddress = address(third)
        enterBarrier("fifth-waiting-for-termination")
        Await.ready(system.whenTerminated, remaining)

        val port = Cluster(system).selfAddress.port.get
        val restartedSystem = ActorSystem(
          system.name,
          ConfigFactory.parseString(s"""
            akka.remote.artery.canonical.port = $port
            akka.coordinated-shutdown.terminate-actor-system = on
            """).withFallback(system.settings.config))
        Cluster(restartedSystem).join(thirdAddress)
        Await.ready(restartedSystem.whenTerminated, remaining)
      }

      // no multi-jvm test facilities on fifth after this
      val remainingRoles = roles.filterNot(_ == fifth)

      runOn(remainingRoles: _*) {
        enterBarrier("fifth-waiting-for-termination")
      }

      runOn(first) {
        for (dc1Node <- dc1; dc2Node <- dc2) {
          testConductor.passThrough(dc1Node, dc2Node, Direction.Both).await
        }
        testConductor.shutdown(fifth)
      }

      runOn(remainingRoles: _*) {
        enterBarrier("fifth-restarted")
      }

      runOn(first, second, third) {
        awaitAssert(clusterView.members.collectFirst {
          case m if m.dataCenter == "dc2" && m.address == fifthOriginalUniqueAddress.get.address => m.uniqueAddress
        } should not be fifthOriginalUniqueAddress) // different uid

        subscribeProbe.expectMsgType[MemberUp].member.uniqueAddress should ===(fifthOriginalUniqueAddress.get)
        subscribeProbe.expectMsgType[MemberRemoved].member.uniqueAddress should ===(fifthOriginalUniqueAddress.get)
        subscribeProbe.expectMsgType[MemberUp].member.address should ===(fifthOriginalUniqueAddress.get.address)
      }

      runOn(remainingRoles: _*) {
        enterBarrier("fifth-re-joined")
      }

      runOn(first) {
        // to shutdown the restartedSystem on fifth
        Cluster(system).leave(fifthOriginalUniqueAddress.get.address)
      }

      runOn(first, second, third) {
        awaitAssert({
          clusterView.members.map(_.address) should ===(Set(address(first), address(second), address(third)))
        })
      }
      runOn(remainingRoles: _*) {
        enterBarrier("restarted-fifth-removed")
      }
    }
  }
}
