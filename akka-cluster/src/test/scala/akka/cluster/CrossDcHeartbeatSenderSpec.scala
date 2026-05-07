/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.collection.immutable.SortedSet

import akka.actor.ActorSelection
import akka.actor.Address
import akka.actor.Props
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterHeartbeatSender.Heartbeat
import akka.cluster.CrossDcHeartbeatSender.ReportStatus
import akka.cluster.CrossDcHeartbeatSenderSpec.TestCrossDcHeartbeatSender
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe
import akka.util.Version

@nowarn("msg=Use Akka Distributed Cluster")
object CrossDcHeartbeatSenderSpec {
  class TestCrossDcHeartbeatSender(heartbeatProbe: TestProbe) extends CrossDcHeartbeatSender {
    // disable register for cluster events
    override def preStart(): Unit = {}

    override def heartbeatReceiver(address: Address): ActorSelection = {
      context.actorSelection(heartbeatProbe.ref.path)
    }
  }
}

class CrossDcHeartbeatSenderSpec extends AkkaSpec("""
    akka.loglevel = DEBUG
    akka.actor.provider = cluster
    # should not be used here
    akka.cluster.failure-detector.heartbeat-interval = 5s
    akka.cluster.multi-data-center {
      self-data-center = "dc1"
      failure-detector.heartbeat-interval = 0.2s
    }
    akka.remote.artery.canonical.port = 0
  """) with ImplicitSender {
  "CrossDcHeartBeatSender" should {
    "increment heart beat sequence nr" in {

      val heartbeatProbe = TestProbe()
      Cluster(system).join(Cluster(system).selfMember.address)
      awaitAssert(Cluster(system).selfMember.status == MemberStatus.Up)
      val underTest = system.actorOf(Props(new TestCrossDcHeartbeatSender(heartbeatProbe)))

      underTest ! CurrentClusterState(
        members = SortedSet(
          Cluster(system).selfMember,
          Member(UniqueAddress(Address("akka", system.name), 2L), Set("dc-dc2"), Version.Zero)
            .copy(status = MemberStatus.Up)))

      awaitAssert {
        underTest ! ReportStatus()
        expectMsgType[CrossDcHeartbeatSender.MonitoringActive]
      }

      heartbeatProbe.expectMsgType[Heartbeat].sequenceNr shouldEqual 1
      heartbeatProbe.expectMsgType[Heartbeat].sequenceNr shouldEqual 2
    }
  }
}
