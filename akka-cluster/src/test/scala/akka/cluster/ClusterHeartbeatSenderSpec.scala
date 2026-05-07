/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import akka.actor.{ ActorSelection, Address, Props }
import akka.cluster.ClusterEvent.{ CurrentClusterState, MemberUp }
import akka.cluster.ClusterHeartbeatSender.Heartbeat
import akka.cluster.ClusterHeartbeatSenderSpec.TestClusterHeartBeatSender
import akka.testkit.{ AkkaSpec, ImplicitSender, TestProbe }
import akka.util.Version

object ClusterHeartbeatSenderSpec {
  class TestClusterHeartBeatSender(probe: TestProbe) extends ClusterHeartbeatSender {
    // don't register for cluster events
    override def preStart(): Unit = {}

    // override where the heart beats go to
    override def heartbeatReceiver(address: Address): ActorSelection = {
      context.actorSelection(probe.ref.path)
    }
  }
}

class ClusterHeartbeatSenderSpec extends AkkaSpec("""
    akka.loglevel = DEBUG
    akka.actor.provider = cluster 
    akka.cluster.failure-detector.heartbeat-interval = 0.2s
    akka.remote.artery.canonical.port = 0
  """.stripMargin) with ImplicitSender {

  "ClusterHeartBeatSender" must {
    "increment heart beat sequence nr" in {
      val probe = TestProbe()
      val underTest = system.actorOf(Props(new TestClusterHeartBeatSender(probe)))
      underTest ! CurrentClusterState()
      underTest ! MemberUp(
        Member(UniqueAddress(Address("akka", system.name), 1L), Set("dc-default"), Version.Zero)
          .copy(status = MemberStatus.Up))

      probe.expectMsgType[Heartbeat].sequenceNr shouldEqual 1
      probe.expectMsgType[Heartbeat].sequenceNr shouldEqual 2
    }
  }

}
