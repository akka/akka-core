/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.Address
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus

object ClusterApiSpec {
  val config =
    ConfigFactory.parseString("""
      akka.actor.provider = cluster
      akka.remote.artery.canonical.port = 0
      akka.remote.artery.canonical.hostname = 127.0.0.1
      akka.cluster.jmx.multi-mbeans-in-same-jvm = on
      akka.coordinated-shutdown.terminate-actor-system = off
      akka.coordinated-shutdown.run-by-actor-system-terminate = off
      # generous timeout for cluster forming probes
      akka.actor.testkit.typed.default-timeout = 10s
      # disable this or we cannot be sure to observe node end state on the leaving side
      akka.cluster.run-coordinated-shutdown-when-down = off
    """)
}

class ClusterApiSpec extends ScalaTestWithActorTestKit(ClusterApiSpec.config) with AnyWordSpecLike with LogCapturing {

  val testSettings = TestKitSettings(system)
  val clusterNode1 = Cluster(system)
  val classicSystem1 = system.toClassic

  "A typed Cluster" must {

    "fail fast in a join attempt if invalid chars are in host names, e.g. docker host given name" in {
      val address = Address("akka", "sys", Some("in_valid"), Some(0))
      intercept[IllegalArgumentException](Join(address))
      intercept[IllegalArgumentException](JoinSeedNodes(scala.collection.immutable.Seq(address)))
    }

    "join a cluster and observe events from both sides" in {

      val system2 = akka.actor.ActorSystem(system.name, system.settings.config)
      val adaptedSystem2 = system2.toTyped

      try {
        val clusterNode2 = Cluster(adaptedSystem2)

        val node1Probe = TestProbe[ClusterDomainEvent]()(system)
        val node2Probe = TestProbe[ClusterDomainEvent]()(adaptedSystem2)

        // initial cached selfMember
        clusterNode1.selfMember.status should ===(MemberStatus.Removed)
        clusterNode2.selfMember.status should ===(MemberStatus.Removed)

        // check that subscriptions work
        clusterNode1.subscriptions ! Subscribe(node1Probe.ref, classOf[MemberEvent])
        clusterNode1.manager ! Join(clusterNode1.selfMember.address)
        node1Probe.expectMessageType[MemberUp].member.uniqueAddress == clusterNode1.selfMember.uniqueAddress

        // check that cached selfMember is updated
        node1Probe.awaitAssert(clusterNode1.selfMember.status should ===(MemberStatus.Up))

        // subscribing to OnSelfUp when already up
        clusterNode1.subscriptions ! Subscribe(node1Probe.ref, classOf[SelfUp])
        node1Probe.expectMessageType[SelfUp]

        // selfMember update and on up subscription on node 2 when joining
        clusterNode2.subscriptions ! Subscribe(node2Probe.ref, classOf[SelfUp])
        clusterNode2.manager ! Join(clusterNode1.selfMember.address)
        node2Probe.awaitAssert(clusterNode2.selfMember.status should ===(MemberStatus.Up))
        node2Probe.expectMessageType[SelfUp]

        // events about node2 joining to subscriber on node1
        node1Probe.expectMessageType[MemberJoined].member.uniqueAddress == clusterNode2.selfMember.uniqueAddress
        node1Probe.expectMessageType[MemberUp].member.uniqueAddress == clusterNode1.selfMember.uniqueAddress

        // OnSelfRemoved and subscription events around node2 leaving
        clusterNode2.subscriptions ! Subscribe(node2Probe.ref, classOf[SelfRemoved])
        clusterNode2.manager ! Leave(clusterNode2.selfMember.address)

        // node1 seeing all those transition events
        node1Probe.expectMessageType[MemberLeft].member.uniqueAddress == clusterNode2.selfMember.uniqueAddress
        node1Probe.expectMessageType[MemberExited].member.uniqueAddress == clusterNode2.selfMember.uniqueAddress
        node1Probe.expectMessageType[MemberRemoved].member.uniqueAddress == clusterNode2.selfMember.uniqueAddress

        // selfMember updated and self removed event gotten
        node2Probe.awaitAssert(clusterNode2.selfMember.status should ===(MemberStatus.Removed))

        node2Probe.expectMessage(SelfRemoved(MemberStatus.Exiting))

        // subscribing to SelfRemoved when already removed yields immediate message back
        clusterNode2.subscriptions ! Subscribe(node2Probe.ref, classOf[SelfRemoved])
        node2Probe.expectMessage(SelfRemoved(MemberStatus.Exiting))

        // subscribing to SelfUp when already removed yields nothing
        clusterNode2.subscriptions ! Subscribe(node2Probe.ref, classOf[SelfUp])
        node2Probe.expectNoMessage()

      } finally {
        ActorTestKit.shutdown(adaptedSystem2)
      }
    }
  }

}
