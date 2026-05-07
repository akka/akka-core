/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.AddressFromURIString
import akka.actor.Props
import akka.actor.RepointableActorRef
import akka.cluster.ClusterRemoteFeatures.AddressPing
import akka.remote.RARP
import akka.remote.RemoteActorRef
import akka.remote.RemoteActorRefProvider
import akka.remote.RemoteWatcher.Heartbeat
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender

object ClusterRemoteFeaturesConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  private val baseConfig = {
    ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port = ${MultiNodeSpec.selfPort}
      akka.log-dead-letters-during-shutdown = off
      """).withFallback(MultiNodeClusterSpec.clusterConfig)
  }

  commonConfig(debugConfig(on = false).withFallback(baseConfig))

  deployOn(first, """/kattdjur.remote = "@second@" """)
  deployOn(third, """/kattdjur.remote = "@second@" """)
  deployOn(second, """/kattdjur.remote = "@third@" """)

}

object ClusterRemoteFeatures {
  class AddressPing extends Actor {
    def receive: Receive = {
      case "ping" => sender() ! self
    }
  }
}

class ClusterRemoteFeaturesMultiJvmNode1 extends ClusterRemoteFeaturesSpec
class ClusterRemoteFeaturesMultiJvmNode2 extends ClusterRemoteFeaturesSpec
class ClusterRemoteFeaturesMultiJvmNode3 extends ClusterRemoteFeaturesSpec

abstract class ClusterRemoteFeaturesSpec
    extends MultiNodeClusterSpec(ClusterRemoteFeaturesConfig)
    with ImplicitSender
    with ScalaFutures {

  import ClusterRemoteFeaturesConfig._

  override def initialParticipants: Int = roles.size

  muteDeadLetters(Heartbeat.getClass)()

  protected val provider: RemoteActorRefProvider = RARP(system).provider

  "Remoting with Cluster" must {

    "have the correct settings" in {
      runOn(first) {
        system.settings.HasCluster shouldBe true
        provider.hasClusterOrUseUnsafe shouldBe true
        provider.transport.system.settings.HasCluster shouldBe true
        provider.remoteSettings.UseUnsafeRemoteFeaturesWithoutCluster shouldBe false
        system.settings.ProviderClass shouldEqual classOf[ClusterActorRefProvider].getName
      }
    }

    "create a ClusterRemoteWatcher" in {
      runOn(roles: _*)(provider.remoteWatcher.isDefined shouldBe true)
    }

    "create a ClusterDeployer" in {
      runOn(roles: _*)(provider.deployer.getClass shouldEqual classOf[ClusterDeployer])
    }

    "have expected `actorOf` behavior" in {
      awaitClusterUp(first, second)
      enterBarrier("cluster-up")

      runOn(first) {
        val actor = system.actorOf(Props[AddressPing](), "kattdjur")
        actor.isInstanceOf[RemoteActorRef] shouldBe true
        actor.path.address shouldEqual node(second).address
        actor.path.address.hasGlobalScope shouldBe true

        val secondAddress = node(second).address
        actor ! "ping"
        expectMsgType[RemoteActorRef].path.address shouldEqual secondAddress
      }
      enterBarrier("CARP-in-cluster-remote-validated")

      def assertIsLocalRef(): Unit = {
        val actor = system.actorOf(Props[AddressPing](), "kattdjur")
        actor.isInstanceOf[RepointableActorRef] shouldBe true
        val localAddress = AddressFromURIString(s"akka://${system.name}")
        actor.path.address shouldEqual localAddress
        actor.path.address.hasLocalScope shouldBe true

        actor ! "ping"
        expectMsgType[ActorRef].path.address shouldEqual localAddress
      }

      runOn(third) {
        Cluster(system).state.isMemberUp(node(third).address) shouldBe false
        assertIsLocalRef()
      }
      enterBarrier("CARP-outside-cluster-local-validated")

      runOn(second) {
        assertIsLocalRef()
      }
      enterBarrier("CARP-inside-cluster-to-non-member-local-validated")
    }
  }
}
