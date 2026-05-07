/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.Props
import akka.actor.RootActorPath
import akka.cluster.MemberStatus._
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._

object RestartNode2SpecMultiJvmSpec extends MultiNodeConfig {
  val seed1 = role("seed1")
  val seed2 = role("seed2")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
      akka.cluster.testkit.auto-down-unreachable-after = 2s
      akka.cluster.retry-unsuccessful-join-after = 3s
      akka.cluster.allow-weakly-up-members = off
      # test is using Java serialization and not priority to rewrite
      akka.actor.allow-java-serialization = on
      akka.actor.warn-about-java-serializer-usage = off
      """))
      .withFallback(MultiNodeClusterSpec.clusterConfig))

}

class RestartNode2SpecMultiJvmNode1 extends RestartNode2SpecSpec
class RestartNode2SpecMultiJvmNode2 extends RestartNode2SpecSpec

abstract class RestartNode2SpecSpec extends MultiNodeClusterSpec(RestartNode2SpecMultiJvmSpec) with ImplicitSender {

  import RestartNode2SpecMultiJvmSpec._

  @volatile var seedNode1Address: Address = _

  // use a separate ActorSystem, to be able to simulate restart
  lazy val seed1System = ActorSystem(system.name, MultiNodeSpec.configureNextPortIfFixed(system.settings.config))

  override def verifySystemShutdown: Boolean = true

  def seedNodes: immutable.IndexedSeq[Address] = Vector(seedNode1Address, seed2)

  // this is the node that will attempt to re-join, keep gate times low so it can retry quickly
  lazy val restartedSeed1System = ActorSystem(
    system.name,
    ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port = ${seedNodes.head.port.get}
      """).withFallback(system.settings.config))

  override def afterAll(): Unit = {
    runOn(seed1) {
      shutdown(if (seed1System.whenTerminated.isCompleted) restartedSeed1System else seed1System)
    }
    super.afterAll()
  }

  "Cluster seed nodes" must {
    "be able to restart first seed node and join other seed nodes" taggedAs LongRunningTest in within(60.seconds) {
      // seed1System is a separate ActorSystem, to be able to simulate restart
      // we must transfer its address to seed2
      runOn(seed2) {
        system.actorOf(Props(new Actor {
          def receive = {
            case ad: Address =>
              seedNode1Address = ad
              sender() ! "ok"
          }
        }).withDeploy(Deploy.local), name = "address-receiver")
        enterBarrier("seed1-address-receiver-ready")
      }

      runOn(seed1) {
        enterBarrier("seed1-address-receiver-ready")
        seedNode1Address = Cluster(seed1System).selfAddress
        List(seed2).foreach { r =>
          system.actorSelection(RootActorPath(r) / "user" / "address-receiver") ! seedNode1Address
          expectMsg(5.seconds, "ok")
        }
      }
      enterBarrier("seed1-address-transferred")

      // now we can join seed1System, seed2 together

      runOn(seed1) {
        Cluster(seed1System).joinSeedNodes(seedNodes)
        awaitAssert(Cluster(seed1System).readView.members.size should be(2))
        awaitAssert(Cluster(seed1System).readView.members.unsorted.map(_.status) should be(Set(Up)))
      }
      runOn(seed2) {
        cluster.joinSeedNodes(seedNodes)
        awaitMembersUp(2)
      }
      enterBarrier("started")

      // shutdown seed1System
      runOn(seed1) {
        shutdown(seed1System, remainingOrDefault)
      }
      enterBarrier("seed1-shutdown")

      // then start restartedSeed1System, which has the same address as seed1System
      runOn(seed1) {
        Cluster(restartedSeed1System).joinSeedNodes(seedNodes)
        within(30.seconds) {
          awaitAssert(Cluster(restartedSeed1System).readView.members.size should be(2))
          awaitAssert(Cluster(restartedSeed1System).readView.members.unsorted.map(_.status) should be(Set(Up)))
        }
      }
      runOn(seed2) {
        awaitMembersUp(2)
      }
      enterBarrier("seed1-restarted")

    }

  }
}
