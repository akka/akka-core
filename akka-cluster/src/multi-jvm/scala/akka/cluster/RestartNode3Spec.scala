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
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._

object RestartNode3MultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.cluster.testkit.auto-down-unreachable-after = off
      akka.cluster.allow-weakly-up-members = off
      # test is using Java serialization and not priority to rewrite
      akka.actor.allow-java-serialization = on
      akka.actor.warn-about-java-serializer-usage = off
      """))
      .withFallback(MultiNodeClusterSpec.clusterConfig))

  testTransport(on = true)
}

class RestartNode3MultiJvmNode1 extends RestartNode3Spec
class RestartNode3MultiJvmNode2 extends RestartNode3Spec
class RestartNode3MultiJvmNode3 extends RestartNode3Spec

abstract class RestartNode3Spec extends MultiNodeClusterSpec(RestartNode3MultiJvmSpec) with ImplicitSender {

  import RestartNode3MultiJvmSpec._

  @volatile var secondUniqueAddress: UniqueAddress = _

  // use a separate ActorSystem, to be able to simulate restart
  lazy val secondSystem = ActorSystem(system.name, MultiNodeSpec.configureNextPortIfFixed(system.settings.config))

  override def verifySystemShutdown: Boolean = true

  def seedNodes: immutable.IndexedSeq[Address] = Vector(first)

  lazy val restartedSecondSystem = ActorSystem(
    system.name,
    ConfigFactory.parseString(s"""
        akka.remote.artery.canonical.port = ${secondUniqueAddress.address.port.get}
        """).withFallback(system.settings.config))

  override def afterAll(): Unit = {
    runOn(second) {
      if (secondSystem.whenTerminated.isCompleted)
        shutdown(restartedSecondSystem)
      else
        shutdown(secondSystem)
    }
    super.afterAll()
  }

  override def expectedTestDuration = 2.minutes

  "Cluster nodes" must {
    "be able to restart and join again when Down before Up" taggedAs LongRunningTest in within(60.seconds) {
      // secondSystem is a separate ActorSystem, to be able to simulate restart
      // we must transfer its address to first
      runOn(first, third) {
        system.actorOf(Props(new Actor {
          def receive = {
            case ad: UniqueAddress =>
              secondUniqueAddress = ad
              sender() ! "ok"
          }
        }).withDeploy(Deploy.local), name = "address-receiver")
        enterBarrier("second-address-receiver-ready")
      }

      runOn(second) {
        enterBarrier("second-address-receiver-ready")
        secondUniqueAddress = Cluster(secondSystem).selfUniqueAddress
        List(first, third).foreach { r =>
          system.actorSelection(RootActorPath(r) / "user" / "address-receiver") ! secondUniqueAddress
          expectMsg(5.seconds, "ok")
        }
      }
      enterBarrier("second-address-transferred")

      // now we can join first, third together
      runOn(first, third) {
        cluster.joinSeedNodes(seedNodes)
        awaitMembersUp(2)
      }
      enterBarrier("first-third-up")

      // make third unreachable, so that leader can't perform its duties
      runOn(first) {
        testConductor.blackhole(first, third, Direction.Both).await
        val thirdAddress = address(third)
        awaitAssert(clusterView.unreachableMembers.map(_.address) should ===(Set(thirdAddress)))
      }
      enterBarrier("third-unreachable")

      runOn(second) {
        Cluster(secondSystem).joinSeedNodes(seedNodes)
        awaitAssert(Cluster(secondSystem).readView.members.size should ===(3))
        awaitAssert(Cluster(secondSystem).readView.members.collectFirst {
          case m if m.address == Cluster(secondSystem).selfAddress => m.status
        } should ===(Some(Joining)))
      }
      enterBarrier("second-joined")

      // shutdown secondSystem
      runOn(second) {
        shutdown(secondSystem, remaining)
      }
      enterBarrier("second-shutdown")

      // then immediately start restartedSecondSystem, which has the same address as secondSystem
      runOn(first) {
        testConductor.passThrough(first, third, Direction.Both).await
      }
      runOn(second) {
        Cluster(restartedSecondSystem).joinSeedNodes(seedNodes)
        awaitAssert(Cluster(restartedSecondSystem).readView.members.size should ===(3))
        awaitAssert(Cluster(restartedSecondSystem).readView.members.unsorted.map(_.status) should ===(Set(Up)))
      }
      runOn(first, third) {
        awaitAssert {
          Cluster(system).readView.members.size should ===(3)
          Cluster(system).readView.members.exists { m =>
            m.address == secondUniqueAddress.address && m.uniqueAddress.longUid != secondUniqueAddress.longUid
          }
        }
      }
      enterBarrier("second-restarted")

    }

  }
}
