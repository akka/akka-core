/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.cluster.MultiNodeClusterSpec.EndActor
import akka.remote.testconductor.RoleName
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object UnreachableNodeJoinsAgainMultiNodeConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(debugConfig(on = false).withFallback(MultiNodeClusterSpec.clusterConfig))

  testTransport(on = true)

}

class UnreachableNodeJoinsAgainMultiJvmNode1 extends UnreachableNodeJoinsAgainSpec
class UnreachableNodeJoinsAgainMultiJvmNode2 extends UnreachableNodeJoinsAgainSpec
class UnreachableNodeJoinsAgainMultiJvmNode3 extends UnreachableNodeJoinsAgainSpec
class UnreachableNodeJoinsAgainMultiJvmNode4 extends UnreachableNodeJoinsAgainSpec

abstract class UnreachableNodeJoinsAgainSpec extends MultiNodeClusterSpec(UnreachableNodeJoinsAgainMultiNodeConfig) {

  import UnreachableNodeJoinsAgainMultiNodeConfig._

  muteMarkingAsUnreachable()

  def allBut(role: RoleName, roles: immutable.Seq[RoleName] = roles): immutable.Seq[RoleName] = {
    roles.filterNot(_ == role)
  }

  lazy val master = second
  lazy val victim = fourth

  var endBarrierNumber = 0
  def endBarrier(): Unit = {
    endBarrierNumber += 1
    enterBarrier("after_" + endBarrierNumber)
  }

  "A cluster of " + roles.size + " members" must {

    "reach initial convergence" taggedAs LongRunningTest in {
      awaitClusterUp(roles: _*)
      endBarrier()
    }

    "mark a node as UNREACHABLE when we pull the network" taggedAs LongRunningTest in {
      // let them send at least one heartbeat to each other after the gossip convergence
      // because for new joining nodes we remove them from the failure detector when
      // receive gossip
      Thread.sleep(2.seconds.dilated.toMillis)

      runOn(first) {
        // pull network for victim node from all nodes
        allBut(victim).foreach { roleName =>
          testConductor.blackhole(victim, roleName, Direction.Both).await
        }
      }

      enterBarrier("unplug_victim")

      val allButVictim = allBut(victim, roles)
      runOn(victim) {
        allButVictim.foreach(markNodeAsUnavailable(_))
        within(30 seconds) {
          // victim becomes all alone
          awaitAssert {
            clusterView.unreachableMembers.size should ===(roles.size - 1)
          }
          clusterView.unreachableMembers.map(_.address) should ===(allButVictim.map(address).toSet)
        }
      }

      runOn(allButVictim: _*) {
        markNodeAsUnavailable(victim)
        within(30 seconds) {
          // victim becomes unreachable
          awaitAssert {
            clusterView.unreachableMembers.size should ===(1)
          }
          awaitSeenSameState(allButVictim.map(address): _*)
          // still one unreachable
          clusterView.unreachableMembers.size should ===(1)
          clusterView.unreachableMembers.head.address should ===(node(victim).address)
          clusterView.unreachableMembers.head.status should ===(MemberStatus.Up)
        }
      }

      endBarrier()
    }

    "mark the node as DOWN" taggedAs LongRunningTest in {
      runOn(master) {
        cluster.down(victim)
      }

      val allButVictim = allBut(victim, roles)
      runOn(allButVictim: _*) {
        // eventually removed
        awaitMembersUp(roles.size - 1, Set(victim))
        awaitAssert(clusterView.unreachableMembers should ===(Set.empty), 15 seconds)
        awaitAssert(clusterView.members.map(_.address) should ===(allButVictim.map(address).toSet))

      }

      endBarrier()
    }

    "allow fresh node with same host:port to join again when the network is plugged back in" taggedAs LongRunningTest in {
      val expectedNumberOfMembers = roles.size

      // victim actor system will be shutdown, not part of testConductor any more
      // so we can't use barriers to synchronize with it
      val masterAddress = address(master)
      runOn(master) {
        system.actorOf(Props(classOf[EndActor], testActor, None), "end")
      }
      enterBarrier("end-actor-created")

      runOn(first) {
        // put the network back in
        allBut(victim).foreach { roleName =>
          testConductor.passThrough(victim, roleName, Direction.Both).await
        }
      }

      enterBarrier("plug_in_victim")

      runOn(first) {
        // will shutdown ActorSystem of victim
        testConductor.shutdown(victim)
      }

      runOn(victim) {
        val victimAddress = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
        val freshConfig =
          ConfigFactory.parseString(s"""
                akka.remote.artery.canonical {
                  hostname = ${victimAddress.host.get}
                  port = ${victimAddress.port.get}
                }
               """).withFallback(system.settings.config)

        Await.ready(system.whenTerminated, 10 seconds)

        // create new ActorSystem with same host:port
        val freshSystem = ActorSystem(system.name, freshConfig)

        try {
          Cluster(freshSystem).join(masterAddress)
          within(30 seconds) {
            awaitAssert(Cluster(freshSystem).readView.members.map(_.address) should contain(victimAddress))
            awaitAssert(Cluster(freshSystem).readView.members.size should ===(expectedNumberOfMembers))
            awaitAssert(Cluster(freshSystem).readView.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up)))
          }

          // signal to master node that victim is done
          val endProbe = TestProbe()(freshSystem)
          val endActor = freshSystem.actorOf(Props(classOf[EndActor], endProbe.ref, Some(masterAddress)), "end")
          endActor ! EndActor.SendEnd
          endProbe.expectMsg(EndActor.EndAck)

        } finally {
          shutdown(freshSystem)
        }
        // no barrier here, because it is not part of testConductor roles any more
      }

      runOn(allBut(victim): _*) {
        awaitMembersUp(expectedNumberOfMembers)
        // don't end the test until the freshSystem is done
        runOn(master) {
          expectMsg(20 seconds, EndActor.End)
        }
        endBarrier()
      }

    }
  }
}
