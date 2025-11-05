/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.actor.Identify
import akka.actor.RootActorPath
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.testkit.LongRunningTest

object SplitBrainQuarantineSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  testTransport(on = true)
  commonConfig(
    debugConfig(on = true)
      .withFallback(MultiNodeClusterSpec.clusterConfig)
      .withFallback(ConfigFactory.parseString(
        """
        akka.remote.artery.enabled = on
        akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        # we dont really want this to hit, but we need the sbr enabled to know the quarantining
        # downing does not trigger
        akka.cluster.split-brain-resolver.stable-after = 5 minutes
        akka.cluster.debug.verbose-gossip-logging = on
        """)))
}

class SplitBrainQuarantineMultiJvmNode1 extends SplitBrainQuarantineSpec
class SplitBrainQuarantineMultiJvmNode2 extends SplitBrainQuarantineSpec
class SplitBrainQuarantineMultiJvmNode3 extends SplitBrainQuarantineSpec
class SplitBrainQuarantineMultiJvmNode4 extends SplitBrainQuarantineSpec

abstract class SplitBrainQuarantineSpec extends MultiNodeClusterSpec(SplitBrainQuarantineSpec) {
  import SplitBrainQuarantineSpec._

  // reproduces the scenario where cluster is partitioned and each side (incorrectly) downs the other,
  // and after that the partition is resolved and the two split brain halves reconnects

  "Cluster node downed by other" must {

    "join cluster" taggedAs LongRunningTest in {
      awaitClusterUp(first, second, third, fourth)
      enterBarrier("after-1")
    }

    "split brain" taggedAs LongRunningTest in {
      runOn(first) {
        testConductor.blackhole(first, third, Direction.Both).await
        testConductor.blackhole(first, fourth, Direction.Both).await
        testConductor.blackhole(second, third, Direction.Both).await
        testConductor.blackhole(second, fourth, Direction.Both).await
      }
      enterBarrier("blackhole")
      system.log.info("cluster split into [JVM-1, JVM-2] and [JVM-3, JVM-4] with blackhole")

      within(15.seconds) {
        runOn(first) {
          cluster.down(third)
          cluster.down(fourth)
        }
        runOn(first, second) {
          awaitAssert {
            cluster.state.members.collect { case m if m.status == MemberStatus.Up => m.address } should ===(
              Set(address(first), address(second)))
            cluster.state.members.size should ===(2)
          }
          system.log.info("JVM-3 and JVM-4 downed from JVM-1")
        }

        runOn(third) {
          cluster.down(first)
          cluster.down(second)
        }
        runOn(third, fourth) {
          awaitAssert {
            cluster.state.members.collect { case m if m.status == MemberStatus.Up => m.address } should ===(
              Set(address(third), address(fourth)))
            cluster.state.members.size should ===(2)
          }
          system.log.info("JVM-1 and JVM-2 downed from JVM-3")
        }
      }
      enterBarrier("brain-split")

      runOn(first) {
        system.log.info("unblackholing cluster")
        testConductor.passThrough(first, third, Direction.Both).await
        testConductor.passThrough(first, fourth, Direction.Both).await
        testConductor.passThrough(second, third, Direction.Both).await
        testConductor.passThrough(second, fourth, Direction.Both).await
      }
      enterBarrier("unblackholed")

      // must send some actual messages that would trigger Quarantine to be sure it does in fact not happen
      runOn(first, second) {
        system.actorSelection(RootActorPath(third) / "user").tell(Identify(None), ActorRef.noSender)
        system.actorSelection(RootActorPath(fourth) / "user").tell(Identify(None), ActorRef.noSender)
      }
      runOn(third, fourth) {
        system.actorSelection(RootActorPath(first) / "user").tell(Identify(None), ActorRef.noSender)
        system.actorSelection(RootActorPath(second) / "user").tell(Identify(None), ActorRef.noSender)
      }
      Thread.sleep(3000)
      runOn(first, second) {
        system.actorSelection(RootActorPath(third) / "user").tell(Identify(None), ActorRef.noSender)
        system.actorSelection(RootActorPath(fourth) / "user").tell(Identify(None), ActorRef.noSender)
      }
      runOn(third, fourth) {
        system.actorSelection(RootActorPath(first) / "user").tell(Identify(None), ActorRef.noSender)
        system.actorSelection(RootActorPath(second) / "user").tell(Identify(None), ActorRef.noSender)
      }
      Thread.sleep(5000)
      enterBarrier("after-pass-through")

      // as the side that would quarantine each node is now not a part of the cluster it is the same as
      // a random node connecting and claiming a node is quarantined and therefore it cannot be trusted
      // enough to trigger a ThisActorSystemQuarantinedEvent-termination
      cluster.isTerminated should ===(false)
      enterBarrier("verify-alive")
    }

  }
}
