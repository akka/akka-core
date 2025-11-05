/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.Identify
import akka.actor.RootActorPath
import akka.remote.RARP
import akka.remote.artery.ThisActorSystemQuarantinedEvent
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.testkit.EventFilter
import akka.testkit.LongRunningTest
import akka.testkit.TestEvent.Mute

object DowningWhenOtherHasQuarantinedThisActorSystemSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(
    debugConfig(on = false)
      .withFallback(MultiNodeClusterSpec.clusterConfig)
      .withFallback(
        ConfigFactory.parseString("""
        akka.remote.artery.enabled = on
        akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        akka.cluster.split-brain-resolver.stable-after = 10s
        """)))

  testTransport(on = true)
}

class DowningWhenOtherHasQuarantinedThisActorSystemMultiJvmNode1
    extends DowningWhenOtherHasQuarantinedThisActorSystemSpec
class DowningWhenOtherHasQuarantinedThisActorSystemMultiJvmNode2
    extends DowningWhenOtherHasQuarantinedThisActorSystemSpec
class DowningWhenOtherHasQuarantinedThisActorSystemMultiJvmNode3
    extends DowningWhenOtherHasQuarantinedThisActorSystemSpec
class DowningWhenOtherHasQuarantinedThisActorSystemMultiJvmNode4
    extends DowningWhenOtherHasQuarantinedThisActorSystemSpec

abstract class DowningWhenOtherHasQuarantinedThisActorSystemSpec
    extends MultiNodeClusterSpec(DowningWhenOtherHasQuarantinedThisActorSystemSpec) {
  import DowningWhenOtherHasQuarantinedThisActorSystemSpec._

  muteDeadLetters(classOf[ActorIdentity])()
  system.eventStream.publish(Mute(EventFilter.info(pattern = ".*Ignoring received gossip from unknown.*")))

  "Cluster node downed by other" must {

    "join cluster" taggedAs LongRunningTest in {
      awaitClusterUp(first, second, third, fourth)
      enterBarrier("after-1")
    }

    "down itself with DownSelfQuarantinedByRemote when other has quarantined" taggedAs LongRunningTest in {
      runOn(first, second) {
        system.eventStream.subscribe(testActor, classOf[ThisActorSystemQuarantinedEvent])
      }
      runOn(first) {
        val secondUniqueAddress = cluster.state.members.find(_.address == address(second)).get.uniqueAddress
        RARP(system).provider
          .quarantine(secondUniqueAddress.address, Some(secondUniqueAddress.longUid), "Quarantine from test")
      }
      enterBarrier("quarantined")

      runOn(second) {
        within(5.seconds) { // this is shorter than split-brain-resolver.stable-after, so it's not normal downing
          awaitAssert {
            // try to ping first (Cluster Heartbeat messages will not trigger the Quarantine message)
            system.actorSelection(RootActorPath(first) / "user").tell(Identify(None), ActorRef.noSender)
            // shutting down itself triggered by ThisActorSystemQuarantinedEvent
            cluster.isTerminated should ===(true)
          }
        }
        expectMsgType[ThisActorSystemQuarantinedEvent]
      }
      enterBarrier("second-shutdown")

      runOn(first) {
        expectNoMessage(1.second) // no ThisActorSystemQuarantinedEvent
      }
      enterBarrier("wait")

      runOn(first) {
        val sel = system.actorSelection(RootActorPath(second) / "user")
        (1 to 15).foreach { _ =>
          sel.tell(Identify(None), ActorRef.noSender) // try to ping second
          expectNoMessage(200.millis) // no ThisActorSystemQuarantinedEvent
        }
      }

      enterBarrier("after-2")
    }

    "not be triggered by another node shutting down" taggedAs LongRunningTest in {
      runOn(first) {
        system.eventStream.subscribe(testActor, classOf[ThisActorSystemQuarantinedEvent])
      }
      enterBarrier("subscribing")

      runOn(third) {
        cluster.shutdown()
      }

      runOn(first) {
        expectNoMessage(1.second) // no ThisActorSystemQuarantinedEvent
      }
      enterBarrier("wait")

      runOn(first) {
        val sel = system.actorSelection(RootActorPath(third) / "user")
        (1 to 15).foreach { _ =>
          sel.tell(Identify(None), ActorRef.noSender) // try to ping third
          expectNoMessage(200.millis) // no ThisActorSystemQuarantinedEvent
        }
      }

      enterBarrier("after-3")
    }

    "not be triggered by another node shutting down during network partition" taggedAs LongRunningTest in {
      runOn(first) {
        system.eventStream.subscribe(testActor, classOf[ThisActorSystemQuarantinedEvent])
      }
      enterBarrier("subscribing")

      runOn(first) {
        testConductor.blackhole(first, fourth, Direction.Both).await
      }
      enterBarrier("blackhole")

      runOn(third) {
        cluster.shutdown()
      }

      runOn(first) {
        expectNoMessage(2.second) // no ThisActorSystemQuarantinedEvent
        testConductor.passThrough(first, fourth, Direction.Both).await
      }

      runOn(first) {
        expectNoMessage(1.second) // no ThisActorSystemQuarantinedEvent
      }
      enterBarrier("wait")

      runOn(first) {
        val sel = system.actorSelection(RootActorPath(fourth) / "user")
        (1 to 15).foreach { _ =>
          sel.tell(Identify(None), ActorRef.noSender) // try to ping fourth
          expectNoMessage(200.millis) // no ThisActorSystemQuarantinedEvent
        }
      }

      enterBarrier("after-4")
    }

  }
}
