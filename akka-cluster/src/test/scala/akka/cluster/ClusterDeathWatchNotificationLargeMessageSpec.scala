/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.remote.artery.ArteryMultiNodeSpec
import akka.remote.artery.ArterySpecSupport
import akka.testkit._

object ClusterDeathWatchNotificationLargeMessageSpec {

  // large-message-destinations enables the large message stream, which is not duplicated
  // across inbound lanes the way the ordinary stream is (see #32963)
  //
  // death-watch-notification-flush-timeout is set far above the `within` bound below (which is
  // itself dilated by akka.test.timefactor, unlike this config value) so that a regression back to
  // completing the flush via the timeout, rather than via actual acks, fails the test instead of
  // passing for the wrong reason.
  val config = ConfigFactory.parseString("""
    akka {
        loglevel = INFO
        actor {
            provider = cluster
        }
    }
    akka.remote.artery.canonical.port = 0
    akka.remote.artery.large-message-destinations = [ "/user/large*" ]
    akka.remote.artery.advanced.death-watch-notification-flush-timeout = 30 seconds
    """).withFallback(ArterySpecSupport.defaultConfig)
}

// https://github.com/akka/akka-core/issues/32963
class ClusterDeathWatchNotificationLargeMessageSpec
    extends ArteryMultiNodeSpec(ClusterDeathWatchNotificationLargeMessageSpec.config)
    with ImplicitSender {

  private def system1: ActorSystem = system
  private val system2 = newRemoteSystem(name = Some(system.name))

  "join cluster" in within(10.seconds) {
    Cluster(system1).join(Cluster(system1).selfAddress)
    Cluster(system2).join(Cluster(system1).selfAddress)
    awaitAssert {
      Vector(system1, system2).foreach { sys =>
        Cluster(sys).state.members.size should ===(2)
        Cluster(sys).state.members.iterator.map(_.status).toSet should ===(Set(MemberStatus.Up))
      }
    }
  }

  "receive Terminated promptly, without waiting for the flush timeout, when the large message stream is enabled" in {
    val watchee = system2.actorOf(Props.empty, "watchee")
    system1.actorSelection(rootActorPath(system2) / "user" / "watchee") ! Identify(None)
    val remoteWatchee = expectMsgType[ActorIdentity](5.seconds).ref.get

    watch(remoteWatchee)
    system2.stop(watchee)

    // the death watch notification flush should complete because of the actual acks it receives,
    // well within the (dilated) bound below, not because of the 30 second flush timeout configured above
    within(2.seconds) {
      expectTerminated(remoteWatchee)
    }
  }
}
