/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.AddressFromURIString
import akka.actor.Identify
import akka.actor.InternalActorRef
import akka.actor.Props
import akka.actor.RootActorPath
import akka.remote.RemoteWatcher.Stats
import akka.remote.RemoteWatcher.UnwatchRemote
import akka.remote.RemoteWatcher.WatchRemote
import akka.remote.artery.ArteryMultiNodeSpec
import akka.remote.artery.ArterySpecSupport
import akka.remote.artery.RemoteDeploymentSpec
import akka.testkit.EventFilter
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe

object RemoteFeaturesSpec {

  val instances = 1

  // string config to pass into `ArteryMultiNodeSpec.extraConfig: Option[String]` for `other` system
  def common(useUnsafe: Boolean): String = s"""
       akka.remote.use-unsafe-remote-features-outside-cluster = $useUnsafe
       akka.remote.artery.canonical.port = 0
       akka.log-dead-letters-during-shutdown = off
       """

  def disabled: Config =
    ConfigFactory.parseString(common(useUnsafe = false)).withFallback(ArterySpecSupport.defaultConfig)
  def enabled: Config = ConfigFactory.parseString(common(useUnsafe = true))

  class EmptyActor extends Actor {
    def receive: Receive = Actor.emptyBehavior
  }
}

abstract class RemoteFeaturesSpec(c: Config) extends ArteryMultiNodeSpec(c) with ImplicitSender {
  import RemoteFeaturesSpec._

  protected final val provider = RARP(system).provider

  protected final val useUnsafe: Boolean = provider.remoteSettings.UseUnsafeRemoteFeaturesWithoutCluster

  protected val remoteSystem1 = newRemoteSystem(name = Some("RS1"), extraConfig = Some(common(useUnsafe)))

  import akka.remote.artery.RemoteWatcherSpec.TestRemoteWatcher
  protected val monitor = system.actorOf(Props(new TestRemoteWatcher), "monitor1")

  protected val watcher = system.actorOf(Props(new EmptyActor), "a1").asInstanceOf[InternalActorRef]

  protected val remoteWatchee = createRemoteActor(Props(new EmptyActor), "b1")

  protected def createRemoteActor(props: Props, name: String): InternalActorRef = {
    remoteSystem1.actorOf(props, name)
    system.actorSelection(RootActorPath(address(remoteSystem1)) / "user" / name) ! Identify(name)
    expectMsgType[ActorIdentity].ref.get.asInstanceOf[InternalActorRef]
  }
}

// all pre-existing remote tests exercise the rest of the unchanged enabled expectations
class RARPRemoteFeaturesEnabledSpec extends RemoteFeaturesSpec(RemoteFeaturesSpec.enabled) {
  "RARP without Cluster: opt-in unsafe enabled" must {

    "have the expected settings" in {
      provider.transport.system.settings.HasCluster shouldBe false
      provider.remoteSettings.UseUnsafeRemoteFeaturesWithoutCluster shouldBe true
      provider.remoteSettings.WarnUnsafeWatchWithoutCluster shouldBe true
      provider.hasClusterOrUseUnsafe shouldBe true
    }

    "create a RemoteWatcher" in {
      provider.remoteWatcher.isDefined shouldBe true
    }
  }
}

// see the multi-jvm RemoteFeaturesSpec for deployer-router tests
class RemoteFeaturesDisabledSpec extends RemoteFeaturesSpec(RemoteFeaturesSpec.disabled) {

  private val actorName = "kattdjur"

  private val port = RARP(system).provider.getDefaultAddress.port.get

  "Remote features without Cluster" must {

    "have the expected settings in a RARP" in {
      provider.transport.system.settings.HasCluster shouldBe false
      provider.remoteSettings.UseUnsafeRemoteFeaturesWithoutCluster shouldBe false
      provider.remoteSettings.WarnUnsafeWatchWithoutCluster shouldBe true
      provider.hasClusterOrUseUnsafe shouldBe false
    }

    "not create a RemoteWatcher in a RARP" in {
      provider.remoteWatcher shouldEqual None
    }

    "not deathwatch a remote actor" in {
      EventFilter
        .warning(pattern = s"Dropped remote Watch: disabled for *", occurrences = 1)
        .intercept(monitor ! WatchRemote(remoteWatchee, watcher))
      monitor ! Stats
      expectMsg(Stats.empty)
      expectNoMessage(100.millis)

      EventFilter
        .warning(pattern = s"Dropped remote Unwatch: disabled for *", occurrences = 1)
        .intercept(monitor ! UnwatchRemote(remoteWatchee, watcher))

      monitor ! Stats
      expectMsg(Stats.empty)
      expectNoMessage(100.millis)
    }

    "fall back to creating local deploy children and supervise children on local node" in {
      // super.newRemoteSystem adds the new system to shutdown hook
      val masterSystem = newRemoteSystem(
        name = Some("RS2"),
        extraConfig = Some(s"""
      akka.actor.deployment {
        /$actorName.remote = "akka://${system.name}@localhost:$port"
        "/parent*/*".remote = "akka://${system.name}@localhost:$port"
      }
    """))

      val masterRef = masterSystem.actorOf(Props[RemoteDeploymentSpec.Echo1](), actorName)
      masterRef.path shouldEqual RootActorPath(AddressFromURIString(s"akka://${masterSystem.name}")) / "user" / actorName
      masterRef.path.address.hasLocalScope shouldBe true

      masterSystem.actorSelection(RootActorPath(address(system)) / "user" / actorName) ! Identify(1)
      expectMsgType[ActorIdentity].ref shouldEqual None

      system.actorSelection(RootActorPath(address(system)) / "user" / actorName) ! Identify(3)
      expectMsgType[ActorIdentity].ref
        .forall(_.path == RootActorPath(address(masterSystem)) / "user" / actorName) shouldBe true

      val senderProbe = TestProbe()(masterSystem)
      masterRef.tell(42, senderProbe.ref)
      senderProbe.expectMsg(42)
      EventFilter[Exception]("crash", occurrences = 1).intercept {
        masterRef ! "throwException"
      }(masterSystem)
      senderProbe.expectMsg("preRestart")
      masterRef.tell(43, senderProbe.ref)
      senderProbe.expectMsg(43)
      masterSystem.stop(masterRef)
      senderProbe.expectMsg("postStop")
    }
  }
}
