/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.Identify
import akka.actor.Props
import akka.actor.Terminated
import akka.remote.RARP
import akka.remote.RemoteWatcher.Heartbeat
import akka.remote.RemoteWatcher.Stats
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe

class ClusterWatcherNoClusterWatcheeConfig(val useUnsafe: Boolean) extends MultiNodeConfig {

  val clustered = role("clustered")
  val remoting = role("remoting")

  commonConfig(debugConfig(on = false).withFallback(ConfigFactory.parseString(s"""
      akka.remote.use-unsafe-remote-features-outside-cluster = $useUnsafe
      akka.log-dead-letters = off
      akka.loggers =["akka.testkit.TestEventListener"]
      akka.actor.allow-java-serialization = on
     """)))

  nodeConfig(remoting)(ConfigFactory.parseString("""
      akka.actor.provider = remote"""))

  nodeConfig(clustered)(ConfigFactory.parseString("""
      akka.actor.provider = cluster
      akka.cluster.jmx.enabled = off"""))

}

class ClusterWatcherNoClusterWatcheeUnsafeSpecMultiJvmNode1
    extends ClusterWatcherNoClusterWatcheeSpec(new ClusterWatcherNoClusterWatcheeConfig(useUnsafe = true))
class ClusterWatcherNoClusterWatcheeUnsafeSpecMultiJvmNode2
    extends ClusterWatcherNoClusterWatcheeSpec(new ClusterWatcherNoClusterWatcheeConfig(useUnsafe = true))

class ClusterWatcherNoClusterWatcheeSafeSpecMultiJvmNode1
    extends ClusterWatcherNoClusterWatcheeSpec(new ClusterWatcherNoClusterWatcheeConfig(useUnsafe = false))
class ClusterWatcherNoClusterWatcheeSafeSpecMultiJvmNode2
    extends ClusterWatcherNoClusterWatcheeSpec(new ClusterWatcherNoClusterWatcheeConfig(useUnsafe = false))

private object ClusterWatcherNoClusterWatcheeSpec {
  final case class WatchIt(watchee: ActorRef)
  case object Ack
  final case class WrappedTerminated(t: Terminated)

  class Listener(testActor: ActorRef) extends Actor {
    def receive: Receive = {
      case WatchIt(watchee) =>
        context.watch(watchee)
        sender() ! Ack
      case t: Terminated =>
        testActor.forward(WrappedTerminated(t))
    }
  }
}

abstract class ClusterWatcherNoClusterWatcheeSpec(multiNodeConfig: ClusterWatcherNoClusterWatcheeConfig)
    extends MultiNodeClusterSpec(multiNodeConfig)
    with ImplicitSender
    with ScalaFutures {

  import ClusterWatcherNoClusterWatcheeSpec._
  import multiNodeConfig._

  override def initialParticipants: Int = roles.size

  muteDeadLetters(Heartbeat.getClass)()

  protected val probe = TestProbe()

  protected def identify(role: RoleName, actorName: String, within: FiniteDuration = 10.seconds): ActorRef =
    identifyWithPath(role, "user", actorName, within)

  protected def identifyWithPath(
      role: RoleName,
      path: String,
      actorName: String,
      within: FiniteDuration = 10.seconds): ActorRef = {
    system.actorSelection(node(role) / path / actorName) ! Identify(actorName)
    val id = expectMsgType[ActorIdentity](within)
    assert(id.ref.isDefined, s"Unable to Identify actor [$actorName] on node [$role].")
    id.ref.get
  }

  private val provider = RARP(system).provider

  s"Remoting with UseUnsafeRemoteFeaturesWithoutCluster enabled=$useUnsafe, " +
  "watcher system using `cluster`, but watchee system using `remote`" must {

    val send = if (system.settings.HasCluster || (!system.settings.HasCluster && useUnsafe)) "send" else "not send"

    s"$send `Watch`/`Unwatch`/`Terminate` when watching from cluster to non-cluster remoting watchee" in {
      runOn(remoting) {
        system.actorOf(Props(classOf[Listener], probe.ref), "watchee")
        enterBarrier("watchee-created")
        enterBarrier("watcher-created")
      }

      runOn(clustered) {
        enterBarrier("watchee-created")
        val watcher = system.actorOf(Props(classOf[Listener], probe.ref), "watcher")
        enterBarrier("watcher-created")

        val watchee = identify(remoting, "watchee")
        probe.send(watcher, WatchIt(watchee))
        probe.expectMsg(1.second, Ack)
        provider.remoteWatcher.get ! Stats
        awaitAssert(expectMsgType[Stats].watchingRefs == Set((watchee, watcher)), 2.seconds)
      }
      enterBarrier("cluster-watching-remote")

      runOn(remoting) {
        system.stop(identify(remoting, "watchee"))
        enterBarrier("watchee-stopped")
      }

      runOn(clustered) {
        enterBarrier("watchee-stopped")
        if (useUnsafe)
          probe.expectMsgType[WrappedTerminated](2.seconds)
        else
          probe.expectNoMessage(2.seconds)
      }
    }

    s"$send `Watch`/`Unwatch`/`Terminate` when watching from non-cluster remoting to cluster watchee" in {
      runOn(clustered) {
        system.actorOf(Props(classOf[Listener], probe.ref), "watchee2")
        enterBarrier("watchee2-created")
        enterBarrier("watcher2-created")
      }

      runOn(remoting) {
        enterBarrier("watchee2-created")
        val watchee = identify(clustered, "watchee2")

        val watcher = system.actorOf(Props(classOf[Listener], probe.ref), "watcher2")
        enterBarrier("watcher2-created")

        probe.send(watcher, WatchIt(watchee))
        probe.expectMsg(1.second, Ack)

        if (useUnsafe) {
          provider.remoteWatcher.get ! Stats
          awaitAssert(expectMsgType[Stats].watchingRefs == Set((watchee, watcher)), 2.seconds)
        }
      }

      runOn(clustered) {
        system.stop(identify(clustered, "watchee2"))
        enterBarrier("watchee2-stopped")
      }

      runOn(remoting) {
        enterBarrier("watchee2-stopped")
        if (useUnsafe)
          probe.expectMsgType[WrappedTerminated](2.seconds)
        else
          probe.expectNoMessage(2.seconds)
      }

      enterBarrier("done")
    }
  }
}
