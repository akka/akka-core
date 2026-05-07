/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.ActorSystemImpl
import akka.actor.Address
import akka.actor.AddressFromURIString
import akka.actor.Identify
import akka.actor.Nobody
import akka.actor.PoisonPill
import akka.actor.Props
import akka.remote.RemoteNodeDeathWatchSpec.UnwatchIt
import akka.remote.RemoteNodeDeathWatchSpec.WatchIt
import akka.remote.RemoteWatcher.Stats
import akka.remote.routing.RemoteRouterConfig
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.routing.Broadcast
import akka.routing.FromConfig
import akka.routing.RoundRobinGroup
import akka.routing.RoundRobinPool
import akka.routing.RoutedActorRef
import akka.testkit.TestProbe

class RemotingFeaturesConfig(val useUnsafe: Boolean) extends MultiNodeConfig {

  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  val workerInstances = 3
  val iterationCount = 10

  protected val baseConfig = ConfigFactory.parseString(s"""
      akka.remote.use-unsafe-remote-features-outside-cluster = $useUnsafe
      """).withFallback(RemotingMultiNodeSpec.commonConfig)

  commonConfig(debugConfig(on = false).withFallback(baseConfig))

  deployOnAll(s"""
      /sampleActor {
        remote = "@second@"
      }
      /service-hello {
        router = round-robin-pool
        nr-of-instances = $workerInstances
        target.nodes = ["@first@", "@second@", "@third@"]
      }

      /service-hello2 {
        router = round-robin-pool
        target.nodes = ["@first@", "@second@", "@third@"]
      }

      /service-hello3 {
        router = round-robin-group
        routees.paths = [
          "@first@/user/target-first",
          "@second@/user/target-second",
          "@third@/user/target-third"]
      }
    """)
}

class RemotingFeaturesSafeMultiJvmNode1 extends RemotingFeaturesSafeSpec
class RemotingFeaturesSafeMultiJvmNode2 extends RemotingFeaturesSafeSpec
class RemotingFeaturesSafeMultiJvmNode3 extends RemotingFeaturesSafeSpec
class RemotingFeaturesSafeMultiJvmNode4 extends RemotingFeaturesSafeSpec

class RemotingFeaturesUnsafeMultiJvmNode1 extends RemotingFeaturesUnsafeSpec
class RemotingFeaturesUnsafeMultiJvmNode2 extends RemotingFeaturesUnsafeSpec
class RemotingFeaturesUnsafeMultiJvmNode3 extends RemotingFeaturesUnsafeSpec
class RemotingFeaturesUnsafeMultiJvmNode4 extends RemotingFeaturesUnsafeSpec

abstract class RemotingFeaturesSafeSpec extends RemotingFeaturesSpec(new RemotingFeaturesConfig(useUnsafe = false)) {

  import RemoteNodeDeathWatchSpec.ProbeActor
  import multiNodeConfig._

  "Remoting without Cluster" must {

    "not intercept and send system messages `Watch`/`Unwatch` to `RemoteWatcher` in the provider" in {
      runOn(second) {
        val watchee = system.actorOf(Props(classOf[ProbeActor], probe.ref), "watchee")
        enterBarrier("started")
        assertWatchNotIntercepted(identify(first, "watcher"), watchee, first)
      }
      runOn(first) {
        val watcher = system.actorOf(Props(classOf[ProbeActor], probe.ref), "watcher")
        enterBarrier("started")
        assertWatchNotIntercepted(watcher, identify(second, "watchee"), second)
      }
      runOn(third, fourth) {
        enterBarrier("started")
        enterBarrier("watch-not-intercepted")
      }
      def assertWatchNotIntercepted(watcher: ActorRef, watchee: ActorRef, remoteRole: RoleName): Unit = {
        val rar = remoteActorRef(remoteRole)
        rar.isWatchIntercepted(watchee = rar, watcher = watcher) shouldBe false
        rar.isWatchIntercepted(watchee = watchee, watcher = watcher) shouldBe false
        enterBarrier("watch-not-intercepted")
      }
    }

    "not create a remote actor from deployment config when remote features are disabled" in {
      runOn(first) {
        val actor = system.actorOf(Props(classOf[ProbeActor], probe.ref), "sampleActor")
        actor ! Identify(1)
        expectMsgType[ActorIdentity].ref.get.path.address.hasGlobalScope shouldBe false
      }
    }

    "not receive Terminated on stop with watch attempt" in {
      runOn(second) {
        system.actorOf(Props(classOf[ProbeActor], probe.ref), "terminating")
      }
      enterBarrier("terminating-started")
      runOn(first) {
        val watcher = system.actorOf(Props(classOf[ProbeActor], probe.ref), "watch-terminating")
        val terminating = identify(second, "terminating")
        watcher ! WatchIt(terminating)
      }
      enterBarrier("watch-t-attempted")

      runOn(second) {
        val terminating = identify(second, "terminating")
        system.stop(terminating)
      }
      enterBarrier("t-stopped")

      runOn(first) {
        probe.expectNoMessage(2.seconds)
      }
    }
  }
}

abstract class RemotingFeaturesUnsafeSpec extends RemotingFeaturesSpec(new RemotingFeaturesConfig(useUnsafe = true)) {

  import RemoteNodeDeathWatchSpec.Ack
  import RemoteNodeDeathWatchSpec.DeathWatchIt
  import RemoteNodeDeathWatchSpec.ProbeActor
  import RemoteNodeDeathWatchSpec.WatchIt
  import multiNodeConfig._

  def stats(watcher: ActorRef, message: DeathWatchIt): Stats = {
    probe.send(watcher, message)
    probe.expectMsg(1.second, Ack)
    provider.remoteWatcher.get ! Stats
    expectMsgType[Stats]
  }

  "Remoting with UseUnsafeRemoteFeaturesWithoutCluster enabled" must {

    "intercept and send system messages `Watch`/`Unwatch` to `RemoteWatcher` in the provider" in {
      runOn(second) {
        val watchee = system.actorOf(Props(classOf[ProbeActor], probe.ref), "watchee")
        enterBarrier("watchee-started")
        enterBarrier("watcher-started")
        assertWatchIntercepted(identify(first, "watcher"), watchee, first)
      }
      runOn(first) {
        enterBarrier("watchee-started")
        val watcher = system.actorOf(Props(classOf[ProbeActor], probe.ref), "watcher")
        enterBarrier("watcher-started")
        assertWatchIntercepted(watcher, identify(second, "watchee"), second)
      }
      runOn(third, fourth) {
        enterBarrier("watchee-started")
        enterBarrier("watcher-started")
        enterBarrier("watch-intercepted")
        enterBarrier("watch-not-intercepted")
      }
      def assertWatchIntercepted(watcher: ActorRef, watchee: ActorRef, remoteRole: RoleName): Unit = {
        val remoteWatcher = identifyWithPath(remoteRole, "system", "remote-watcher")
        val rar = remoteActorRef(remoteRole)
        rar.isWatchIntercepted(watchee = rar, watcher = watcher) shouldBe true
        rar.isWatchIntercepted(watchee = rar, watcher = remoteWatcher) shouldBe true
        enterBarrier("watch-intercepted")

        rar.isWatchIntercepted(watchee = watchee, watcher = rar) shouldBe false
        enterBarrier("watch-not-intercepted")
      }
    }

    "create a remote actor from deployment config when remote features are disabled" in {
      runOn(first) {
        val secondAddress = node(second).address
        val actor = system.actorOf(Props(classOf[ProbeActor], probe.ref), "sampleActor")
        actor.path.address shouldEqual secondAddress
        actor.isInstanceOf[RemoteActorRef] shouldBe true
        actor.path.address.hasGlobalScope shouldBe true
      }
      enterBarrier("remote-actorOf-validated")
    }

    "`Watch` and `Unwatch` from `RemoteWatcher`" in {
      runOn(first) {
        val watcher = identify(first, "watcher")
        val watchee = identify(second, "watchee")
        awaitAssert(stats(watcher, WatchIt(watchee)).watchingRefs == Set((watchee, watcher)), 2.seconds)
        enterBarrier("system-message1-received-by-remoteWatcher")

        awaitAssert(stats(watcher, UnwatchIt(watchee)).watching == 0, 2.seconds)
        enterBarrier("system-message2-received-by-remoteWatcher")
      }
      runOn(second, third, fourth) {
        enterBarrier("system-message1-received-by-remoteWatcher")
        enterBarrier("system-message2-received-by-remoteWatcher")
      }
      enterBarrier("done")
    }
  }
}

abstract class RemotingFeaturesSpec(val multiNodeConfig: RemotingFeaturesConfig)
    extends RemotingMultiNodeSpec(multiNodeConfig) {

  import RemoteWatcher._
  import multiNodeConfig._

  import akka.remote.routing.RemoteRoundRobinSpec._

  override def initialParticipants: Int = roles.size

  muteDeadLetters(Heartbeat.getClass)()

  protected val probe = TestProbe()

  protected val provider: RemoteActorRefProvider = RARP(system).provider

  protected def remoteActorRef(role: RoleName): RemoteActorRef = {
    val remotePath = node(role)
    val rar = new RemoteActorRef(
      provider.transport,
      provider.transport.localAddressForRemote(remotePath.address),
      remotePath,
      Nobody,
      None,
      None)

    rar.start()
    rar
  }

  protected def identify(role: RoleName, actorName: String, within: FiniteDuration = 10.seconds): ActorRef =
    identifyWithPath(role, "user", actorName, within)

  protected def identifyWithPath(
      role: RoleName,
      path: String,
      actorName: String,
      within: FiniteDuration = 10.seconds): ActorRef = {
    system.actorSelection(node(role) / path / actorName) ! Identify(actorName)
    val id = expectMsgType[ActorIdentity](within)
    assert(id.ref.isDefined, s"Unable to Identify actor [$actorName] on node [$role]")
    id.ref.get
  }

  "A remote round robin group" must {
    "send messages to remote paths" in {

      runOn(first, second, third) {
        system.actorOf(Props[SomeActor](), name = "target-" + myself.name)
        enterBarrier("start", "end")
      }

      runOn(fourth) {
        enterBarrier("start")
        val actor = system.actorOf(FromConfig.props(), "service-hello3")
        actor.isInstanceOf[RoutedActorRef] should ===(true)

        for (_ <- 0 until iterationCount; _ <- 0 until workerInstances) {
          actor ! "hit"
        }

        val replies: Map[Address, Int] = receiveWhile(5.seconds, messages = workerInstances * iterationCount) {
          case ref: ActorRef => ref.path.address
        }.foldLeft(Map(node(first).address -> 0, node(second).address -> 0, node(third).address -> 0)) {
          case (replyMap, address) => replyMap + (address -> (replyMap(address) + 1))
        }

        enterBarrier("end")
        replies.values.forall(_ == iterationCount) shouldBe true
        replies.get(node(fourth).address) should ===(None)
      }

      enterBarrier("finished")
    }
  }

  "A remote round robin pool" must {
    s"${if (useUnsafe) "be instantiated on remote node and communicate through its RemoteActorRef"
    else "not be instantiated on remote node and communicate through its LocalActorRef "} " in {

      runOn(first, second, third) {
        enterBarrier("start", "broadcast-end", "end")
      }

      runOn(fourth) {
        enterBarrier("start")
        val actor = system.actorOf(RoundRobinPool(nrOfInstances = 0).props(Props[SomeActor]()), "service-hello")
        actor.isInstanceOf[RoutedActorRef] should ===(true)

        for (_ <- 0 until iterationCount; _ <- 0 until workerInstances) {
          actor ! "hit"
        }

        val replies = receiveWhile(5.seconds, messages = workerInstances * iterationCount) {
          case ref: ActorRef => ref.path.address
        }.foldLeft(Map(node(first).address -> 0, node(second).address -> 0, node(third).address -> 0)) {
          case (replyMap, address) =>
            if (useUnsafe) address.hasLocalScope shouldBe false
            else address.hasLocalScope shouldBe true
            replyMap + (address -> (replyMap.getOrElse(address, 0) + 1))
        }

        if (useUnsafe) {
          enterBarrier("broadcast-end")
          actor ! Broadcast(PoisonPill)

          enterBarrier("end")
          replies.values.foreach { _ should ===(iterationCount) }
        } else {
          enterBarrier("broadcast-end")
          actor ! Broadcast(PoisonPill)

          enterBarrier("end")
          val (local, remote) = replies.partition { case (address, _) => address.hasLocalScope }
          local.size shouldEqual 1
          remote.size shouldEqual 3
          val others = Set(first, second, third).map(node(_).address)
          remote.forall { case (address, _) => others.contains(address) } shouldBe true
          remote.values.forall(_ == 0) shouldBe true
          local.values.foreach(_ should ===(iterationCount * remote.size))
        }
        replies.get(node(fourth).address) should ===(None)
        system.stop(actor)
      }
    }
  }

  s"Deploy routers with expected behavior if 'akka.remote.use-unsafe-remote-features-outside-cluster=$useUnsafe'" must {
    "deployments" in {
      runOn(first, second, third, fourth) {
        val deployment1 = system.asInstanceOf[ActorSystemImpl].provider.deployer.lookup(List("service-hello"))
        val deployment2 = system.asInstanceOf[ActorSystemImpl].provider.deployer.lookup(List("service-hello2"))
        val deployment3 = system.asInstanceOf[ActorSystemImpl].provider.deployer.lookup(List("service-hello3"))
        val deploys = Set(deployment1, deployment2, deployment3)
        deploys.flatten.size shouldEqual 3
        deploys.foreach { deploy =>
          deploy.get.routerConfig match {
            case RemoteRouterConfig(pool, nodes) =>
              pool.nrOfInstances(system) == workerInstances && nodes.size == 3 && !nodes.forall(_.hasLocalScope)
            case RoundRobinGroup(paths, _) =>
              paths.size == 3 && !paths.forall(AddressFromURIString(_).hasLocalScope)
            case _ =>
          }
        }
      }
    }
  }
}
