/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures

import akka.actor._
import akka.cluster.MultiNodeClusterSpec.EndActor
import akka.remote.RemoteActorRef
import akka.remote.RemoteWatcher
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._
import akka.testkit.TestEvent._

object ClusterDeathWatchMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      # test is using Java serialization and not priority to rewrite
      akka.actor.allow-java-serialization = on
      akka.actor.warn-about-java-serializer-usage = off
      """))
      .withFallback(MultiNodeClusterSpec.clusterConfigWithFailureDetectorPuppet))

  deployOn(fourth, """/hello.remote = "@first@" """)

  class Hello extends Actor {
    def receive = Actor.emptyBehavior
  }

}

class ClusterDeathWatchMultiJvmNode1 extends ClusterDeathWatchSpec
class ClusterDeathWatchMultiJvmNode2 extends ClusterDeathWatchSpec
class ClusterDeathWatchMultiJvmNode3 extends ClusterDeathWatchSpec
class ClusterDeathWatchMultiJvmNode4 extends ClusterDeathWatchSpec
class ClusterDeathWatchMultiJvmNode5 extends ClusterDeathWatchSpec

abstract class ClusterDeathWatchSpec
    extends MultiNodeClusterSpec(ClusterDeathWatchMultiJvmSpec)
    with ImplicitSender
    with ScalaFutures {

  import ClusterDeathWatchMultiJvmSpec._

  override def atStartup(): Unit = {
    super.atStartup()
    if (!log.isDebugEnabled) {
      muteMarkingAsUnreachable()
      system.eventStream.publish(Mute(EventFilter[java.net.UnknownHostException]()))
    }
  }

  lazy val remoteWatcher: ActorRef = {
    system.actorSelection("/system/remote-watcher") ! Identify(None)
    expectMsgType[ActorIdentity].ref.get
  }

  // only assigned on node 1
  private var subject6: ActorRef = _

  "An actor watching a remote actor in the cluster" must {

    "receive Terminated when watched node becomes Down/Removed" in within(20 seconds) {
      awaitClusterUp(first, second, third, fourth)
      enterBarrier("cluster-up")

      runOn(first) {
        enterBarrier("subjected-started")

        val path2 = RootActorPath(second) / "user" / "subject"
        val path3 = RootActorPath(third) / "user" / "subject"
        val watchEstablished = TestLatch(2)
        system.actorOf(Props(new Actor {
          context.actorSelection(path2) ! Identify(path2)
          context.actorSelection(path3) ! Identify(path3)

          def receive = {
            case ActorIdentity(`path2`, Some(ref)) =>
              context.watch(ref)
              watchEstablished.countDown()
            case ActorIdentity(`path3`, Some(ref)) =>
              context.watch(ref)
              watchEstablished.countDown()
            case Terminated(actor) => testActor ! actor.path
          }
        }).withDeploy(Deploy.local), name = "observer1")

        watchEstablished.await
        enterBarrier("watch-established")
        expectMsg(path2)
        expectNoMessage(2.seconds)
        enterBarrier("second-terminated")
        awaitAssert(clusterView.members.map(_.address) should not contain address(second))

        markNodeAsUnavailable(third)
        awaitAssert(clusterView.unreachableMembers.map(_.address) should contain(address(third)))
        cluster.down(third)
        // removed
        awaitAssert(clusterView.members.map(_.address) should not contain address(third))
        awaitAssert(clusterView.unreachableMembers.map(_.address) should not contain address(third))
        expectMsg(path3)
        enterBarrier("third-terminated")

      }

      runOn(second, third, fourth) {
        system.actorOf(
          Props(new Actor { def receive = Actor.emptyBehavior }).withDeploy(Deploy.local),
          name = "subject")
        enterBarrier("subjected-started")
        enterBarrier("watch-established")
        runOn(third) {
          markNodeAsUnavailable(second)
          awaitAssert(clusterView.unreachableMembers.map(_.address) should contain(address(second)))
          cluster.down(second)
          // removed
          awaitAssert(clusterView.members.map(_.address) should not contain address(second))
          awaitAssert(clusterView.unreachableMembers.map(_.address) should not contain address(second))
        }
        enterBarrier("second-terminated")
        enterBarrier("third-terminated")
        runOn(fourth) {
          awaitAssert(clusterView.members.map(_.address) should not contain address(second))
          awaitAssert(clusterView.members.map(_.address) should not contain address(third))
        }
      }

      runOn(fifth) {
        enterBarrier("subjected-started")
        enterBarrier("watch-established")
        enterBarrier("second-terminated")
        enterBarrier("third-terminated")
      }

      enterBarrier("after-1")

    }

    "not be able to watch an actor before node joins cluster, ClusterRemoteWatcher takes over from RemoteWatcher" in within(
      20 seconds) {
      runOn(fifth) {
        system.actorOf(
          Props(new Actor { def receive = Actor.emptyBehavior }).withDeploy(Deploy.local),
          name = "subject5")
      }
      enterBarrier("subjected-started")

      runOn(first) {
        system.actorSelection(RootActorPath(fifth) / "user" / "subject5") ! Identify("subject5")
        val subject5 = expectMsgType[ActorIdentity].ref.get
        watch(subject5)

        // fifth is not cluster member, watch is dropped
        awaitAssert {
          remoteWatcher ! RemoteWatcher.Stats
          expectMsg(RemoteWatcher.Stats.empty)
        }
      }

      // second and third are already removed
      awaitClusterUp(first, fourth, fifth)

      runOn(fifth) {
        // fifth is a member, the watch for subject5 previously deployed would not be in
        // RemoteWatcher. Therefore we create a new one to test that now, being a member,
        // will be in RemoteWatcher.
        system.actorOf(
          Props(new Actor { def receive = Actor.emptyBehavior }).withDeploy(Deploy.local),
          name = "subject6")
      }
      enterBarrier("subject6-started")

      runOn(first) {
        // fifth is member, so the node is handled by the ClusterRemoteWatcher.
        system.actorSelection(RootActorPath(fifth) / "user" / "subject6") ! Identify("subject6")
        subject6 = expectMsgType[ActorIdentity].ref.get
        watch(subject6)

        system.actorSelection(RootActorPath(fifth) / "user" / "subject5") ! Identify("subject5")
        val subject5 = expectMsgType[ActorIdentity].ref.get

        awaitAssert {
          remoteWatcher ! RemoteWatcher.Stats
          val stats = expectMsgType[RemoteWatcher.Stats]
          stats.watchingRefs should contain(subject6 -> testActor)
          stats.watchingRefs should not contain (subject5 -> testActor)
          stats.watching shouldEqual 1
          stats.watchingAddresses should not contain address(fifth)
        }
      }
      enterBarrier("remote-watch")
      enterBarrier("cluster-watch")

      runOn(fourth) {
        markNodeAsUnavailable(fifth)
        awaitAssert(clusterView.unreachableMembers.map(_.address) should contain(address(fifth)))
        cluster.down(fifth)
        // removed
        awaitAssert(clusterView.unreachableMembers.map(_.address) should not contain (address(fifth)))
        awaitAssert(clusterView.members.map(_.address) should not contain (address(fifth)))
      }

      enterBarrier("fifth-terminated")
      runOn(first) {
        // subject5 is not in RemoteWatcher.watching, the terminated for subject5 is from testActor.watch.
        // You can not verify that it is the testActor receiving it, though the remoteWatcher stats proves
        // it above
        receiveWhile(messages = 2) {
          case Terminated(ref) => ref.path.name
        }.toSet shouldEqual Set("subject5", "subject6")

        awaitAssert {
          remoteWatcher ! RemoteWatcher.Stats
          expectMsg(RemoteWatcher.Stats.empty)
        }
      }
      enterBarrier("terminated-subject6")
      enterBarrier("after-3")
    }

    "get a terminated when trying to watch actor after node was downed" in within(max = 20.seconds) {
      runOn(first) {
        // node 5 is long gone (downed)
        watch(subject6)
        expectTerminated(subject6, 10.seconds)
      }
      enterBarrier("after-4")
    }

    "be able to shutdown system when using remote deployed actor on node that crash" in within(20 seconds) {
      // fourth actor system will be shutdown, not part of testConductor any more
      // so we can't use barriers to synchronize with it
      val firstAddress = address(first)
      runOn(first) {
        system.actorOf(Props(classOf[EndActor], testActor, None), "end")
      }
      enterBarrier("end-actor-created")

      runOn(fourth) {
        val hello = system.actorOf(Props[Hello](), "hello")
        hello.isInstanceOf[RemoteActorRef] should ===(true)
        hello.path.address should ===(address(first))
        watch(hello)
        enterBarrier("hello-deployed")

        markNodeAsUnavailable(first)
        awaitAssert(clusterView.unreachableMembers.map(_.address) should contain(address(first)))
        cluster.down(first)
        // removed
        awaitAssert(clusterView.unreachableMembers.map(_.address) should not contain (address(first)))
        awaitAssert(clusterView.members.map(_.address) should not contain (address(first)))

        expectTerminated(hello)

        enterBarrier("first-unavailable")

        val timeout = remainingOrDefault
        try Await.ready(system.whenTerminated, timeout)
        catch {
          case _: TimeoutException =>
            fail(
              "Failed to stop [%s] within [%s] \n%s"
                .format(system.name, timeout, system.asInstanceOf[ActorSystemImpl].printTree))
        }

        // signal to the first node that fourth is done
        val endSystem = ActorSystem("EndSystem", system.settings.config)
        try {
          val endProbe = TestProbe()(endSystem)
          val endActor = endSystem.actorOf(Props(classOf[EndActor], endProbe.ref, Some(firstAddress)), "end")
          endActor ! EndActor.SendEnd
          endProbe.expectMsg(EndActor.EndAck)

        } finally {
          shutdown(endSystem, 10 seconds)
        }
        // no barrier here, because it is not part of testConductor roles any more

      }

      runOn(first, second, third, fifth) {
        enterBarrier("hello-deployed")
        enterBarrier("first-unavailable")

        // don't end the test until the fourth is done
        runOn(first) {
          // fourth system will be shutdown, remove to not participate in barriers any more
          testConductor.shutdown(fourth).await
          expectMsg(EndActor.End)
        }

        enterBarrier("after-5")
      }

    }

  }
}
