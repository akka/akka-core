/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.actor.Actor
import akka.actor.ActorIdentity
import akka.actor.ActorRef
import akka.actor.Identify
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Terminated
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.testkit._

object RemoteNodeDeathWatchConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString(
        """
      akka.loglevel = INFO
      ## Use a tighter setting than the default, otherwise it takes 20s for DeathWatch to trigger
      akka.remote.watch-failure-detector.acceptable-heartbeat-pause = 3 s
      akka.remote.use-unsafe-remote-features-outside-cluster = on
      """))
      .withFallback(RemotingMultiNodeSpec.commonConfig))

}

// Several different variations of the test

class RemoteNodeDeathWatchFastMultiJvmNode1 extends RemoteNodeDeathWatchFastSpec
class RemoteNodeDeathWatchFastMultiJvmNode2 extends RemoteNodeDeathWatchFastSpec
class RemoteNodeDeathWatchFastMultiJvmNode3 extends RemoteNodeDeathWatchFastSpec

abstract class RemoteNodeDeathWatchFastSpec extends RemoteNodeDeathWatchSpec {
  override def scenario = "fast"
}

class RemoteNodeDeathWatchSlowMultiJvmNode1 extends RemoteNodeDeathWatchSlowSpec
class RemoteNodeDeathWatchSlowMultiJvmNode2 extends RemoteNodeDeathWatchSlowSpec
class RemoteNodeDeathWatchSlowMultiJvmNode3 extends RemoteNodeDeathWatchSlowSpec

abstract class RemoteNodeDeathWatchSlowSpec extends RemoteNodeDeathWatchSpec {
  override def scenario = "slow"
  override def sleep(): Unit = Thread.sleep(3000)
}

object RemoteNodeDeathWatchSpec {
  sealed trait DeathWatchIt
  final case class WatchIt(watchee: ActorRef) extends DeathWatchIt
  final case class UnwatchIt(watchee: ActorRef) extends DeathWatchIt
  case object Ack

  /**
   * Forwarding `Terminated` to non-watching testActor is not possible,
   * and therefore the `Terminated` message is wrapped.
   */
  final case class WrappedTerminated(t: Terminated)

  class ProbeActor(testActor: ActorRef) extends Actor {
    def receive = {
      case WatchIt(watchee) =>
        context.watch(watchee)
        sender() ! Ack
      case UnwatchIt(watchee) =>
        context.unwatch(watchee)
        sender() ! Ack
      case t: Terminated =>
        testActor.forward(WrappedTerminated(t))
      case msg => testActor.forward(msg)
    }
  }
}

abstract class RemoteNodeDeathWatchSpec extends RemotingMultiNodeSpec(RemoteNodeDeathWatchConfig) {
  import RemoteNodeDeathWatchConfig._
  import RemoteNodeDeathWatchSpec._
  import RemoteWatcher._

  def scenario: String
  // Possible to override to let them heartbeat for a while.
  def sleep(): Unit = ()

  override def initialParticipants = roles.size

  muteDeadLetters(Heartbeat.getClass)()

  lazy val remoteWatcher: ActorRef = {
    system.actorSelection("/system/remote-watcher") ! Identify(None)
    expectMsgType[ActorIdentity].ref.get
  }

  def identify(role: RoleName, actorName: String): ActorRef = {
    system.actorSelection(node(role) / "user" / actorName) ! Identify(actorName)
    val actorIdentity = expectMsgType[ActorIdentity]
    assert(actorIdentity.ref.isDefined, s"Unable to Identify actor: $actorName on node: $role")
    actorIdentity.ref.get
  }

  def assertCleanup(timeout: FiniteDuration = 5.seconds): Unit = {
    within(timeout) {
      awaitAssert {
        remoteWatcher ! Stats
        expectMsg(Stats.empty)
      }
    }
  }

  "RemoteNodeDeathWatch (" + scenario + ")" must {

    "receive Terminated when remote actor is stopped" in {
      runOn(first) {
        val watcher = system.actorOf(Props(classOf[ProbeActor], testActor), "watcher1")
        enterBarrier("actors-started-1")

        val subject = identify(second, "subject1")
        watcher ! WatchIt(subject)
        expectMsg(1 second, Ack)
        subject ! "hello1"
        enterBarrier("hello1-message-sent")
        enterBarrier("watch-established-1")

        sleep()
        expectMsgType[WrappedTerminated].t.actor should ===(subject)
      }

      runOn(second) {
        val subject = system.actorOf(Props(classOf[ProbeActor], testActor), "subject1")
        enterBarrier("actors-started-1")

        enterBarrier("hello1-message-sent")
        expectMsg(3 seconds, "hello1")
        enterBarrier("watch-established-1")

        sleep()
        system.stop(subject)
      }

      runOn(third) {
        enterBarrier("actors-started-1")
        enterBarrier("hello1-message-sent")
        enterBarrier("watch-established-1")
      }

      enterBarrier("terminated-verified-1")

      // verify that things are cleaned up, and heartbeating is stopped
      assertCleanup()
      expectNoMessage(2.seconds)
      assertCleanup()

      enterBarrier("after-1")
    }

    "cleanup after watch/unwatch" in {
      runOn(first) {
        val watcher = system.actorOf(Props(classOf[ProbeActor], testActor), "watcher2")
        enterBarrier("actors-started-2")

        val subject = identify(second, "subject2")
        watcher ! WatchIt(subject)
        expectMsg(1 second, Ack)
        enterBarrier("watch-2")

        sleep()
        watcher ! UnwatchIt(subject)
        expectMsg(1 second, Ack)
        enterBarrier("unwatch-2")
      }

      runOn(second) {
        system.actorOf(Props(classOf[ProbeActor], testActor), "subject2")
      }
      runOn(second, third) {
        enterBarrier("actors-started-2")
        enterBarrier("watch-2")
        enterBarrier("unwatch-2")
      }

      // verify that things are cleaned up, and heartbeating is stopped
      assertCleanup()
      expectNoMessage(2.seconds)
      assertCleanup()

      enterBarrier("after-2")
    }

    "cleanup after bi-directional watch/unwatch" in {
      runOn(first, second) {
        val watcher = system.actorOf(Props(classOf[ProbeActor], testActor), "watcher3")
        system.actorOf(Props(classOf[ProbeActor], testActor), "subject3")
        enterBarrier("actors-started-3")

        val other = if (myself == first) second else first
        val subject = identify(other, "subject3")
        watcher ! WatchIt(subject)
        expectMsg(1 second, Ack)
        enterBarrier("watch-3")

        sleep()
        watcher ! UnwatchIt(subject)
        expectMsg(1 second, Ack)
        enterBarrier("unwatch-3")
      }

      runOn(third) {
        enterBarrier("actors-started-3")
        enterBarrier("watch-3")
        enterBarrier("unwatch-3")
      }

      // verify that things are cleaned up, and heartbeating is stopped
      assertCleanup()
      expectNoMessage(2.seconds)
      assertCleanup()

      enterBarrier("after-3")
    }

    "cleanup after bi-directional watch/stop/unwatch" in {
      runOn(first, second) {
        val watcher1 = system.actorOf(Props(classOf[ProbeActor], testActor), "w1")
        val watcher2 = system.actorOf(Props(classOf[ProbeActor], testActor), "w2")
        val s1 = system.actorOf(Props(classOf[ProbeActor], testActor), "s1")
        val s2 = system.actorOf(Props(classOf[ProbeActor], testActor), "s2")
        enterBarrier("actors-started-4")

        val other = if (myself == first) second else first
        val subject1 = identify(other, "s1")
        val subject2 = identify(other, "s2")
        watcher1 ! WatchIt(subject1)
        expectMsg(1 second, Ack)
        watcher2 ! WatchIt(subject2)
        expectMsg(1 second, Ack)
        enterBarrier("watch-4")

        sleep()
        watcher1 ! UnwatchIt(subject1)
        expectMsg(1 second, Ack)
        enterBarrier("unwatch-s1-4")
        system.stop(s1)
        expectNoMessage(2 seconds)
        enterBarrier("stop-s1-4")

        system.stop(s2)
        enterBarrier("stop-s2-4")

        expectMsgType[WrappedTerminated].t.actor should ===(subject2)
      }

      runOn(third) {
        enterBarrier("actors-started-4")
        enterBarrier("watch-4")
        enterBarrier("unwatch-s1-4")
        enterBarrier("stop-s1-4")
        enterBarrier("stop-s2-4")
      }

      // verify that things are cleaned up, and heartbeating is stopped
      assertCleanup()
      expectNoMessage(2.seconds)
      assertCleanup()

      enterBarrier("after-4")
    }

    "cleanup after stop" in {
      runOn(first) {
        val p1, p2, p3 = TestProbe()
        val a1 = system.actorOf(Props(classOf[ProbeActor], p1.ref), "a1")
        val a2 = system.actorOf(Props(classOf[ProbeActor], p2.ref), "a2")
        val a3 = system.actorOf(Props(classOf[ProbeActor], p3.ref), "a3")
        enterBarrier("actors-started-5")

        val b1 = identify(second, "b1")
        val b2 = identify(second, "b2")
        val b3 = identify(second, "b3")

        a1 ! WatchIt(b1)
        expectMsg(1 second, Ack)
        a1 ! WatchIt(b2)
        expectMsg(1 second, Ack)
        a2 ! WatchIt(b2)
        expectMsg(1 second, Ack)
        a3 ! WatchIt(b3)
        expectMsg(1 second, Ack)
        sleep()
        a2 ! UnwatchIt(b2)
        expectMsg(1 second, Ack)

        enterBarrier("watch-established-5")

        sleep()
        a1 ! PoisonPill
        a2 ! PoisonPill
        a3 ! PoisonPill

        enterBarrier("stopped-5")
        enterBarrier("terminated-verified-5")

        // verify that things are cleaned up, and heartbeating is stopped
        assertCleanup()
        expectNoMessage(2.seconds)
        assertCleanup()
      }

      runOn(second) {
        val p1, p2, p3 = TestProbe()
        val b1 = system.actorOf(Props(classOf[ProbeActor], p1.ref), "b1")
        val b2 = system.actorOf(Props(classOf[ProbeActor], p2.ref), "b2")
        val b3 = system.actorOf(Props(classOf[ProbeActor], p3.ref), "b3")
        enterBarrier("actors-started-5")

        val a1 = identify(first, "a1")
        val a2 = identify(first, "a2")
        val a3 = identify(first, "a3")

        b1 ! WatchIt(a1)
        expectMsg(1 second, Ack)
        b1 ! WatchIt(a2)
        expectMsg(1 second, Ack)
        b2 ! WatchIt(a2)
        expectMsg(1 second, Ack)
        b3 ! WatchIt(a3)
        expectMsg(1 second, Ack)
        b3 ! WatchIt(a3)
        expectMsg(1 second, Ack)
        sleep()
        b2 ! UnwatchIt(a2)
        expectMsg(1 second, Ack)

        enterBarrier("watch-established-5")
        enterBarrier("stopped-5")

        p1.receiveN(2, 5 seconds).collect { case WrappedTerminated(t) => t.actor }.toSet should ===(Set(a1, a2))
        p3.expectMsgType[WrappedTerminated](5 seconds).t.actor should ===(a3)
        p2.expectNoMessage(2 seconds)
        enterBarrier("terminated-verified-5")

        // verify that things are cleaned up, and heartbeating is stopped
        assertCleanup()
        expectNoMessage(2.seconds)
        p1.expectNoMessage(100 millis)
        p2.expectNoMessage(100 millis)
        p3.expectNoMessage(100 millis)
        assertCleanup()
      }

      runOn(third) {
        enterBarrier("actors-started-5")
        enterBarrier("watch-established-5")
        enterBarrier("stopped-5")
        enterBarrier("terminated-verified-5")
      }

      enterBarrier("after-5")
    }

    "receive Terminated when watched node crash" in {
      runOn(first) {
        val watcher = system.actorOf(Props(classOf[ProbeActor], testActor), "watcher6")
        val watcher2 = system.actorOf(Props(classOf[ProbeActor], system.deadLetters))
        enterBarrier("actors-started-6")

        val subject = identify(second, "subject6")
        watcher ! WatchIt(subject)
        expectMsg(1 second, Ack)
        watcher2 ! WatchIt(subject)
        expectMsg(1 second, Ack)
        subject ! "hello6"

        // testing with this watch/unwatch of watcher2 to make sure that the unwatch doesn't
        // remove the first watch
        watcher2 ! UnwatchIt(subject)
        expectMsg(1 second, Ack)

        enterBarrier("watch-established-6")

        sleep()

        log.info("exit second")
        testConductor.exit(second, 0).await
        expectMsgType[WrappedTerminated](15 seconds).t.actor should ===(subject)

        // verify that things are cleaned up, and heartbeating is stopped
        assertCleanup()
        expectNoMessage(2.seconds)
        assertCleanup()
      }

      runOn(second) {
        system.actorOf(Props(classOf[ProbeActor], testActor), "subject6")
        enterBarrier("actors-started-6")

        expectMsg(3 seconds, "hello6")
        enterBarrier("watch-established-6")
      }

      runOn(third) {
        enterBarrier("actors-started-6")
        enterBarrier("watch-established-6")
      }

      enterBarrier("after-6")
    }

    "cleanup when watching node crash" in {
      runOn(third) {
        val watcher = system.actorOf(Props(classOf[ProbeActor], testActor), "watcher7")
        enterBarrier("actors-started-7")

        val subject = identify(first, "subject7")
        watcher ! WatchIt(subject)
        expectMsg(1 second, Ack)
        subject ! "hello7"
        enterBarrier("watch-established-7")
      }

      runOn(first) {
        system.actorOf(Props(classOf[ProbeActor], testActor), "subject7")
        enterBarrier("actors-started-7")

        expectMsg(3 seconds, "hello7")
        enterBarrier("watch-established-7")

        sleep()
        log.info("exit third")
        testConductor.exit(third, 0).await

        // verify that things are cleaned up, and heartbeating is stopped
        assertCleanup(20 seconds)
        expectNoMessage(2.seconds)
        assertCleanup()
      }

      enterBarrier("after-7")
    }

  }
}
