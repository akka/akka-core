/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import scala.collection.immutable.SortedSet
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.FSM
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.RootActorPath
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.UniqueAddress
import akka.testkit.AkkaSpec
import akka.testkit.EventFilter
import akka.testkit.TestActors
import akka.testkit.TestProbe

object ClusterSingletonManagerTakeOverSpec {

  /** Stands in for the singleton manager of another node, at the same path. */
  class FakeManager(probe: ActorRef) extends Actor {
    def receive = {
      case msg => probe.forward(msg)
    }
  }
}

/**
 * Verifies from which nodes the hand-over is accepted when becoming oldest.
 *
 * The nodes are, in age order, first, second, third and fourth. Only third runs a real singleton manager. The test plays
 * the managers of the other nodes, and tells third's manager in which order the older nodes are exiting. No node is
 * actually leaving, so the outcome doesn't depend on the order in which membership changes are gossiped.
 */
class ClusterSingletonManagerTakeOverSpec extends AkkaSpec("""
  akka.loglevel = INFO
  akka.actor.provider = cluster
  akka.remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 0
  }
  """) {

  import ClusterSingletonManager.Internal._
  import ClusterSingletonManager.Internal.OldestChangedBuffer.OldestChanged
  import ClusterSingletonManagerTakeOverSpec._

  private val firstSys = ActorSystem(system.name, system.settings.config)
  private val secondSys = ActorSystem(system.name, system.settings.config)
  private val thirdSys = ActorSystem(system.name, system.settings.config)
  private val fourthSys = ActorSystem(system.name, system.settings.config)
  // not joining the cluster
  private val nonMemberSys = ActorSystem(system.name, system.settings.config)
  private val systemsByAge = List(firstSys, secondSys, thirdSys, fourthSys)

  override def afterTermination(): Unit =
    (nonMemberSys :: systemsByAge.reverse).foreach(shutdown(_))

  private def exitingMember(sys: ActorSystem): Member =
    Cluster(sys).selfMember.copy(MemberStatus.Leaving).copy(MemberStatus.Exiting)

  private def oldest(sys: ActorSystem) = OldestChanged(Some(Cluster(sys).selfUniqueAddress))

  /** A previous incarnation, with the same address, that is not a member any more. */
  private def oldestPreviousIncarnation(sys: ActorSystem) = {
    val current = Cluster(sys).selfUniqueAddress
    OldestChanged(Some(UniqueAddress(current.address, current.longUid + 1)))
  }

  // the changes of oldest that third observes, for the two possible orders of observing the older nodes as exiting
  private val firstExitsBeforeSecond = List(oldest(secondSys), oldest(thirdSys))
  private val secondExitsBeforeFirst = List(oldest(thirdSys))

  /** The test's side of the singleton manager at another node. */
  private class Peer(sys: ActorSystem, name: String) {
    private val probe = TestProbe()(sys)
    private val ref = sys.actorOf(Props(new FakeManager(probe.ref)), name)
    private val thirdManager =
      sys.actorSelection(RootActorPath(Cluster(thirdSys).selfAddress) / "user" / name)

    def send(msg: Any): Unit = thirdManager.tell(msg, ref)
    def expectHandOverToMe(): Unit = probe.expectMsg(HandOverToMe)
    def expectNoHandOverToMe(max: FiniteDuration = 500.millis): Unit = probe.expectNoMessage(max)
    def expectTakeOverFromMe(): Unit = probe.expectMsg(TakeOverFromMe)
    def expectHandOverDone(): Unit = probe.expectMsg(HandOverDone)

    def handOver(): Unit = {
      send(HandOverInProgress)
      send(HandOverDone)
    }
  }

  /** A new singleton manager at third. */
  private class YoungerSetup(name: String) {
    val first = new Peer(firstSys, name)
    val second = new Peer(secondSys, name)
    val fourth = new Peer(fourthSys, name)

    // when false the test replies to the manager's request for the cluster state
    @volatile var replyWithClusterState = true
    @volatile private var exitingProgress: Future[Done] = _
    val transitions = TestProbe()(thirdSys)
    val manager = thirdSys.actorOf(
      Props(
        new ClusterSingletonManager(
          TestActors.blackholeProps,
          PoisonPill,
          ClusterSingletonManagerSettings(thirdSys).withRemovalMargin(1.minute)) {
          exitingProgress = memberExitingProgress.future
          override private[akka] def requestClusterState(): Unit =
            if (replyWithClusterState) super.requestClusterState()
        }),
      name)

    manager ! FSM.SubscribeTransitionCallBack(transitions.ref)
    transitions.fishForMessage(10.seconds, hint = "Younger") {
      case FSM.CurrentState(_, Younger)  => true
      case FSM.Transition(_, _, Younger) => true
      case _                             => false
    }

    /** The manager is told to exit, as from CoordinatedShutdown. */
    def selfExiting(): Unit = {
      val probe = TestProbe()(thirdSys)
      manager.tell(SelfExiting, probe.ref)
      probe.expectMsg(Done)
    }

    def exitingCompleted: Boolean = exitingProgress.isCompleted

    /** The manager sees that the node has been removed from the cluster. */
    def removed(sys: ActorSystem): Unit =
      manager ! MemberRemoved(exitingMember(sys).copy(MemberStatus.Removed), MemberStatus.Exiting)

    /** The manager sees that the removal margin has passed for the node, i.e. it's not previous oldest any more. */
    def removalMarginPassed(sys: ActorSystem): Unit =
      manager ! DelayedMemberRemoved(exitingMember(sys).copy(MemberStatus.Removed))
  }

  /** A new singleton manager at third, that has observed the given changes and is becoming oldest. */
  private class Setup(name: String, observed: List[OldestChanged], alreadyRemoved: List[ActorSystem] = Nil)
      extends YoungerSetup(name) {
    alreadyRemoved.foreach(removalMarginPassed)
    observed.foreach(manager ! _)
    transitions.expectMsg(FSM.Transition(manager, Younger, BecomingOldest))

    def expectOldest(): Unit = transitions.expectMsg(FSM.Transition(manager, BecomingOldest, Oldest))
    def expectStillBecomingOldest(): Unit = transitions.expectNoMessage(500.millis)

    /** The hand-over to third is done, but third is exiting so it should be handed over to fourth. */
    def expectHandOverToFourth(): Unit = {
      // longer than the hand-over-retry-interval
      transitions.expectNoMessage(1500.millis)
      exitingCompleted should ===(false)

      manager ! oldest(fourthSys)
      transitions.expectMsg(FSM.Transition(manager, BecomingOldest, WasOldest))
      fourth.expectTakeOverFromMe()
      exitingCompleted should ===(false)

      fourth.send(HandOverToMe)
      fourth.expectHandOverDone()
      transitions.expectMsg(FSM.Transition(manager, WasOldest, End))
      exitingCompleted should ===(true)
    }
  }

  "A ClusterSingletonManager becoming oldest" must {

    "be in a cluster with well defined ages" in {
      systemsByAge.zipWithIndex.foreach {
        case (sys, i) =>
          Cluster(sys).join(Cluster(firstSys).selfAddress)
          within(15.seconds) {
            awaitAssert {
              systemsByAge.take(i + 1).foreach { s =>
                val members = Cluster(s).state.members
                members.size should ===(i + 1)
                members.unsorted.map(_.status) should ===(Set[MemberStatus](MemberStatus.Up))
              }
            }
          }
      }
    }

    "take over from first, when first exits before second" in {
      val setup = new Setup("exit-1-2-holder-1", firstExitsBeforeSecond)
      setup.first.send(TakeOverFromMe)
      setup.first.expectHandOverToMe()
      setup.first.handOver()
      setup.expectOldest()
    }

    "take over from second, when first exits before second" in {
      val setup = new Setup("exit-1-2-holder-2", firstExitsBeforeSecond)
      setup.second.send(TakeOverFromMe)
      setup.second.expectHandOverToMe()
      setup.second.handOver()
      setup.expectOldest()
    }

    "take over from first, when second exits before first" in {
      val setup = new Setup("exit-2-1-holder-1", secondExitsBeforeFirst)
      setup.first.send(TakeOverFromMe)
      setup.first.expectHandOverToMe()
      setup.first.handOver()
      setup.expectOldest()
    }

    "take over from second, when second exits before first" in {
      val setup = new Setup("exit-2-1-holder-2", secondExitsBeforeFirst)
      setup.second.send(TakeOverFromMe)
      setup.second.expectHandOverToMe()
      setup.second.handOver()
      setup.expectOldest()
    }

    "take over from the oldest, when it's not the first of previous oldest" in {
      // second is dropped from the initial list [first, second], and then added first when it's observed as oldest
      val setup = new Setup("head-not-oldest", firstExitsBeforeSecond, alreadyRemoved = List(secondSys))
      setup.second.expectHandOverToMe()
      setup.first.send(TakeOverFromMe)
      setup.first.expectHandOverToMe()
      setup.first.handOver()
      setup.expectOldest()
    }

    "complete the hand-over even if another previous oldest also requested take over" in {
      val setup = new Setup("two-requests", secondExitsBeforeFirst)
      setup.second.send(TakeOverFromMe)
      setup.second.expectHandOverToMe()
      // e.g. delayed, sent before it handed over to second
      setup.first.send(TakeOverFromMe)
      setup.first.expectHandOverToMe()
      setup.second.handOver()
      setup.expectOldest()
    }

    "not take over from a node that has not been oldest before it" in {
      val setup = new Setup("not-listed", firstExitsBeforeSecond)
      EventFilter
        .info(start = "Ignoring TakeOver request", occurrences = 1)
        .intercept {
          setup.fourth.send(TakeOverFromMe)
        }(thirdSys)
      setup.fourth.expectNoHandOverToMe()
      EventFilter
        .info(start = "Ignoring HandOverDone", occurrences = 1)
        .intercept {
          setup.fourth.send(HandOverDone)
        }(thirdSys)
      setup.expectStillBecomingOldest()
    }

    "hand over to the next oldest, without starting the singleton, when exiting during hand-over" in {
      val setup = new Setup("exiting-confirmed", secondExitsBeforeFirst)
      setup.first.expectHandOverToMe()
      setup.first.send(HandOverInProgress)
      // confirmation was received when the HandOverToMe isn't retried any more
      setup.first.expectNoHandOverToMe(1500.millis)

      setup.selfExiting()
      // longer than the hand-over-retry-interval, since the previous singleton may take a while to stop
      setup.transitions.expectNoMessage(2.seconds)
      setup.exitingCompleted should ===(false)

      setup.first.send(HandOverDone)
      setup.expectHandOverToFourth()
    }

    "hand over to the next oldest when hand-over is confirmed after exiting" in {
      val setup = new Setup("exiting-late-confirm", secondExitsBeforeFirst)
      setup.first.expectHandOverToMe()
      setup.selfExiting()
      setup.exitingCompleted should ===(false)
      setup.first.handOver()
      setup.expectHandOverToFourth()
    }

    "hand over to the next oldest when take over is requested after exiting" in {
      val setup = new Setup("exiting-take-over", secondExitsBeforeFirst)
      setup.selfExiting()
      setup.second.send(TakeOverFromMe)
      setup.second.expectHandOverToMe()
      setup.second.handOver()
      setup.expectHandOverToFourth()
    }

    "hand over to the next oldest when hand-over is confirmed long after exiting" in {
      val setup = new Setup("exiting-very-late-confirm", secondExitsBeforeFirst)
      setup.first.expectHandOverToMe()
      setup.selfExiting()
      // several hand-over-retry-interval
      setup.transitions.expectNoMessage(3500.millis)
      setup.exitingCompleted should ===(false)
      setup.first.handOver()
      setup.expectHandOverToFourth()
    }

    "complete exiting when there is no previous oldest left that can hand over" in {
      val setup = new Setup("exiting-not-confirmed", secondExitsBeforeFirst)
      setup.first.expectHandOverToMe()
      setup.selfExiting()
      setup.removed(firstSys)
      // second could still hand over
      setup.transitions.expectNoMessage(500.millis)
      setup.exitingCompleted should ===(false)
      // not waiting for the removal margin or the timeout of the CoordinatedShutdown phase
      setup.removed(secondSys)
      setup.transitions.expectMsg(FSM.Transition(setup.manager, BecomingOldest, End))
      setup.exitingCompleted should ===(true)
    }

    // In these tests the removed nodes are still in cluster.state, which is updated asynchronously.
    "complete exiting when previous oldest were removed before exiting" in {
      val setup = new Setup("exiting-already-removed", secondExitsBeforeFirst)
      setup.first.expectHandOverToMe()
      setup.removed(firstSys)
      setup.selfExiting()
      // second could still hand over
      setup.transitions.expectNoMessage(500.millis)
      setup.exitingCompleted should ===(false)
      setup.removed(secondSys)
      setup.transitions.expectMsg(FSM.Transition(setup.manager, BecomingOldest, End))
      setup.exitingCompleted should ===(true)
    }

    "complete exiting when previous oldest were removed before becoming oldest" in {
      val setup = new YoungerSetup("exiting-removed-younger")
      setup.removed(firstSys)
      setup.removed(secondSys)
      setup.manager ! oldest(thirdSys)
      // the removal margin hasn't passed
      setup.transitions.expectMsg(FSM.Transition(setup.manager, Younger, BecomingOldest))
      setup.selfExiting()
      setup.transitions.expectMsg(FSM.Transition(setup.manager, BecomingOldest, End))
      setup.exitingCompleted should ===(true)
    }

    "not complete exiting when previous oldest is removed after its hand-over was done" in {
      val setup = new Setup("exiting-removed-after-done", secondExitsBeforeFirst)
      setup.first.expectHandOverToMe()
      setup.selfExiting()
      setup.first.handOver()
      setup.transitions.expectNoMessage(500.millis)
      setup.removed(firstSys)
      setup.removed(secondSys)
      setup.expectHandOverToFourth()
    }

    "not request hand-over when becoming oldest after exiting" in {
      val setup = new YoungerSetup("exiting-younger")
      setup.selfExiting()
      setup.exitingCompleted should ===(true)
      setup.manager ! oldest(thirdSys)
      setup.first.expectNoHandOverToMe()
      setup.transitions.expectNoMessage(500.millis)
    }

    "not request hand-over when becoming oldest while the own status is Exiting" in {
      val setup = new YoungerSetup("exiting-status")
      setup.replyWithClusterState = false
      setup.manager ! oldest(thirdSys)
      // Exiting in the same membership change, but that has not been seen via cluster.selfMember, and
      // CoordinatedShutdown hasn't reached the singleton
      setup.manager ! CurrentClusterState(members = SortedSet(exitingMember(thirdSys)))
      setup.first.expectNoHandOverToMe()
      setup.transitions.expectNoMessage(500.millis)
      // doesn't have to wait for anything
      setup.selfExiting()
      setup.exitingCompleted should ===(true)
    }

    "not request hand-over when exiting before the own status has been confirmed" in {
      val setup = new YoungerSetup("exiting-before-status")
      setup.replyWithClusterState = false
      setup.manager ! oldest(thirdSys)
      setup.selfExiting()
      setup.exitingCompleted should ===(true)
      setup.manager ! Cluster(thirdSys).state
      setup.first.expectNoHandOverToMe()
      setup.transitions.expectNoMessage(500.millis)
    }

    "request hand-over when the own status has been confirmed" in {
      val setup = new YoungerSetup("confirmed-status")
      setup.replyWithClusterState = false
      setup.manager ! oldest(thirdSys)
      setup.first.expectNoHandOverToMe()
      setup.manager ! Cluster(thirdSys).state
      setup.first.expectHandOverToMe()
      setup.first.handOver()
      setup.transitions.expectMsg(FSM.Transition(setup.manager, Younger, BecomingOldest))
      setup.transitions.expectMsg(FSM.Transition(setup.manager, BecomingOldest, Oldest))
    }

    "not take over when another incarnation confirms, but a previous oldest may still run the singleton" in {
      val setup = new Setup(
        "restarted-head",
        List(oldestPreviousIncarnation(firstSys), oldest(thirdSys)),
        alreadyRemoved = List(firstSys))
      setup.first.expectHandOverToMe()
      // "probably quickly restarted with same hostname:port", but second may have taken over from the old incarnation
      setup.first.send(HandOverDone)
      setup.expectStillBecomingOldest()

      setup.second.expectHandOverToMe()
      // and retries
      setup.second.expectHandOverToMe()
      setup.first.expectNoHandOverToMe()
      setup.second.handOver()
      setup.expectOldest()
    }

    "ask the next previous oldest again when another incarnation confirms after hand-over in progress" in {
      val setup = new Setup(
        "restarted-in-progress",
        List(oldestPreviousIncarnation(firstSys), oldest(thirdSys)),
        alreadyRemoved = List(firstSys))
      setup.second.send(TakeOverFromMe)
      setup.second.expectHandOverToMe()
      // stops the retries
      setup.second.send(HandOverInProgress)
      setup.first.send(HandOverDone)
      setup.expectStillBecomingOldest()
      setup.second.expectHandOverToMe()
      setup.second.handOver()
      setup.expectOldest()
    }

    "not take over when another incarnation, that isn't the first of previous oldest, confirms" in {
      val setup = new Setup(
        "restarted-not-head",
        List(oldestPreviousIncarnation(firstSys), oldest(nonMemberSys), oldest(thirdSys)),
        alreadyRemoved = List(firstSys))
      setup.first.send(HandOverDone)
      setup.expectStillBecomingOldest()
      setup.second.handOver()
      setup.expectOldest()
    }

    "take over, without waiting for the removal margin, when all previous oldest are other incarnations" in {
      val setup = new Setup(
        "restarted-all",
        List(oldestPreviousIncarnation(secondSys), oldestPreviousIncarnation(firstSys), oldest(thirdSys)),
        alreadyRemoved = List(firstSys, secondSys))
      setup.first.expectHandOverToMe()
      setup.first.send(HandOverDone)
      setup.expectStillBecomingOldest()
      setup.second.expectHandOverToMe()
      setup.second.send(HandOverDone)
      setup.expectOldest()
    }

    "take over when a previous oldest, that is not a member any more, confirms" in {
      val setup =
        new Setup("not-member", List(oldest(nonMemberSys), oldest(thirdSys)), alreadyRemoved = List(firstSys))
      val nonMember = new Peer(nonMemberSys, "not-member")
      nonMember.send(HandOverDone)
      setup.expectOldest()
    }

    "not hand over to the next oldest when another incarnation confirms after exiting" in {
      val setup = new Setup(
        "restarted-exiting",
        List(oldestPreviousIncarnation(firstSys), oldest(thirdSys)),
        alreadyRemoved = List(firstSys))
      setup.first.expectHandOverToMe()
      setup.selfExiting()
      setup.first.send(HandOverDone)
      setup.manager ! oldest(fourthSys)
      setup.expectStillBecomingOldest()
      setup.exitingCompleted should ===(false)
      setup.removed(secondSys)
      setup.transitions.expectMsg(FSM.Transition(setup.manager, BecomingOldest, End))
      setup.exitingCompleted should ===(true)
    }
  }
}
