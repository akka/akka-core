/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.FSM
import akka.actor.Props
import akka.actor.Timers
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe

object ClusterSingletonRestartedPreviousOldestSpec {

  /** Number of running singleton instances, all nodes are running in the same JVM. */
  class Instances {
    private val _running = new AtomicInteger
    private val _maxRunning = new AtomicInteger
    private val _startCount = new AtomicInteger
    def started(): Unit = {
      val n = _running.incrementAndGet()
      _maxRunning.accumulateAndGet(n, (a: Int, b: Int) => math.max(a, b))
      _startCount.incrementAndGet()
    }
    def stopped(): Unit = _running.decrementAndGet()
    def running: Int = _running.get
    def maxRunning: Int = _maxRunning.get
    def startCount: Int = _startCount.get
  }

  case object StopSlowly
  private case object Stop

  /** Takes a while to stop, so that a new instance that doesn't wait for it would be noticed. */
  class Singleton(instances: Instances) extends Actor with Timers {
    override def preStart(): Unit = instances.started()
    override def postStop(): Unit = instances.stopped()
    def receive = {
      case StopSlowly => timers.startSingleTimer(Stop, Stop, 3.seconds)
      case Stop       => context.stop(self)
    }
  }
}

/**
 * A previous oldest is restarted with the same host:port, and thereby answers the `HandOverToMe` that was intended for
 * the previous incarnation.
 */
class ClusterSingletonRestartedPreviousOldestSpec extends AkkaSpec("""
  akka.loglevel = INFO
  akka.actor.provider = cluster
  akka.remote.artery.canonical {
    hostname = "127.0.0.1"
    port = 0
  }
  """) {

  import ClusterSingletonManager.Internal.{ Oldest, Younger }
  import ClusterSingletonRestartedPreviousOldestSpec._

  // much longer than the test, to verify that it's not needed
  private val removalMargin = 2.minutes

  private var systems = List.empty[ActorSystem]

  private def newSystem(config: String = ""): ActorSystem = {
    val sys = ActorSystem(system.name, ConfigFactory.parseString(config).withFallback(system.settings.config))
    systems ::= sys
    sys
  }

  private def restart(sys: ActorSystem): ActorSystem =
    newSystem(s"akka.remote.artery.canonical.port = ${Cluster(sys).selfAddress.port.get}")

  override def afterTermination(): Unit =
    systems.foreach(shutdown(_))

  private def startManager(sys: ActorSystem, instances: Instances): TestProbe = {
    val manager = sys.actorOf(
      ClusterSingletonManager.props(
        Props(new Singleton(instances)),
        StopSlowly,
        ClusterSingletonManagerSettings(sys).withRemovalMargin(removalMargin)),
      "singleton")
    val transitions = TestProbe()(sys)
    manager ! FSM.SubscribeTransitionCallBack(transitions.ref)
    transitions
  }

  private def expectState(transitions: TestProbe, state: ClusterSingletonManager.State): Unit =
    transitions.fishForMessage(10.seconds, hint = state.toString) {
      case FSM.CurrentState(_, `state`)  => true
      case FSM.Transition(_, _, `state`) => true
      case _                             => false
    }

  /** The singleton has been started the given number of times, each time after the previous was stopped. */
  private def expectStarted(instances: Instances, startCount: Int): Unit = {
    // started asynchronously when the manager has become Oldest
    awaitAssert(instances.startCount should ===(startCount))
    instances.maxRunning should ===(1)
    instances.running should ===(1)
  }

  private def join(from: ActorSystem, to: ActorSystem): Unit =
    within(30.seconds) {
      awaitAssert {
        Cluster(from).join(Cluster(to).selfAddress)
        Cluster(from).selfMember.status should ===(MemberStatus.Up)
      }
    }

  "A ClusterSingletonManager becoming oldest" must {

    "not start the singleton when a restarted previous oldest confirms, but the next one is still running it" in {
      val instances = new Instances
      val first = newSystem()
      val second = newSystem()
      val third = newSystem()
      val firstUniqueAddress = Cluster(first).selfUniqueAddress

      val firstTransitions = startManager(first, instances)
      join(first, first)
      expectState(firstTransitions, Oldest)
      val secondTransitions = startManager(second, instances)
      join(second, first)
      expectState(secondTransitions, Younger)
      val thirdTransitions = startManager(third, instances)
      join(third, first)
      expectState(thirdTransitions, Younger)

      Cluster(first).leave(Cluster(first).selfAddress)
      expectState(secondTransitions, Oldest)
      within(20.seconds) {
        awaitAssert {
          first.whenTerminated.isCompleted should ===(true)
          Cluster(third).state.members.map(_.uniqueAddress) should not contain firstUniqueAddress
        }
      }

      val restartedFirst = restart(first)
      val restartedFirstTransitions = startManager(restartedFirst, instances)
      join(restartedFirst, third)
      expectState(restartedFirstTransitions, Younger)
      expectStarted(instances, 2)

      // third still has the previous incarnation of first as previous oldest, within the removal margin
      Cluster(second).leave(Cluster(second).selfAddress)
      expectState(thirdTransitions, Oldest)
      expectStarted(instances, 3)
    }

    "start the singleton when the only previous oldest has been restarted" in {
      val instances = new Instances
      // not leaving when the ActorSystem is terminated
      val first = newSystem("""
        akka.coordinated-shutdown.run-by-actor-system-terminate = off
        akka.coordinated-shutdown.terminate-actor-system = off
        """)
      val second = newSystem()

      val firstTransitions = startManager(first, instances)
      join(first, first)
      expectState(firstTransitions, Oldest)
      val secondTransitions = startManager(second, instances)
      join(second, first)
      expectState(secondTransitions, Younger)

      shutdown(first)
      // the previous incarnation is downed by the join attempt of the new incarnation
      val restartedFirst = restart(first)
      startManager(restartedFirst, instances)
      join(restartedFirst, second)

      expectState(secondTransitions, Oldest)
      expectStarted(instances, 2)
    }
  }
}
