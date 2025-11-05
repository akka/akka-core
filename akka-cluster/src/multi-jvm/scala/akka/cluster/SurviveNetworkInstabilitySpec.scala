/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Terminated
import akka.remote.RARP
import akka.remote.artery.QuarantinedEvent
import akka.remote.testconductor.RoleName
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.serialization.jackson.CborSerializable
import akka.testkit._

object SurviveNetworkInstabilityMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")
  val sixth = role("sixth")
  val seventh = role("seventh")
  val eighth = role("eighth")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.remote.artery.advanced.system-message-buffer-size=100
      """))
      .withFallback(MultiNodeClusterSpec.clusterConfig))

  testTransport(on = true)

  class Echo extends Actor {
    def receive = {
      case m => sender() ! m
    }
  }

  case class Targets(refs: Set[ActorRef]) extends CborSerializable
  case object TargetsRegistered extends CborSerializable

  class Watcher extends Actor {
    var targets = Set.empty[ActorRef]

    def receive = {
      case Targets(refs) =>
        targets = refs
        sender() ! TargetsRegistered
      case "boom" =>
        targets.foreach(context.watch)
      case Terminated(_) =>
    }
  }

  class SimulatedException extends RuntimeException("Simulated") with NoStackTrace
}

class SurviveNetworkInstabilityMultiJvmNode1 extends SurviveNetworkInstabilitySpec
class SurviveNetworkInstabilityMultiJvmNode2 extends SurviveNetworkInstabilitySpec
class SurviveNetworkInstabilityMultiJvmNode3 extends SurviveNetworkInstabilitySpec
class SurviveNetworkInstabilityMultiJvmNode4 extends SurviveNetworkInstabilitySpec
class SurviveNetworkInstabilityMultiJvmNode5 extends SurviveNetworkInstabilitySpec
class SurviveNetworkInstabilityMultiJvmNode6 extends SurviveNetworkInstabilitySpec
class SurviveNetworkInstabilityMultiJvmNode7 extends SurviveNetworkInstabilitySpec
class SurviveNetworkInstabilityMultiJvmNode8 extends SurviveNetworkInstabilitySpec

abstract class SurviveNetworkInstabilitySpec
    extends MultiNodeClusterSpec(SurviveNetworkInstabilityMultiJvmSpec)
    with ImplicitSender {

  import SurviveNetworkInstabilityMultiJvmSpec._

  //  muteMarkingAsUnreachable()
  //  muteMarkingAsReachable()

  override def expectedTestDuration = 3.minutes

  private val remoteSettings = RARP(system).provider.remoteSettings

  def quarantinedEventClass: Class[_] = classOf[QuarantinedEvent]

  @nowarn
  def quarantinedEventFrom(event: Any): Address = {
    event match {
      case QuarantinedEvent(uniqueAddress) => uniqueAddress.address
    }

  }

  def sysMsgBufferSize: Int = remoteSettings.Artery.Advanced.SysMsgBufferSize

  def assertUnreachable(subjects: RoleName*): Unit = {
    val expected = subjects.toSet.map(address)
    awaitAssert(clusterView.unreachableMembers.map(_.address) should ===(expected))
  }

  system.actorOf(Props[Echo](), "echo")

  def assertCanTalk(alive: RoleName*): Unit = {
    runOn(alive: _*) {
      awaitAllReachable()
    }
    enterBarrier("reachable-ok")

    runOn(alive: _*) {
      for (to <- alive) {
        val sel = system.actorSelection(node(to) / "user" / "echo")
        val msg = s"ping-$to"
        val p = TestProbe()
        awaitAssert {
          sel.tell(msg, p.ref)
          p.expectMsg(1.second, msg)
        }
        p.ref ! PoisonPill
      }
    }
    enterBarrier("ping-ok")
  }

  "A network partition tolerant cluster" must {

    "reach initial convergence" taggedAs LongRunningTest in {
      awaitClusterUp(first, second, third, fourth, fifth)

      enterBarrier("after-1")
      assertCanTalk(first, second, third, fourth, fifth)
    }

    "heal after a broken pair" taggedAs LongRunningTest in within(45.seconds) {
      runOn(first) {
        testConductor.blackhole(first, second, Direction.Both).await
      }
      enterBarrier("blackhole-2")

      runOn(first) { assertUnreachable(second) }
      runOn(second) { assertUnreachable(first) }
      runOn(third, fourth, fifth) {
        assertUnreachable(first, second)
      }

      enterBarrier("unreachable-2")

      runOn(first) {
        testConductor.passThrough(first, second, Direction.Both).await
      }
      enterBarrier("repair-2")

      // This test illustrates why we can't ignore gossip from unreachable aggregated
      // status. If all third, fourth, and fifth has been infected by first and second
      // unreachable they must accept gossip from first and second when their
      // broken connection has healed, otherwise they will be isolated forever.

      enterBarrier("after-2")
      assertCanTalk(first, second, third, fourth, fifth)
    }

    "heal after one isolated node" taggedAs LongRunningTest in within(45.seconds) {
      val others = Vector(second, third, fourth, fifth)
      runOn(first) {
        for (other <- others) {
          testConductor.blackhole(first, other, Direction.Both).await
        }
      }
      enterBarrier("blackhole-3")

      runOn(first) { assertUnreachable(others: _*) }
      runOn(others: _*) {
        assertUnreachable(first)
      }

      enterBarrier("unreachable-3")

      runOn(first) {
        for (other <- others) {
          testConductor.passThrough(first, other, Direction.Both).await
        }
      }
      enterBarrier("repair-3")
      assertCanTalk((others :+ first): _*)
    }

    "heal two isolated islands" taggedAs LongRunningTest in within(45.seconds) {
      val island1 = Vector(first, second)
      val island2 = Vector(third, fourth, fifth)
      runOn(first) {
        // split the cluster in two parts (first, second) / (third, fourth, fifth)
        for (role1 <- island1; role2 <- island2) {
          testConductor.blackhole(role1, role2, Direction.Both).await
        }
      }
      enterBarrier("blackhole-4")

      runOn(island1: _*) {
        assertUnreachable(island2: _*)
      }
      runOn(island2: _*) {
        assertUnreachable(island1: _*)
      }

      enterBarrier("unreachable-4")

      runOn(first) {
        for (role1 <- island1; role2 <- island2) {
          testConductor.passThrough(role1, role2, Direction.Both).await
        }
      }
      enterBarrier("repair-4")
      assertCanTalk((island1 ++ island2): _*)
    }

    "heal after unreachable when ring is changed" taggedAs LongRunningTest in within(60.seconds) {
      val joining = Vector(sixth, seventh)
      val others = Vector(second, third, fourth, fifth)
      runOn(first) {
        for (role1 <- (joining :+ first); role2 <- others) {
          testConductor.blackhole(role1, role2, Direction.Both).await
        }
      }
      enterBarrier("blackhole-5")

      runOn(first) { assertUnreachable(others: _*) }
      runOn(others: _*) { assertUnreachable(first) }

      enterBarrier("unreachable-5")

      runOn(joining: _*) {
        cluster.join(first)

        // let them join and stabilize heartbeating
        Thread.sleep(5000.millis.dilated.toMillis)
      }

      enterBarrier("joined-5")

      runOn((joining :+ first): _*) { assertUnreachable(others: _*) }
      // others doesn't know about the joining nodes yet, no gossip passed through
      runOn(others: _*) { assertUnreachable(first) }

      enterBarrier("more-unreachable-5")

      runOn(first) {
        for (role1 <- (joining :+ first); role2 <- others) {
          testConductor.passThrough(role1, role2, Direction.Both).await
        }
      }

      enterBarrier("repair-5")
      runOn((joining ++ others :+ first): _*) {
        // eighth not joined yet
        awaitMembersUp(roles.size - 1, timeout = remaining)
      }
      enterBarrier("after-5")
      assertCanTalk((joining ++ others :+ first): _*)
    }

    "mark quarantined node with reachability status Terminated" taggedAs LongRunningTest in within(60.seconds) {
      val others = Vector(first, third, fourth, fifth, sixth, seventh)

      runOn(third) {
        system.actorOf(Props[Watcher](), "watcher")

        // undelivered system messages in RemoteChild on third should trigger QuarantinedEvent
        system.eventStream.subscribe(testActor, quarantinedEventClass)
      }
      enterBarrier("watcher-created")

      runOn(second) {
        val refs = Vector.fill(sysMsgBufferSize + 1)(system.actorOf(Props[Echo]())).toSet
        system.actorSelection(node(third) / "user" / "watcher") ! Targets(refs)
        expectMsg(TargetsRegistered)
      }

      enterBarrier("targets-registered")

      runOn(first) {
        for (role <- others)
          testConductor.blackhole(role, second, Direction.Both).await
      }
      enterBarrier("blackhole-6")

      runOn(third) {
        // this will trigger watch of targets on second, resulting in too many outstanding
        // system messages and quarantine
        system.actorSelection("/user/watcher") ! "boom"
        within(10.seconds) {
          quarantinedEventFrom(expectMsgClass(quarantinedEventClass)) should ===(address(second))
        }
        system.eventStream.unsubscribe(testActor, quarantinedEventClass)
      }
      enterBarrier("quarantined")

      runOn(others: _*) {
        // not be downed, see issue #25632
        Thread.sleep(2000)
        val secondUniqueAddress = cluster.state.members.find(_.address == address(second)) match {
          case None => fail("Unexpected removal of quarantined node")
          case Some(m) =>
            m.status should ===(MemberStatus.Up) // not Down
            m.uniqueAddress
        }

        // second should be marked with reachability status Terminated removed because of quarantine
        awaitAssert(clusterView.reachability.status(secondUniqueAddress) should ===(Reachability.Terminated))
      }
      enterBarrier("reachability-terminated")

      runOn(fourth) {
        cluster.down(address(second))
      }
      runOn(others: _*) {
        // second should be removed because of quarantine
        awaitAssert(clusterView.members.map(_.address) should not contain address(second))
        // and also removed from reachability table
        awaitAssert(clusterView.reachability.allUnreachableOrTerminated should ===(Set.empty))
      }
      enterBarrier("removed-after-down")

      enterBarrier("after-6")
      assertCanTalk(others: _*)
    }

    "continue and move Joining to Up after downing of one half" taggedAs LongRunningTest in within(60.seconds) {
      // note that second is already removed in previous step
      val side1 = Vector(first, third, fourth)
      val side1AfterJoin = side1 :+ eighth
      val side2 = Vector(fifth, sixth, seventh)
      runOn(first) {
        for (role1 <- side1AfterJoin; role2 <- side2) {
          testConductor.blackhole(role1, role2, Direction.Both).await
        }
      }
      enterBarrier("blackhole-7")

      runOn(side1: _*) { assertUnreachable(side2: _*) }
      runOn(side2: _*) { assertUnreachable(side1: _*) }

      enterBarrier("unreachable-7")

      runOn(eighth) {
        cluster.join(third)
      }
      runOn(fourth) {
        for (role2 <- side2) {
          cluster.down(role2)
        }
      }

      enterBarrier("downed-7")

      runOn(side1AfterJoin: _*) {
        // side2 removed
        val expected = side1AfterJoin.map(address).toSet
        awaitAssert {
          // repeat the downing in case it was not successful, which may
          // happen if the removal was reverted due to gossip merge, see issue #18767
          runOn(fourth) {
            for (role2 <- side2) {
              cluster.down(role2)
            }
          }

          clusterView.members.map(_.address) should ===(expected)
          clusterView.members.collectFirst { case m if m.address == address(eighth) => m.status } should ===(
            Some(MemberStatus.Up))
        }
      }

      enterBarrier("side2-removed")

      runOn(first) {
        for (role1 <- side1AfterJoin; role2 <- side2) {
          testConductor.passThrough(role1, role2, Direction.Both).await
        }
      }
      enterBarrier("repair-7")

      // side2 should not detect side1 as reachable again
      Thread.sleep(10000)

      runOn(side1AfterJoin: _*) {
        val expected = side1AfterJoin.map(address).toSet
        clusterView.members.map(_.address) should ===(expected)
      }

      runOn(side2: _*) {
        // side2 comes back but stays unreachable
        val expected = (side2 ++ side1).map(address).toSet
        clusterView.members.map(_.address) should ===(expected)
        assertUnreachable(side1: _*)
      }

      enterBarrier("after-7")
      assertCanTalk(side1AfterJoin: _*)
    }

  }

}
