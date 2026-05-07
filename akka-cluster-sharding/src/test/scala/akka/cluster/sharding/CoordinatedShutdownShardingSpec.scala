/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.ActorSystem
import akka.actor.CoordinatedShutdown
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.testkit.AkkaSpec
import akka.testkit.TestActors.EchoActor
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing

object CoordinatedShutdownShardingSpec {
  val config =
    """
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.port = 0
    akka.cluster.sharding.verbose-debug-logging = on
    """

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: Int => (msg.toString, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case msg: Int => (msg % 10).toString
    case _        => throw new IllegalArgumentException()
  }
}

class CoordinatedShutdownShardingSpec extends AkkaSpec(CoordinatedShutdownShardingSpec.config) with WithLogCapturing {
  import CoordinatedShutdownShardingSpec._

  val sys1 = ActorSystem(system.name, system.settings.config)
  val sys2 = ActorSystem(system.name, system.settings.config)
  val sys3 = system

  val region1 = ClusterSharding(sys1).start(
    "type1",
    Props[EchoActor](),
    ClusterShardingSettings(sys1),
    extractEntityId,
    extractShardId)
  val region2 = ClusterSharding(sys2).start(
    "type1",
    Props[EchoActor](),
    ClusterShardingSettings(sys2),
    extractEntityId,
    extractShardId)
  val region3 = ClusterSharding(sys3).start(
    "type1",
    Props[EchoActor](),
    ClusterShardingSettings(sys3),
    extractEntityId,
    extractShardId)

  val probe1 = TestProbe()(sys1)
  val probe2 = TestProbe()(sys2)
  val probe3 = TestProbe()(sys3)

  CoordinatedShutdown(sys1).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "unbind") { () =>
    probe1.ref ! "CS-unbind-1"
    Future.successful(Done)
  }
  CoordinatedShutdown(sys2).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "unbind") { () =>
    probe2.ref ! "CS-unbind-2"
    Future.successful(Done)
  }
  CoordinatedShutdown(sys3).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "unbind") { () =>
    probe3.ref ! "CS-unbind-3"
    Future.successful(Done)
  }

  override def beforeTermination(): Unit = {
    shutdown(sys1)
    shutdown(sys2)
  }

  // Using region 2 as it is not shutdown in either test
  def pingEntities(): Unit = {
    awaitAssert({
      val p1 = TestProbe()(sys2)
      region2.tell(1, p1.ref)
      p1.expectMsg(1.seconds, 1)
      val p2 = TestProbe()(sys2)
      region2.tell(2, p2.ref)
      p2.expectMsg(1.seconds, 2)
      val p3 = TestProbe()(sys2)
      region2.tell(3, p3.ref)
      p3.expectMsg(1.seconds, 3)
    }, 10.seconds)
  }

  "Sharding and CoordinatedShutdown" must {
    "init cluster" in {
      Cluster(sys1).join(Cluster(sys1).selfAddress) // coordinator will initially run on sys1
      awaitAssert(Cluster(sys1).selfMember.status should ===(MemberStatus.Up))

      Cluster(sys2).join(Cluster(sys1).selfAddress)
      within(10.seconds) {
        awaitAssert {
          Cluster(sys1).state.members.size should ===(2)
          Cluster(sys1).state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
          Cluster(sys2).state.members.size should ===(2)
          Cluster(sys2).state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
        }
      }

      Cluster(sys3).join(Cluster(sys1).selfAddress)
      within(10.seconds) {
        awaitAssert {
          Cluster(sys1).state.members.size should ===(3)
          Cluster(sys1).state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
          Cluster(sys2).state.members.size should ===(3)
          Cluster(sys2).state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
          Cluster(sys3).state.members.size should ===(3)
          Cluster(sys3).state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
        }
      }

      pingEntities()
    }

    "run coordinated shutdown when leaving" in {
      Cluster(sys3).leave(Cluster(sys1).selfAddress)
      probe1.expectMsg(10.seconds, "CS-unbind-1")

      within(20.seconds) {
        awaitAssert {
          Cluster(sys2).state.members.size should ===(2)
          Cluster(sys3).state.members.size should ===(2)
        }
      }
      within(10.seconds) {
        awaitAssert {
          Cluster(sys1).isTerminated should ===(true)
          sys1.whenTerminated.isCompleted should ===(true)
        }
      }

      pingEntities()
    }

    "run coordinated shutdown when downing" in {
      // coordinator is on sys2
      Cluster(sys2).down(Cluster(sys3).selfAddress)
      probe3.expectMsg(10.seconds, "CS-unbind-3")

      within(20.seconds) {
        awaitAssert {
          Cluster(sys2).state.members.size should ===(1)
        }
      }
      within(10.seconds) {
        awaitAssert {
          Cluster(sys3).isTerminated should ===(true)
          sys3.whenTerminated.isCompleted should ===(true)
        }
      }

      pingEntities()
    }
  }
}
