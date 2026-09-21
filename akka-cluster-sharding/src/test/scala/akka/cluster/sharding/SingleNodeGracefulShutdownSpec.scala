/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.CoordinatedShutdown
import akka.actor.DeadLetter
import akka.actor.Dropped
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.testkit.AkkaSpec
import akka.testkit.TestActors.EchoActor
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing

object SingleNodeGracefulShutdownSpec {
  val config =
    """
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.port = 0
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.cluster.sharding.verbose-debug-logging = on
    akka.cluster.sharding.remember-entities = on
    akka.cluster.sharding.remember-entities-store = eventsourced
    akka.cluster.sharding.state-store-mode = ddata
    """

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: Int => (msg.toString, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case msg: Int => msg.toString
    case _        => throw new IllegalArgumentException()
  }

  val ShardCount = 14

  case object StopEntity
  case object ReleaseStop

  // The entity with id "2" does not stop until it is released, to keep its shard in hand-off
  class SlowStoppingEntity(stopRequested: ActorRef) extends Actor {
    override def receive: Receive = {
      case "ping" => sender() ! self
      case StopEntity =>
        if (self.path.name == "2") stopRequested ! StopEntity
        else context.stop(self)
      case ReleaseStop => context.stop(self)
    }
  }

  val extractSlowStoppingEntityId: ShardRegion.ExtractEntityId = {
    case (id: Int, msg) => (id.toString, msg)
  }

  val extractSlowStoppingShardId: ShardRegion.ExtractShardId = {
    case (id: Int, _)                => id.toString
    case ShardRegion.StartEntity(id) => id
    case _                           => throw new IllegalArgumentException()
  }
}

class SingleNodeGracefulShutdownSpec extends AkkaSpec(SingleNodeGracefulShutdownSpec.config) with WithLogCapturing {
  import SingleNodeGracefulShutdownSpec._

  "A single node cluster with remember-entities" must {
    "drop buffered messages and stop the region when the last shard has stopped" in {
      val cluster = Cluster(system)
      cluster.join(cluster.selfAddress)
      awaitAssert(cluster.selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val stopRequested = TestProbe()
      val region = ClusterSharding(system).start(
        "slowStopping",
        Props(new SlowStoppingEntity(stopRequested.ref)),
        ClusterShardingSettings(system),
        extractSlowStoppingEntityId,
        extractSlowStoppingShardId,
        ShardCoordinator.ShardAllocationStrategy.leastShardAllocationStrategy(absoluteLimit = 2, relativeLimit = 1.0),
        StopEntity)

      val probe = TestProbe()
      region.tell((1, "ping"), probe.ref)
      val entity1 = probe.expectMsgType[ActorRef](10.seconds)
      region.tell((2, "ping"), probe.ref)
      val entity2 = probe.expectMsgType[ActorRef](10.seconds)

      val droppedProbe = TestProbe()
      system.eventStream.subscribe(droppedProbe.ref, classOf[DeadLetter])
      probe.watch(entity1)
      probe.watch(region)

      region ! ShardRegion.GracefulShutdown
      probe.expectTerminated(entity1, 10.seconds)
      stopRequested.expectMsg(10.seconds, StopEntity)

      // shard 1 is handed off, and shard 2 is still in hand-off, so this is buffered
      region ! ((1, "buffered"))

      entity2 ! ReleaseStop
      droppedProbe.fishForSpecificMessage(10.seconds) {
        case DeadLetter(Dropped((1, "buffered"), _, _, _), _, _) => true
      }
      probe.expectTerminated(region, 10.seconds)
    }

    "shut its shard regions down without waiting for shard home retries" in {
      val cluster = Cluster(system)
      cluster.join(cluster.selfAddress)
      awaitAssert(cluster.selfMember.status shouldEqual MemberStatus.Up, 10.seconds)

      val region = ClusterSharding(system).start(
        "type1",
        Props[EchoActor](),
        ClusterShardingSettings(system),
        extractEntityId,
        extractShardId)

      // One entity per shard, so several shards are deallocated concurrently on the way out.
      val probe = TestProbe()
      (1 to ShardCount).foreach { id =>
        region.tell(id, probe.ref)
        probe.expectMsg(10.seconds, id)
      }

      // Traffic that continues into the shutdown. A message arriving for a shard the coordinator
      // has just deallocated makes the region ask for that shard's home again.
      @volatile var keepSending = true
      val sender = new Thread(() => {
        while (keepSending) {
          (1 to ShardCount).foreach(id => region.tell(id, system.deadLetters))
          Thread.sleep(5)
        }
      })
      sender.setDaemon(true)
      sender.start()

      val started = System.nanoTime()
      Await.result(CoordinatedShutdown(system).run(CoordinatedShutdown.UnknownReason), 30.seconds)
      val elapsed = (System.nanoTime() - started).nanos
      keepSending = false

      info(s"CoordinatedShutdown took ${elapsed.toMillis}ms")
      // One node has no hand-over partner and nothing to rebalance, so nothing here has to wait.
      elapsed should be < 5.seconds
    }
  }
}
