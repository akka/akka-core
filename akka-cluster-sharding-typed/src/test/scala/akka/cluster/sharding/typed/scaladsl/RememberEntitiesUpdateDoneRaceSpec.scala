/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.scaladsl

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.sharding.{
  ClusterSharding => ClassicClusterSharding,
  ClusterShardingSettings => ClassicClusterShardingSettings
}
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.internal.RememberEntitiesProvider
import akka.cluster.sharding.internal.RememberEntitiesShardStore
import akka.cluster.sharding.typed.HashCodeMessageExtractor
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.internal.ExtractorAdapter
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join

/**
 * Reproduces a ClassCastException bug: a `RememberEntitiesShardStore.UpdateDone` reply that arrives
 * late - after the Shard has already restarted due to `updating-state-timeout` - so it is handled by
 * `Shard.idle`'s catch-all `case msg if extractEntityId.isDefinedAt(msg)`.
 *
 * Note: The public typed `ClusterSharding.init` API can't be used to reproduce this directly: typed's own
 * `ClusterShardingSettings.RememberEntitiesStoreMode.byName` only accepts `ddata`/`eventsourced`, rejecting
 * `remember-entities-store = custom` before it ever reaches classic sharding - so there is no way to inject
 * a slow/controllable store through the typed API. Instead this drives the **classic** `ClusterSharding`
 * extension directly (which does support `custom`), but builds `extractEntityId`/`extractShardId` the exact
 * same way typed sharding's `ClusterShardingImpl.internalInit` does: wrapping a `HashCodeMessageExtractor`
 * in an `ExtractorAdapter`. This exercises the identical code path a typed `ClusterSharding.init(...)` call
 * would produce.
 */
object RememberEntitiesUpdateDoneRaceSpec {
  val config = ConfigFactory.parseString(s"""
      akka.loglevel = DEBUG
      akka.actor.provider = cluster
      akka.remote.artery.canonical.port = 0
      akka.remote.artery.canonical.hostname = 127.0.0.1
      akka.cluster.sharding.distributed-data.durable.keys = []
      # must be ddata (not persistence) so the custom store provider hook is honoured
      akka.cluster.sharding.state-store-mode = ddata
      akka.cluster.sharding.remember-entities = on
      akka.cluster.sharding.remember-entities-store = custom
      akka.cluster.sharding.remember-entities-custom-store = "akka.cluster.sharding.typed.scaladsl.RememberEntitiesUpdateDoneRaceSpec$$SlowUpdateStore"
      akka.cluster.sharding.updating-state-timeout = 1s
      akka.cluster.sharding.entity-restart-backoff = 1s
      akka.cluster.sharding.shard-failure-backoff = 1s
      akka.cluster.sharding.coordinator-failure-backoff = 1s
      akka.cluster.sharding.verbose-debug-logging = on
    """)

  val TypeName = "update-done-race"

  // delay applied to the *first* Update the store sees, to race past updating-state-timeout (1s)
  @volatile var delayFirstUpdate: FiniteDuration = 3.seconds

  case class ShardStoreCreated(store: ActorRef, shardId: ShardId)

  @nowarn("msg=never used")
  class SlowUpdateStore(settings: ClassicClusterShardingSettings, typeName: String) extends RememberEntitiesProvider {
    override def shardStoreProps(shardId: ShardId): Props = SlowShardStoreActor.props(shardId)
    override def coordinatorStoreProps(): Props = SlowCoordinatorStoreActor.props()
  }

  object SlowShardStoreActor {
    def props(shardId: ShardId): Props = Props(new SlowShardStoreActor(shardId))
    // shared across restarts: the store actor itself is recreated by the Shard on every
    // restart, but we only want to delay the very first real Update, not the first one seen
    // by each new store instance - otherwise the delayed reply keeps re-triggering the same
    // updating-state-timeout race forever and the shard never gets a chance to stabilize
    @volatile var delayedOnce = false
  }
  class SlowShardStoreActor(shardId: ShardId) extends Actor with ActorLogging {
    import SlowShardStoreActor.delayedOnce
    implicit val ec: ExecutionContext = context.system.dispatcher

    context.system.eventStream.publish(ShardStoreCreated(self, shardId))

    override def receive: Receive = {
      case RememberEntitiesShardStore.GetEntities =>
        sender() ! RememberEntitiesShardStore.RememberedEntities(Set.empty)
      case RememberEntitiesShardStore.Update(started, stopped) =>
        if (!delayedOnce) {
          SlowShardStoreActor.delayedOnce = true
          log.info("Delaying UpdateDone by {} to race past updating-state-timeout", delayFirstUpdate)
          // scheduled directly against the Shard's ActorRef (which survives a restart) via the system
          // scheduler, NOT via this actor's own `timers` - this actor is a child of the Shard and gets
          // stopped when the Shard restarts, which would silently cancel a `timers`-based delayed send
          // before it ever fires, masking the race we're trying to reproduce
          context.system.scheduler
            .scheduleOnce(delayFirstUpdate, sender(), RememberEntitiesShardStore.UpdateDone(started, stopped))
        } else {
          sender() ! RememberEntitiesShardStore.UpdateDone(started, stopped)
        }
    }
  }

  object SlowCoordinatorStoreActor {
    def props(): Props = Props(new SlowCoordinatorStoreActor)
  }
  class SlowCoordinatorStoreActor extends Actor with ActorLogging {
    import akka.cluster.sharding.internal.RememberEntitiesCoordinatorStore
    override def receive: Receive = {
      case RememberEntitiesCoordinatorStore.GetShards =>
        sender() ! RememberEntitiesCoordinatorStore.RememberedShards(Set.empty)
      case RememberEntitiesCoordinatorStore.AddShard(shardId) =>
        sender() ! RememberEntitiesCoordinatorStore.UpdateDone(shardId)
    }
  }

  class TestEntity extends Actor {
    override def receive: Receive = {
      case msg => sender() ! msg
    }
  }
}

class RememberEntitiesUpdateDoneRaceSpec
    extends ScalaTestWithActorTestKit(RememberEntitiesUpdateDoneRaceSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  import RememberEntitiesUpdateDoneRaceSpec._

  "Typed remember-entities sharding (via ExtractorAdapter)" must {

    "not crash with ClassCastException when an UpdateDone reply arrives after a shard restart caused by updating-state-timeout" in {
      val cluster = Cluster(system)
      cluster.manager ! Join(cluster.selfMember.address)

      delayFirstUpdate = 3.seconds // longer than updating-state-timeout (1s), triggers the restart race
      SlowShardStoreActor.delayedOnce = false

      val classicSystem = system.toClassic
      val classicSettings = ClassicClusterShardingSettings(classicSystem)

      // exactly how akka.cluster.sharding.typed.internal.ClusterShardingImpl.internalInit builds these
      val numberOfShards = classicSystem.settings.config.getInt("akka.cluster.sharding.number-of-shards")
      val extractor = new HashCodeMessageExtractor[String](numberOfShards)
      val extractorAdapter = new ExtractorAdapter(extractor)
      val extractEntityId: ShardRegion.ExtractEntityId = {
        case message if extractorAdapter.entityId(message) != null =>
          (extractorAdapter.entityId(message), extractorAdapter.unwrapMessage(message))
      }
      val extractShardId: ShardRegion.ExtractShardId = { message =>
        extractorAdapter.entityId(message) match {
          case null => null
          case eid  => extractorAdapter.shardId(eid)
        }
      }

      val shardRegion = ClassicClusterSharding(classicSystem).start(
        TypeName,
        Props(new TestEntity),
        classicSettings,
        extractEntityId,
        extractShardId,
        ClassicClusterSharding(classicSystem).defaultShardAllocationStrategy(classicSettings),
        akka.actor.PoisonPill)

      val probe = akka.testkit.TestProbe()(classicSystem)

      // no ClassCastException should ever be logged: a stray/late UpdateDone reaching the
      // restarted shard's `idle` state must be ignored, not crash the shard again
      LoggingTestKit.error[ClassCastException].withOccurrences(0).expect {
        // triggers a remember-entities Update to the (slow) store; the shard should hit
        // updating-state-timeout (1s) and restart before the delayed UpdateDone (at 3s) arrives,
        // at which point the stray UpdateDone hits the restarted shard's `idle` state and must
        // be safely ignored rather than crashing with a ClassCastException
        shardRegion.tell(ShardingEnvelope("entity-1", "hello"), probe.ref)

        probe.expectNoMessage(4.seconds)

        // the shard must still be alive and functional after the race has played out
        probe.awaitAssert({
          shardRegion.tell(ShardingEnvelope("entity-1", "hello-again"), probe.ref)
          probe.expectMsg("hello-again")
        }, 10.seconds)
      }
    }
  }
}
