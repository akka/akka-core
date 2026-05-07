/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.Done
import akka.actor.{ Actor, ActorLogging, ActorRef, Props, Timers }
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.internal.RememberEntitiesCoordinatorStore
import akka.cluster.sharding.internal.RememberEntitiesProvider
import akka.cluster.sharding.internal.RememberEntitiesShardStore
import akka.testkit.AkkaSpec
import akka.testkit.TestException
import akka.testkit.TestProbe
import akka.testkit.WithLogCapturing

object RememberEntitiesFailureSpec {
  val config = ConfigFactory.parseString(s"""
      akka.loglevel = DEBUG
      akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
      akka.actor.provider = cluster
      akka.remote.artery.canonical.port = 0
      akka.cluster.sharding.distributed-data.durable.keys = []
      # must be ddata or else remember entities store is ignored
      akka.cluster.sharding.state-store-mode = ddata
      akka.cluster.sharding.remember-entities = on
      akka.cluster.sharding.remember-entities-store = custom
      akka.cluster.sharding.remember-entities-custom-store = "akka.cluster.sharding.RememberEntitiesFailureSpec$$FakeStore"
      # quick backoffs
      akka.cluster.sharding.entity-restart-backoff = 1s
      akka.cluster.sharding.shard-failure-backoff = 1s
      akka.cluster.sharding.coordinator-failure-backoff = 1s
      akka.cluster.sharding.updating-state-timeout = 1s
      akka.cluster.sharding.verbose-debug-logging = on
      akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
    """)

  class EntityActor extends Actor with ActorLogging {
    log.info("Entity actor [{}] starting up", context.self.path.name)
    override def receive: Receive = {
      case "stop" =>
        log.info("Stopping myself!")
        context.stop(self)
      case "graceful-stop" =>
        context.parent ! ShardRegion.Passivate("stop")
      case "incarnation" =>
        // Don't do this at home, kids...
        sender() ! self
      case msg => sender() ! msg
    }
  }

  case class EntityEnvelope(entityId: Int, msg: Any)

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case EntityEnvelope(id, payload) => (id.toString, payload)
    case _                           => throw new IllegalArgumentException()
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case EntityEnvelope(id, _) => (id % 10).toString
    case _                     => throw new IllegalArgumentException()
  }

  sealed trait Fail
  case object NoResponse extends Fail
  case object CrashStore extends Fail
  case object StopStore extends Fail
  // not really a failure but close enough
  case class Delay(howLong: FiniteDuration) extends Fail

  // outside store since we need to be able to set them before sharding initializes
  @volatile var failShardGetEntities = Map.empty[ShardId, Fail]
  @volatile var failCoordinatorGetShards: Option[Fail] = None

  case class ShardStoreCreated(store: ActorRef, shardId: ShardId)
  case class CoordinatorStoreCreated(store: ActorRef)

  @nowarn("msg=never used")
  class FakeStore(settings: ClusterShardingSettings, typeName: String) extends RememberEntitiesProvider {
    override def shardStoreProps(shardId: ShardId): Props = FakeShardStoreActor.props(shardId)
    override def coordinatorStoreProps(): Props = FakeCoordinatorStoreActor.props()
  }

  object FakeShardStoreActor {
    def props(shardId: ShardId): Props = Props(new FakeShardStoreActor(shardId))

    case class FailUpdateEntity(whichWay: Fail)
    case object ClearFail

    case class Delayed(replyTo: ActorRef, msg: Any)
  }
  class FakeShardStoreActor(shardId: ShardId) extends Actor with ActorLogging with Timers {
    import FakeShardStoreActor._

    implicit val ec: ExecutionContext = context.system.dispatcher
    private var failUpdate: Option[Fail] = None

    context.system.eventStream.publish(ShardStoreCreated(self, shardId))

    override def receive: Receive = {
      case RememberEntitiesShardStore.GetEntities =>
        failShardGetEntities.get(shardId) match {
          case None             => sender() ! RememberEntitiesShardStore.RememberedEntities(Set.empty)
          case Some(NoResponse) => log.debug("Sending no response for GetEntities")
          case Some(CrashStore) => throw TestException("store crash on GetEntities")
          case Some(StopStore)  => context.stop(self)
          case Some(Delay(howLong)) =>
            log.debug("Delaying initial entities listing with {}", howLong)
            timers.startSingleTimer("get-entities-delay", Delayed(sender(), Set.empty), howLong)
        }
      case RememberEntitiesShardStore.Update(started, stopped) =>
        failUpdate match {
          case None             => sender() ! RememberEntitiesShardStore.UpdateDone(started, stopped)
          case Some(NoResponse) => log.debug("Sending no response for AddEntity")
          case Some(CrashStore) => throw TestException("store crash on AddEntity")
          case Some(StopStore)  => context.stop(self)
          case Some(Delay(howLong)) =>
            log.debug("Delaying response for AddEntity with {}", howLong)
            timers.startSingleTimer("add-entity-delay", Delayed(sender(), Set.empty), howLong)
        }
      case FailUpdateEntity(whichWay) =>
        failUpdate = Some(whichWay)
        sender() ! Done
      case ClearFail =>
        failUpdate = None
        sender() ! Done
      case Delayed(to, msg) =>
        to ! msg
    }
  }

  object FakeCoordinatorStoreActor {
    def props(): Props = Props(new FakeCoordinatorStoreActor)

    case class FailAddShard(shardId: ShardId, wayToFail: Fail)
    case class ClearFailShard(shardId: ShardId)
  }
  class FakeCoordinatorStoreActor extends Actor with ActorLogging with Timers {
    import FakeCoordinatorStoreActor._
    import FakeShardStoreActor.Delayed

    context.system.eventStream.publish(CoordinatorStoreCreated(context.self))

    private var failAddShard = Map.empty[ShardId, Fail]

    override def receive: Receive = {
      case RememberEntitiesCoordinatorStore.GetShards =>
        failCoordinatorGetShards match {
          case None             => sender() ! RememberEntitiesCoordinatorStore.RememberedShards(Set.empty)
          case Some(NoResponse) =>
          case Some(CrashStore) => throw TestException("store crash on load")
          case Some(StopStore)  => context.stop(self)
          case Some(Delay(howLong)) =>
            log.debug("Delaying initial shard listing with {}", howLong)
            timers.startSingleTimer("list-shards-delay", Delayed(sender(), Set.empty), howLong)
        }
      case RememberEntitiesCoordinatorStore.AddShard(shardId) =>
        failAddShard.get(shardId) match {
          case None             => sender() ! RememberEntitiesCoordinatorStore.UpdateDone(shardId)
          case Some(NoResponse) =>
          case Some(CrashStore) => throw TestException("store crash on add")
          case Some(StopStore)  => context.stop(self)
          case Some(Delay(howLong)) =>
            log.debug("Delaying adding shard with {}", howLong)
            timers.startSingleTimer("add-shard-delay", Delayed(sender(), Set.empty), howLong)
        }
      case FailAddShard(shardId, wayToFail) =>
        log.debug("Failing store of {} with {}", shardId, wayToFail)
        failAddShard = failAddShard.updated(shardId, wayToFail)
        sender() ! Done
      case ClearFailShard(shardId) =>
        log.debug("No longer failing store of {}", shardId)
        failAddShard = failAddShard - shardId
        sender() ! Done
      case Delayed(to, msg) =>
        to ! msg
    }
  }

}

class RememberEntitiesFailureSpec
    extends AkkaSpec(RememberEntitiesFailureSpec.config)
    with AnyWordSpecLike
    with WithLogCapturing {

  import RememberEntitiesFailureSpec._

  override def atStartup(): Unit = {
    // Form a one node cluster
    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
    awaitAssert(cluster.readView.members.count(_.status == MemberStatus.Up) should ===(1))
  }

  "Remember entities handling in sharding" must {

    List(NoResponse, CrashStore, StopStore, Delay(500.millis), Delay(1.second)).foreach { (wayToFail: Fail) =>
      s"recover when initial remember entities load fails $wayToFail" in {
        log.debug("Getting entities for shard 1 will fail")
        failShardGetEntities = Map("1" -> wayToFail)

        try {
          val probe = TestProbe()
          val sharding = ClusterSharding(system).start(
            s"initial-$wayToFail",
            Props[EntityActor](),
            ClusterShardingSettings(system).withRememberEntities(true),
            extractEntityId,
            extractShardId)

          sharding.tell(EntityEnvelope(1, "hello-1"), probe.ref)
          probe.expectNoMessage() // message is lost because shard crashes

          log.debug("Resetting initial fail")
          failShardGetEntities = Map.empty

          // shard should be restarted and eventually succeed
          awaitAssert {
            sharding.tell(EntityEnvelope(1, "hello-1"), probe.ref)
            probe.expectMsg("hello-1")
          }

          system.stop(sharding)
        } finally {
          failShardGetEntities = Map.empty
        }
      }

      s"recover when shard storing a start event fails $wayToFail" in {
        val storeProbe = TestProbe()
        system.eventStream.subscribe(storeProbe.ref, classOf[ShardStoreCreated])

        val sharding = ClusterSharding(system).start(
          s"shardStoreStart-$wayToFail",
          Props[EntityActor](),
          ClusterShardingSettings(system).withRememberEntities(true),
          extractEntityId,
          extractShardId)

        // trigger shard start and store creation
        val probe = TestProbe()
        sharding.tell(EntityEnvelope(1, "hello-1"), probe.ref)
        var shardStore = storeProbe.expectMsgType[ShardStoreCreated].store
        probe.expectMsg("hello-1")

        // hit shard with other entity that will fail
        shardStore.tell(FakeShardStoreActor.FailUpdateEntity(wayToFail), storeProbe.ref)
        storeProbe.expectMsg(Done)

        sharding.tell(EntityEnvelope(11, "hello-11"), probe.ref)

        // do we get an answer here? shard crashes
        probe.expectNoMessage()
        if (wayToFail == StopStore || wayToFail == CrashStore) {
          // a new store should be started
          shardStore = storeProbe.expectMsgType[ShardStoreCreated].store
        }

        val stopFailingProbe = TestProbe()
        shardStore.tell(FakeShardStoreActor.ClearFail, stopFailingProbe.ref)
        stopFailingProbe.expectMsg(Done)

        // it takes a while - timeout hits and then backoff
        awaitAssert({
          sharding.tell(EntityEnvelope(11, "hello-11-2"), probe.ref)
          probe.expectMsg("hello-11-2")
        }, 10.seconds)
        system.stop(sharding)
      }

      s"recover on abrupt entity stop when storing a stop event fails $wayToFail" in {
        val storeProbe = TestProbe()
        system.eventStream.subscribe(storeProbe.ref, classOf[ShardStoreCreated])

        val sharding = ClusterSharding(system).start(
          s"shardStoreStopAbrupt-$wayToFail",
          Props[EntityActor](),
          ClusterShardingSettings(system).withRememberEntities(true),
          extractEntityId,
          extractShardId)

        val probe = TestProbe()

        // trigger shard start and store creation
        sharding.tell(EntityEnvelope(1, "hello-1"), probe.ref)
        val shard1Store = storeProbe.expectMsgType[ShardStoreCreated].store
        probe.expectMsg("hello-1")

        // fail it when stopping
        shard1Store.tell(FakeShardStoreActor.FailUpdateEntity(wayToFail), storeProbe.ref)
        storeProbe.expectMsg(Done)

        // FIXME restart without passivating is not saved and re-started again without storing the stop so this isn't testing anything
        sharding ! EntityEnvelope(1, "stop")

        shard1Store.tell(FakeShardStoreActor.ClearFail, storeProbe.ref)
        storeProbe.expectMsg(Done)

        // it takes a while - timeout hits and then backoff
        awaitAssert({
          sharding.tell(EntityEnvelope(1, "hello-2"), probe.ref)
          probe.expectMsg("hello-2")
        }, 10.seconds)
        system.stop(sharding)
      }

      s"recover on graceful entity stop when storing a stop event fails $wayToFail" in {
        val storeProbe = TestProbe()
        system.eventStream.subscribe(storeProbe.ref, classOf[ShardStoreCreated])

        val sharding = ClusterSharding(system).start(
          s"shardStoreStopGraceful-$wayToFail",
          Props[EntityActor](),
          ClusterShardingSettings(system).withRememberEntities(true),
          extractEntityId,
          extractShardId,
          ShardAllocationStrategy.leastShardAllocationStrategy(absoluteLimit = 1, relativeLimit = 0.1),
          "graceful-stop")

        val probe = TestProbe()

        // trigger shard start and store creation
        sharding.tell(EntityEnvelope(1, "hello-1"), probe.ref)
        val shard1Store = storeProbe.expectMsgType[ShardStoreCreated].store
        probe.expectMsg("hello-1")

        // fail it when stopping
        shard1Store.tell(FakeShardStoreActor.FailUpdateEntity(wayToFail), storeProbe.ref)
        storeProbe.expectMsg(Done)

        sharding ! EntityEnvelope(1, "graceful-stop")

        if (wayToFail != CrashStore && wayToFail != StopStore) {
          // race, give the shard some time to see the passivation before restoring the fake shard store
          Thread.sleep(250)
          shard1Store.tell(FakeShardStoreActor.ClearFail, probe.ref)
          probe.expectMsg(Done)
        }

        // it takes a while?
        awaitAssert({
          sharding.tell(EntityEnvelope(1, "hello-2"), probe.ref)
          probe.expectMsg("hello-2")
        }, 5.seconds)
        system.stop(sharding)
      }

      s"recover when coordinator storing shard start fails $wayToFail" in {
        val storeProbe = TestProbe()
        system.eventStream.subscribe(storeProbe.ref, classOf[CoordinatorStoreCreated])

        val sharding = ClusterSharding(system).start(
          s"coordinatorStoreStopGraceful-$wayToFail",
          Props[EntityActor](),
          ClusterShardingSettings(system).withRememberEntities(true),
          extractEntityId,
          extractShardId,
          ShardAllocationStrategy.leastShardAllocationStrategy(absoluteLimit = 1, relativeLimit = 0.1),
          "graceful-stop")

        val probe = TestProbe()

        // coordinator store is triggered by coordinator starting up
        var coordinatorStore = storeProbe.expectMsgType[CoordinatorStoreCreated].store
        coordinatorStore.tell(FakeCoordinatorStoreActor.FailAddShard("1", wayToFail), probe.ref)
        probe.expectMsg(Done)

        sharding.tell(EntityEnvelope(1, "hello-1"), probe.ref)
        probe.expectNoMessage(1.second) // because shard cannot start while store failing

        if (wayToFail == StopStore || wayToFail == CrashStore) {
          // a new store should be started
          coordinatorStore = storeProbe.expectMsgType[CoordinatorStoreCreated].store
        }

        // fail it when stopping
        coordinatorStore.tell(FakeCoordinatorStoreActor.ClearFailShard("1"), storeProbe.ref)
        storeProbe.expectMsg(Done)

        probe.awaitAssert({
          sharding.tell(EntityEnvelope(1, "hello-2"), probe.ref)
          probe.expectMsg("hello-2") // should now work again
        }, 5.seconds)

        system.stop(sharding)
      }
    }

    "neither restart entity nor crash when entity passivates after being eagerly restarted" in {
      val shardingSettings = ClusterShardingSettings(system).withRememberEntities(true)

      var sharding: ActorRef = null
      val probe = TestProbe()
      implicit val sender: ActorRef = probe.ref

      val spawnProbe = TestProbe()
      val props = Props[EntityActor] {
        spawnProbe.ref.tell("spawned", ActorRef.noSender)
        new EntityActor()
      }

      sharding = ClusterSharding(system).start(
        "failStartPassivate",
        props,
        shardingSettings,
        extractEntityId,
        extractShardId,
        ShardAllocationStrategy.leastShardAllocationStrategy(absoluteLimit = 1, relativeLimit = 0.1),
        "graceful-stop")

      sharding ! EntityEnvelope(1, "incarnation")
      spawnProbe.expectMsg("spawned")
      var currentIncarnation = probe.receiveN(1).head.asInstanceOf[ActorRef]

      probe.watch(currentIncarnation)
      currentIncarnation ! "stop"
      probe.expectTerminated(currentIncarnation)
      // The restart timer is active
      currentIncarnation = null

      // restart the entity early
      sharding ! EntityEnvelope(1, "incarnation")
      spawnProbe.expectMsg("spawned")
      sharding ! EntityEnvelope(1, "graceful-stop")
      currentIncarnation = probe.receiveN(1).head.asInstanceOf[ActorRef]

      probe.watch(currentIncarnation)
      probe.expectTerminated(currentIncarnation)
      // entity is now passivated

      probe.watch(sharding)
      spawnProbe.expectNoMessage(1.second)
      probe.expectNoMessage(1.second)
    }
  }

}
