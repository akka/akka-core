/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.io.File

import scala.annotation.nowarn
import scala.concurrent.duration._

import org.apache.commons.io.FileUtils

import akka.actor.{ Actor, ActorIdentity, ActorLogging, ActorRef, ActorSystem, Identify, PoisonPill, Props }
import akka.cluster.MultiNodeClusterSpec
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.persistence.Persistence
import akka.persistence.journal.leveldb.{ SharedLeveldbJournal, SharedLeveldbStore }
import akka.remote.testconductor.RoleName
import akka.serialization.jackson.CborSerializable
import akka.testkit.{ TestActors, TestProbe }

object MultiNodeClusterShardingSpec {

  object EntityActor {
    final case class Started(ref: ActorRef)
  }

  class EntityActor(probe: ActorRef) extends Actor {
    probe ! EntityActor.Started(self)

    def receive: Receive = {
      case m => sender() ! m
    }
  }

  object PingPongActor {
    case object Stop extends CborSerializable
    case class Ping(id: Long) extends CborSerializable
    case object Pong extends CborSerializable
  }

  class PingPongActor extends Actor with ActorLogging {
    import PingPongActor._
    log.info(s"entity started {}", self.path)
    def receive: Receive = {
      case Stop    => context.stop(self)
      case _: Ping => sender() ! Pong
    }
  }

  object ShardedEntity {
    case object Stop
  }

  class ShardedEntity extends Actor {
    def receive: Receive = {
      case id: Int => sender() ! id
      case ShardedEntity.Stop =>
        context.stop(self)
    }
  }

  val intExtractEntityId: ShardRegion.ExtractEntityId = {
    case id: Int => (id.toString, id)
  }
  val intExtractShardId: ShardRegion.ExtractShardId = msg =>
    msg match {
      case id: Int                     => id.toString
      case ShardRegion.StartEntity(id) => id
      case _                           => throw new IllegalArgumentException()
    }

}

/**
 * Note that this class is not used anywhere yet, but could be a good starting point
 * for new or refactored multi-node sharding specs
 */
abstract class MultiNodeClusterShardingSpec(val config: MultiNodeClusterShardingConfig)
    extends MultiNodeClusterSpec(config) {

  import MultiNodeClusterShardingSpec._
  import config._

  override def initialParticipants: Int = roles.size

  protected lazy val settings = ClusterShardingSettings(system).withRememberEntities(config.rememberEntities)

  private lazy val defaultShardAllocationStrategy =
    ClusterSharding(system).defaultShardAllocationStrategy(settings)

  protected lazy val storageLocations = List(
    new File(system.settings.config.getString("akka.cluster.sharding.distributed-data.durable.lmdb.dir")).getParentFile)

  override def expectedTestDuration = 120.seconds

  override protected def atStartup(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
    enterBarrier("startup")
    super.atStartup()
  }

  override protected def afterTermination(): Unit = {
    storageLocations.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
    super.afterTermination()
  }

  /**
   * Flexible cluster join pattern usage.
   *
   * @param from the node the `Cluster.join` is `runOn`
   * @param to the node to join to
   * @param onJoinedRunOnFrom optionally execute a function after join validation
   *                          is successful, e.g. start sharding or create coordinator
   * @param assertNodeUp if disabled - false, the joining member's `MemberStatus.Up`
   *                      and similar assertions are not run. This allows tests that were
   *                      not doing assertions (e.g. ClusterShardingMinMembersSpec) or
   *                      doing them after `onJoinedRunOnFrom` more flexibility.
   *                      Defaults to true, running member status checks.
   */
  protected def join(
      from: RoleName,
      to: RoleName,
      onJoinedRunOnFrom: => Unit = (),
      assertNodeUp: Boolean = true,
      max: FiniteDuration = 20.seconds): Unit = {

    runOn(from) {
      cluster.join(node(to).address)
      if (assertNodeUp) {
        within(max) {
          awaitAssert {
            cluster.state.isMemberUp(node(from).address) should ===(true)
          }
        }
      }
      onJoinedRunOnFrom
    }
    enterBarrier(from.name + "-joined")
  }

  protected def startSharding(
      sys: ActorSystem,
      typeName: String,
      entityProps: Props = TestActors.echoActorProps,
      settings: ClusterShardingSettings = settings,
      extractEntityId: ShardRegion.ExtractEntityId = intExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId = intExtractShardId,
      allocationStrategy: ShardAllocationStrategy = defaultShardAllocationStrategy,
      handOffStopMessage: Any = PoisonPill): ActorRef = {

    ClusterSharding(sys).start(
      typeName,
      entityProps,
      settings,
      extractEntityId,
      extractShardId,
      allocationStrategy,
      handOffStopMessage)
  }

  protected def startProxy(
      sys: ActorSystem,
      typeName: String,
      role: Option[String],
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId): ActorRef = {
    ClusterSharding(sys).startProxy(typeName, role, extractEntityId, extractShardId)
  }

  protected def isDdataMode = mode == ClusterShardingSettings.StateStoreModeDData
  protected def persistenceIsNeeded: Boolean =
    mode == ClusterShardingSettings.StateStoreModePersistence ||
    system.settings.config
      .getString("akka.cluster.sharding.remember-entities-store") == ClusterShardingSettings.RememberEntitiesStoreEventsourced

  protected def setStoreIfNeeded(sys: ActorSystem, storeOn: RoleName): Unit =
    if (persistenceIsNeeded) setStore(sys, storeOn)

  protected def setStore(sys: ActorSystem, storeOn: RoleName): Unit = {
    val probe = TestProbe()(sys)
    sys.actorSelection(node(storeOn) / "user" / "store").tell(Identify(None), probe.ref)
    val sharedStore = probe.expectMsgType[ActorIdentity](20.seconds).ref.get
    SharedLeveldbJournal.setStore(sharedStore, sys)
  }

  /**
   * {{{
   *    startPersistenceIfNeeded(startOn = first, setStoreOn = Seq(first, second, third))
   * }}}
   *
   * @param startOn the node to start the `SharedLeveldbStore` store on
   * @param setStoreOn the nodes to `SharedLeveldbJournal.setStore` on
   */
  protected def startPersistenceIfNeeded(startOn: RoleName, setStoreOn: Seq[RoleName]): Unit =
    if (persistenceIsNeeded) startPersistence(startOn, setStoreOn)

  /**
   * {{{
   *    startPersistence(startOn = first, setStoreOn = Seq(first, second, third))
   * }}}
   *
   * @param startOn the node to start the `SharedLeveldbStore` store on
   * @param setStoreOn the nodes to `SharedLeveldbJournal.setStore` on
   */
  @nowarn("msg=deprecated")
  protected def startPersistence(startOn: RoleName, setStoreOn: Seq[RoleName]): Unit = {
    info("Setting up setup shared journal.")

    Persistence(system)
    runOn(startOn) {
      system.actorOf(Props[SharedLeveldbStore](), "store")
    }
    enterBarrier("persistence-started")

    runOn(setStoreOn: _*) {
      setStore(system, startOn)
    }

    enterBarrier(s"after-${startOn.name}")
  }

}
