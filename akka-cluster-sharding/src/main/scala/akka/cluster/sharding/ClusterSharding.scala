/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.net.URLEncoder
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.Await
import scala.util.control.NonFatal

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.Deploy
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Status
import akka.annotation.InternalApi
import akka.cluster.Cluster
import akka.cluster.ClusterSettings
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.ddata.Replicator
import akka.cluster.ddata.ReplicatorSettings
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.internal.CustomStateStoreModeProvider
import akka.cluster.sharding.internal.DDataRememberEntitiesProvider
import akka.cluster.sharding.internal.EventSourcedRememberEntitiesProvider
import akka.cluster.sharding.internal.RememberEntitiesProvider
import akka.cluster.singleton.ClusterSingletonManager
import akka.event.Logging
import akka.pattern.BackoffOpts
import akka.pattern.ask
import akka.util.ByteString
import scala.jdk.CollectionConverters._

/**
 * This extension provides sharding functionality of actors in a cluster.
 * The typical use case is when you have many stateful actors that together consume
 * more resources (e.g. memory) than fit on one machine.
 *   - Distribution: You need to distribute them across several nodes in the cluster
 *   - Location Transparency: You need to interact with them using their logical identifier,
 * without having to care about their physical location in the cluster, which can change over time.
 *
 * '''Entities''':
 * It could for example be actors representing Aggregate Roots in Domain-Driven Design
 * terminology. Here we call these actors "entities" which typically have persistent
 * (durable) state, but this feature is not limited to persistent state actors.
 *
 * '''Sharding''':
 * In this context sharding means that actors with an identifier, or entities,
 * can be automatically distributed across multiple nodes in the cluster.
 *
 * '''ShardRegion''':
 * Each entity actor runs only at one place, and messages can be sent to the entity without
 * requiring the sender to know the location of the destination actor. This is achieved by
 * sending the messages via a [[ShardRegion]] actor, provided by this extension. The [[ShardRegion]]
 * knows the shard mappings and routes inbound messages to the entity with the entity id.
 * Messages to the entities are always sent via the local `ShardRegion`.
 * The `ShardRegion` actor is started on each node in the cluster, or group of nodes
 * tagged with a specific role. The `ShardRegion` is created with two application specific
 * functions to extract the entity identifier and the shard identifier from incoming messages.
 *
 * Typical usage of this extension:
 *   1. At system startup on each cluster node by registering the supported entity types with
 * the [[ClusterSharding#start]] method
 *   1. Retrieve the `ShardRegion` actor for a named entity type with [[ClusterSharding#shardRegion]]
 * Settings can be configured as described in the `akka.cluster.sharding` section of the `reference.conf`.
 *
 * '''Shard and ShardCoordinator''':
 * A shard is a group of entities that will be managed together. For the first message in a
 * specific shard the `ShardRegion` requests the location of the shard from a central
 * [[ShardCoordinator]]. The `ShardCoordinator` decides which `ShardRegion`
 * owns the shard. The `ShardRegion` receives the decided home of the shard
 * and if that is the `ShardRegion` instance itself it will create a local child
 * actor representing the entity and direct all messages for that entity to it.
 * If the shard home is another `ShardRegion`, instance messages will be forwarded
 * to that `ShardRegion` instance instead. While resolving the location of a
 * shard, incoming messages for that shard are buffered and later delivered when the
 * shard location is known. Subsequent messages to the resolved shard can be delivered
 * to the target destination immediately without involving the `ShardCoordinator`.
 * To make sure at-most-one instance of a specific entity actor is running somewhere
 * in the cluster it is important that all nodes have the same view of where the shards
 * are located. Therefore the shard allocation decisions are taken by the central
 * `ShardCoordinator`, a cluster singleton, i.e. one instance on
 * the oldest member among all cluster nodes or a group of nodes tagged with a specific
 * role. The oldest member can be determined by [[akka.cluster.Member#isOlderThan]].
 *
 * '''Shard Rebalancing''':
 * To be able to use newly added members in the cluster the coordinator facilitates rebalancing
 * of shards, migrating entities from one node to another. In the rebalance process the
 * coordinator first notifies all `ShardRegion` actors that a handoff for a shard has begun.
 * `ShardRegion` actors will start buffering incoming messages for that shard, as they do when
 * shard location is unknown. During the rebalance process the coordinator will not answer any
 * requests for the location of shards that are being rebalanced, i.e. local buffering will
 * continue until the handoff is complete. The `ShardRegion` responsible for the rebalanced shard
 * will stop all entities in that shard by sending them a `PoisonPill`. When all entities have
 * been terminated the `ShardRegion` owning the entities will acknowledge to the coordinator that
 * the handoff has completed. Thereafter the coordinator will reply to requests for the location of
 * the shard, allocate a new home for the shard and then buffered messages in the
 * `ShardRegion` actors are delivered to the new location. This means that the state of the entities
 * are not transferred or migrated. If the state of the entities are of importance it should be
 * persistent (durable), e.g. with `akka-persistence` so that it can be recovered at the new
 * location.
 *
 * '''Shard Allocation''':
 * The logic deciding which shards to rebalance is defined in a plugable shard allocation
 * strategy. The default implementation `LeastShardAllocationStrategy`
 * picks shards for handoff from the `ShardRegion` with highest number of previously allocated shards.
 * They will then be allocated to the `ShardRegion` with lowest number of previously allocated shards,
 * i.e. new members in the cluster. This strategy can be replaced by an application
 * specific implementation.
 *
 * '''Recovery''':
 * The state of shard locations in the `ShardCoordinator` is stored with `akka-distributed-data` or
 * `akka-persistence` to survive failures. When a crashed or unreachable coordinator
 * node has been removed (via down) from the cluster a new `ShardCoordinator` singleton
 * actor will take over and the state is recovered. During such a failure period shards
 * with known location are still available, while messages for new (unknown) shards
 * are buffered until the new `ShardCoordinator` becomes available.
 *
 * '''Delivery Semantics''':
 * As long as a sender uses the same `ShardRegion` actor to deliver messages to an entity
 * actor the order of the messages is preserved. As long as the buffer limit is not reached
 * messages are delivered on a best effort basis, with at-most once delivery semantics,
 * in the same way as ordinary message sending. Reliable end-to-end messaging, with
 * at-least-once semantics can be added by using `AtLeastOnceDelivery` in `akka-persistence`.
 *
 * Some additional latency is introduced for messages targeted to new or previously
 * unused shards due to the round-trip to the coordinator. Rebalancing of shards may
 * also add latency. This should be considered when designing the application specific
 * shard resolution, e.g. to avoid too fine grained shards.
 *
 * The `ShardRegion` actor can also be started in proxy only mode, i.e. it will not
 * host any entities itself, but knows how to delegate messages to the right location.
 *
 * If the state of the entities are persistent you may stop entities that are not used to
 * reduce memory consumption. This is done by the application specific implementation of
 * the entity actors for example by defining receive timeout (`context.setReceiveTimeout`).
 * If a message is already enqueued to the entity when it stops itself the enqueued message
 * in the mailbox will be dropped. To support graceful passivation without losing such
 * messages the entity actor can send [[ShardRegion.Passivate]] to its parent `ShardRegion`.
 * The specified wrapped message in `Passivate` will be sent back to the entity, which is
 * then supposed to stop itself. Incoming messages will be buffered by the `ShardRegion`
 * between reception of `Passivate` and termination of the entity. Such buffered messages
 * are thereafter delivered to a new incarnation of the entity.
 *
 */
object ClusterSharding extends ExtensionId[ClusterSharding] with ExtensionIdProvider {

  override def get(system: ActorSystem): ClusterSharding = super.get(system)
  override def get(system: ClassicActorSystemProvider): ClusterSharding = super.get(system)

  override def lookup = ClusterSharding

  override def createExtension(system: ExtendedActorSystem): ClusterSharding =
    new ClusterSharding(system)

}

/**
 * @see [[ClusterSharding$ ClusterSharding companion object]]
 */
@nowarn("msg=Use Akka Distributed Cluster")
class ClusterSharding(system: ExtendedActorSystem) extends Extension {
  import ClusterShardingGuardian._
  import ShardCoordinator.ShardAllocationStrategy

  private val log = Logging(system, classOf[ClusterSharding])

  private val cluster = Cluster(system)

  private val regions: ConcurrentHashMap[String, ActorRef] = new ConcurrentHashMap
  private val proxies: ConcurrentHashMap[String, ActorRef] = new ConcurrentHashMap

  private lazy val guardian: ActorRef = {
    val guardianName: String =
      system.settings.config.getString("akka.cluster.sharding.guardian-name")
    val dispatcher = system.settings.config.getString("akka.cluster.sharding.use-dispatcher")
    system.systemActorOf(Props(new ClusterShardingGuardian).withDispatcher(dispatcher), guardianName)
  }

  /**
   * Scala API: Register a named entity type by defining the [[akka.actor.Props]] of the entity actor
   * and functions to extract entity and shard identifier from messages. The [[ShardRegion]] actor
   * for this type can later be retrieved with the [[shardRegion]] method.
   *
   * This method will start a [[ShardRegion]] in proxy mode when there is no match between the roles of
   * the current cluster node and the role specified in [[ClusterShardingSettings]] passed to this method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param entityProps the `Props` of the entity actors that will be created by the `ShardRegion`
   * @param settings configuration settings, see [[ClusterShardingSettings]]
   * @param extractEntityId partial function to extract the entity id and the message to send to the
   *   entity from the incoming message, if the partial function does not match the message will
   *   be `unhandled`, i.e. posted as `Unhandled` messages on the event stream
   * @param extractShardId function to determine the shard id for an incoming message, only messages
   *   that passed the `extractEntityId` will be used
   * @param allocationStrategy possibility to use a custom shard allocation and
   *   rebalancing logic
   * @param handOffStopMessage the message that will be sent to entities when they are to be stopped
   *   for a rebalance or graceful shutdown of a `ShardRegion`, e.g. `PoisonPill`.
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  def start(
      typeName: String,
      entityProps: Props,
      settings: ClusterShardingSettings,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId,
      allocationStrategy: ShardAllocationStrategy,
      handOffStopMessage: Any): ActorRef = {

    internalStart(
      typeName,
      _ => entityProps,
      settings,
      extractEntityId,
      extractShardId,
      allocationStrategy,
      handOffStopMessage)
  }

  /**
   * Scala API: Register a named entity type by defining the [[akka.actor.Props]] of the entity actor
   * and functions to extract entity and shard identifier from messages. The [[ShardRegion]] actor
   * for this type can later be retrieved with the [[shardRegion]] method.
   *
   * This method will start a [[ShardRegion]] in proxy mode when there is no match between the roles of
   * the current cluster node and the role specified in [[ClusterShardingSettings]] passed to this method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param entityProps the `Props` of the entity actors that will be created by the `ShardRegion`
   * @param extractEntityId partial function to extract the entity id and the message to send to the
   *   entity from the incoming message, if the partial function does not match the message will
   *   be `unhandled`, i.e. posted as `Unhandled` messages on the event stream
   * @param extractShardId function to determine the shard id for an incoming message, only messages
   *   that passed the `extractEntityId` will be used
   * @param allocationStrategy possibility to use a custom shard allocation and
   *   rebalancing logic
   * @param handOffStopMessage the message that will be sent to entities when they are to be stopped
   *   for a rebalance or graceful shutdown of a `ShardRegion`, e.g. `PoisonPill`.
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  def start(
      typeName: String,
      entityProps: Props,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId,
      allocationStrategy: ShardAllocationStrategy,
      handOffStopMessage: Any): ActorRef = {

    start(
      typeName,
      entityProps,
      ClusterShardingSettings(system),
      extractEntityId,
      extractShardId,
      allocationStrategy,
      handOffStopMessage)
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def internalStart(
      typeName: String,
      entityProps: String => Props,
      settings: ClusterShardingSettings,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId,
      allocationStrategy: ShardAllocationStrategy,
      handOffStopMessage: Any): ActorRef = {

    if (settings.stateStoreMode == ClusterShardingSettings.StateStoreModePersistence)
      log.warning("Cluster Sharding has been set to use the deprecated `persistence` state store mode.")

    if (settings.shouldHostShard(cluster)) {
      regions.get(typeName) match {
        case null =>
          // it's ok to Start several time, the guardian will deduplicate concurrent requests
          implicit val timeout = system.settings.CreationTimeout
          val startMsg = Start(
            typeName,
            entityProps,
            settings,
            extractEntityId,
            extractShardId,
            allocationStrategy,
            handOffStopMessage)
          val shardRegion = Await.result((guardian ? startMsg).mapTo[Started], timeout.duration).shardRegion
          regions.put(typeName, shardRegion)
          shardRegion
        case ref => ref // already started, use cached ActorRef
      }
    } else {
      log.debug("Starting Shard Region Proxy [{}] (no actors will be hosted on this node)...", typeName)

      if (settings.shouldHostCoordinator(cluster)) {
        // when using another coordinator role than the shard role the coordinator may have to be started
        val startCoordinatorMsg = StartCoordinatorIfNeeded(typeName, settings, allocationStrategy)
        guardian ! startCoordinatorMsg
      }

      startProxy(
        typeName,
        settings.role,
        dataCenter = None, // startProxy method must be used directly to start a proxy for another DC
        extractEntityId,
        extractShardId)
    }
  }

  /**
   * Register a named entity type by defining the [[akka.actor.Props]] of the entity actor and
   * functions to extract entity and shard identifier from messages. The [[ShardRegion]] actor
   * for this type can later be retrieved with the [[shardRegion]] method.
   *
   * The default shard allocation strategy [[ShardCoordinator.LeastShardAllocationStrategy]]
   * is used. [[akka.actor.PoisonPill]] is used as `handOffStopMessage`.
   *
   * This method will start a [[ShardRegion]] in proxy mode when there is no match between the
   * node roles and the role specified in the [[ClusterShardingSettings]] passed to this method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param entityProps the `Props` of the entity actors that will be created by the `ShardRegion`
   * @param settings configuration settings, see [[ClusterShardingSettings]]
   * @param extractEntityId partial function to extract the entity id and the message to send to the
   *   entity from the incoming message, if the partial function does not match the message will
   *   be `unhandled`, i.e. posted as `Unhandled` messages on the event stream
   * @param extractShardId function to determine the shard id for an incoming message, only messages
   *   that passed the `extractEntityId` will be used
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  def start(
      typeName: String,
      entityProps: Props,
      settings: ClusterShardingSettings,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId): ActorRef = {

    val allocationStrategy = defaultShardAllocationStrategy(settings)

    start(typeName, entityProps, settings, extractEntityId, extractShardId, allocationStrategy, PoisonPill)
  }

  /**
   * Register a named entity type by defining the [[akka.actor.Props]] of the entity actor and
   * functions to extract entity and shard identifier from messages. The [[ShardRegion]] actor
   * for this type can later be retrieved with the [[shardRegion]] method.
   *
   * The default shard allocation strategy [[ShardCoordinator.LeastShardAllocationStrategy]]
   * is used. [[akka.actor.PoisonPill]] is used as `handOffStopMessage`.
   *
   * This method will start a [[ShardRegion]] in proxy mode when there is no match between the
   * node roles and the role specified in the [[ClusterShardingSettings]] passed to this method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param entityProps the `Props` of the entity actors that will be created by the `ShardRegion`
   * @param extractEntityId partial function to extract the entity id and the message to send to the
   *   entity from the incoming message, if the partial function does not match the message will
   *   be `unhandled`, i.e. posted as `Unhandled` messages on the event stream
   * @param extractShardId function to determine the shard id for an incoming message, only messages
   *   that passed the `extractEntityId` will be used
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  def start(
      typeName: String,
      entityProps: Props,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId): ActorRef = {

    start(typeName, entityProps, ClusterShardingSettings(system), extractEntityId, extractShardId)
  }

  /**
   * Java/Scala API: Register a named entity type by defining the [[akka.actor.Props]] of the entity actor
   * and functions to extract entity and shard identifier from messages. The [[ShardRegion]] actor
   * for this type can later be retrieved with the [[#shardRegion]] method.
   *
   * This method will start a [[ShardRegion]] in proxy mode when there is no match between the
   * node roles and the role specified in the [[ClusterShardingSettings]] passed to this method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param entityProps the `Props` of the entity actors that will be created by the `ShardRegion`
   * @param settings configuration settings, see [[ClusterShardingSettings]]
   * @param messageExtractor functions to extract the entity id, shard id, and the message to send to the
   *   entity from the incoming message, see [[ShardRegion.MessageExtractor]]
   * @param allocationStrategy possibility to use a custom shard allocation and
   *   rebalancing logic
   * @param handOffStopMessage the message that will be sent to entities when they are to be stopped
   *   for a rebalance or graceful shutdown of a `ShardRegion`, e.g. `PoisonPill`.
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  def start(
      typeName: String,
      entityProps: Props,
      settings: ClusterShardingSettings,
      messageExtractor: ShardRegion.MessageExtractor,
      allocationStrategy: ShardAllocationStrategy,
      handOffStopMessage: Any): ActorRef = {

    internalStart(
      typeName,
      _ => entityProps,
      settings,
      extractEntityId = {
        case msg if messageExtractor.entityId(msg) ne null =>
          (messageExtractor.entityId(msg), messageExtractor.entityMessage(msg))
      },
      extractShardId = msg => messageExtractor.shardId(msg),
      allocationStrategy = allocationStrategy,
      handOffStopMessage = handOffStopMessage)
  }

  /**
   * Java/Scala API: Register a named entity type by defining the [[akka.actor.Props]] of the entity actor
   * and functions to extract entity and shard identifier from messages. The [[ShardRegion]] actor
   * for this type can later be retrieved with the [[#shardRegion]] method.
   *
   * The default shard allocation strategy [[ShardCoordinator.LeastShardAllocationStrategy]]
   * is used. [[akka.actor.PoisonPill]] is used as `handOffStopMessage`.
   *
   * This method will start a [[ShardRegion]] in proxy mode when there is no match between the
   * node roles and the role specified in the [[ClusterShardingSettings]] passed to this method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param entityProps the `Props` of the entity actors that will be created by the `ShardRegion`
   * @param settings configuration settings, see [[ClusterShardingSettings]]
   * @param messageExtractor functions to extract the entity id, shard id, and the message to send to the
   *   entity from the incoming message
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  def start(
      typeName: String,
      entityProps: Props,
      settings: ClusterShardingSettings,
      messageExtractor: ShardRegion.MessageExtractor): ActorRef = {

    val allocationStrategy = defaultShardAllocationStrategy(settings)

    start(typeName, entityProps, settings, messageExtractor, allocationStrategy, PoisonPill)
  }

  /**
   * Java/Scala API: Register a named entity type by defining the [[akka.actor.Props]] of the entity actor
   * and functions to extract entity and shard identifier from messages. The [[ShardRegion]] actor
   * for this type can later be retrieved with the [[#shardRegion]] method.
   *
   * The default shard allocation strategy [[ShardCoordinator.LeastShardAllocationStrategy]]
   * is used. [[akka.actor.PoisonPill]] is used as `handOffStopMessage`.
   *
   * This method will start a [[ShardRegion]] in proxy mode when there is no match between the
   * node roles and the role specified in the [[ClusterShardingSettings]] passed to this method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param entityProps the `Props` of the entity actors that will be created by the `ShardRegion`
   * @param messageExtractor functions to extract the entity id, shard id, and the message to send to the
   *   entity from the incoming message
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  def start(typeName: String, entityProps: Props, messageExtractor: ShardRegion.MessageExtractor): ActorRef = {
    start(typeName, entityProps, ClusterShardingSettings(system), messageExtractor)
  }

  /**
   * Scala API: Register a named entity type `ShardRegion` on this node that will run in proxy only mode,
   * i.e. it will delegate messages to other `ShardRegion` actors on other nodes, but not host any
   * entity actors itself. The [[ShardRegion]] actor for this type can later be retrieved with the
   * [[#shardRegion]] method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param role specifies that this entity type is located on cluster nodes with a specific role.
   *   If the role is not specified all nodes in the cluster are used.
   * @param extractEntityId partial function to extract the entity id and the message to send to the
   *   entity from the incoming message, if the partial function does not match the message will
   *   be `unhandled`, i.e. posted as `Unhandled` messages on the event stream
   * @param extractShardId function to determine the shard id for an incoming message, only messages
   *   that passed the `extractEntityId` will be used
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  def startProxy(
      typeName: String,
      role: Option[String],
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId): ActorRef =
    startProxy(typeName, role, dataCenter = None, extractEntityId, extractShardId)

  /**
   * Scala API: Register a named entity type `ShardRegion` on this node that will run in proxy only mode,
   * i.e. it will delegate messages to other `ShardRegion` actors on other nodes, but not host any
   * entity actors itself. The [[ShardRegion]] actor for this type can later be retrieved with the
   * [[#shardRegion]] method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param role specifies that this entity type is located on cluster nodes with a specific role.
   *   If the role is not specified all nodes in the cluster are used.
   * @param dataCenter The data center of the cluster nodes where the cluster sharding is running.
   *   If None then the same data center as current node.
   * @param extractEntityId partial function to extract the entity id and the message to send to the
   *   entity from the incoming message, if the partial function does not match the message will
   *   be `unhandled`, i.e. posted as `Unhandled` messages on the event stream
   * @param extractShardId function to determine the shard id for an incoming message, only messages
   *   that passed the `extractEntityId` will be used
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def startProxy(
      typeName: String,
      role: Option[String],
      dataCenter: Option[DataCenter],
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId): ActorRef = {

    proxies.get(proxyName(typeName, dataCenter)) match {
      case null =>
        // it's ok to StartProxy several time, the guardian will deduplicate concurrent requests
        implicit val timeout = system.settings.CreationTimeout
        val settings = ClusterShardingSettings(system).withRole(role)
        val startMsg = StartProxy(typeName, dataCenter, settings, extractEntityId, extractShardId)
        val shardRegion = Await.result((guardian ? startMsg).mapTo[Started], timeout.duration).shardRegion
        // it must be possible to start several proxies, one per data center
        proxies.put(proxyName(typeName, dataCenter), shardRegion)
        shardRegion
      case ref => ref // already started, use cached ActorRef
    }
  }

  private def proxyName(typeName: String, dataCenter: Option[DataCenter]): String = {
    dataCenter match {
      case None    => s"${typeName}Proxy"
      case Some(t) => s"${typeName}Proxy" + "-" + t
    }
  }

  /**
   * Java/Scala API: Register a named entity type `ShardRegion` on this node that will run in proxy only mode,
   * i.e. it will delegate messages to other `ShardRegion` actors on other nodes, but not host any
   * entity actors itself. The [[ShardRegion]] actor for this type can later be retrieved with the
   * [[#shardRegion]] method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param role specifies that this entity type is located on cluster nodes with a specific role.
   *   If the role is not specified all nodes in the cluster are used.
   * @param messageExtractor functions to extract the entity id, shard id, and the message to send to the
   *   entity from the incoming message
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  def startProxy(typeName: String, role: Optional[String], messageExtractor: ShardRegion.MessageExtractor): ActorRef =
    startProxy(typeName, role, dataCenter = Optional.empty(), messageExtractor)

  /**
   * Java/Scala API: Register a named entity type `ShardRegion` on this node that will run in proxy only mode,
   * i.e. it will delegate messages to other `ShardRegion` actors on other nodes, but not host any
   * entity actors itself. The [[ShardRegion]] actor for this type can later be retrieved with the
   * [[#shardRegion]] method.
   *
   * Some settings can be configured as described in the `akka.cluster.sharding` section
   * of the `reference.conf`.
   *
   * @param typeName the name of the entity type
   * @param role specifies that this entity type is located on cluster nodes with a specific role.
   *   If the role is not specified all nodes in the cluster are used.
   * @param dataCenter The data center of the cluster nodes where the cluster sharding is running.
   *   If None then the same data center as current node.
   * @param messageExtractor functions to extract the entity id, shard id, and the message to send to the
   *   entity from the incoming message
   * @return the actor ref of the [[ShardRegion]] that is to be responsible for the shard
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def startProxy(
      typeName: String,
      role: Optional[String],
      dataCenter: Optional[String],
      messageExtractor: ShardRegion.MessageExtractor): ActorRef = {

    startProxy(typeName, Option(role.orElse(null)), Option(dataCenter.orElse(null)), extractEntityId = {
      case msg if messageExtractor.entityId(msg) ne null =>
        (messageExtractor.entityId(msg), messageExtractor.entityMessage(msg))
    }, extractShardId = msg => messageExtractor.shardId(msg))

  }

  /**
   * Scala API: get all currently defined sharding type names.
   */
  def shardTypeNames: immutable.Set[String] = regions.keySet().asScala.toSet

  /**
   * Java API: get all currently defined sharding type names.
   */
  def getShardTypeNames: java.util.Set[String] = regions.keySet()

  /**
   * Retrieve the actor reference of the [[ShardRegion]] actor responsible for the named entity type.
   * The entity type must be registered with the [[#start]] or [[#startProxy]] method before it
   * can be used here. Messages to the entity is always sent via the `ShardRegion`.
   */
  def shardRegion(typeName: String): ActorRef = {
    regions.get(typeName) match {
      case null =>
        proxies.get(proxyName(typeName, None)) match {
          case null =>
            throw new IllegalStateException(
              s"Shard type [$typeName] must be started first. Started ${regions.keySet()} proxies ${proxies.keySet()}")
          case ref => ref
        }
      case ref => ref
    }
  }

  /**
   * Retrieve the actor reference of the [[ShardRegion]] actor that will act as a proxy to the
   * named entity type running in another data center. A proxy within the same data center can be accessed
   * with [[#shardRegion]] instead of this method. The entity type must be registered with the
   * [[#startProxy]] method before it can be used here. Messages to the entity is always sent
   * via the `ShardRegion`.
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def shardRegionProxy(typeName: String, dataCenter: DataCenter): ActorRef = {
    proxies.get(proxyName(typeName, Some(dataCenter))) match {
      case null =>
        throw new IllegalStateException(s"Shard type [$typeName] must be started first")
      case ref => ref
    }
  }

  /**
   * The default `ShardAllocationStrategy` is configured by `least-shard-allocation-strategy` properties.
   */
  def defaultShardAllocationStrategy(settings: ClusterShardingSettings): ShardAllocationStrategy = {
    if (settings.tuningParameters.leastShardAllocationAbsoluteLimit > 0) {
      // new algorithm
      val absoluteLimit = settings.tuningParameters.leastShardAllocationAbsoluteLimit
      val relativeLimit = settings.tuningParameters.leastShardAllocationRelativeLimit
      ShardAllocationStrategy.leastShardAllocationStrategy(absoluteLimit, relativeLimit)
    } else {
      // old algorithm
      val threshold = settings.tuningParameters.leastShardAllocationRebalanceThreshold
      val maxSimultaneousRebalance = settings.tuningParameters.leastShardAllocationMaxSimultaneousRebalance
      new ShardCoordinator.LeastShardAllocationStrategy(threshold, maxSimultaneousRebalance)
    }
  }
}

/**
 * INTERNAL API.
 */
private[akka] object ClusterShardingGuardian {
  import ShardCoordinator.ShardAllocationStrategy
  final case class Start(
      typeName: String,
      entityProps: String => Props,
      settings: ClusterShardingSettings,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId,
      allocationStrategy: ShardAllocationStrategy,
      handOffStopMessage: Any)
      extends NoSerializationVerificationNeeded
  final case class StartProxy(
      typeName: String,
      dataCenter: Option[DataCenter],
      settings: ClusterShardingSettings,
      extractEntityId: ShardRegion.ExtractEntityId,
      extractShardId: ShardRegion.ExtractShardId)
      extends NoSerializationVerificationNeeded
  final case class StartCoordinatorIfNeeded(
      typeName: String,
      settings: ClusterShardingSettings,
      allocationStrategy: ShardAllocationStrategy)
      extends NoSerializationVerificationNeeded
  final case class Started(shardRegion: ActorRef) extends NoSerializationVerificationNeeded
}

/**
 * INTERNAL API. [[ShardRegion]] and [[ShardCoordinator]] actors are created as children
 * of this actor.
 */
private[akka] class ClusterShardingGuardian extends Actor {
  import ClusterShardingGuardian._

  val cluster = Cluster(context.system)
  val sharding = ClusterSharding(context.system)

  val majorityMinCap = context.system.settings.config.getInt("akka.cluster.sharding.distributed-data.majority-min-cap")
  private var replicatorByRole = Map.empty[Option[String], ActorRef]

  private def coordinatorSingletonManagerName(encName: String): String =
    encName + "Coordinator"

  private def coordinatorPath(encName: String): String =
    (self.path / coordinatorSingletonManagerName(encName) / "singleton" / "coordinator").toStringWithoutAddress

  private def replicatorSettings(role: Option[String], settings: ClusterShardingSettings) = {
    val configuredSettings =
      ReplicatorSettings(context.system.settings.config.getConfig("akka.cluster.sharding.distributed-data"))
    // Use members within the data center and with the given role (if any)
    val replicatorRoles = Set(ClusterSettings.DcRolePrefix + cluster.settings.SelfDataCenter) ++ role
    val settingsWithRoles = configuredSettings.withRoles(replicatorRoles)
    if (settings.rememberEntities)
      settingsWithRoles
    else
      settingsWithRoles.withDurableKeys(Set.empty[String])
  }

  private def replicator(role: Option[String], settings: ClusterShardingSettings): ActorRef = {
    // one Replicator per role
    replicatorByRole.get(role) match {
      case Some(ref) => ref
      case None =>
        val name = role match {
          case Some(r) => URLEncoder.encode(r, ByteString.UTF_8) + "Replicator"
          case None    => "replicator"
        }
        val ref = context.actorOf(Replicator.props(replicatorSettings(role, settings)), name)
        replicatorByRole = replicatorByRole.updated(role, ref)
        ref
    }
  }

  private def rememberEntitiesStoreProvider(
      typeName: String,
      settings: ClusterShardingSettings): Option[RememberEntitiesProvider] = {
    if (!settings.rememberEntities) None
    else {
      // with the deprecated persistence state store mode we always use the event sourced provider for shard regions
      // and no store for coordinator (the coordinator is a PersistentActor in that case)
      val rememberEntitiesProvider =
        if (settings.stateStoreMode == ClusterShardingSettings.StateStoreModePersistence) {
          ClusterShardingSettings.RememberEntitiesStoreEventsourced
        } else {
          settings.rememberEntitiesStore
        }
      Some(rememberEntitiesProvider match {
        case ClusterShardingSettings.RememberEntitiesStoreDData =>
          // must run Replicator for DDataRememberEntities on both shard and coordinator roles (if different)
          val role = if (settings.role == settings.coordinatorSingletonRole) settings.role else None
          val rep = replicator(role, settings)
          new DDataRememberEntitiesProvider(typeName, settings, majorityMinCap, rep)
        case ClusterShardingSettings.RememberEntitiesStoreEventsourced =>
          new EventSourcedRememberEntitiesProvider(typeName, settings)
        case ClusterShardingSettings.RememberEntitiesStoreCustom =>
          new CustomStateStoreModeProvider(typeName, context.system, settings)
        case unknown =>
          throw new IllegalArgumentException(s"Unknown store type: $unknown") // compiler exhaustiveness check pleaser
      })
    }
  }

  private def startCoordinatorIfNeeded(
      typeName: String,
      allocationStrategy: ShardAllocationStrategy,
      rememberEntitiesStoreProvider: Option[RememberEntitiesProvider],
      settings: ClusterShardingSettings): Unit = {
    import settings.tuningParameters.coordinatorFailureBackoff

    val encName = URLEncoder.encode(typeName, ByteString.UTF_8)
    val cName = coordinatorSingletonManagerName(encName)
    if (settings.shouldHostCoordinator(cluster) && context.child(cName).isEmpty) {
      val coordinatorProps =
        if (settings.stateStoreMode == ClusterShardingSettings.StateStoreModePersistence) {
          ShardCoordinator.props(typeName, settings, allocationStrategy)
        } else {
          val rep = replicator(settings.coordinatorSingletonRole, settings)
          ShardCoordinator.props(
            typeName,
            settings,
            allocationStrategy,
            rep,
            majorityMinCap,
            rememberEntitiesStoreProvider)
        }
      val singletonProps =
        BackoffOpts
          .onStop(
            childProps = coordinatorProps,
            childName = "coordinator",
            minBackoff = coordinatorFailureBackoff,
            maxBackoff = coordinatorFailureBackoff * 5,
            randomFactor = 0.2)
          .withFinalStopMessage(_ == ShardCoordinator.Internal.Terminate)
          .props
          .withDeploy(Deploy.local)

      val singletonSettings =
        settings.coordinatorSingletonSettings.withSingletonName("singleton").withRole(settings.coordinatorSingletonRole)

      context.actorOf(
        ClusterSingletonManager
          .props(singletonProps, terminationMessage = ShardCoordinator.Internal.Terminate, singletonSettings)
          .withDispatcher(context.props.dispatcher),
        name = cName)
    }

  }

  def receive: Receive = {
    case Start(
        typeName,
        entityProps,
        settings,
        extractEntityId,
        extractShardId,
        allocationStrategy,
        handOffStopMessage) =>
      try {
        val encName = URLEncoder.encode(typeName, ByteString.UTF_8)
        val cPath = coordinatorPath(encName)
        val shardRegion = context.child(encName).getOrElse {
          val remEntitiesStoreProvider = rememberEntitiesStoreProvider(typeName, settings)
          startCoordinatorIfNeeded(typeName, allocationStrategy, remEntitiesStoreProvider, settings)

          context.actorOf(
            ShardRegion
              .props(
                typeName = typeName,
                entityProps = entityProps,
                settings = settings,
                coordinatorPath = cPath,
                extractEntityId = extractEntityId,
                extractShardId = extractShardId,
                handOffStopMessage = handOffStopMessage,
                remEntitiesStoreProvider)
              .withDispatcher(context.props.dispatcher),
            name = encName)
        }
        sender() ! Started(shardRegion)
      } catch {
        case NonFatal(e) =>
          // don't restart
          // could be invalid ReplicatorSettings, or InvalidActorNameException
          // if it has already been started
          sender() ! Status.Failure(e)
      }

    case StartProxy(typeName, dataCenter, settings, extractEntityId, extractShardId) =>
      try {

        val encName = URLEncoder.encode(s"${typeName}Proxy", ByteString.UTF_8)
        val cPath = coordinatorPath(URLEncoder.encode(typeName, ByteString.UTF_8))
        // it must be possible to start several proxies, one per data center
        val actorName = dataCenter match {
          case None    => encName
          case Some(t) => URLEncoder.encode(typeName + "-" + t, ByteString.UTF_8)
        }
        val shardRegion = context.child(actorName).getOrElse {
          context.actorOf(
            ShardRegion
              .proxyProps(
                typeName = typeName,
                dataCenter = dataCenter,
                settings = settings,
                coordinatorPath = cPath,
                extractEntityId = extractEntityId,
                extractShardId = extractShardId)
              .withDispatcher(context.props.dispatcher),
            name = actorName)
        }
        sender() ! Started(shardRegion)
      } catch {
        case NonFatal(e) =>
          // don't restart
          // could be InvalidActorNameException if it has already been started
          sender() ! Status.Failure(e)
      }

    case StartCoordinatorIfNeeded(typeName, settings, allocationStrategy) =>
      try {
        val remEntitiesStoreProvider = rememberEntitiesStoreProvider(typeName, settings)
        startCoordinatorIfNeeded(typeName, allocationStrategy, remEntitiesStoreProvider, settings)
      } catch {
        case NonFatal(_) =>
        // don't restart
      }

  }

}
