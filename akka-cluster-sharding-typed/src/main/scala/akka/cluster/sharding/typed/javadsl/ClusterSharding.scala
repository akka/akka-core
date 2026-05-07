/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed
package javadsl

import java.time.Duration
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.annotation.nowarn
import scala.jdk.OptionConverters._

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Props
import akka.actor.typed.RecipientRef
import akka.actor.typed.internal.InternalRecipientRef
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.typed.internal.EntityTypeKeyImpl
import akka.japi.function.{ Function => JFunction }
import akka.pattern.StatusReply
@FunctionalInterface
trait EntityFactory[M] {
  def apply(shardRegion: ActorRef[ClusterSharding.ShardCommand], entityId: String): Behavior[M]
}

object ClusterSharding {
  def get(system: ActorSystem[_]): ClusterSharding =
    scaladsl.ClusterSharding(system).asJava

  /**
   * When an entity is created an `ActorRef[ShardCommand]` is passed to the
   * factory method. The entity can request passivation by sending the [[Passivate]]
   * message to this ref. Sharding will then send back the specified
   * `stopMessage` message to the entity, which is then supposed to stop itself.
   *
   * Not for user extension.
   */
  @DoNotInherit trait ShardCommand extends scaladsl.ClusterSharding.ShardCommand

  /**
   * The entity can request passivation by sending the [[Passivate]] message
   * to the `ActorRef[ShardCommand]` that was passed in to the factory method
   * when creating the entity. Sharding will then send back the specified
   * `stopMessage` message to the entity, which is then supposed to stop
   * itself.
   */
  final case class Passivate[M](entity: ActorRef[M]) extends ShardCommand
}

/**
 * This extension provides sharding functionality of actors in a cluster.
 * The typical use case is when you have many stateful actors that together consume
 * more resources (e.g. memory) than fit on one machine. You need to distribute them across
 * several nodes in the cluster and you want to be able to interact with them using their
 * logical identifier, but without having to care about their physical location in the cluster,
 * which might also change over time. It could for example be actors representing Aggregate Roots in
 * Domain-Driven Design terminology. Here we call these actors "entities". These actors
 * typically have persistent (durable) state, but this feature is not limited to
 * actors with persistent state.
 *
 * In this context sharding means that actors with an identifier, so called entities,
 * can be automatically distributed across multiple nodes in the cluster. Each entity
 * actor runs only at one place, and messages can be sent to the entity without requiring
 * the sender to know the location of the destination actor. This is achieved by sending
 * the messages via a `ShardRegion` actor provided by this extension, which knows how
 * to route the message with the entity id to the final destination.
 *
 * This extension is supposed to be used by first, typically at system startup on each node
 * in the cluster, registering the supported entity types with the [[ClusterSharding#init]]
 * method, which returns the `ShardRegion` actor reference for a named entity type.
 * Messages to the entities are always sent via that `ActorRef`, i.e. the local `ShardRegion`.
 * Messages can also be sent via the [[EntityRef]] retrieved with [[ClusterSharding#entityRefFor]],
 * which will also send via the local `ShardRegion`.
 *
 * Some settings can be configured as described in the `akka.cluster.sharding`
 * section of the `reference.conf`.
 *
 * The `ShardRegion` actor is started on each node in the cluster, or group of nodes
 * tagged with a specific role. The `ShardRegion` is created with a [[ShardingMessageExtractor]]
 * to extract the entity identifier and the shard identifier from incoming messages.
 * A shard is a group of entities that will be managed together. For the first message in a
 * specific shard the `ShardRegion` requests the location of the shard from a central coordinator,
 * the [[akka.cluster.sharding.ShardCoordinator]]. The `ShardCoordinator` decides which `ShardRegion`
 * owns the shard. The `ShardRegion` receives the decided home of the shard
 * and if that is the `ShardRegion` instance itself it will create a local child
 * actor representing the entity and direct all messages for that entity to it.
 * If the shard home is another `ShardRegion` instance messages will be forwarded
 * to that `ShardRegion` instance instead. While resolving the location of a
 * shard incoming messages for that shard are buffered and later delivered when the
 * shard location is known. Subsequent messages to the resolved shard can be delivered
 * to the target destination immediately without involving the `ShardCoordinator`.
 *
 * To make sure that at most one instance of a specific entity actor is running somewhere
 * in the cluster it is important that all nodes have the same view of where the shards
 * are located. Therefore the shard allocation decisions are taken by the central
 * `ShardCoordinator`, which is running as a cluster singleton, i.e. one instance on
 * the oldest member among all cluster nodes or a group of nodes tagged with a specific
 * role. The oldest member can be determined by [[akka.cluster.Member#isOlderThan]].
 *
 * To be able to use newly added members in the cluster the coordinator facilitates rebalancing
 * of shards, i.e. migrate entities from one node to another. In the rebalance process the
 * coordinator first notifies all `ShardRegion` actors that a handoff for a shard has started.
 * That means they will start buffering incoming messages for that shard, in the same way as if the
 * shard location is unknown. During the rebalance process the coordinator will not answer any
 * requests for the location of shards that are being rebalanced, i.e. local buffering will
 * continue until the handoff is completed. The `ShardRegion` responsible for the rebalanced shard
 * will stop all entities in that shard by sending the `handOffMessage` to them. When all entities have
 * been terminated the `ShardRegion` owning the entities will acknowledge the handoff as completed
 * to the coordinator. Thereafter the coordinator will reply to requests for the location of
 * the shard and thereby allocate a new home for the shard and then buffered messages in the
 * `ShardRegion` actors are delivered to the new location. This means that the state of the entities
 * are not transferred or migrated. If the state of the entities are of importance it should be
 * persistent (durable), e.g. with `akka-persistence`, so that it can be recovered at the new
 * location.
 *
 * The logic that decides which shards to rebalance is defined in a plugable shard
 * allocation strategy. The default implementation `LeastShardAllocationStrategy`
 * picks shards for handoff from the ShardRegion` with most number of previously allocated shards.
 * They will then be allocated to the `ShardRegion` with least number of previously allocated shards,
 * i.e. new members in the cluster. This strategy can be replaced by an application specific
 * implementation.
 *
 * The state of shard locations in the `ShardCoordinator` is stored with `akka-distributed-data` or
 * `akka-persistence` to survive failures. When a crashed or unreachable coordinator
 * node has been removed (via down) from the cluster a new `ShardCoordinator` singleton
 * actor will take over and the state is recovered. During such a failure period shards
 * with known location are still available, while messages for new (unknown) shards
 * are buffered until the new `ShardCoordinator` becomes available.
 *
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
 * messages the entity actor can send [[ClusterSharding#Passivate]] to the `ActorRef[ShardCommand]`
 * that was passed in to the factory method when creating the entity..
 * The specified `stopMessage` message will be sent back to the entity, which is
 * then supposed to stop itself. Incoming messages will be buffered by the `ShardRegion`
 * between reception of `Passivate` and termination of the entity. Such buffered messages
 * are thereafter delivered to a new incarnation of the entity.
 *
 * This class is not intended for user extension other than for test purposes (e.g.
 * stub implementation). More methods may be added in the future and that may break
 * such implementations.
 */
@DoNotInherit
abstract class ClusterSharding {

  /**
   * Initialize sharding for the given `entity` factory settings.
   *
   * It will start a shard region or a proxy depending on if the settings require role and if this node has
   * such a role.
   *
   * @tparam M The type of message the entity accepts
   * @tparam E A possible envelope around the message the entity accepts
   */
  def init[M, E](entity: Entity[M, E]): ActorRef[E]

  /**
   * Create an `ActorRef`-like reference to a specific sharded entity.
   *
   * You have to correctly specify the type of messages the target can handle via the `typeKey`.
   *
   * Messages sent through this [[EntityRef]] will be wrapped in a [[ShardingEnvelope]] including the
   * here provided `entityId`.
   *
   * This can only be used if the default [[ShardingEnvelope]] is used, when using custom envelopes or in message
   * entity ids you will need to use the `ActorRef&lt;E>` returned by sharding init for messaging with the sharded actors.
   *
   * For in-depth documentation of its semantics, see [[EntityRef]].
   */
  def entityRefFor[M](typeKey: EntityTypeKey[M], entityId: String): EntityRef[M]

  /**
   * Create an `ActorRef`-like reference to a specific sharded entity running in another data center.
   *
   * You have to correctly specify the type of messages the target can handle via the `typeKey`.
   *
   * Messages sent through this [[EntityRef]] will be wrapped in a [[ShardingEnvelope]] including the
   * provided `entityId`.
   *
   * This can only be used if the default [[ShardingEnvelope]] is used, when using custom envelopes or in message
   * entity ids you will need to use the `ActorRef[E]` returned by sharding init for messaging with the sharded actors.
   *
   * For in-depth documentation of its semantics, see [[EntityRef]].
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def entityRefFor[M](typeKey: EntityTypeKey[M], entityId: String, dataCenter: String): EntityRef[M]

  /**
   * Actor for querying Cluster Sharding state
   */
  def shardState: ActorRef[ClusterShardingQuery]

  /**
   * Access to the `ActorRef` to send `ShardCommand` for a given entity type. For example
   * [[ClusterSharding.Passivate]] can be sent to this `ActorRef`. Note that this `ActorRef`
   * is also available in the [[EntityContext]]. The entity type must first be initialized
   * with the [[ClusterSharding.init]] method.
   */
  def shard(typeKey: EntityTypeKey[_]): ActorRef[ClusterSharding.ShardCommand]

  /**
   * The default `ShardAllocationStrategy` is configured by `least-shard-allocation-strategy` properties.
   */
  def defaultShardAllocationStrategy(settings: ClusterShardingSettings): ShardAllocationStrategy
}

object Entity {

  /**
   * Defines how the entity should be created. Used in [[ClusterSharding#init]]. More optional
   * settings can be defined using the `with` methods of the returned [[Entity]].
   *
   * @param typeKey A key that uniquely identifies the type of entity in this cluster
   * @param createBehavior Create the behavior for an entity given a [[EntityContext]] (includes entityId)
   * @tparam M The type of message the entity accepts
   */
  def of[M](
      typeKey: EntityTypeKey[M],
      createBehavior: JFunction[EntityContext[M], Behavior[M]]): Entity[M, ShardingEnvelope[M]] = {
    new Entity(
      createBehavior,
      typeKey,
      Optional.empty(),
      Props.empty,
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty())
  }

}

/**
 * Defines how the entity should be created. Used in [[ClusterSharding#init]].
 */
final class Entity[M, E] private (
    val createBehavior: JFunction[EntityContext[M], Behavior[M]],
    val typeKey: EntityTypeKey[M],
    val stopMessage: Optional[M],
    val entityProps: Props,
    val settings: Optional[ClusterShardingSettings],
    val messageExtractor: Optional[ShardingMessageExtractor[E, M]],
    val allocationStrategy: Optional[ShardAllocationStrategy],
    val role: Optional[String],
    val dataCenter: Optional[String]) {

  /**
   * [[akka.actor.typed.Props]] of the entity actors, such as dispatcher settings.
   */
  def withEntityProps(newEntityProps: Props): Entity[M, E] =
    copy(entityProps = newEntityProps)

  /**
   * Additional settings, typically loaded from configuration.
   */
  def withSettings(newSettings: ClusterShardingSettings): Entity[M, E] =
    copy(settings = Optional.ofNullable(newSettings))

  /**
   * Message sent to an entity to tell it to stop, e.g. when rebalanced or passivated.
   * If this is not defined it will be stopped automatically.
   * It can be useful to define a custom stop message if the entity needs to perform
   * some asynchronous cleanup or interactions before stopping.
   */
  def withStopMessage(newStopMessage: M): Entity[M, E] =
    copy(stopMessage = Optional.ofNullable(newStopMessage))

  /**
   *
   * If a `messageExtractor` is not specified the messages are sent to the entities by wrapping
   * them in [[ShardingEnvelope]] with the entityId of the recipient actor. That envelope
   * is used by the [[HashCodeMessageExtractor]] for extracting entityId and shardId. The number of
   * shards is then defined by `numberOfShards` in `ClusterShardingSettings`, which by default
   * is configured with `akka.cluster.sharding.number-of-shards`.
   */
  def withMessageExtractor[Envelope](newExtractor: ShardingMessageExtractor[Envelope, M]): Entity[M, Envelope] =
    new Entity(
      createBehavior,
      typeKey,
      stopMessage,
      entityProps,
      settings,
      Optional.ofNullable(newExtractor),
      allocationStrategy,
      role,
      dataCenter)

  /**
   *  Run the Entity actors on nodes with the given role.
   */
  def withRole(role: String): Entity[M, E] =
    copy(role = Optional.ofNullable(role))

  /**
   * The data center of the cluster nodes where the cluster sharding is running.
   * If the dataCenter is not specified then the same data center as current node. If the given
   * dataCenter does not match the data center of the current node the `ShardRegion` will be started
   * in proxy mode.
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def withDataCenter(newDataCenter: String): Entity[M, E] = copy(dataCenter = Optional.ofNullable(newDataCenter))

  /**
   * Allocation strategy which decides on which nodes to allocate new shards,
   * [[ClusterSharding#defaultShardAllocationStrategy]] is used if this is not specified.
   */
  def withAllocationStrategy(newAllocationStrategy: ShardAllocationStrategy): Entity[M, E] =
    copy(allocationStrategy = Optional.ofNullable(newAllocationStrategy))

  private def copy(
      createBehavior: JFunction[EntityContext[M], Behavior[M]] = createBehavior,
      typeKey: EntityTypeKey[M] = typeKey,
      stopMessage: Optional[M] = stopMessage,
      entityProps: Props = entityProps,
      settings: Optional[ClusterShardingSettings] = settings,
      allocationStrategy: Optional[ShardAllocationStrategy] = allocationStrategy,
      role: Optional[String] = role,
      dataCenter: Optional[String] = dataCenter): Entity[M, E] = {
    new Entity(
      createBehavior,
      typeKey,
      stopMessage,
      entityProps,
      settings,
      messageExtractor,
      allocationStrategy,
      role,
      dataCenter)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def toScala: akka.cluster.sharding.typed.scaladsl.Entity[M, E] =
    new akka.cluster.sharding.typed.scaladsl.Entity(
      eCtx => createBehavior(eCtx.toJava),
      typeKey.asScala,
      stopMessage.toScala,
      entityProps,
      settings.toScala,
      messageExtractor.toScala,
      allocationStrategy.toScala,
      role.toScala,
      dataCenter.toScala)

}

/**
 * Parameter to `createBehavior` function in [[Entity.of]].
 *
 * Cluster Sharding is often used together with [[akka.persistence.typed.javadsl.EventSourcedBehavior]]
 * for the entities. See more considerations in [[akka.persistence.typed.PersistenceId]].
 * The `PersistenceId` of the `EventSourcedBehavior` can typically be constructed with:
 * {{{
 * PersistenceId.of(entityContext.getEntityTypeKey().name(), entityContext.getEntityId())
 * }}}
 *
 * @param entityTypeKey the key of the entity type
 * @param entityId the business domain identifier of the entity
 */
final class EntityContext[M](
    entityTypeKey: EntityTypeKey[M],
    entityId: String,
    shard: ActorRef[ClusterSharding.ShardCommand]) {

  def getEntityTypeKey: EntityTypeKey[M] = entityTypeKey

  def getEntityId: String = entityId

  def getShard: ActorRef[ClusterSharding.ShardCommand] = shard

}

@nowarn // for unused msgClass to make class type explicit in the Java API. Not using @nowarn("msg=never used") as the user is likely to see it
/** Allows starting a specific Sharded Entity by its entity identifier */
object StartEntity {

  /**
   * Returns [[ShardingEnvelope]] which can be sent via Cluster Sharding in order to wake up the
   * specified (by `entityId`) Sharded Entity, ''without'' delivering a real message to it.
   */
  def create[M](msgClass: Class[M], entityId: String): ShardingEnvelope[M] =
    scaladsl.StartEntity[M](entityId)
}

/**
 * The key of an entity type, the `name` must be unique.
 *
 * Not for user extension.
 */
@DoNotInherit abstract class EntityTypeKey[-T] { scaladslSelf: scaladsl.EntityTypeKey[T] =>

  /**
   * Name of the entity type.
   */
  def name: String

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def asScala: scaladsl.EntityTypeKey[T] = scaladslSelf

}

object EntityTypeKey {

  /**
   * Creates an `EntityTypeKey`. The `name` must be unique.
   */
  def create[T](messageClass: Class[T], name: String): EntityTypeKey[T] =
    EntityTypeKeyImpl(name, messageClass.getName)

}

/**
 * A reference to an sharded Entity, which allows `ActorRef`-like usage.
 *
 * An [[EntityRef]] is NOT an [[ActorRef]]–by design–in order to be explicit about the fact that the life-cycle
 * of a sharded Entity is very different than a plain Actor. Most notably, this is shown by features of Entities
 * such as re-balancing (an active Entity to a different node) or passivation. Both of which are aimed to be completely
 * transparent to users of such Entity. In other words, if this were to be a plain ActorRef, it would be possible to
 * apply DeathWatch to it, which in turn would then trigger when the sharded Actor stopped, breaking the illusion that
 * Entity refs are "always there". Please note that while not encouraged, it is possible to expose an Actor's `self`
 * [[ActorRef]] and watch it in case such notification is desired.
 *
 * Not for user extension.
 */
@DoNotInherit abstract class EntityRef[-M] extends RecipientRef[M] {
  scaladslSelf: scaladsl.EntityRef[M] with InternalRecipientRef[M] =>

  /**
   * The identifier for the particular entity referenced by this EntityRef.
   */
  def getEntityId: String = entityId

  /**
   * The name of the EntityTypeKey associated with this EntityRef
   */
  def getTypeKey: javadsl.EntityTypeKey[M] = typeKey.asJava

  /**
   * The specified datacenter of the incarnation of the particular entity referenced by this EntityRef,
   * if a datacenter was specified.
   */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def getDataCenter: Optional[String] =
    Optional.ofNullable(dataCenter.orNull)

  /**
   * Send a message to the entity referenced by this EntityRef using *at-most-once*
   * messaging semantics.
   */
  def tell(msg: M): Unit

  /**
   * Allows to "ask" the [[EntityRef]] for a reply.
   * See [[akka.actor.typed.javadsl.AskPattern]] for a complete write-up of this pattern
   *
   * Note that if you are inside of an actor you should prefer [[akka.actor.typed.javadsl.ActorContext.ask]]
   * as that provides better safety.
   *
   * @tparam Res The response protocol, what the other actor sends back
   */
  def ask[Res](message: JFunction[ActorRef[Res], M], timeout: Duration): CompletionStage[Res]

  /**
   * The same as [[ask]] but only for requests that result in a response of type [[akka.pattern.StatusReply]].
   * If the response is a [[akka.pattern.StatusReply#success]] the returned future is completed successfully with the wrapped response.
   * If the status response is a [[akka.pattern.StatusReply#error]] the returned future will be failed with the
   * exception in the error (normally a [[akka.pattern.StatusReply.ErrorMessage]]).
   */
  def askWithStatus[Res](f: ActorRef[StatusReply[Res]] => M, timeout: Duration): CompletionStage[Res]

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def asScala: scaladsl.EntityRef[M] = scaladslSelf

}
