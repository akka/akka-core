/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.util.UUID
import java.util.concurrent.CompletionStage

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.util.Success

import akka.actor._
import akka.actor.DeadLetterSuppression
import akka.annotation.{ InternalApi, InternalStableApi }
import akka.annotation.DoNotInherit
import akka.cluster.Cluster
import akka.cluster.ClusterEvent
import akka.cluster.ClusterEvent._
import akka.cluster.ddata.LWWRegister
import akka.cluster.ddata.LWWRegisterKey
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.internal.{
  EventSourcedRememberEntitiesCoordinatorStore,
  RememberEntitiesCoordinatorStore,
  RememberEntitiesProvider
}
import akka.cluster.sharding.internal.AbstractLeastShardAllocationStrategy
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.RegionEntry
import akka.cluster.sharding.internal.ClusterShardAllocationMixin.ShardSuitabilityOrdering
import akka.cluster.sharding.internal.EventSourcedRememberEntitiesCoordinatorStore.MigrationMarker
import akka.event.{ BusLogging, Logging }
import akka.pattern.{ pipe, AskTimeoutException }
import akka.persistence._
import akka.util.PrettyDuration._
import akka.util.Timeout

/**
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
object ShardCoordinator {

  import ShardRegion.ShardId

  /**
   * INTERNAL API
   * Factory method for the [[akka.actor.Props]] of the [[ShardCoordinator]] actor.
   */
  @nowarn("msg=deprecated")
  private[akka] def props(
      typeName: String,
      settings: ClusterShardingSettings,
      allocationStrategy: ShardAllocationStrategy): Props =
    Props(new PersistentShardCoordinator(typeName: String, settings, allocationStrategy)).withDeploy(Deploy.local)

  /**
   * INTERNAL API
   * Factory method for the [[akka.actor.Props]] of the [[ShardCoordinator]] actor with state based on ddata.
   */
  @InternalStableApi
  private[akka] def props(
      typeName: String,
      settings: ClusterShardingSettings,
      allocationStrategy: ShardAllocationStrategy,
      replicator: ActorRef,
      majorityMinCap: Int,
      rememberEntitiesStoreProvider: Option[RememberEntitiesProvider]): Props =
    Props(
      new DDataShardCoordinator(
        typeName: String,
        settings,
        allocationStrategy,
        replicator,
        majorityMinCap,
        rememberEntitiesStoreProvider)).withDeploy(Deploy.local)

  /**
   * Java API: `ShardAllocationStrategy` that  allocates new shards to the `ShardRegion` (node) with least
   * number of previously allocated shards.
   *
   * When a node is added to the cluster the shards on the existing nodes will be rebalanced to the new node.
   * The `LeastShardAllocationStrategy` picks shards for rebalancing from the `ShardRegion`s with most number
   * of previously allocated shards. They will then be allocated to the `ShardRegion` with least number of
   * previously allocated shards, i.e. new members in the cluster. The amount of shards to rebalance in each
   * round can be limited to make it progress slower since rebalancing too many shards at the same time could
   * result in additional load on the system. For example, causing many Event Sourced entites to be started
   * at the same time.
   *
   * It will not rebalance when there is already an ongoing rebalance in progress.
   *
   * @param absoluteLimit the maximum number of shards that will be rebalanced in one rebalance round
   * @param relativeLimit fraction (< 1.0) of total number of (known) shards that will be rebalanced
   *                      in one rebalance round
   */
  def leastShardAllocationStrategy(absoluteLimit: Int, relativeLimit: Double): ShardAllocationStrategy =
    ShardAllocationStrategy.leastShardAllocationStrategy(absoluteLimit, relativeLimit)

  object ShardAllocationStrategy {

    /**
     * Scala API: `ShardAllocationStrategy` that  allocates new shards to the `ShardRegion` (node) with least
     * number of previously allocated shards.
     *
     * When a node is added to the cluster the shards on the existing nodes will be rebalanced to the new node.
     * The `LeastShardAllocationStrategy` picks shards for rebalancing from the `ShardRegion`s with most number
     * of previously allocated shards. They will then be allocated to the `ShardRegion` with least number of
     * previously allocated shards, i.e. new members in the cluster. The amount of shards to rebalance in each
     * round can be limited to make it progress slower since rebalancing too many shards at the same time could
     * result in additional load on the system. For example, causing many Event Sourced entites to be started
     * at the same time.
     *
     * It will not rebalance when there is already an ongoing rebalance in progress.
     *
     * @param absoluteLimit the maximum number of shards that will be rebalanced in one rebalance round
     * @param relativeLimit fraction (< 1.0) of total number of (known) shards that will be rebalanced
     *                      in one rebalance round
     */
    def leastShardAllocationStrategy(absoluteLimit: Int, relativeLimit: Double): ShardAllocationStrategy =
      new internal.LeastShardAllocationStrategy(absoluteLimit, relativeLimit)
  }

  /**
   * Interface of the pluggable shard allocation and rebalancing logic used by the [[ShardCoordinator]].
   *
   * Java implementations should extend [[AbstractShardAllocationStrategy]].
   */
  trait ShardAllocationStrategy extends NoSerializationVerificationNeeded {

    /**
     * Invoked when the location of a new shard is to be decided.
     *
     * @param requester               actor reference to the [[ShardRegion]] that requested the location of the
     *                                shard, can be returned if preference should be given to the node where the shard was first accessed
     * @param shardId                 the id of the shard to allocate
     * @param currentShardAllocations all actor refs to `ShardRegion` and their current allocated shards,
     *                                in the order they were allocated
     * @return a `Future` of the actor ref of the [[ShardRegion]] that is to be responsible for the shard, must be one of
     *         the references included in the `currentShardAllocations` parameter
     */
    def allocateShard(
        requester: ActorRef,
        shardId: ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef]

    /**
     * Invoked periodically to decide which shards to rebalance to another location.
     *
     * @param currentShardAllocations all actor refs to `ShardRegion` and their current allocated shards,
     *                                in the order they were allocated
     * @param rebalanceInProgress     set of shards that are currently being rebalanced, i.e.
     *                                you should not include these in the returned set
     * @return a `Future` of the shards to be migrated, may be empty to skip rebalance in this round
     */
    def rebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
        rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]]
  }

  /**
   * Shard allocation strategy where start is called by the shard coordinator before any calls to
   * rebalance or allocate shard. This can be used if there is any expensive initialization to be done
   * that you do not want to to in the constructor as it will happen on every node rather than just
   * the node that hosts the ShardCoordinator
   */
  trait StartableAllocationStrategy extends ShardAllocationStrategy {

    /**
     * Called before any calls to allocate/rebalance.
     * Do not block. If asynchronous actions are required they can be started here and
     * delay the Futures returned by allocate/rebalance.
     */
    def start(): Unit
  }

  /**
   * Shard allocation strategy where start is called by the shard coordinator before any calls to
   * rebalance or allocate shard. This is much like the [[StartableAllocationStrategy]] but will
   * get access to the actor system when started, for example to interact with extensions.
   *
   * INTERNAL API
   */
  @InternalApi
  private[akka] trait ActorSystemDependentAllocationStrategy extends ShardAllocationStrategy {

    /**
     * Called before any calls to allocate/rebalance.
     * Do not block. If asynchronous actions are required they can be started here and
     * delay the Futures returned by allocate/rebalance.
     */
    def start(system: ActorSystem): Unit
  }

  /**
   * Java API: Java implementations of custom shard allocation and rebalancing logic used by the [[ShardCoordinator]]
   * should extend this abstract class and override [[allocateNewShard]] and [[rebalanceShard]].
   *
   * Earlier versions of this API had different extension points.  Overriding those is still supported
   * but they may be removed in a future release.
   */
  abstract class AbstractShardAllocationStrategy extends ShardAllocationStrategy {
    import java.util.concurrent.CompletableFuture

    @nowarn("cat=deprecation") // calls deprecated allocateShard
    override final def allocateShard(
        requester: ActorRef,
        shardId: ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] = {

      import scala.jdk.CollectionConverters._
      allocateShard(requester, shardId, currentShardAllocations.asJava)
    }

    @nowarn("cat=deprecation") // calls deprecated rebalance
    override final def rebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
        rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] = {
      import scala.jdk.CollectionConverters._
      implicit val ec = ExecutionContext.parasitic
      rebalance(currentShardAllocations.asJava, rebalanceInProgress.asJava).map(_.asScala.toSet)
    }

    /**
     * Invoked when the location of a new shard is to be decided.
     *
     * The default implementation defers to [[allocateNewShard]].  In earlier versions of this API, this
     * Scala Future-returning method was the extension point.
     *
     * New implementations of this class should prefer to override [[allocateNewShard]].  This method may be removed
     * in a future release.
     *
     * @param requester               actor reference to the [[ShardRegion]] that requested the location of the
     *                                shard, can be returned if preference should be given to the node where the shard was first accessed
     * @param shardId                 the id of the shard to allocate
     * @param currentShardAllocations all actor refs to `ShardRegion` and their current allocated shards,
     *                                in the order they were allocated
     * @return a `Future` of the actor ref of the [[ShardRegion]] that is to be responsible for the shard, must be one of
     *         the references included in the `currentShardAllocations` parameter
     */
    @deprecated("prefer allocateNewShard", "2.10.10")
    def allocateShard(
        requester: ActorRef,
        shardId: String,
        currentShardAllocations: java.util.Map[ActorRef, immutable.IndexedSeq[String]]): Future[ActorRef] = {
      import scala.jdk.FutureConverters.CompletionStageOps
      allocateNewShard(requester, shardId, currentShardAllocations).asScala
    }

    /**
     * Invoked when the location of a new shard is to be decided.
     *
     * New implementations of this class should override this method instead of overriding [[allocateShard]].
     *
     * For compatibility with earlier versions of this API, this method's default implementation returns
     * an immediately-failing [[java.util.concurrent.CompletionStage]].
     *
     * @param requester               actor reference to the [[ShardRegion]] that requested the location of the
     *                                shard, can be returned if preference should be given to the node where the shard was first accessed
     * @param shardId                 the id of the shard to allocate
     * @param currentShardAllocations all actor refs to `ShardRegion` and their current allocated shards,
     *                                in the order they were allocated
     *  @return a [[java.util.concurrent.CompletionStage]] of the actor ref of the [[ShardRegion]] that is to be responsible for the shard,
     *          must be one of the references included in the `currentShardAllocations` parameter
     */
    @nowarn("msg=never used")
    def allocateNewShard(
        requester: ActorRef,
        shardId: String,
        currentShardAllocations: java.util.Map[ActorRef, immutable.IndexedSeq[String]]): CompletionStage[ActorRef] =
      CompletableFuture.failedStage(new scala.NotImplementedError("Must override allocateNewShard or allocateShard"))

    /**
     * Invoked periodically to decide which shards to rebalance to another location.
     *
     * The default implementation defers to [[rebalanceShards]].  In earlier versions of this API, this
     * Scala Future-returning method was the extension point.
     *
     * New implementations of this class should prefer to override [[rebalanceShards]].  This method may be removed in a
     * future release
     *
     * @param currentShardAllocations all actor refs to `ShardRegion` and their current allocated shards,
     *                                in the order they were allocated
     * @param rebalanceInProgress     set of shards that are currently being rebalanced, i.e.
     *                                you should not include these in the returned set
     * @return a `Future` of the shards to be migrated, may be empty to skip rebalance in this round
     */
    @deprecated("prefer rebalanceShards", "2.10.10")
    def rebalance(
        currentShardAllocations: java.util.Map[ActorRef, immutable.IndexedSeq[String]],
        rebalanceInProgress: java.util.Set[String]): Future[java.util.Set[String]] = {
      import scala.jdk.FutureConverters.CompletionStageOps
      rebalanceShards(currentShardAllocations, rebalanceInProgress).asScala
    }

    /**
     * Invoked periodically to decide which shards to rebalance to another location.
     *
     * New implementations of this class should override this method instead of overriding [[rebalance]].
     *
     * For compatibility with earlier versions of this API, this method's default implementation returns
     * an immediately-failing [[java.util.concurrent.CompletionStage]].
     *
     * @param currentShardAllocations all actor refs to `ShardRegion` and their current allocated shards,
     *                                in the order they were allocated
     * @param rebalanceInProgress     set of shards that are currently being rebalanced, i.e.
     *                                you should not include these in the returned set
     * @return a [[java.util.concurrent.CompletionStage]] of the set of shards to be migrated, may be empty to skip rebalance in this round
     */
    @nowarn("msg=never used")
    def rebalanceShards(
        currentShardAllocations: java.util.Map[ActorRef, immutable.IndexedSeq[String]],
        rebalanceInProgress: java.util.Set[String]): CompletionStage[java.util.Set[String]] =
      CompletableFuture.failedStage(new scala.NotImplementedError("Must override rebalanceShard or rebalance"))

  }

  private val emptyRebalanceResult = Future.successful(Set.empty[ShardId])

  /**
   * Use [[akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy.leastShardAllocationStrategy]] instead.
   * The new rebalance algorithm was included in Akka 2.6.10. It can reach optimal balance in
   * less rebalance rounds (typically 1 or 2 rounds). The amount of shards to rebalance in each
   * round can still be limited to make it progress slower.
   *
   * This implementation of [[ShardCoordinator.ShardAllocationStrategy]]
   * allocates new shards to the `ShardRegion` with least number of previously allocated shards.
   *
   * When a node is removed from the cluster the shards on that node will be started on the remaining nodes,
   * evenly spread on the remaining nodes (by picking regions with least shards).
   *
   * When a node is added to the cluster the shards on the existing nodes will be rebalanced to the new node.
   * It picks shards for rebalancing from the `ShardRegion` with most number of previously allocated shards.
   * They will then be allocated to the `ShardRegion` with least number of previously allocated shards,
   * i.e. new members in the cluster. There is a configurable threshold of how large the difference
   * must be to begin the rebalancing. The difference between number of shards in the region with most shards and
   * the region with least shards must be greater than the `rebalanceThreshold` for the rebalance to occur.
   *
   * A `rebalanceThreshold` of 1 gives the best distribution and therefore typically the best choice.
   * A higher threshold means that more shards can be rebalanced at the same time instead of one-by-one.
   * That has the advantage that the rebalance process can be quicker but has the drawback that the
   * the number of shards (and therefore load) between different nodes may be significantly different.
   * Given the recommendation of using 10x shards than number of nodes and `rebalanceThreshold=10` can result
   * in one node hosting ~2 times the number of shards of other nodes. Example: 1000 shards on 100 nodes means
   * 10 shards per node. One node may have 19 shards and others 10 without a rebalance occurring.
   *
   * The number of ongoing rebalancing processes can be limited by `maxSimultaneousRebalance`.
   *
   * During a rolling upgrade (when nodes with multiple application versions are present) allocating to
   * old nodes are avoided.
   *
   * Not intended for user extension.
   */
  @SerialVersionUID(1L)
  @DoNotInherit
  class LeastShardAllocationStrategy(rebalanceThreshold: Int, maxSimultaneousRebalance: Int)
      extends AbstractLeastShardAllocationStrategy
      with Serializable {

    override def rebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
        rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] = {
      if (rebalanceInProgress.size < maxSimultaneousRebalance) {
        val sortedRegionEntries = regionEntriesFor(currentShardAllocations).toVector.sorted(ShardSuitabilityOrdering)
        if (isAGoodTimeToRebalance(sortedRegionEntries)) {
          val (_, leastShards) = mostSuitableRegion(sortedRegionEntries)
          // even if it is to another new node.
          val mostShards = sortedRegionEntries
            .collect {
              case RegionEntry(_, _, shardIds) => shardIds.filterNot(id => rebalanceInProgress(id))
            }
            .maxBy(_.size)
          val difference = mostShards.size - leastShards.size
          if (difference > rebalanceThreshold) {
            val n = math.min(
              math.min(difference - rebalanceThreshold, rebalanceThreshold),
              maxSimultaneousRebalance - rebalanceInProgress.size)
            Future.successful(mostShards.sorted.take(n).toSet)
          } else
            emptyRebalanceResult
        } else emptyRebalanceResult
      } else emptyRebalanceResult
    }
  }

  /**
   * INTERNAL API
   */
  private[akka] object Internal {

    /**
     * Used as a special termination message from [[ClusterSharding]]
     */
    @InternalApi private[cluster] case object Terminate extends DeadLetterSuppression

    /**
     * Messages sent to the coordinator
     */
    sealed trait CoordinatorCommand extends ClusterShardingSerializable

    /**
     * Messages sent from the coordinator
     */
    sealed trait CoordinatorMessage extends ClusterShardingSerializable

    /**
     * `ShardRegion` registers to `ShardCoordinator`, until it receives [[RegisterAck]].
     */
    @SerialVersionUID(1L) final case class Register(shardRegion: ActorRef)
        extends CoordinatorCommand
        with DeadLetterSuppression

    /**
     * `ShardRegion` in proxy only mode registers to `ShardCoordinator`, until it receives [[RegisterAck]].
     */
    @SerialVersionUID(1L) final case class RegisterProxy(shardRegionProxy: ActorRef)
        extends CoordinatorCommand
        with DeadLetterSuppression

    /**
     * Acknowledgement from `ShardCoordinator` that [[Register]] or [[RegisterProxy]] was successful.
     */
    @SerialVersionUID(1L) final case class RegisterAck(coordinator: ActorRef) extends CoordinatorMessage

    /**
     * `ShardRegion` requests the location of a shard by sending this message
     * to the `ShardCoordinator`.
     */
    @SerialVersionUID(1L) final case class GetShardHome(shard: ShardId)
        extends CoordinatorCommand
        with DeadLetterSuppression

    /**
     * `ShardCoordinator` replies with this message for [[GetShardHome]] requests.
     */
    @SerialVersionUID(1L) final case class ShardHome(shard: ShardId, ref: ActorRef) extends CoordinatorMessage

    /**
     * One or more sent to region directly after registration to speed up new shard startup.
     */
    final case class ShardHomes(homes: Map[ActorRef, immutable.Seq[ShardId]])
        extends CoordinatorMessage
        with DeadLetterSuppression

    /**
     * `ShardCoordinator` informs a `ShardRegion` that it is hosting this shard
     */
    @SerialVersionUID(1L) final case class HostShard(shard: ShardId) extends CoordinatorMessage

    /**
     * `ShardRegion` replies with this message for [[HostShard]] requests which lead to it hosting the shard
     */
    @SerialVersionUID(1L) final case class ShardStarted(shard: ShardId) extends CoordinatorMessage

    /**
     * `ShardCoordinator` initiates rebalancing process by sending this message
     * to all registered `ShardRegion` actors (including proxy only). They are
     * supposed to discard their known location of the shard, i.e. start buffering
     * incoming messages for the shard. They reply with [[BeginHandOffAck]].
     * When all have replied the `ShardCoordinator` continues by sending
     * `HandOff` to the `ShardRegion` responsible for the shard.
     */
    @SerialVersionUID(1L) final case class BeginHandOff(shard: ShardId) extends CoordinatorMessage

    /**
     * Acknowledgement of [[BeginHandOff]]
     */
    @SerialVersionUID(1L) final case class BeginHandOffAck(shard: ShardId) extends CoordinatorCommand

    /**
     * When all `ShardRegion` actors have acknowledged the `BeginHandOff` the
     * `ShardCoordinator` sends this message to the `ShardRegion` responsible for the
     * shard. The `ShardRegion` is supposed to stop all entities in that shard and when
     * all entities have terminated reply with `ShardStopped` to the `ShardCoordinator`.
     */
    @SerialVersionUID(1L) final case class HandOff(shard: ShardId) extends CoordinatorMessage

    /**
     * Reply to `HandOff` when all entities in the shard have been terminated.
     */
    @SerialVersionUID(1L) final case class ShardStopped(shard: ShardId) extends CoordinatorCommand

    /**
     * Notification when the entire shard region has stopped
     */
    @SerialVersionUID(1L) final case class RegionStopped(shardRegion: ActorRef) extends CoordinatorCommand

    /**
     * Stop all the listed shards, sender will get a ShardStopped ack for each shard once stopped
     */
    final case class StopShards(shards: Set[ShardId]) extends CoordinatorCommand

    /**
     * `ShardRegion` requests full handoff to be able to shutdown gracefully.
     */
    @SerialVersionUID(1L) final case class GracefulShutdownReq(shardRegion: ActorRef)
        extends CoordinatorCommand
        with DeadLetterSuppression

    // DomainEvents for the persistent state of the event sourced ShardCoordinator
    sealed trait DomainEvent extends ClusterShardingSerializable
    @SerialVersionUID(1L) final case class ShardRegionRegistered(region: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) final case class ShardRegionProxyRegistered(regionProxy: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) final case class ShardRegionTerminated(region: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) final case class ShardRegionProxyTerminated(regionProxy: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) final case class ShardHomeAllocated(shard: ShardId, region: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) final case class ShardHomeDeallocated(shard: ShardId) extends DomainEvent
    @SerialVersionUID(1L) case object ShardCoordinatorInitialized extends DomainEvent

    case object StateInitialized

    object State {
      val empty = State()
    }

    /**
     * State of the shard coordinator.
     * Was also used as the persistent state in the old persistent coordinator.
     */
    @SerialVersionUID(1L) final case class State private[akka] (
        // region for each shard
        shards: Map[ShardId, ActorRef] = Map.empty,
        // shards for each region
        regions: Map[ActorRef, Vector[ShardId]] = Map.empty,
        regionProxies: Set[ActorRef] = Set.empty,
        // Only used if remembered entities is enabled
        unallocatedShards: Set[ShardId] = Set.empty,
        rememberEntities: Boolean = false)
        extends ClusterShardingSerializable {

      override def toString = s"State($shards)"

      def withRememberEntities(enabled: Boolean): State = {
        if (enabled)
          copy(rememberEntities = enabled)
        else
          copy(unallocatedShards = Set.empty, rememberEntities = enabled)
      }

      def isEmpty: Boolean =
        shards.isEmpty && regions.isEmpty && regionProxies.isEmpty

      def allShards: Set[ShardId] = shards.keySet.union(unallocatedShards)

      def updated(event: DomainEvent): State = event match {
        case ShardRegionRegistered(region) =>
          require(!regions.contains(region), s"Region $region already registered: $this")
          copy(regions = regions.updated(region, Vector.empty))
        case ShardRegionProxyRegistered(proxy) =>
          require(!regionProxies.contains(proxy), s"Region proxy $proxy already registered: $this")
          copy(regionProxies = regionProxies + proxy)
        case ShardRegionTerminated(region) =>
          require(regions.contains(region), s"Terminated region $region not registered: $this")
          val newUnallocatedShards =
            if (rememberEntities) (unallocatedShards ++ regions(region)) else unallocatedShards
          copy(regions = regions - region, shards = shards -- regions(region), unallocatedShards = newUnallocatedShards)
        case ShardRegionProxyTerminated(proxy) =>
          require(regionProxies.contains(proxy), s"Terminated region proxy $proxy not registered: $this")
          copy(regionProxies = regionProxies - proxy)
        case ShardHomeAllocated(shard, region) =>
          require(regions.contains(region), s"Region $region not registered: $this")
          require(!shards.contains(shard), s"Shard [$shard] already allocated: $this")
          val newUnallocatedShards =
            if (rememberEntities) (unallocatedShards - shard) else unallocatedShards
          copy(
            shards = shards.updated(shard, region),
            regions = regions.updated(region, regions(region) :+ shard),
            unallocatedShards = newUnallocatedShards)
        case ShardHomeDeallocated(shard) =>
          require(shards.contains(shard), s"Shard [$shard] not allocated: $this")
          val region = shards(shard)
          require(regions.contains(region), s"Region $region for shard [$shard] not registered: $this")
          val newUnallocatedShards =
            if (rememberEntities) (unallocatedShards + shard) else unallocatedShards
          copy(
            shards = shards - shard,
            regions = regions.updated(region, regions(region).filterNot(_ == shard)),
            unallocatedShards = newUnallocatedShards)
        case ShardCoordinatorInitialized =>
          this
      }
    }

  }

  /**
   * Periodic message to trigger rebalance
   */
  private case object RebalanceTick

  /**
   * End of rebalance process performed by [[RebalanceWorker]]
   */
  private final case class RebalanceDone(shard: ShardId, ok: Boolean)

  /**
   * Check if we've received a shard start request
   */
  private final case class ResendShardHost(shard: ShardId, region: ActorRef)

  private final case class DelayedShardRegionTerminated(region: ActorRef)

  private case object StaleRegionCheckTick

  private final case class StopShardTimeout(requestId: UUID)

  /**
   * Result of `allocateShard` is piped to self with this message.
   */
  private final case class AllocateShardResult(
      shard: ShardId,
      shardRegion: Option[ActorRef],
      getShardHomeSender: ActorRef)

  /**
   * Result of `rebalance` is piped to self with this message.
   */
  private final case class RebalanceResult(shards: Set[ShardId])

  private[akka] object RebalanceWorker {
    final case class ShardRegionTerminated(region: ActorRef)
  }

  /**
   * INTERNAL API. Rebalancing process is performed by this actor.
   * It sends `BeginHandOff` to all `ShardRegion` actors followed by
   * `HandOff` to the `ShardRegion` responsible for the shard.
   * When the handoff is completed it sends [[akka.cluster.sharding.ShardCoordinator.RebalanceDone]]
   * to its parent `ShardCoordinator`. If the process takes longer than the
   * `handOffTimeout` it also sends [[akka.cluster.sharding.ShardCoordinator.RebalanceDone]].
   */
  private[akka] class RebalanceWorker(
      typeName: String,
      shard: String,
      shardRegionFrom: ActorRef,
      handOffTimeout: FiniteDuration,
      regions: Set[ActorRef],
      isRebalance: Boolean)
      extends Actor
      with ActorLogging
      with Timers {

    import Internal._

    regions.foreach { region =>
      region ! BeginHandOff(shard)
    }
    var remaining = regions

    if (isRebalance)
      log.debug("{}: Rebalance [{}] from [{}] regions", typeName, shard, regions.size)
    else
      log.debug(
        "{}: Shutting down shard [{}] from region [{}]. Asking [{}] region(s) to hand-off shard",
        typeName,
        shard,
        shardRegionFrom,
        regions.size)

    timers.startSingleTimer("hand-off-timeout", ReceiveTimeout, handOffTimeout)

    def receive: Receive = {
      case BeginHandOffAck(`shard`) =>
        log.debug("{}: BeginHandOffAck for shard [{}] received from [{}].", typeName, shard, sender())
        acked(sender())
      case RebalanceWorker.ShardRegionTerminated(shardRegion) =>
        if (remaining.contains(shardRegion)) {
          log.debug(
            "{}: ShardRegion [{}] terminated while waiting for BeginHandOffAck for shard [{}].",
            typeName,
            shardRegion,
            shard)
          acked(shardRegion)
        }
      case ReceiveTimeout =>
        if (isRebalance)
          log.debug("{}: Rebalance of [{}] from [{}] timed out", typeName, shard, shardRegionFrom)
        else
          log.debug("{}: Shutting down [{}] shard from [{}] timed out", typeName, shard, shardRegionFrom)

        done(ok = false)
    }

    private def acked(shardRegion: ActorRef) = {
      remaining -= shardRegion
      if (remaining.isEmpty) {
        log.debug("{}: All shard regions acked, handing off shard [{}].", typeName, shard)
        shardRegionFrom ! HandOff(shard)
        context.become(stoppingShard, discardOld = true)
      } else {
        log.debug("{}: Remaining shard regions for shard [{}]: {}", typeName, shard, remaining.size)
      }
    }

    def stoppingShard: Receive = {
      case ShardStopped(`shard`) => done(ok = true)
      case ReceiveTimeout        => done(ok = false)
      case RebalanceWorker.ShardRegionTerminated(`shardRegionFrom`) =>
        log.debug(
          "{}: ShardRegion [{}] terminated while waiting for ShardStopped for shard [{}].",
          typeName,
          shardRegionFrom,
          shard)
        done(ok = true)
    }

    def done(ok: Boolean): Unit = {
      context.parent ! RebalanceDone(shard, ok)
      context.stop(self)
    }
  }

  private[akka] def rebalanceWorkerProps(
      typeName: String,
      shard: String,
      shardRegionFrom: ActorRef,
      handOffTimeout: FiniteDuration,
      regions: Set[ActorRef],
      isRebalance: Boolean): Props = {
    Props(new RebalanceWorker(typeName, shard, shardRegionFrom, handOffTimeout, regions, isRebalance))
  }

  /** INTERNAL API */
  @InternalApi
  private[akka] final class PendingGetShardHomes(val requestsByShard: Map[ShardRegion.ShardId, Set[ActorRef]]) {
    import ShardRegion.ShardId

    def addRequest(shard: ShardId, replyTo: Option[ActorRef]): PendingGetShardHomes =
      replyTo match {
        case None =>
          // demand, but no replyTo
          if (requestsByShard.contains(shard)) this
          else new PendingGetShardHomes(requestsByShard + (shard -> Set.empty))

        case Some(r) =>
          new PendingGetShardHomes(requestsByShard.updatedWith(shard) { current =>
            current.orElse(Some(Set.empty[ActorRef])).map(_.incl(r))
          })
      }

    def removeRequestsForShard(shard: ShardId): (Set[ActorRef], PendingGetShardHomes) =
      if (requestsByShard.contains(shard)) {
        requestsByShard(shard) -> new PendingGetShardHomes(requestsByShard - shard)
      } else Set.empty -> this

    def removeRequest(): (Option[(ShardId, Option[ActorRef])], PendingGetShardHomes) =
      if (requestsByShard.isEmpty) None -> this
      else {
        val iterator = requestsByShard.iterator
        val first = {
          val (shard, r2s) = iterator.next()
          shard -> r2s.size
        }

        val (selectedShard, _) = iterator.foldLeft(first) { (acc, req) =>
          val (_, selectedSize) = acc
          val (requestedShard, r2s) = req

          if (r2s.size > selectedSize) requestedShard -> r2s.size
          else acc
        }

        val selectedReplyTo = requestsByShard(selectedShard).headOption
        val nextPending = selectedReplyTo match {
          case None => new PendingGetShardHomes(requestsByShard - selectedShard)
          case Some(r) =>
            new PendingGetShardHomes(requestsByShard.updatedWith(selectedShard)(_.flatMap { replyTos =>
              val next = replyTos.excl(r)

              if (next.isEmpty) None // no need to leave any ignored replyTo after removal
              else Some(next)
            }))
        }

        Some(selectedShard -> selectedReplyTo) -> nextPending
      }
  }
}

/**
 * Singleton coordinator that decides where to allocate shards.
 *
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
abstract class ShardCoordinator(
    settings: ClusterShardingSettings,
    allocationStrategy: ShardCoordinator.ShardAllocationStrategy)
    extends Actor
    with Timers {

  import ShardCoordinator._
  import ShardCoordinator.Internal._
  import ShardRegion.ShardId
  import settings.tuningParameters._

  val log = Logging.withMarker(context.system, this)
  private val verboseDebug = context.system.settings.config.getBoolean("akka.cluster.sharding.verbose-debug-logging")
  protected val ignoreRef = context.system.asInstanceOf[ExtendedActorSystem].provider.ignoreRef

  val cluster = Cluster(context.system)
  val removalMargin = cluster.downingProvider.downRemovalMargin
  val minMembers = settings.role match {
    case None =>
      cluster.settings.MinNrOfMembers
    case Some(r) =>
      cluster.settings.MinNrOfMembersOfRole.getOrElse(r, 1)
  }
  var allRegionsRegistered = false

  var state = State.empty.withRememberEntities(settings.rememberEntities)
  var preparingForShutdown = false
  // rebalanceInProgress for the ShardId keys, pending GetShardHome requests by the ActorRef values
  var rebalanceInProgress = Map.empty[ShardId, Set[ActorRef]]
  var rebalanceWorkers: Set[ActorRef] = Set.empty
  var unAckedHostShards = Map.empty[ShardId, Cancellable]
  // regions that have requested handoff, for graceful shutdown
  var gracefulShutdownInProgress = Set.empty[ActorRef]
  var waitingForLocalRegionToTerminate = false
  var aliveRegions = Set.empty[ActorRef]
  var regionTerminationInProgress = Set.empty[ActorRef]
  // each waiting actor together with a request identifier to clear out all waiting for one request on timeout
  var waitingForShardsToStop: Map[ShardId, Set[(ActorRef, UUID)]] = Map.empty

  private val staleRegionDetectionConfig = {
    val c = context.system.settings.config.getConfig("akka.cluster.sharding.stale-region-detection")
    val enabled = c.getBoolean("enabled")
    val checkInterval = c.getDuration("check-interval").toScala
    val startupGracePeriod = c.getDuration("startup-grace-period").toScala
    (enabled, checkInterval, startupGracePeriod)
  }

  import context.dispatcher

  cluster.subscribe(
    self,
    initialStateMode = InitialStateAsEvents,
    ClusterShuttingDown.getClass,
    classOf[MemberReadyForShutdown],
    classOf[MemberPreparingForShutdown])

  protected def typeName: String

  override def preStart(): Unit = {
    timers.startTimerWithFixedDelay(RebalanceTick, RebalanceTick, rebalanceInterval)
    allocationStrategy match {
      case strategy: StartableAllocationStrategy =>
        strategy.start()
      case strategy: ActorSystemDependentAllocationStrategy =>
        strategy.start(context.system)
      case _ =>
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster.unsubscribe(self)
  }

  def isMember(region: ActorRef): Boolean = {
    val regionAddress = region.path.address
    regionAddress == self.path.address ||
    cluster.state.isMemberUp(regionAddress)
  }

  def active: Receive =
    ({
      case Register(region) =>
        if (isMember(region)) {
          log.debug("{}: ShardRegion registered: [{}]", typeName, region)
          aliveRegions += region
          if (state.regions.contains(region)) {
            region ! RegisterAck(self)
            informAboutCurrentShards(region)
            allocateShardHomesForRememberEntities()
          } else {
            gracefulShutdownInProgress -= region
            informAboutCurrentShards(region) // we don't have to wait for updated state for this
            update(ShardRegionRegistered(region)) { evt =>
              state = state.updated(evt)
              context.watch(region)
              region ! RegisterAck(self)
              allocateShardHomesForRememberEntities()
            }
          }
        } else {
          log.debug(
            "{}: ShardRegion [{}] was not registered since the coordinator currently does not know about a node of that region",
            typeName,
            region)
        }

      case RegisterProxy(proxy) =>
        if (isMember(proxy)) {
          log.debug("{}: ShardRegion proxy registered: [{}]", typeName, proxy)
          informAboutCurrentShards(proxy) // we don't have to wait for updated state for this
          if (state.regionProxies.contains(proxy))
            proxy ! RegisterAck(self)
          else {
            update(ShardRegionProxyRegistered(proxy)) { evt =>
              state = state.updated(evt)
              context.watch(proxy)
              proxy ! RegisterAck(self)
            }
          }
        }
      case GetShardHome(shard) =>
        if (!handleGetShardHome(shard)) {
          // location not know, yet
          val activeRegions = (state.regions -- gracefulShutdownInProgress) -- regionTerminationInProgress
          if (activeRegions.nonEmpty) {
            val getShardHomeSender = sender()
            val regionFuture = allocationStrategy.allocateShard(getShardHomeSender, shard, activeRegions)
            regionFuture.value match {
              case Some(Success(region)) =>
                continueGetShardHome(shard, region, getShardHomeSender)
              case _ =>
                // continue when future is completed
                regionFuture
                  .map { region =>
                    AllocateShardResult(shard, Some(region), getShardHomeSender)
                  }
                  .recover {
                    case t =>
                      log.error(t, "{}: Shard [{}] allocation failed.", typeName, shard)
                      AllocateShardResult(shard, None, getShardHomeSender)
                  }
                  .pipeTo(self)
            }
          }
        }

      case AllocateShardResult(shard, None, _) =>
        log.debug("{}: Shard [{}] allocation failed. It will be retried.", typeName, shard)

      case AllocateShardResult(shard, Some(region), getShardHomeSender) =>
        continueGetShardHome(shard, region, getShardHomeSender)

      case ShardStarted(shard) =>
        unAckedHostShards.get(shard) match {
          case Some(cancel) =>
            cancel.cancel()
            unAckedHostShards = unAckedHostShards - shard
          case _ =>
        }

      case ResendShardHost(shard, region) =>
        state.shards.get(shard) match {
          case Some(`region`) => sendHostShardMsg(shard, region)
          case _              => //Reallocated to another region
        }

      case StopShards(shardIds) =>
        if (settings.rememberEntities) {
          log.error(
            "{}: Stop shards cannot be combined with remember entities, ignoring command to stop [{}]",
            typeName,
            shardIds.mkString(", "))
        } else if (state.regions.nonEmpty && !preparingForShutdown) {
          val requestId = UUID.randomUUID()
          val (runningShards, alreadyStoppedShards) = shardIds.partition(state.shards.contains)
          alreadyStoppedShards.foreach(shardId => sender() ! ShardStopped(shardId))
          if (runningShards.nonEmpty) {
            waitingForShardsToStop = runningShards.foldLeft(waitingForShardsToStop) {
              case (acc, shard) =>
                val newWaiting = acc.get(shard) match {
                  case Some(waiting) => waiting + ((sender(), requestId))
                  case None          => Set((sender(), requestId))
                }
                acc.updated(shard, newWaiting)
            }
            // no need to stop already rebalancing
            val shardsToStop = runningShards.filter(shard => !rebalanceInProgress.contains(shard))
            log.info(
              "{}: Explicitly stopping shards [{}] (request id [{}])",
              typeName,
              shardsToStop.mkString(", "),
              requestId)
            val shardsPerRegion =
              shardsToStop.flatMap(shardId => state.shards.get(shardId).map(region => region -> shardId)).groupBy(_._1)
            shardsPerRegion.foreach {
              case (region, shards) => shutdownShards(region, shards.map(_._2))
            }
            val timeout = StopShardTimeout(requestId)
            timers.startSingleTimer(timeout, timeout, settings.tuningParameters.handOffTimeout)
          }

        } else {
          log.warning(
            "{}: Explicit stop shards of shards [{}] ignored (no known regions or sharding shutting down)",
            typeName,
            shardIds.mkString(", "))
        }

      case RebalanceTick =>
        if (state.regions.nonEmpty && !preparingForShutdown) {
          val shardsFuture = allocationStrategy.rebalance(state.regions, rebalanceInProgress.keySet)
          shardsFuture.value match {
            case Some(Success(shards)) =>
              continueRebalance(shards)
            case _ =>
              // continue when future is completed
              shardsFuture
                .map { shards =>
                  RebalanceResult(shards)
                }
                .recover {
                  case _ => RebalanceResult(Set.empty)
                }
                .pipeTo(self)
          }
        }

      case RebalanceResult(shards) =>
        continueRebalance(shards)

      case RebalanceDone(shard, ok) =>
        rebalanceWorkers -= sender()

        if (ok) {
          log.debug("{}: Shard [{}] deallocation completed successfully.", typeName, shard)

          // ack to external trigger of rebalance/stop
          waitingForShardsToStop.get(shard) match {
            case Some(waiting) =>
              waiting.foreach { case (replyTo, _) => replyTo ! ShardStopped(shard) }
              waitingForShardsToStop -= shard
            case None =>
          }

          // The shard could have been removed by ShardRegionTerminated
          if (state.shards.contains(shard)) {
            update(ShardHomeDeallocated(shard)) { evt =>
              log.debug("{}: Shard [{}] deallocated after", typeName, shard)
              state = state.updated(evt)
              clearRebalanceInProgress(shard)
              allocateShardHomesForRememberEntities()
              self.tell(GetShardHome(shard), ignoreRef)
            }
          } else {
            clearRebalanceInProgress(shard)
          }

        } else {
          log.warning(
            "{}: Shard [{}] deallocation didn't complete within [{}].",
            typeName,
            shard,
            handOffTimeout.pretty)

          // was that due to a graceful region shutdown?
          // if so, consider the region as still alive and let it retry to gracefully shutdown later
          state.shards.get(shard).foreach { region =>
            gracefulShutdownInProgress -= region
          }

          clearRebalanceInProgress(shard)
        }

      case GracefulShutdownReq(region) =>
        if (!gracefulShutdownInProgress(region)) {
          state.regions.get(region) match {
            case Some(shards) =>
              if (log.isDebugEnabled) {
                if (verboseDebug)
                  log.debug(
                    "{}: Graceful shutdown of {} region [{}] with [{}] shards [{}] started",
                    Array[Any](
                      typeName,
                      if (region.path.address.hasLocalScope) "local" else "",
                      region,
                      shards.size,
                      shards.mkString(", ")))
                else
                  log.debug("{}: Graceful shutdown of region [{}] with [{}] shards", typeName, region, shards.size)
              }
              gracefulShutdownInProgress += region
              shutdownShards(region, shards.toSet)

            case None =>
              log.debug("{}: Unknown region requested graceful shutdown [{}]", typeName, region)
          }
        }

      case ShardRegion.GetClusterShardingStats(waitMax) =>
        import akka.pattern.ask
        implicit val timeout: Timeout = waitMax
        Future
          .sequence(aliveRegions.map { regionActor =>
            (regionActor ? ShardRegion.GetShardRegionStats)
              .mapTo[ShardRegion.ShardRegionStats]
              .map(stats => regionActor -> stats)
          })
          .map { allRegionStats =>
            ShardRegion.ClusterShardingStats(allRegionStats.map {
              case (region, stats) =>
                val regionAddress = region.path.address
                val address: Address =
                  if (regionAddress.hasLocalScope && regionAddress.system == cluster.selfAddress.system)
                    cluster.selfAddress
                  else regionAddress

                address -> stats
            }.toMap)
          }
          .recover {
            case _: AskTimeoutException => ShardRegion.ClusterShardingStats(Map.empty)
          }
          .pipeTo(sender())

      case ClusterShuttingDown =>
        log.debug("{}: Shutting down ShardCoordinator", typeName)
        // can't stop because supervisor will start it again,
        // it will soon be stopped when singleton is stopped
        context.become(shuttingDown)

      case _: MemberPreparingForShutdown | _: MemberReadyForShutdown =>
        if (!preparingForShutdown) {
          log.info(
            "{}: Shard coordinator detected prepare for full cluster shutdown. No new rebalances will take place.",
            typeName)
          timers.cancel(RebalanceTick)
          preparingForShutdown = true
        }

      case ShardRegion.GetCurrentRegions =>
        val reply = ShardRegion.CurrentRegions(state.regions.keySet.map { ref =>
          if (ref.path.address.host.isEmpty) cluster.selfAddress
          else ref.path.address
        })
        sender() ! reply

      case StopShardTimeout(requestId) =>
        val timedOutShards = waitingForShardsToStop.collect {
          case (shard, waiting) if waiting.exists(_._2 == requestId) => shard
        }
        if (timedOutShards.nonEmpty) {
          log.info(
            "{}: Stop shard request [{}] timed out for shards [{}]",
            typeName,
            requestId,
            timedOutShards.mkString(", "))
          waitingForShardsToStop = timedOutShards.foldLeft(waitingForShardsToStop) {
            case (acc, shard) =>
              val waiting = acc(shard)
              if (waiting.size == 1) acc - shard
              else acc.updated(shard, waiting.filterNot { case (_, id) => id == requestId })
          }
        }

      case StaleRegionCheckTick =>
        // Re-watch all remote regions.
        // If the node is gone, ClusterRemoteWatcher.addWatch() will immediately
        // trigger a new Terminated via the memberTombstones check.
        // If the node is alive, the watch is simply re-established (no-op).
        state.regions.keys.foreach { ref =>
          if (!ref.path.address.hasLocalScope) {
            log.warning("{}: Stale region detection, re-watching region [{}]", typeName, ref)
            context.unwatch(ref)
            context.watch(ref)
          }
        }

      case ShardCoordinator.Internal.Terminate =>
        terminate()
    }: Receive).orElse[Any, Unit](receiveTerminated)

  private def terminate(): Unit = {
    if (aliveRegions.exists(_.path.address.hasLocalScope) || gracefulShutdownInProgress.exists(
          _.path.address.hasLocalScope)) {
      aliveRegions
        .find(_.path.address.hasLocalScope)
        .foreach(region =>
          // region will get this from taking part in coordinated shutdown, but for good measure
          region ! ShardRegion.GracefulShutdown)

      log.debug("{}: Deferring coordinator termination until local region has terminated", typeName)
      waitingForLocalRegionToTerminate = true
    } else {
      if (rebalanceInProgress.isEmpty)
        log.debug("{}: Received termination message.", typeName)
      else if (log.isDebugEnabled) {
        if (verboseDebug)
          log.debug(
            "{}: Received termination message. Rebalance in progress of [{}] shards [{}].",
            typeName,
            rebalanceInProgress.size,
            rebalanceInProgress.keySet.mkString(", "))
        else
          log.debug(
            "{}: Received termination message. Rebalance in progress of [{}] shards.",
            typeName,
            rebalanceInProgress.size)
      }
      context.stop(self)
    }
  }

  private def clearRebalanceInProgress(shard: String): Unit = {
    rebalanceInProgress.get(shard) match {
      case Some(pendingGetShardHome) =>
        val msg = GetShardHome(shard)
        pendingGetShardHome.foreach { getShardHomeSender =>
          self.tell(msg, getShardHomeSender)
        }
        rebalanceInProgress -= shard
      case None =>
    }
  }

  private def deferGetShardHomeRequest(shard: ShardId, from: ActorRef): Unit = {
    log.debug(
      "{}: GetShardHome [{}] request from [{}] deferred, because rebalance is in progress for this shard. " +
      "It will be handled when rebalance is done.",
      typeName,
      shard,
      from)
    rebalanceInProgress = rebalanceInProgress.updated(shard, rebalanceInProgress(shard) + from)
  }

  private def informAboutCurrentShards(ref: ActorRef): Unit = {
    // hardcoded to a safe low value rather than configurable, for now
    val batchSize = 500
    if (state.shards.isEmpty) {
      // No shards - NOP
    } else {
      log.debug(
        "{}: Informing [{}] about (up to) [{}] shards in batches of [{}]",
        typeName,
        ref,
        state.shards.size,
        batchSize)
      // Filter out shards currently rebalancing and then split up into multiple messages
      // if needed, to not get a single too large message, but group by region to send the
      // same region actor as few times as possible.
      state.regions.iterator
        .flatMap {
          case (regionRef, shards) =>
            shards.filterNot(rebalanceInProgress.contains).map(shard => regionRef -> shard)
        }
        .grouped(batchSize)
        // cap how much is sent in case of a large number of shards (> 5 000)
        // to not delay registration ack too much
        .take(10)
        .foreach { regions =>
          val shardsSubMap = regions.foldLeft(Map.empty[ActorRef, List[ShardId]]) {
            case (map, (regionRef, shardId)) =>
              if (map.contains(regionRef)) map.updated(regionRef, shardId :: map(regionRef))
              else map.updated(regionRef, shardId :: Nil)
          }
          ref ! ShardHomes(shardsSubMap)
        }
    }
  }

  /**
   * @return `true` if the message could be handled without state update, i.e.
   *         the shard location was known or the request was deferred or ignored
   */
  def handleGetShardHome(shard: ShardId): Boolean = {
    if (rebalanceInProgress.contains(shard)) {
      deferGetShardHomeRequest(shard, sender())
      unstashOneGetShardHomeRequest() // continue unstashing
      true
    } else if (!hasAllRegionsRegistered()) {
      log.debug(
        "{}: GetShardHome [{}] request from [{}] ignored, because not all regions have registered yet.",
        typeName,
        shard,
        sender())
      true
    } else {
      state.shards.get(shard) match {
        case Some(shardRegionRef) =>
          if (regionTerminationInProgress(shardRegionRef))
            log.debug(
              "{}: GetShardHome [{}] request ignored, due to region [{}] termination in progress.",
              typeName,
              shard,
              shardRegionRef)
          else
            sender() ! ShardHome(shard, shardRegionRef)

          unstashOneGetShardHomeRequest() // continue unstashing
          true
        case None =>
          false // location not known, yet, caller will handle allocation
      }
    }
  }

  def receiveTerminated: Receive = {
    case t @ Terminated(ref) =>
      if (state.regions.contains(ref)) {
        if (removalMargin != Duration.Zero && t.addressTerminated && aliveRegions(ref)) {
          context.system.scheduler.scheduleOnce(removalMargin, self, DelayedShardRegionTerminated(ref))
          regionTerminationInProgress += ref
        } else
          regionTerminated(ref)
      } else if (state.regionProxies.contains(ref)) {
        regionProxyTerminated(ref)
      }

    case RegionStopped(ref) =>
      log.debug("{}: ShardRegion stopped: [{}]", typeName, ref)
      regionTerminated(ref)

    case DelayedShardRegionTerminated(ref) =>
      regionTerminated(ref)
  }

  def update[E <: DomainEvent](evt: E)(f: E => Unit): Unit

  def watchStateActors(): Unit = {

    // Optimization:
    // Consider regions that don't belong to the current cluster to be terminated.
    // This is an optimization that makes it operationally faster and reduces the
    // amount of lost messages during startup.
    val nodes = cluster.state.members.map(_.address)
    state.regions.foreach {
      case (ref, _) =>
        val a = ref.path.address
        if (a.hasLocalScope || nodes(a))
          context.watch(ref)
        else
          regionTerminated(ref) // not part of cluster
    }
    state.regionProxies.foreach { ref =>
      val a = ref.path.address
      if (a.hasLocalScope || nodes(a))
        context.watch(ref)
      else
        regionProxyTerminated(ref) // not part of cluster
    }

    // Let the quick (those not involving failure detection) Terminated messages
    // be processed before starting to reply to GetShardHome.
    // This is an optimization that makes it operational faster and reduces the
    // amount of lost messages during startup.
    context.system.scheduler.scheduleOnce(500.millis, self, StateInitialized)

    // Start stale region detection after the startup grace period
    val (srdEnabled, srdCheckInterval, srdStartupGrace) = staleRegionDetectionConfig
    if (srdEnabled) {
      timers.startTimerWithFixedDelay(StaleRegionCheckTick, StaleRegionCheckTick, srdStartupGrace, srdCheckInterval)
    }
  }

  def stateInitialized(): Unit = {
    state.shards.foreach { case (a, r) => sendHostShardMsg(a, r) }
    allocateShardHomesForRememberEntities()
  }

  def hasAllRegionsRegistered(): Boolean = {
    // the check is only for startup, i.e. once all have registered we don't check more
    if (allRegionsRegistered) true
    else {
      allRegionsRegistered = aliveRegions.size >= minMembers
      allRegionsRegistered
    }
  }

  def regionTerminated(ref: ActorRef): Unit = {
    rebalanceWorkers.foreach(_ ! RebalanceWorker.ShardRegionTerminated(ref))
    if (state.regions.contains(ref)) {
      if (log.isDebugEnabled) {
        log.debug(
          "{}: ShardRegion terminated{}: [{}] {}",
          typeName,
          if (gracefulShutdownInProgress.contains(ref)) " (gracefully)" else "",
          ref)
      }
      regionTerminationInProgress += ref
      state.regions(ref).foreach { s =>
        self.tell(GetShardHome(s), ignoreRef)
      }

      update(ShardRegionTerminated(ref)) { evt =>
        state = state.updated(evt)
        gracefulShutdownInProgress -= ref
        regionTerminationInProgress -= ref
        aliveRegions -= ref
        allocateShardHomesForRememberEntities()
        if (ref.path.address.hasLocalScope && waitingForLocalRegionToTerminate) {
          // handoff optimization: singleton told coordinator to stop but we deferred stop until the local region
          // had completed the handoff
          log.debug("{}: Local region stopped, terminating coordinator", typeName)
          terminate()
        }
      }
    }
  }

  def regionProxyTerminated(ref: ActorRef): Unit = {
    rebalanceWorkers.foreach(_ ! RebalanceWorker.ShardRegionTerminated(ref))
    if (state.regionProxies.contains(ref)) {
      log.debug("{}: ShardRegion proxy terminated: [{}]", typeName, ref)
      update(ShardRegionProxyTerminated(ref)) { evt =>
        state = state.updated(evt)
      }
    }
  }

  def shuttingDown: Receive = {
    case _ => // ignore all
  }

  def sendHostShardMsg(shard: ShardId, region: ActorRef): Unit = {
    region ! HostShard(shard)
    val cancel = context.system.scheduler.scheduleOnce(shardStartTimeout, self, ResendShardHost(shard, region))
    unAckedHostShards = unAckedHostShards.updated(shard, cancel)
  }

  def allocateShardHomesForRememberEntities(): Unit = {
    if (settings.rememberEntities && state.unallocatedShards.nonEmpty)
      state.unallocatedShards.foreach { shard =>
        self.tell(GetShardHome(shard), ignoreRef)
      }
  }

  def continueGetShardHome(shard: ShardId, region: ActorRef, getShardHomeSender: ActorRef): Unit =
    if (rebalanceInProgress.contains(shard)) {
      deferGetShardHomeRequest(shard, getShardHomeSender)
    } else {
      state.shards.get(shard) match {
        case Some(ref) => getShardHomeSender ! ShardHome(shard, ref)
        case None =>
          if (state.regions.contains(region) && !gracefulShutdownInProgress(region) && !regionTerminationInProgress
                .contains(region)) {
            update(ShardHomeAllocated(shard, region)) { evt =>
              state = state.updated(evt)
              log.debug(
                ShardingLogMarker.shardAllocated(typeName, shard, regionAddress(region)),
                "{}: Shard [{}] allocated at [{}]",
                typeName,
                evt.shard,
                evt.region)

              sendHostShardMsg(evt.shard, evt.region)
              getShardHomeSender ! ShardHome(evt.shard, evt.region)
              unstashGetShardHomeRequestsForShard(evt.shard)
            }
          } else {
            if (verboseDebug)
              log.debug(
                "{}: Allocated region [{}] for shard [{}] is not (any longer) one of the registered regions: {}",
                typeName,
                region,
                shard,
                state)
            else
              log.debug(
                "{}: Allocated region [{}] for shard [{}] is not (any longer) one of the registered regions.",
                typeName,
                region,
                shard)
          }
      }
    }

  protected def unstashOneGetShardHomeRequest(): Unit
  protected def unstashGetShardHomeRequestsForShard(shard: ShardId): Unit

  private def regionAddress(region: ActorRef): Address = {
    if (region.path.address.host.isEmpty) cluster.selfAddress
    else region.path.address
  }

  /**
   * Start a RebalanceWorker to manage the shard rebalance.
   * Does nothing if the shard is already in the process of being rebalanced.
   */
  private def startShardRebalanceIfNeeded(
      shard: String,
      from: ActorRef,
      handOffTimeout: FiniteDuration,
      isRebalance: Boolean): Unit = {
    if (!rebalanceInProgress.contains(shard)) {
      rebalanceInProgress = rebalanceInProgress.updated(shard, Set.empty)
      rebalanceWorkers += context.actorOf(
        rebalanceWorkerProps(
          typeName,
          shard,
          from,
          handOffTimeout,
          state.regions.keySet.union(state.regionProxies),
          isRebalance = isRebalance).withDispatcher(context.props.dispatcher))
    }
  }

  def continueRebalance(shards: Set[ShardId]): Unit = {
    if ((log: BusLogging).isInfoEnabled && (shards.nonEmpty || rebalanceInProgress.nonEmpty)) {
      log.info(
        "{}: Starting rebalance for shards [{}]. Current shards rebalancing: [{}]",
        typeName,
        shards.mkString(","),
        rebalanceInProgress.keySet.mkString(","))
    }
    shards.foreach { shard =>
      // optimisation: check if not already in progress before fetching region
      if (!rebalanceInProgress.contains(shard)) {
        state.shards.get(shard) match {
          case Some(rebalanceFromRegion) =>
            log.debug("{}: Rebalance shard [{}] from [{}]", typeName, shard, rebalanceFromRegion)
            startShardRebalanceIfNeeded(shard, rebalanceFromRegion, handOffTimeout, isRebalance = true)
          case None =>
            log.debug("{}: Rebalance of non-existing shard [{}] is ignored", typeName, shard)
        }
      }
    }
  }

  def shutdownShards(shuttingDownRegion: ActorRef, shards: Set[ShardId]): Unit = {
    if ((log: BusLogging).isInfoEnabled && (shards.nonEmpty)) {
      log.info(
        "{}: Starting shutting down shards [{}] due to region shutting down or explicit stopping of shards.",
        typeName,
        shards.mkString(","))
    }
    shards.foreach { shard =>
      startShardRebalanceIfNeeded(shard, shuttingDownRegion, handOffTimeout, isRebalance = false)
    }
  }
}

/**
 * Singleton coordinator that decides where to allocate shards.
 *
 * Users can migrate to using DData to store state then either Event Sourcing or ddata to store
 * the remembered entities.
 *
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
@deprecated("Use `ddata` mode, persistence mode is deprecated.", "2.6.0")
class PersistentShardCoordinator(
    override val typeName: String,
    settings: ClusterShardingSettings,
    allocationStrategy: ShardCoordinator.ShardAllocationStrategy)
    extends ShardCoordinator(settings, allocationStrategy)
    with PersistentActor {

  import ShardCoordinator.Internal._
  import settings.tuningParameters._

  private val verboseDebug = context.system.settings.config.getBoolean("akka.cluster.sharding.verbose-debug-logging")

  override def persistenceId = s"/sharding/${typeName}Coordinator"

  override def journalPluginId: String = settings.journalPluginId

  override def snapshotPluginId: String = settings.snapshotPluginId

  override def receiveRecover: Receive = {
    case MigrationMarker | SnapshotOffer(_, _: EventSourcedRememberEntitiesCoordinatorStore.State) =>
      throw new IllegalStateException(
        "state-store is set to persistence but a migration has taken place to remember-entities-store=eventsourced. You can not downgrade.")
    case evt: DomainEvent =>
      if (verboseDebug)
        log.debug("{}: receiveRecover {}", typeName, evt)
      evt match {
        case _: ShardRegionRegistered =>
          state = state.updated(evt)
        case _: ShardRegionProxyRegistered =>
          state = state.updated(evt)
        case ShardRegionTerminated(region) =>
          if (state.regions.contains(region))
            state = state.updated(evt)
          else {
            log.debug(
              "{}: ShardRegionTerminated, but region {} was not registered. This inconsistency is due to that " +
              " some stored ActorRef in Akka v2.3.0 and v2.3.1 did not contain full address information. It will be " +
              "removed by later watch.",
              typeName,
              region)
          }
        case ShardRegionProxyTerminated(proxy) =>
          if (state.regionProxies.contains(proxy))
            state = state.updated(evt)
        case _: ShardHomeAllocated =>
          state = state.updated(evt)
        case _: ShardHomeDeallocated =>
          state = state.updated(evt)
        case ShardCoordinatorInitialized =>
          () // not used here
      }

    case SnapshotOffer(_, st: State) =>
      if (verboseDebug)
        log.debug("{}: receiveRecover SnapshotOffer {}", typeName, st)
      state = st.withRememberEntities(settings.rememberEntities)
      //Old versions of the state object may not have unallocatedShard set,
      // thus it will be null.
      if (state.unallocatedShards == null)
        state = state.copy(unallocatedShards = Set.empty)

    case RecoveryCompleted =>
      state = state.withRememberEntities(settings.rememberEntities)
      watchStateActors()
  }

  override def receiveCommand: Receive = waitingForStateInitialized

  def waitingForStateInitialized: Receive =
    ({
      case ShardCoordinator.Internal.Terminate =>
        log.debug("{}: Received termination message before state was initialized", typeName)
        context.stop(self)

      case StateInitialized =>
        stateInitialized()
        log.debug("{}: Coordinator initialization completed", typeName)
        context.become(active.orElse[Any, Unit](receiveSnapshotResult))

      case Register(region) =>
        // region will retry so ok to ignore
        log.debug("{}: Ignoring registration from region [{}] while initializing", typeName, region)

    }: Receive).orElse[Any, Unit](receiveTerminated).orElse[Any, Unit](receiveSnapshotResult)

  def receiveSnapshotResult: Receive = {
    case e: SaveSnapshotSuccess =>
      log.debug("{}: Persistent snapshot to [{}] saved successfully", typeName, e.metadata.sequenceNr)
      internalDeleteMessagesBeforeSnapshot(e, keepNrOfBatches, snapshotAfter)

    case SaveSnapshotFailure(_, reason) =>
      log.warning("{}: Persistent snapshot failure: {}", typeName, reason)

    case DeleteMessagesSuccess(toSequenceNr) =>
      log.debug("{}: Persistent messages to [{}] deleted successfully", typeName, toSequenceNr)
      deleteSnapshots(SnapshotSelectionCriteria(maxSequenceNr = toSequenceNr - 1))

    case DeleteMessagesFailure(reason, toSequenceNr) =>
      log.warning("{}: Persistent messages to [{}] deletion failure: {}", typeName, toSequenceNr, reason)

    case DeleteSnapshotsSuccess(m) =>
      log.debug("{}: Persistent snapshots to [{}] deleted successfully", typeName, m.maxSequenceNr)

    case DeleteSnapshotsFailure(m, reason) =>
      log.warning("{}: Persistent snapshots to [{}] deletion failure: {}", typeName, m.maxSequenceNr, reason)
  }

  def update[E <: DomainEvent](evt: E)(f: E => Unit): Unit = {
    saveSnapshotWhenNeeded()
    persist(evt)(f)
  }

  def saveSnapshotWhenNeeded(): Unit = {
    if (lastSequenceNr % snapshotAfter == 0 && lastSequenceNr != 0) {
      log.debug("{}: Saving snapshot, sequence number [{}]", typeName, snapshotSequenceNr)
      saveSnapshot(state)
    }
  }

  override protected def unstashOneGetShardHomeRequest(): Unit = ()
  override protected def unstashGetShardHomeRequestsForShard(shard: ShardId): Unit = ()
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object DDataShardCoordinator {

  private case object RememberEntitiesStoreStopped

  private case class RememberEntitiesTimeout(shardId: ShardId)

  private case object RememberEntitiesLoadTimeout

  private val RememberEntitiesTimeoutKey = "RememberEntityTimeout"
}

/**
 * INTERNAL API
 * Singleton coordinator (with state based on ddata) that decides where to allocate shards.
 *
 * The plan is for this to be the only type of ShardCoordinator. A full cluster shutdown will rely
 * on remembered entities to re-initialize and reallocate the existing shards.
 *
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
@InternalApi
private[akka] class DDataShardCoordinator(
    override val typeName: String,
    settings: ClusterShardingSettings,
    allocationStrategy: ShardCoordinator.ShardAllocationStrategy,
    replicator: ActorRef,
    majorityMinCap: Int,
    rememberEntitiesStoreProvider: Option[RememberEntitiesProvider])
    extends ShardCoordinator(settings, allocationStrategy)
    with Stash
    with Timers {
  import DDataShardCoordinator._
  import ShardCoordinator.Internal._
  import ShardCoordinator.PendingGetShardHomes

  import akka.cluster.ddata.Replicator.Update

  private val verboseDebug = context.system.settings.config.getBoolean("akka.cluster.sharding.verbose-debug-logging")

  private val stateReadConsistency = settings.tuningParameters.coordinatorStateReadMajorityPlus match {
    case Int.MaxValue => ReadAll(settings.tuningParameters.waitingForStateTimeout)
    case additional   => ReadMajorityPlus(settings.tuningParameters.waitingForStateTimeout, additional, majorityMinCap)
  }
  private val stateWriteConsistency = settings.tuningParameters.coordinatorStateWriteMajorityPlus match {
    case Int.MaxValue => WriteAll(settings.tuningParameters.updatingStateTimeout)
    case additional   => WriteMajorityPlus(settings.tuningParameters.updatingStateTimeout, additional, majorityMinCap)
  }

  implicit val node: Cluster = Cluster(context.system)
  private implicit val selfUniqueAddress: SelfUniqueAddress = SelfUniqueAddress(node.selfUniqueAddress)
  private val CoordinatorStateKey = LWWRegisterKey[State](s"${typeName}CoordinatorState")
  private val initEmptyState = State.empty.withRememberEntities(settings.rememberEntities)

  private var terminating = false
  private var getShardHomeRequests = new PendingGetShardHomes(Map.empty)
  private var initialStateRetries = 0
  private var updateStateRetries = 0

  private val rememberEntitiesStore =
    rememberEntitiesStoreProvider.map { provider =>
      log.debug("{}: Starting remember entities store from provider {}", typeName, provider)
      context.watchWith(
        context.actorOf(provider.coordinatorStoreProps(), "RememberEntitiesStore"),
        RememberEntitiesStoreStopped)
    }
  private val rememberEntities = rememberEntitiesStore.isDefined
  node.subscribe(self, ClusterEvent.InitialStateAsEvents, ClusterShuttingDown.getClass)

  // get state from ddata replicator, repeat until GetSuccess
  getCoordinatorState()
  if (settings.rememberEntities)
    getAllRememberedShards()

  override def receive: Receive =
    waitingForInitialState(Set.empty)

  // This state will drop all other messages since they will be retried
  // Note remembered entities initial set of shards can arrive here or later, does not keep us in this state
  def waitingForInitialState(rememberedShards: Set[ShardId]): Receive =
    ({

      case g @ GetSuccess(CoordinatorStateKey, _) =>
        val existingState = g.get(CoordinatorStateKey).value.withRememberEntities(settings.rememberEntities)
        if (verboseDebug)
          log.debug("{}: Received initial coordinator state [{}]", typeName, existingState)
        else
          log.debug(
            "{}: Received initial coordinator state with [{}] shards",
            typeName,
            existingState.shards.size + existingState.unallocatedShards.size)
        onInitialState(existingState, rememberedShards)

      case GetFailure(CoordinatorStateKey, _) =>
        initialStateRetries += 1
        val template =
          "{}: The ShardCoordinator was unable to get an initial state within 'waiting-for-state-timeout': {} millis (retrying). Has ClusterSharding been started on all nodes?"
        if (initialStateRetries == 1)
          log.info(template, typeName, stateReadConsistency.timeout.toMillis)
        else if (initialStateRetries < 5)
          log.warning(template, typeName, stateReadConsistency.timeout.toMillis)
        else
          log.error(template, typeName, stateReadConsistency.timeout.toMillis)

        // repeat until GetSuccess
        getCoordinatorState()

      case NotFound(CoordinatorStateKey, _) =>
        log.debug("{}: Initial coordinator is empty.", typeName)
        // this.state is empty initially
        onInitialState(this.state, rememberedShards)

      case RememberEntitiesCoordinatorStore.RememberedShards(shardIds) =>
        log.debug("{}: Received [{}] remembered shard ids (when waitingForInitialState)", typeName, shardIds.size)
        context.become(waitingForInitialState(shardIds))
        timers.cancel(RememberEntitiesTimeoutKey)

      case RememberEntitiesLoadTimeout =>
        // repeat until successful
        getAllRememberedShards()

      case ShardCoordinator.Internal.Terminate =>
        log.debug("{}: Received termination message while waiting for state", typeName)
        context.stop(self)

      case Register(region) =>
        log.debug("{}: ShardRegion tried to register but ShardCoordinator not initialized yet: [{}]", typeName, region)

      case RegisterProxy(region) =>
        log.debug(
          "{}: ShardRegion proxy tried to register but ShardCoordinator not initialized yet: [{}]",
          typeName,
          region)

    }: Receive).orElse[Any, Unit](receiveTerminated)

  private def onInitialState(loadedState: State, rememberedShards: Set[ShardId]): Unit = {
    state = if (settings.rememberEntities && rememberedShards.nonEmpty) {
      // Note that we don't wait for shards from store so they could also arrive later
      val newUnallocatedShards = state.unallocatedShards.union(rememberedShards.diff(state.shards.keySet))
      loadedState.copy(unallocatedShards = newUnallocatedShards)
    } else loadedState
    if (state.isEmpty) {
      // empty state, activate immediately
      activate()
    } else {
      context.become(waitingForStateInitialized)
      // note that watchStateActors may call update
      watchStateActors()
    }
  }

  // this state will stash all messages until it receives StateInitialized,
  // which was scheduled by previous watchStateActors
  def waitingForStateInitialized: Receive = {
    case StateInitialized =>
      unstashOneGetShardHomeRequest()
      unstashAll()
      stateInitialized()
      activate()

    case g: GetShardHome =>
      stashGetShardHomeRequest(sender(), g)

    case ShardCoordinator.Internal.Terminate =>
      log.debug("{}: Received termination message while waiting for state initialized", typeName)
      context.stop(self)

    case RememberEntitiesCoordinatorStore.RememberedShards(rememberedShards) =>
      log.debug(
        "{}: Received [{}] remembered shard ids (when waitingForStateInitialized)",
        typeName,
        rememberedShards.size)
      val newUnallocatedShards = state.unallocatedShards.union(rememberedShards.diff(state.shards.keySet))
      state.copy(unallocatedShards = newUnallocatedShards)
      timers.cancel(RememberEntitiesTimeoutKey)

    case RememberEntitiesLoadTimeout =>
      // repeat until successful
      getAllRememberedShards()

    case RememberEntitiesStoreStopped =>
      onRememberEntitiesStoreStopped()

    case _ => stash()
  }

  // this state will stash all messages until it receives UpdateSuccess and a successful remember shard started
  // if remember entities is enabled
  def waitingForUpdate[E <: DomainEvent](
      evt: E,
      shardId: Option[ShardId],
      waitingForStateWrite: Boolean,
      waitingForRememberShard: Boolean,
      afterUpdateCallback: E => Unit): Receive = {

    case UpdateSuccess(CoordinatorStateKey, Some(`evt`)) =>
      updateStateRetries = 0
      if (!waitingForRememberShard) {
        log.debug("{}: The coordinator state was successfully updated with {}", typeName, evt)
        if (shardId.isDefined) timers.cancel(RememberEntitiesTimeoutKey)
        unbecomeAfterUpdate(evt, afterUpdateCallback)
      } else {
        log.debug(
          "{}: The coordinator state was successfully updated with {}, waiting for remember shard update",
          typeName,
          evt)
        context.become(
          waitingForUpdate(
            evt,
            shardId,
            waitingForStateWrite = false,
            waitingForRememberShard = true,
            afterUpdateCallback = afterUpdateCallback))
      }

    case UpdateTimeout(CoordinatorStateKey, Some(`evt`)) =>
      updateStateRetries += 1

      val template = s"$typeName: The ShardCoordinator was unable to update a distributed state within 'updating-state-timeout': ${stateWriteConsistency.timeout.toMillis} millis (${if (terminating) "terminating"
        else "retrying"}). Attempt $updateStateRetries. " +
        s"Perhaps the ShardRegion has not started on all active nodes yet? event=$evt"

      if (updateStateRetries < 5) {
        log.warning(template)
        if (terminating) {
          context.stop(self)
        } else {
          // repeat until UpdateSuccess
          sendCoordinatorStateUpdate(evt)
        }
      } else {
        log.error(template)
        if (terminating) {
          context.stop(self)
        } else {
          // repeat until UpdateSuccess
          sendCoordinatorStateUpdate(evt)
        }
      }

    case ModifyFailure(key, error, cause, _) =>
      log.error(
        cause,
        s"$typeName: The ShardCoordinator was unable to update a distributed state {} with error {} and event {}. {}",
        key,
        error,
        evt,
        if (terminating) "Coordinator will be terminated due to Terminate message received"
        else "Coordinator will be restarted")
      if (terminating) {
        context.stop(self)
      } else {
        throw cause
      }

    case g @ GetShardHome(shard) =>
      if (!handleGetShardHome(shard))
        stashGetShardHomeRequest(sender(), g) // must wait for update that is in progress

    case ShardCoordinator.Internal.Terminate =>
      log.debug("{}: The ShardCoordinator received termination message while waiting for update", typeName)
      terminating = true
      stash()

    case RememberEntitiesCoordinatorStore.UpdateDone(shard) =>
      if (!shardId.contains(shard)) {
        log.warning(
          "{}: Saw remember entities update complete for shard id [{}], while waiting for [{}]",
          typeName,
          shard,
          shardId.getOrElse(""))
      } else {
        if (!waitingForStateWrite) {
          log.debug("{}: The ShardCoordinator saw remember shard start successfully written {}", typeName, evt)
          if (shardId.isDefined) timers.cancel(RememberEntitiesTimeoutKey)
          unbecomeAfterUpdate(evt, afterUpdateCallback)
        } else {
          log.debug(
            "{}: The ShardCoordinator saw remember shard start successfully written {}, waiting for state update",
            typeName,
            evt)
          context.become(
            waitingForUpdate(
              evt,
              shardId,
              waitingForStateWrite = true,
              waitingForRememberShard = false,
              afterUpdateCallback = afterUpdateCallback))
        }
      }

    case RememberEntitiesCoordinatorStore.UpdateFailed(shard) =>
      if (shardId.contains(shard)) {
        onRememberEntitiesUpdateFailed(shard)
      } else {
        log.warning(
          "{}: Got an remember entities update failed for [{}] while waiting for [{}], ignoring",
          typeName,
          shard,
          shardId.getOrElse(""))
      }

    case RememberEntitiesTimeout(shard) =>
      if (shardId.contains(shard)) {
        onRememberEntitiesUpdateFailed(shard)
      } else {
        log.warning(
          "{}: Got an remember entities update timeout for [{}] while waiting for [{}], ignoring",
          typeName,
          shard,
          shardId.getOrElse(""))
      }

    case RememberEntitiesStoreStopped =>
      onRememberEntitiesStoreStopped()

    case _: RememberEntitiesCoordinatorStore.RememberedShards =>
      log.debug("{}: Late arrival of remembered shards while waiting for update, stashing", typeName)
      stash()

    case _ => stash()
  }

  private def unbecomeAfterUpdate[E <: DomainEvent](evt: E, afterUpdateCallback: E => Unit): Unit = {
    context.unbecome()
    afterUpdateCallback(evt)
    if (verboseDebug)
      log.debug("{}: New coordinator state after [{}]: [{}]", typeName, evt, state)
    unstashOneGetShardHomeRequest()
    unstashAll()
  }

  private def stashGetShardHomeRequest(sender: ActorRef, request: GetShardHome): Unit = {
    log.debug(
      "{}: GetShardHome [{}] request from [{}] stashed, because waiting for initial state or update of state. " +
      "It will be handled afterwards.",
      typeName,
      request.shard,
      sender)
    val replyTo = if (sender == ignoreRef) None else Some(sender)
    getShardHomeRequests = getShardHomeRequests.addRequest(request.shard, replyTo)
  }

  override protected def unstashOneGetShardHomeRequest(): Unit = {
    // prefers shard with most requests from regions
    val (req, nextPending) = getShardHomeRequests.removeRequest()
    req.foreach {
      case (shard, replyTo) =>
        val effectiveSender = replyTo.getOrElse(ignoreRef)
        self.tell(GetShardHome(shard), effectiveSender)
    }
    getShardHomeRequests = nextPending
  }

  override protected def unstashGetShardHomeRequestsForShard(shard: ShardId): Unit = {
    val req = GetShardHome(shard)
    val (r2s, nextPending) = getShardHomeRequests.removeRequestsForShard(shard)
    r2s.foreach { originalSender =>
      self.tell(req, originalSender)
    }
    getShardHomeRequests = nextPending
  }

  def activate(): Unit = {
    context.become(active.orElse(receiveLateRememberedEntities))
    log.info("{}: ShardCoordinator was moved to the active state with [{}] shards", typeName, state.shards.size)
    if (verboseDebug)
      log.debug("{}: Full ShardCoordinator initial state {}", typeName, state)
  }

  // only used once the coordinator is initialized
  def receiveLateRememberedEntities: Receive = {
    case RememberEntitiesCoordinatorStore.RememberedShards(shardIds) =>
      log.debug("{}: Received [{}] remembered shard ids (after state initialized)", typeName, shardIds.size)
      if (shardIds.nonEmpty) {
        val newUnallocatedShards = state.unallocatedShards.union(shardIds.diff(state.shards.keySet))
        state = state.copy(unallocatedShards = newUnallocatedShards)
        allocateShardHomesForRememberEntities()
      }
      timers.cancel(RememberEntitiesTimeoutKey)

    case RememberEntitiesLoadTimeout =>
      // repeat until successful
      getAllRememberedShards()
  }

  def update[E <: DomainEvent](evt: E)(f: E => Unit): Unit = {
    sendCoordinatorStateUpdate(evt)
    val waitingReceive =
      evt match {
        case s: ShardHomeAllocated if rememberEntities && !state.shards.contains(s.shard) =>
          rememberShardAllocated(s.shard)
          waitingForUpdate(
            evt,
            shardId = Some(s.shard),
            waitingForStateWrite = true,
            waitingForRememberShard = true,
            afterUpdateCallback = f)

        case _ =>
          // no update of shards, already known
          waitingForUpdate(
            evt,
            shardId = None,
            waitingForStateWrite = true,
            waitingForRememberShard = false,
            afterUpdateCallback = f)
      }
    context.become(waitingReceive, discardOld = false)
  }

  def getCoordinatorState(): Unit = {
    replicator ! Get(CoordinatorStateKey, stateReadConsistency)
  }

  def getAllRememberedShards(): Unit = {
    timers.startSingleTimer(
      RememberEntitiesTimeoutKey,
      RememberEntitiesLoadTimeout,
      settings.tuningParameters.waitingForStateTimeout)
    rememberEntitiesStore.foreach(_ ! RememberEntitiesCoordinatorStore.GetShards)
  }

  def sendCoordinatorStateUpdate(evt: DomainEvent) = {
    val s = state.updated(evt)
    if (verboseDebug)
      log.debug("{}: Storing new coordinator state [{}]", typeName, state)
    replicator ! Update(
      CoordinatorStateKey,
      LWWRegister(selfUniqueAddress, initEmptyState),
      stateWriteConsistency,
      Some(evt)) { reg =>
      reg.withValueOf(s)
    }
  }

  def rememberShardAllocated(newShard: String) = {
    log.debug("{}: Remembering shard allocation [{}]", typeName, newShard)
    rememberEntitiesStore.foreach(_ ! RememberEntitiesCoordinatorStore.AddShard(newShard))
    timers.startSingleTimer(
      RememberEntitiesTimeoutKey,
      RememberEntitiesTimeout(newShard),
      settings.tuningParameters.updatingStateTimeout)
  }

  override def receiveTerminated: Receive =
    super.receiveTerminated.orElse {
      case RememberEntitiesStoreStopped =>
        onRememberEntitiesStoreStopped()
    }

  def onRememberEntitiesUpdateFailed(shardId: ShardId): Unit = {
    log.error(
      "{}: The ShardCoordinator was unable to update remembered shard [{}] within 'updating-state-timeout': {} millis, {}",
      typeName,
      shardId,
      settings.tuningParameters.updatingStateTimeout.toMillis,
      if (terminating) "terminating" else "retrying")
    if (terminating) context.stop(self)
    else {
      // retry until successful
      rememberShardAllocated(shardId)
    }
  }

  def onRememberEntitiesStoreStopped(): Unit = {
    // rely on backoff supervision of coordinator
    log.error("{}: The ShardCoordinator stopping because the remember entities store stopped", typeName)
    context.stop(self)
  }

}
