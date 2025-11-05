/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.external

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorRefScope
import akka.actor.Address
import akka.actor.AddressFromURIString
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.Props
import akka.actor.Stash
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.Replicator.Changed
import akka.cluster.ddata.Replicator.Subscribe
import akka.cluster.sharding.ShardCoordinator
import akka.cluster.sharding.ShardRegion.ShardId
import akka.event.Logging
import akka.pattern.AskTimeoutException
import akka.util.Timeout

object ExternalShardAllocationStrategy {

  type ShardRegion = ActorRef

  /**
   * Scala API
   */
  def apply(systemProvider: ClassicActorSystemProvider, typeName: String): ExternalShardAllocationStrategy =
    new ExternalShardAllocationStrategy(systemProvider, typeName)

  /**
   * Java API
   */
  def create(systemProvider: ClassicActorSystemProvider, typeName: String): ExternalShardAllocationStrategy =
    apply(systemProvider, typeName)

  // local only messages
  private[akka] final case class GetShardLocation(shard: ShardId)
  private[akka] case object GetShardLocations
  private[akka] final case class GetShardLocationsResponse(desiredAllocations: Map[ShardId, Address])
  private[akka] final case class GetShardLocationResponse(address: Option[Address])

  // only returned locally, serialized as a string
  final case class ShardLocation(address: Address) extends NoSerializationVerificationNeeded

  private object DDataStateActor {
    def props(typeName: String) = Props(new DDataStateActor(typeName))
  }

  // uses a string primitive types are optimized in ddata to not serialize every entity
  // separately
  private[akka] def ddataKey(typeName: String): LWWMapKey[ShardId, String] = {
    LWWMapKey[ShardId, String](s"external-sharding-$typeName")
  }

  private class DDataStateActor(typeName: String) extends Actor with ActorLogging with Stash {

    private val replicator = DistributedData(context.system).replicator

    private val Key = ddataKey(typeName)

    override def preStart(): Unit = {
      log.debug("Starting ddata state actor for [{}]", typeName)
      replicator ! Subscribe(Key, self)
    }

    var currentLocations: Map[ShardId, String] = Map.empty

    override def receive: Receive = {
      case c @ Changed(key: LWWMapKey[ShardId, String] @unchecked) =>
        val newLocations = c.get(key).entries
        currentLocations ++= newLocations
        log.debug("Received updated shard locations [{}] all locations are now [{}]", newLocations, currentLocations)
      case GetShardLocation(shard) =>
        log.debug("GetShardLocation [{}]", shard)
        val shardLocation = currentLocations.get(shard).map(asStr => AddressFromURIString(asStr))
        sender() ! GetShardLocationResponse(shardLocation)
      case GetShardLocations =>
        log.debug("GetShardLocations")
        sender() ! GetShardLocationsResponse(currentLocations.transform((_, asStr) => AddressFromURIString(asStr)))
    }

  }
}

class ExternalShardAllocationStrategy(systemProvider: ClassicActorSystemProvider, typeName: String)(
    // local only ask
    implicit val timeout: Timeout = Timeout(5.seconds))
    extends ShardCoordinator.StartableAllocationStrategy {

  private val system = systemProvider.classicSystem

  import ExternalShardAllocationStrategy._
  import system.dispatcher

  import akka.pattern.ask

  private val log = Logging(system, classOf[ExternalShardAllocationStrategy])

  private var shardState: ActorRef = _

  // Eagerly initialize the DistributedData extension on all nodes. Otherwise, if not used for other things,
  // the DistributedData Replicator is started via the DDataStateActor by the shard coordinator on one node,
  // but not on other nodes.
  DistributedData(systemProvider)

  private[akka] def createShardStateActor(): ActorRef = {
    system
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(DDataStateActor.props(typeName), s"external-allocation-state-$typeName")
  }

  private val cluster = Cluster(system)

  override def start(): Unit = {
    shardState = createShardStateActor()
  }

  override def allocateShard(
      requester: ShardRegion,
      shardId: ShardId,
      currentShardAllocations: Map[ShardRegion, immutable.IndexedSeq[ShardId]]): Future[ShardRegion] = {

    log.debug("allocateShard [{}] [{}] [{}]", shardId, requester, currentShardAllocations)

    // current shard allocations include all current shard regions
    (shardState ? GetShardLocation(shardId))
      .mapTo[GetShardLocationResponse]
      .map {
        case GetShardLocationResponse(None) =>
          log.debug("No specific location for shard [{}]. Allocating to requester [{}]", shardId, requester)
          requester
        case GetShardLocationResponse(Some(address)) =>
          // if it is the local address, convert it so it is found in the shards
          if (address == cluster.selfAddress) {
            currentShardAllocations.keys.find(_.path.address.hasLocalScope) match {
              case None =>
                log.debug("unable to find local shard in currentShardAllocation. Using requester")
                requester
              case Some(localShardRegion) =>
                log.debug("allocating to local shard")
                localShardRegion
            }
          } else {
            currentShardAllocations.keys.find(_.path.address == address) match {
              case None =>
                log.debug(
                  "External shard location [{}] for shard [{}] not found in members [{}]",
                  address,
                  shardId,
                  currentShardAllocations.keys.mkString(","))
                requester
              case Some(location) =>
                log.debug("Allocating shard to location [{}]", location)
                location
            }
          }
      }
      .recover {
        case _: AskTimeoutException =>
          log.warning(
            "allocate timed out waiting for shard allocation state [{}]. Allocating to requester [{}]",
            shardId,
            requester)
          requester
      }

  }

  override def rebalance(
      currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
      rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] = {

    log.debug("rebalance [{}] [{}]", currentShardAllocations, rebalanceInProgress)

    val currentAllocationByAddress: Map[Address, immutable.IndexedSeq[ShardId]] = currentShardAllocations.map {
      case (ref: ActorRefScope, value) if ref.isLocal =>
        (cluster.selfAddress, value) // so it can be compared to a address with host and port
      case (key, value) => (key.path.address, value)
    }

    val currentlyAllocatedShards: Set[ShardId] = currentShardAllocations.foldLeft(Set.empty[ShardId]) {
      case (acc, next) => acc ++ next._2.toSet
    }

    log.debug("Current allocations by address: [{}]", currentAllocationByAddress)

    val shardsThatNeedRebalanced: Future[Set[ShardId]] = for {
      desiredMappings <- (shardState ? GetShardLocations).mapTo[GetShardLocationsResponse]
    } yield {
      log.debug("desired allocations: [{}]", desiredMappings.desiredAllocations)
      desiredMappings.desiredAllocations.filter {
        case (shardId, expectedLocation) if currentlyAllocatedShards.contains(shardId) =>
          currentAllocationByAddress.get(expectedLocation) match {
            case None =>
              log.debug(
                "Shard [{}] desired location [{}] is not part of the cluster, not rebalancing",
                shardId,
                expectedLocation)
              false // not a current allocation so don't rebalance yet
            case Some(shards) =>
              val inCorrectLocation = shards.contains(shardId)
              !inCorrectLocation
          }
        case (shardId, _) =>
          log.debug("Shard [{}] not currently allocated so not rebalancing to desired location", shardId)
          false
      }
    }.keys.toSet

    shardsThatNeedRebalanced
      .map { done =>
        if (done.nonEmpty) {
          log.debug("Shards not currently in their desired location [{}]", done)
        }
        done
      }
      .recover {
        case _: AskTimeoutException =>
          log.warning("rebalance timed out waiting for shard allocation state. Keeping existing allocations")
          Set.empty
      }
  }

}
