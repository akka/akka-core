/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.external.internal

import java.util.concurrent.CompletionStage

import scala.concurrent.Future
import scala.jdk.DurationConverters._
import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.AddressFromURIString
import akka.annotation.InternalApi
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.Replicator.Get
import akka.cluster.ddata.Replicator.GetFailure
import akka.cluster.ddata.Replicator.GetSuccess
import akka.cluster.ddata.Replicator.NotFound
import akka.cluster.ddata.Replicator.ReadMajority
import akka.cluster.ddata.Replicator.Update
import akka.cluster.ddata.Replicator.UpdateSuccess
import akka.cluster.ddata.Replicator.UpdateTimeout
import akka.cluster.ddata.Replicator.WriteLocal
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.external.ClientTimeoutException
import akka.cluster.sharding.external.ExternalShardAllocationStrategy
import akka.cluster.sharding.external.ExternalShardAllocationStrategy.ShardLocation
import akka.cluster.sharding.external.ShardLocations
import akka.dispatch.MessageDispatcher
import akka.event.Logging
import akka.pattern.ask
import akka.util.PrettyDuration._
import akka.util.Timeout
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 */
@InternalApi
final private[external] class ExternalShardAllocationClientImpl(system: ActorSystem, typeName: String)
    extends akka.cluster.sharding.external.scaladsl.ExternalShardAllocationClient
    with akka.cluster.sharding.external.javadsl.ExternalShardAllocationClient {

  private val log = Logging(system, classOf[ExternalShardAllocationClientImpl])

  private val replicator: ActorRef = DistributedData(system).replicator
  private val self: SelfUniqueAddress = DistributedData(system).selfUniqueAddress

  private val timeout =
    system.settings.config
      .getDuration("akka.cluster.sharding.external-shard-allocation-strategy.client-timeout")
      .toScala
  private implicit val askTimeout: Timeout = Timeout(timeout * 2)
  private implicit val ec: MessageDispatcher = system.dispatchers.internalDispatcher

  private val Key = ExternalShardAllocationStrategy.ddataKey(typeName)

  override def updateShardLocation(shard: ShardId, location: Address): Future[Done] = {
    log.debug("updateShardLocation {} {} key {}", shard, location, Key)
    (replicator ? Update(Key, LWWMap.empty[ShardId, String], WriteLocal, None) { existing =>
      existing.put(self, shard, location.toString)
    }).flatMap {
      case UpdateSuccess(_, _) => Future.successful(Done)
      case UpdateTimeout =>
        Future.failed(new ClientTimeoutException(s"Unable to update shard location after ${timeout.duration.pretty}"))
      case _ => throw new IllegalArgumentException() // compiler exhaustiveness check pleaser
    }
  }

  override def setShardLocation(shard: ShardId, location: Address): CompletionStage[Done] =
    updateShardLocation(shard, location).asJava

  override def shardLocations(): Future[ShardLocations] = {
    (replicator ? Get(Key, ReadMajority(timeout)))
      .flatMap {
        case success @ GetSuccess(`Key`, _) =>
          Future.successful(
            success.get(Key).entries.transform((_, asStr) => ShardLocation(AddressFromURIString(asStr))))
        case NotFound(_, _) =>
          Future.successful(Map.empty[ShardId, ShardLocation])
        case GetFailure(_, _) =>
          Future.failed((new ClientTimeoutException(s"Unable to get shard locations after ${timeout.duration.pretty}")))
        case _ => throw new IllegalArgumentException() // compiler exhaustiveness check pleaser
      }
      .map { locations =>
        new ShardLocations(locations)
      }
  }

  override def getShardLocations(): CompletionStage[ShardLocations] = shardLocations().asJava

  override def updateShardLocations(locations: Map[ShardId, Address]): Future[Done] = {
    log.debug("updateShardLocations {} for {}", locations, Key)
    (replicator ? Update(Key, LWWMap.empty[ShardId, String], WriteLocal, None) { existing =>
      locations.foldLeft(existing) {
        case (acc, (shardId, address)) => acc.put(self, shardId, address.toString)
      }
    }).flatMap {
      case UpdateSuccess(_, _) => Future.successful(Done)
      case UpdateTimeout =>
        Future.failed(new ClientTimeoutException(s"Unable to update shard location after ${timeout.duration.pretty}"))
      case _ => throw new IllegalArgumentException() // compiler exhaustiveness check pleaser
    }
  }

  override def setShardLocations(locations: java.util.Map[ShardId, Address]): CompletionStage[Done] = {
    updateShardLocations(locations.asScala.toMap).asJava
  }
}
