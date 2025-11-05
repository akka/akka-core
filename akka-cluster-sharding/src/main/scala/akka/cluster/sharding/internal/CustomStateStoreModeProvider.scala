/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.annotation.InternalApi
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion.ShardId
import akka.event.Logging

/**
 * INTERNAL API
 *
 * Only intended for testing, not an extension point.
 */
@InternalApi
private[akka] final class CustomStateStoreModeProvider(
    typeName: String,
    system: ActorSystem,
    settings: ClusterShardingSettings)
    extends RememberEntitiesProvider {

  private val log = Logging(system, classOf[CustomStateStoreModeProvider])
  log.warning("Using custom remember entities store for [{}], not intended for production use.", typeName)
  val customStore = if (system.settings.config.hasPath("akka.cluster.sharding.remember-entities-custom-store")) {
    val customClassName = system.settings.config.getString("akka.cluster.sharding.remember-entities-custom-store")

    val store = system
      .asInstanceOf[ExtendedActorSystem]
      .dynamicAccess
      .createInstanceFor[RememberEntitiesProvider](
        customClassName,
        Vector((classOf[ClusterShardingSettings], settings), (classOf[String], typeName)))
    log.debug("Will use custom remember entities store provider [{}]", store)
    store.get

  } else {
    log.error("Missing custom store class configuration for CustomStateStoreModeProvider")
    throw new RuntimeException("Missing custom store class configuration")
  }

  override def shardStoreProps(shardId: ShardId): Props = customStore.shardStoreProps(shardId)

  override def coordinatorStoreProps(): Props = customStore.coordinatorStoreProps()
}
