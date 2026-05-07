/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.state

import scala.annotation.nowarn
import scala.reflect.ClassTag

import com.typesafe.config.Config

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.PersistencePlugin
import akka.persistence.PluginProvider
import akka.persistence.state.scaladsl.DurableStateStore

/**
 * Persistence extension for queries.
 */
object DurableStateStoreRegistry extends ExtensionId[DurableStateStoreRegistry] with ExtensionIdProvider {

  override def get(system: ActorSystem): DurableStateStoreRegistry = super.get(system)
  override def get(system: ClassicActorSystemProvider): DurableStateStoreRegistry = super.get(system)

  def createExtension(system: ExtendedActorSystem): DurableStateStoreRegistry = new DurableStateStoreRegistry(system)

  def lookup: DurableStateStoreRegistry.type = DurableStateStoreRegistry

  @InternalApi
  private[akka] val pluginProvider
      : PluginProvider[DurableStateStoreProvider, DurableStateStore[_], javadsl.DurableStateStore[_]] =
    new PluginProvider[DurableStateStoreProvider, scaladsl.DurableStateStore[_], javadsl.DurableStateStore[_]] {
      override def scalaDsl(t: DurableStateStoreProvider): DurableStateStore[_] = t.scaladslDurableStateStore()
      override def javaDsl(t: DurableStateStoreProvider): javadsl.DurableStateStore[_] = t.javadslDurableStateStore()
    }

}

class DurableStateStoreRegistry(system: ExtendedActorSystem)
    extends PersistencePlugin[scaladsl.DurableStateStore[_], javadsl.DurableStateStore[_], DurableStateStoreProvider](
      system)(ClassTag(classOf[DurableStateStoreProvider]), DurableStateStoreRegistry.pluginProvider)
    with Extension {

  private val systemConfig = system.settings.config

  private lazy val defaultPluginId = {
    val configPath = systemConfig.getString("akka.persistence.state.plugin")
    Persistence.verifyPluginConfigIsDefined(configPath, "Default DurableStateStore")
    Persistence.verifyPluginConfigExists(systemConfig, configPath, "DurableStateStore")
    configPath
  }

  private def pluginIdOrDefault(pluginId: String): String = {
    val configPath = if (isEmpty(pluginId)) defaultPluginId else pluginId
    Persistence.verifyPluginConfigExists(systemConfig, configPath, "DurableStateStore")
    configPath
  }

  private def pluginConfig(pluginId: String): Config = {
    val configPath = pluginIdOrDefault(pluginId)
    systemConfig.getConfig(configPath).withFallback(systemConfig.getConfig("akka.persistence.state-plugin-fallback"))
  }

  /** Check for default or missing identity. */
  private def isEmpty(text: String) = {
    text == null || text.isEmpty
  }

  /**
   * Scala API: Returns the [[akka.persistence.state.scaladsl.DurableStateStore]] specified by the given
   * configuration entry.
   */
  final def durableStateStoreFor[T <: scaladsl.DurableStateStore[_]](pluginId: String): T = {
    pluginFor(pluginIdOrDefault(pluginId), pluginConfig(pluginId)).scaladslPlugin.asInstanceOf[T]
  }

  /**
   * Java API: Returns the [[akka.persistence.state.javadsl.DurableStateStore]] specified by the given
   * configuration entry.
   */
  final def getDurableStateStoreFor[T <: javadsl.DurableStateStore[_]](
      @nowarn("msg=never used") clazz: Class[T],
      pluginId: String): T = {
    pluginFor(pluginIdOrDefault(pluginId), pluginConfig(pluginId)).javadslPlugin.asInstanceOf[T]
  }

}
