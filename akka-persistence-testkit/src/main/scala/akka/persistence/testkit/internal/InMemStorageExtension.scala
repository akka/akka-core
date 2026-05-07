/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.internal

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ ActorSystem, ExtendedActorSystem, ExtensionId, ExtensionIdProvider }
import akka.actor.Extension
import akka.annotation.InternalApi
import akka.persistence.testkit.EventStorage
import akka.persistence.testkit.JournalOperation
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.ProcessingPolicy
import akka.persistence.testkit.scaladsl.PersistenceTestKit

/**
 * INTERNAL API
 */
@InternalApi
private[testkit] object InMemStorageExtension extends ExtensionId[InMemStorageExtension] with ExtensionIdProvider {

  override def get(system: ActorSystem): InMemStorageExtension = super.get(system)

  override def createExtension(system: ExtendedActorSystem): InMemStorageExtension =
    new InMemStorageExtension(system)

  override def lookup = InMemStorageExtension

}

/**
 * INTERNAL API
 */
@InternalApi
final class InMemStorageExtension(system: ExtendedActorSystem) extends Extension {

  private val stores = new ConcurrentHashMap[String, EventStorage]()

  def defaultStorage(): EventStorage = storageFor(PersistenceTestKitPlugin.PluginId)

  // shortcuts for default policy
  def currentPolicy: ProcessingPolicy[JournalOperation] = defaultStorage().currentPolicy
  def setPolicy(policy: ProcessingPolicy[JournalOperation]): Unit = defaultStorage().setPolicy(policy)
  def resetPolicy(): Unit = defaultStorage().resetPolicy()

  def storageFor(key: String): EventStorage =
    stores.computeIfAbsent(key, _ => {
      // we don't really care about the key here, we just want separate instances
      if (PersistenceTestKit.Settings(system).serialize) {
        new SerializedEventStorageImpl(system)
      } else {
        new SimpleEventStorageImpl
      }
    })

}
