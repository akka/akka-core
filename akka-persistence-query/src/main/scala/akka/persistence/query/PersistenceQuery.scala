/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query

import scala.annotation.nowarn
import scala.reflect.ClassTag

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor._
import akka.annotation.InternalApi
import akka.persistence.{ PersistencePlugin, PluginProvider }
import akka.persistence.query.scaladsl.ReadJournal

/**
 * Persistence extension for queries.
 */
object PersistenceQuery extends ExtensionId[PersistenceQuery] with ExtensionIdProvider {

  override def get(system: ActorSystem): PersistenceQuery = super.get(system)
  override def get(system: ClassicActorSystemProvider): PersistenceQuery = super.get(system)

  def createExtension(system: ExtendedActorSystem): PersistenceQuery = new PersistenceQuery(system)

  def lookup: PersistenceQuery.type = PersistenceQuery

  @InternalApi
  private[akka] val pluginProvider: PluginProvider[ReadJournalProvider, ReadJournal, javadsl.ReadJournal] =
    new PluginProvider[ReadJournalProvider, scaladsl.ReadJournal, javadsl.ReadJournal] {
      override def scalaDsl(t: ReadJournalProvider): ReadJournal = t.scaladslReadJournal()
      override def javaDsl(t: ReadJournalProvider): javadsl.ReadJournal = t.javadslReadJournal()
    }

}

class PersistenceQuery(system: ExtendedActorSystem)
    extends PersistencePlugin[scaladsl.ReadJournal, javadsl.ReadJournal, ReadJournalProvider](system)(
      ClassTag(classOf[ReadJournalProvider]),
      PersistenceQuery.pluginProvider)
    with Extension {

  /**
   * Scala API: Returns the [[akka.persistence.query.scaladsl.ReadJournal]] specified by the given
   * read journal configuration entry.
   *
   * The provided readJournalPluginConfig will be used to configure the journal plugin instead of the actor system
   * config.
   */
  final def readJournalFor[T <: scaladsl.ReadJournal](readJournalPluginId: String, readJournalPluginConfig: Config): T =
    pluginFor(readJournalPluginId, readJournalPluginConfig).scaladslPlugin.asInstanceOf[T]

  /**
   * Scala API: Returns the [[akka.persistence.query.scaladsl.ReadJournal]] specified by the given
   * read journal configuration entry.
   */
  final def readJournalFor[T <: scaladsl.ReadJournal](readJournalPluginId: String): T =
    readJournalFor(readJournalPluginId, ConfigFactory.empty)

  /**
   * Java API: Returns the [[akka.persistence.query.javadsl.ReadJournal]] specified by the given
   * read journal configuration entry.
   */
  final def getReadJournalFor[T <: javadsl.ReadJournal](
      @nowarn("msg=never used") clazz: Class[T],
      readJournalPluginId: String,
      readJournalPluginConfig: Config): T =
    pluginFor(readJournalPluginId, readJournalPluginConfig).javadslPlugin.asInstanceOf[T]

  final def getReadJournalFor[T <: javadsl.ReadJournal](clazz: Class[T], readJournalPluginId: String): T =
    getReadJournalFor[T](clazz, readJournalPluginId, ConfigFactory.empty())

}
