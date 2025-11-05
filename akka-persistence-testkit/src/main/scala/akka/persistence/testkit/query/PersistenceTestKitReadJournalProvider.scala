/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.query
import com.typesafe.config.Config

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider

class PersistenceTestKitReadJournalProvider(system: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournalProvider {
  private val _scaladslReadJournal =
    new scaladsl.PersistenceTestKitReadJournal(system, config, configPath)
  override def scaladslReadJournal(): scaladsl.PersistenceTestKitReadJournal =
    _scaladslReadJournal

  override def javadslReadJournal(): javadsl.PersistenceTestKitReadJournal =
    new javadsl.PersistenceTestKitReadJournal(_scaladslReadJournal)
}
