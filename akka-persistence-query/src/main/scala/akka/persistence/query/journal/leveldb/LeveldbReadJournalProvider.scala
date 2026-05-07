/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.journal.leveldb

import com.typesafe.config.Config

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider

@deprecated("Use another journal/query implementation", "2.6.15")
class LeveldbReadJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  val readJournal: scaladsl.LeveldbReadJournal = new scaladsl.LeveldbReadJournal(system, config)

  override def scaladslReadJournal(): akka.persistence.query.scaladsl.ReadJournal =
    readJournal

  val javaReadJournal = new javadsl.LeveldbReadJournal(readJournal)

  override def javadslReadJournal(): akka.persistence.query.javadsl.ReadJournal =
    javaReadJournal

}
