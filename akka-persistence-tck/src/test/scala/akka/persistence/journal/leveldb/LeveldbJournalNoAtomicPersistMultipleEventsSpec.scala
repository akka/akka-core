/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal.leveldb

import akka.persistence.{ PersistenceSpec, PluginCleanup }
import akka.persistence.journal.JournalSpec

class LeveldbJournalNoAtomicPersistMultipleEventsSpec
    extends JournalSpec(
      config = PersistenceSpec.config(
        "leveldb",
        "LeveldbJournalNoAtomicPersistMultipleEventsSpec",
        extraConfig = Some("""
        akka.persistence.journal.leveldb.native = off
        akka.actor.allow-java-serialization = off
        akka.actor.warn-about-java-serializer-usage = on
        """)))
    with PluginCleanup {

  /**
   * Setting to false to test the single message atomic write behavior of JournalSpec
   */
  override def supportsAtomicPersistAllOfSeveralEvents = false

  override def supportsRejectingNonSerializableObjects = true

  override def supportsSerialization = true

}
