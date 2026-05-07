/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.scaladsl

import java.io.NotSerializableException

import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.persistence.testkit._
import akka.persistence.testkit.EventStorage.JournalPolicies
import akka.persistence.testkit.Reject
import akka.persistence.testkit.internal.InMemStorageExtension

class PersistenceTestKitJournalCompatSpec extends JournalSpec(config = PersistenceTestKitPlugin.config) {

  override def beforeAll(): Unit = {
    super.beforeAll()
    InMemStorageExtension(system).setPolicy(new JournalPolicies.PolicyType {
      override def tryProcess(persistenceId: String, op: JournalOperation): ProcessingResult = {
        op match {
          case WriteEvents(batch) =>
            val allSerializable =
              batch.filter(_.isInstanceOf[AnyRef]).forall(_.isInstanceOf[java.io.Serializable])
            if (allSerializable) {
              ProcessingSuccess
            } else {
              Reject(new NotSerializableException("Some objects in the batch were not serializable"))
            }
          case _ => ProcessingSuccess
        }

      }
    })
  }

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = true
  override protected def supportsMetadata: CapabilityFlag = true
  override protected def supportsReplayOnlyLast: CapabilityFlag = true

}

class PersistenceTestKitSnapshotStoreCompatSpec
    extends SnapshotStoreSpec(config = PersistenceTestKitSnapshotPlugin.config)
