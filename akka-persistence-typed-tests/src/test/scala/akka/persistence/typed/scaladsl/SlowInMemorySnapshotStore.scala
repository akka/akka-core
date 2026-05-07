/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import scala.concurrent.Future

import akka.persistence.{ SnapshotMetadata => ClassicSnapshotMetadata }
import akka.persistence.{ SnapshotSelectionCriteria => ClassicSnapshotSelectionCriteria }
import akka.persistence.SelectedSnapshot
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.typed.scaladsl.SnapshotMutableStateSpec.MutableState

class SlowInMemorySnapshotStore extends SnapshotStore {

  private var state = Map.empty[String, (Any, ClassicSnapshotMetadata)]

  def loadAsync(persistenceId: String, criteria: ClassicSnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    Future.successful(state.get(persistenceId).map {
      case (snap, meta) => SelectedSnapshot(meta, snap)
    })
  }

  def saveAsync(metadata: ClassicSnapshotMetadata, snapshot: Any): Future[Unit] = {
    snapshot match {
      case mutableState: MutableState =>
        val value1 = mutableState.value
        Thread.sleep(50)
        val value2 = mutableState.value
        // it mustn't have been modified by another command/event
        if (value1 != value2)
          Future.failed(new IllegalStateException(s"State changed from $value1 to $value2"))
        else {
          // copy to simulate serialization, and subsequent recovery shouldn't get same instance
          state = state.updated(metadata.persistenceId, (new MutableState(mutableState.value), metadata))
          Future.successful(())
        }
      case _ =>
        Thread.sleep(50)
        state = state.updated(metadata.persistenceId, (snapshot, metadata))
        Future.successful(())
    }
  }

  override def deleteAsync(metadata: ClassicSnapshotMetadata): Future[Unit] = {
    state = state.filterNot {
      case (pid, (_, meta)) => pid == metadata.persistenceId && meta.sequenceNr == metadata.sequenceNr
    }
    Future.successful(())
  }

  override def deleteAsync(persistenceId: String, criteria: ClassicSnapshotSelectionCriteria): Future[Unit] = {
    state = state.filterNot {
      case (pid, (_, meta)) => pid == persistenceId && criteria.matches(meta)
    }
    Future.successful(())
  }
}
