/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.snapshot.japi;

import akka.persistence.SelectedSnapshot;
import akka.persistence.SnapshotMetadata;
import akka.persistence.SnapshotSelectionCriteria;
import java.util.Optional;
import scala.concurrent.Future;

interface SnapshotStorePlugin {
  // #snapshot-store-plugin-api
  /**
   * Java API, Plugin API: asynchronously loads a snapshot.
   *
   * @param persistenceId id of the persistent actor.
   * @param criteria selection criteria for loading.
   */
  Future<Optional<SelectedSnapshot>> doLoadAsync(
      String persistenceId, SnapshotSelectionCriteria criteria);

  /**
   * Java API, Plugin API: asynchronously saves a snapshot.
   *
   * @param metadata snapshot metadata.
   * @param snapshot snapshot.
   */
  Future<Void> doSaveAsync(SnapshotMetadata metadata, Object snapshot);

  /**
   * Java API, Plugin API: deletes the snapshot identified by `metadata`.
   *
   * @param metadata snapshot metadata.
   */
  Future<Void> doDeleteAsync(SnapshotMetadata metadata);

  /**
   * Java API, Plugin API: deletes all snapshots matching `criteria`.
   *
   * @param persistenceId id of the persistent actor.
   * @param criteria selection criteria for deleting.
   */
  Future<Void> doDeleteAsync(String persistenceId, SnapshotSelectionCriteria criteria);
  // #snapshot-store-plugin-api
}
