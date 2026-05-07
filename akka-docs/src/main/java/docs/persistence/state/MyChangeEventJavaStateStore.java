/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence.state;

// #plugin-imports

import akka.Done;
import akka.actor.ExtendedActorSystem;
import akka.persistence.state.javadsl.DurableStateUpdateWithChangeEventStore;
import akka.persistence.state.javadsl.GetObjectResult;
import com.typesafe.config.Config;
import java.util.concurrent.CompletionStage;

// #plugin-imports

// #state-store-plugin-api
class MyChangeEventJavaStateStore<A> implements DurableStateUpdateWithChangeEventStore<A> {

  private ExtendedActorSystem system;
  private Config config;
  private String cfgPath;

  public MyChangeEventJavaStateStore(ExtendedActorSystem system, Config config, String cfgPath) {
    this.system = system;
    this.config = config;
    this.cfgPath = cfgPath;
  }

  /**
   * Will delete the state by setting it to the empty state and the revision number will be
   * incremented by 1.
   */
  @Override
  public CompletionStage<Done> deleteObject(String persistenceId, long revision) {
    // implement deleteObject here
    return null;
  }

  @Override
  public CompletionStage<Done> deleteObject(
      String persistenceId, long revision, Object changeEvent) {
    // implement deleteObject here
    return null;
  }

  /** Returns the current state for the given persistence id. */
  @Override
  public CompletionStage<GetObjectResult<A>> getObject(String persistenceId) {
    // implement getObject here
    return null;
  }

  /**
   * Will persist the latest state. If it’s a new persistence id, the record will be inserted.
   *
   * <p>In case of an existing persistence id, the record will be updated only if the revision
   * number of the incoming record is 1 more than the already existing record. Otherwise persist
   * will fail.
   */
  @Override
  public CompletionStage<Done> upsertObject(
      String persistenceId, long revision, Object value, String tag) {
    // implement upsertObject here
    return null;
  }

  /** Deprecated. Use the deleteObject overload with revision instead. */
  @Override
  public CompletionStage<Done> deleteObject(String persistenceId) {
    return deleteObject(persistenceId, 0);
  }

  @Override
  public CompletionStage<Done> upsertObject(
      String persistenceId, long revision, A value, String tag, Object changeEvent) {
    // implement deleteObject here
    return null;
  }
}
// #state-store-plugin-api
