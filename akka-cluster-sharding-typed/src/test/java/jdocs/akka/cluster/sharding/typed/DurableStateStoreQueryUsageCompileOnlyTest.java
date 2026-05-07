/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.persistence.query.Offset;
import akka.stream.javadsl.Source;

// #get-durable-state-store-query-example
import akka.persistence.state.DurableStateStoreRegistry;
import akka.persistence.query.javadsl.DurableStateStoreQuery;
import akka.persistence.query.DurableStateChange;
import akka.persistence.query.UpdatedDurableState;

// #get-durable-state-store-query-example

class DurableStateStoreQueryUsageCompileOnlySpec {

  @SuppressWarnings("unchecked")
  public <Record> DurableStateStoreQuery<Record> getQuery(
      ActorSystem system, String pluginId, Offset offset) {
    // #get-durable-state-store-query-example
    DurableStateStoreQuery<Record> durableStateStoreQuery =
        DurableStateStoreRegistry.get(system)
            .getDurableStateStoreFor(DurableStateStoreQuery.class, pluginId);
    Source<DurableStateChange<Record>, NotUsed> source =
        durableStateStoreQuery.changes("tag", offset);
    source.map(
        chg -> {
          if (chg instanceof UpdatedDurableState) {
            UpdatedDurableState<Record> upd = (UpdatedDurableState<Record>) chg;
            return upd.value();
          } else {
            throw new IllegalArgumentException("Unexpected DurableStateChange " + chg.getClass());
          }
        });
    // #get-durable-state-store-query-example
    return durableStateStoreQuery;
  }
}
