/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class UnfoldResourceAsync {
  // imaginary async API we need to use
  // #unfoldResource-async-api
  interface Database {
    // async query
    CompletionStage<QueryResult> doQuery();
  }

  interface QueryResult {

    // are there more results
    CompletionStage<Boolean> hasMore();

    // async retrieval of each element
    CompletionStage<DatabaseEntry> nextEntry();

    CompletionStage<Void> close();
  }

  interface DatabaseEntry {}

  // #unfoldResource-async-api

  void unfoldResourceExample() {
    ActorSystem system = null;

    // #unfoldResourceAsync
    // we don't actually have one, it was just made up for the sample
    Database database = null;

    Source<DatabaseEntry, NotUsed> queryResultSource =
        Source.unfoldResourceAsync(
            // open
            database::doQuery,
            // read
            this::readQueryResult,
            // close
            queryResult -> queryResult.close().thenApply(__ -> Done.done()));

    queryResultSource.runForeach(entry -> System.out.println(entry.toString()), system);
    // #unfoldResourceAsync
  }

  // #unfoldResourceAsync
  private CompletionStage<Optional<DatabaseEntry>> readQueryResult(QueryResult queryResult) {
    return queryResult
        .hasMore()
        .thenCompose(
            more -> {
              if (more) {
                return queryResult.nextEntry().thenApply(Optional::of);
              } else {
                return CompletableFuture.completedFuture(Optional.empty());
              }
            });
  }
  // #unfoldResourceAsync
}
