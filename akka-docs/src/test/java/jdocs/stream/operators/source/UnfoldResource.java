/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import java.util.Optional;

interface UnfoldResource {
  // imaginary blocking API we need to use
  // #unfoldResource-blocking-api
  interface Database {
    // blocking query
    QueryResult doQuery();
  }

  interface QueryResult {
    boolean hasMore();
    // potentially blocking retrieval of each element
    DatabaseEntry nextEntry();

    void close();
  }

  interface DatabaseEntry {}

  // #unfoldResource-blocking-api

  default void unfoldResourceExample() {
    ActorSystem system = null;

    // #unfoldResource
    // we don't actually have one, it was just made up for the sample
    Database database = null;

    Source<DatabaseEntry, NotUsed> queryResultSource =
        Source.unfoldResource(
            // open
            () -> database.doQuery(),
            // read
            (queryResult) -> {
              if (queryResult.hasMore()) return Optional.of(queryResult.nextEntry());
              else return Optional.empty();
            },
            // close
            QueryResult::close);

    queryResultSource.runForeach(entry -> System.out.println(entry.toString()), system);
    // #unfoldResource
  }
}
