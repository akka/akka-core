/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public interface MapWithResource {
  // #mapWithResource-blocking-api
  interface DBDriver {
    Connection create(URL url, String userName, String password);
  }

  interface Connection {
    void close();
  }

  interface Database {
    // blocking query
    QueryResult doQuery(Connection connection, String query);
  }

  interface QueryResult {
    boolean hasMore();

    // potentially blocking retrieval of each element
    DatabaseRecord next();

    // potentially blocking retrieval all element
    List<DatabaseRecord> toList();
  }

  interface DatabaseRecord {}
  // #mapWithResource-blocking-api

  default void mapWithResourceExample() {
    final ActorSystem system = null;
    final URL url = null;
    final String userName = "Akka";
    final String password = "Hakking";
    final DBDriver dbDriver = null;
    // #mapWithResource
    // some database for JVM
    final Database db = null;
    Source.from(
            Arrays.asList(
                "SELECT * FROM shop ORDER BY article-0000 order by gmtModified desc limit 100;",
                "SELECT * FROM shop ORDER BY article-0001 order by gmtModified desc limit 100;"))
        .mapWithResource(
            () -> dbDriver.create(url, userName, password),
            (connection, query) -> db.doQuery(connection, query).toList(),
            connection -> {
              connection.close();
              return Optional.empty();
            })
        .mapConcat(elems -> elems)
        .runForeach(System.out::println, system);
    // #mapWithResource
  }
}
