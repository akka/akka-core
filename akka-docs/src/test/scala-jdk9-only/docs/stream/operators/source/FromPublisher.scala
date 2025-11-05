/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.source;

//#imports
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Publisher;

import akka.NotUsed;
import akka.stream.scaladsl.Source;
import akka.stream.scaladsl.JavaFlowSupport;

//#imports

object FromPublisher {
    case class Row(name: String)

    class DatabaseClient {
        def fetchRows(): Publisher[Row] = ???
    }

    val databaseClient: DatabaseClient = ???

    // #example
    val names: Source[String, NotUsed] =
      // A new subscriber will subscribe to the supplied publisher for each
      // materialization, so depending on whether the database client supports
      // this the Source can be materialized more than once.
      JavaFlowSupport.Source.fromPublisher(databaseClient.fetchRows())
        .map(row => row.name);
    //#example
}
