/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.source;

//#imports
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Publisher;

import akka.NotUsed;
import akka.stream.scaladsl.Source;
import akka.stream.scaladsl.JavaFlowSupport;

//#imports

object AsSubscriber {
    case class Row(name: String)

    class DatabaseClient {
        def fetchRows(): Publisher[Row] = ???
    }

    val databaseClient: DatabaseClient = ???;

    // #example
    val rowSource: Source[Row, NotUsed] =
      JavaFlowSupport.Source.asSubscriber
        .mapMaterializedValue(
          (subscriber: Subscriber[Row]) => {
            // For each materialization, fetch the rows from the database:
            val rows: Publisher[Row] = databaseClient.fetchRows()
            rows.subscribe(subscriber)
            NotUsed
          });

    val names: Source[String, NotUsed] =
      // rowSource can be re-used, since it will start a new
      // query for each materialization, fully supporting backpressure
      // for each materialized stream:
      rowSource.map(row => row.name);
    //#example
}
