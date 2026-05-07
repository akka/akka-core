/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

//#imports
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Publisher;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.JavaFlowSupport;

//#imports

import org.apache.commons.lang.NotImplementedException;

public interface AsSubscriber {
    // We are 'faking' the JavaFlowSupport API here so we can include the signature as a snippet in the API,
    // because we're not publishing those (jdk9+) classes in our API docs yet.
    static class JavaFlowSupport {
        public static final class Source {
            public
            // #api
            static <T> akka.stream.javadsl.Source<T, Subscriber<T>> asSubscriber()
            // #api
            {
                return akka.stream.javadsl.JavaFlowSupport.Source.<T>asSubscriber();
            }
        }
    }

    static class Row {
        public String getField(String fieldName) {
            throw new NotImplementedException();
        }
    }

    static class DatabaseClient {
        Publisher<Row> fetchRows() {
            throw new NotImplementedException();
        }
    }

    DatabaseClient databaseClient = null;

    // #example
    class Example {
        Source<Row, NotUsed> rowSource =
                JavaFlowSupport.Source.<Row>asSubscriber()
                        .mapMaterializedValue(
                                subscriber -> {
                                    // For each materialization, fetch the rows from the database:
                                    Publisher<Row> rows = databaseClient.fetchRows();
                                    rows.subscribe(subscriber);

                                    return NotUsed.getInstance();
                                });

        public Source<String, NotUsed> names() {
            // rowSource can be re-used, since it will start a new
            // query for each materialization, fully supporting backpressure
            // for each materialized stream:
            return rowSource.map(row -> row.getField("name"));
        }
    }
    // #example
}
