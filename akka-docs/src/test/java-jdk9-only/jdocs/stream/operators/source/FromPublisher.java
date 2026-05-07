/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

//#imports
import java.util.concurrent.Flow.Publisher;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.JavaFlowSupport;

//#imports

import org.apache.commons.lang.NotImplementedException;

public interface FromPublisher {
    // We are 'faking' the JavaFlowSupport API here so we can include the signature as a snippet in the API,
    // because we're not publishing those (jdk9+) classes in our API docs yet.
    static class JavaFlowSupport {
        public static final class Source {
            public
            // #api
            static <T> akka.stream.javadsl.Source<T, NotUsed> fromPublisher(Publisher<T> publisher)
            // #api
            {
                return akka.stream.javadsl.JavaFlowSupport.Source.<T>fromPublisher(publisher);
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
        public Source<String, NotUsed> names() {
            // A new subscriber will subscribe to the supplied publisher for each
            // materialization, so depending on whether the database client supports
            // this the Source can be materialized more than once.
            return JavaFlowSupport.Source.<Row>fromPublisher(databaseClient.fetchRows())
                .map(row -> row.getField("name"));
        }
    }
    // #example
}
