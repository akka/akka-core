/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import java.util.Arrays;

public class FlatMapConcat {
  private static ActorSystem system = null;

  // #flatmap-concat
  // e.g. could be a query to a database
  private Source<String, NotUsed> lookupCustomerEvents(String customerId) {
    return Source.from(Arrays.asList(customerId + "-event-1", customerId + "-event-2"));
  }
  // #flatmap-concat

  void example() {
    // #flatmap-concat
    Source.from(Arrays.asList("customer-1", "customer-2"))
        .flatMapConcat(this::lookupCustomerEvents)
        .runForeach(System.out::println, system);
    // prints - events from each customer consecutively
    // customer-1-event-1
    // customer-1-event-2
    // customer-2-event-1
    // customer-2-event-2
    // #flatmap-concat
  }
}
