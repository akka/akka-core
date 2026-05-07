/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.flow;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class FutureFlow {

  private ActorSystem system = null;

  // #base-on-first-element
  CompletionStage<Flow<Integer, String, NotUsed>> processingFlow(int id) {
    return CompletableFuture.completedFuture(
        Flow.of(Integer.class).map(n -> "id: " + id + " value: " + n));
  }
  // #base-on-first-element

  public void compileOnlyBaseOnFirst() {
    // #base-on-first-element

    Source<String, NotUsed> source =
        Source.range(1, 10)
            .prefixAndTail(1)
            .flatMapConcat(
                (pair) -> {
                  List<Integer> head = pair.first();
                  Source<Integer, NotUsed> tail = pair.second();

                  int id = head.get(0);

                  return tail.via(Flow.completionStageFlow(processingFlow(id)));
                });
    // #base-on-first-element
  }
}
