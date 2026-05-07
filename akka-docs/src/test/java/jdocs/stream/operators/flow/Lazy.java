/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.flow;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class Lazy {
  private ActorSystem system = null;

  void example() {
    // #simple-example
    Source<Integer, NotUsed> numbers =
        Source.unfold(
                0,
                n -> {
                  int next = n + 1;
                  System.out.println("Source producing " + next);
                  return Optional.of(Pair.create(next, next));
                })
            .take(3);

    Flow<Integer, Integer, CompletionStage<NotUsed>> flow =
        Flow.lazyFlow(
            () -> {
              System.out.println("Creating the actual flow");
              return Flow.fromFunction(
                  element -> {
                    System.out.println("Actual flow mapped " + element);
                    return element;
                  });
            });

    numbers.via(flow).run(system);
    // prints:
    // Source producing 1
    // Creating the actual flow
    // Actual flow mapped 1
    // Source producing 2
    // Actual flow mapped 2
    // #simple-example
  }

  void statefulMap() {
    // #mutable-example
    Flow<Integer, List<Integer>, CompletionStage<NotUsed>> mutableFold =
        Flow.lazyFlow(
            () -> {
              List<Integer> zero = new ArrayList<>();

              return Flow.of(Integer.class)
                  .fold(
                      zero,
                      (list, element) -> {
                        list.add(element);
                        return list;
                      });
            });

    RunnableGraph<NotUsed> stream =
        Source.range(1, 3).via(mutableFold).to(Sink.foreach(System.out::println));

    stream.run(system);
    stream.run(system);
    stream.run(system);
    // prints:
    // [1, 2, 3]
    // [1, 2, 3]
    // [1, 2, 3]
    // #mutable-example
  }
}
