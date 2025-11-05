/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.SinkQueueWithCancel;
import akka.stream.javadsl.Source;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class Lazy {

  private ActorSystem system = null;

  private Source<String, NotUsed> createExpensiveSource() {
    throw new UnsupportedOperationException("Not implemented in sample");
  }

  void notReallyThatLazy() {
    // #not-a-good-example
    Source<String, CompletionStage<NotUsed>> source =
        Source.lazySource(
            () -> {
              System.out.println("Creating the actual source");
              return createExpensiveSource();
            });

    SinkQueueWithCancel<String> queue = source.runWith(Sink.queue(), system);

    // ... time passes ...
    // at some point in time we pull the first time
    // but the source creation may already have been triggered
    queue.pull();
    // #not-a-good-example
  }

  static class IteratorLikeThing {
    boolean thereAreMore() {
      throw new UnsupportedOperationException("Not implemented in sample");
    }

    String extractNext() {
      throw new UnsupportedOperationException("Not implemented in sample");
    }
  }

  void safeMutableSource() {
    // #one-per-materialization
    RunnableGraph<CompletionStage<NotUsed>> stream =
        Source.lazySource(
                () -> {
                  IteratorLikeThing instance = new IteratorLikeThing();
                  return Source.unfold(
                      instance,
                      sameInstance -> {
                        if (sameInstance.thereAreMore())
                          return Optional.of(Pair.create(sameInstance, sameInstance.extractNext()));
                        else return Optional.empty();
                      });
                })
            .to(Sink.foreach(System.out::println));

    // each of the three materializations will have their own instance of IteratorLikeThing
    stream.run(system);
    stream.run(system);
    stream.run(system);
    // #one-per-materialization
  }
}
