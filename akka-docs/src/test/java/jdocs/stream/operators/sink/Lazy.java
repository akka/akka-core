/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sink;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class Lazy {

  private ActorSystem system = null;

  void example() {
    // #simple-example
    CompletionStage<Optional<String>> matVal =
        Source.<String>maybe()
            .map(
                element -> {
                  System.out.println("mapped " + element);
                  return element;
                })
            .toMat(
                Sink.lazySink(
                    () -> {
                      System.out.println("Sink created");
                      return Sink.foreach(elem -> System.out.println("foreach " + elem));
                    }),
                Keep.left())
            .run(system);

    // some time passes
    // nothing has been printed
    matVal.toCompletableFuture().complete(Optional.of("one"));
    // now prints:
    // mapped one
    // Sink created
    // foreach one

    // #simple-example
  }
}
