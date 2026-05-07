/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.util.Arrays;

public class MapError {

  public static void main(String[] args) {

    // #map-error

    final ActorSystem system = ActorSystem.create("mapError-operator-example");
    Source.from(Arrays.asList(-1, 0, 1))
        .map(x -> 1 / x)
        .mapError(
            ArithmeticException.class,
            (ArithmeticException e) ->
                new UnsupportedOperationException("Divide by Zero Operation is not supported."))
        .runWith(Sink.seq(), system)
        .whenComplete(
            (result, exception) -> {
              if (result != null) System.out.println(result.toString());
              else System.out.println(exception.getMessage());
            });

    // prints "Divide by Zero Operation is not supported."
    // #map-error
  }
}
