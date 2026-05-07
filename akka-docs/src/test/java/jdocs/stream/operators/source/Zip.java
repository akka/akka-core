/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import java.util.Arrays;
import java.util.List;

public class Zip {

  void zipNSample() {
    ActorSystem system = null;

    // #zipN-simple
    Source<Object, NotUsed> chars = Source.from(Arrays.asList("a", "b", "c", "e", "f"));
    Source<Object, NotUsed> numbers = Source.from(Arrays.asList(1, 2, 3, 4, 5, 6));
    Source<Object, NotUsed> colors =
        Source.from(Arrays.asList("red", "green", "blue", "yellow", "purple"));

    Source.zipN(Arrays.asList(chars, numbers, colors)).runForeach(System.out::println, system);
    // prints:
    // [a, 1, red]
    // [b, 2, green]
    // [c, 3, blue]
    // [e, 4, yellow]
    // [f, 5, purple]

    // #zipN-simple
  }

  void zipWithNSample() {
    ActorSystem system = null;

    // #zipWithN-simple
    Source<Integer, NotUsed> numbers = Source.from(Arrays.asList(1, 2, 3, 4, 5, 6));
    Source<Integer, NotUsed> otherNumbers = Source.from(Arrays.asList(5, 2, 1, 4, 10, 4));
    Source<Integer, NotUsed> andSomeOtherNumbers = Source.from(Arrays.asList(3, 7, 2, 1, 1));

    Source.zipWithN(
            (List<Integer> seq) -> seq.stream().mapToInt(i -> i).max().getAsInt(),
            Arrays.asList(numbers, otherNumbers, andSomeOtherNumbers))
        .runForeach(System.out::println, system);
    // prints:
    // 5
    // 7
    // 3
    // 4
    // 10

    // #zipWithN-simple
  }

  void zipAllSample() {
    ActorSystem system = null;
    // #zipAll-simple

    Source<Integer, NotUsed> numbers = Source.from(Arrays.asList(1, 2, 3, 4));
    Source<String, NotUsed> letters = Source.from(Arrays.asList("a", "b", "c"));

    numbers.zipAll(letters, -1, "default").runForeach(System.out::println, system);
    // prints:
    // Pair(1,a)
    // Pair(2,b)
    // Pair(3,c)
    // Pair(4,default)
    // #zipAll-simple
  }

  void zipLatestSample() {
    final ActorSystem system = ActorSystem.create("zipLatest-example");
    // #zipLatest-example

    Source<Integer, NotUsed> numbers = Source.from(Arrays.asList(1, 2));
    Source<String, NotUsed> letters = Source.from(Arrays.asList("a", "b"));

    numbers.zipLatest(letters).runForeach(System.out::println, system);
    // this will print
    // Pair(1,a)
    // Pair(2,a)
    // Pair(2,b)

    //
    // NOTE : The output is not always deterministic and also depends on order of elements flowing
    // from the streams.
    // Sometimes the output could also be :
    // Pair(1, a)
    // Pair(1, b)
    // Pair(2, b)
    // #zipLatest-example
  }
}
