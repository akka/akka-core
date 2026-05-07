/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.stream.javadsl.Source;
import java.util.Arrays;

public class Sliding {

  private final ActorSystem<Void> system = null;

  public void slidingExample1() {
    // #sliding-1
    Source<Integer, NotUsed> source = Source.range(1, 4);
    source.sliding(2, 1).runForeach(n -> System.out.println(n), system);
    // prints:
    // [1, 2]
    // [2, 3]
    // [3, 4]
    // #sliding-1
  }

  public void slidingExample2() {
    // #sliding-2
    Source<Integer, NotUsed> source = Source.range(1, 4);
    source.sliding(3, 2).runForeach(n -> System.out.println(n), system);
    // prints:
    // Vector(1, 2, 3)
    // [1, 2, 3]
    // [3, 4] - shorter because stream ended before we got 3 elements
    // #sliding-2
  }

  public void slidingExample3() {
    // #moving-average
    Source<Integer, NotUsed> numbers = Source.from(Arrays.asList(1, 3, 10, 2, 3, 4, 2, 10, 11));
    Source<Float, NotUsed> movingAverage =
        numbers
            .sliding(5, 1)
            .map(window -> ((float) window.stream().mapToInt(i -> i).sum()) / window.size());
    movingAverage.runForeach(n -> System.out.println(n), system);
    // prints
    // 3.8 = average of 1, 3, 10, 2, 3
    // 4.4 = average of 3, 10, 2, 3, 4
    // 4.2 = average of 10, 2, 3, 4, 2
    // 4.2 = average of 2, 3, 4, 2, 10
    // 6.0 = average of 3, 4, 2, 10, 11
    // #moving-average
  }
}
