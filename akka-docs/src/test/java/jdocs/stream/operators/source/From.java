/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import java.util.Arrays;
import java.util.stream.IntStream;

public class From {

  private ActorSystem system = null;

  void fromIteratorSample() {
    // #from-iterator
    Source.fromIterator(() -> Arrays.asList(1, 2, 3).iterator())
        .runForeach(System.out::println, system);
    // could print
    // 1
    // 2
    // 3
    // #from-iterator
  }

  void fromJavaStreamSample() {
    // #from-javaStream
    Source.fromJavaStream(() -> IntStream.rangeClosed(1, 3))
        .runForeach(System.out::println, system);
    // could print
    // 1
    // 2
    // 3
    // #from-javaStream
  }
}
