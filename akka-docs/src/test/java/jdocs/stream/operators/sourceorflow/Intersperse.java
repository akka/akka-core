/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import java.util.Arrays;

public class Intersperse {
  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create();
    // #intersperse
    Source.from(Arrays.asList(1, 2, 3))
        .map(String::valueOf)
        .intersperse("[", ", ", "]")
        .runForeach(System.out::print, system);
    // prints
    // [1, 2, 3]
    // #intersperse
    system.terminate();
  }
}
