/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class JavaCollectorDocExamples {

  static void example() {
    ActorSystem system = null;

    // #javaCollector
    Source<String, NotUsed> source = Source.from(Arrays.asList("one", "two", "three"));

    Sink<String, CompletionStage<List<String>>> sink =
        StreamConverters.javaCollector(Collectors::toList);

    CompletionStage<List<String>> result = source.runWith(sink, system);
    // #javaCollector
  }
}
