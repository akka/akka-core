/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class Limit {
  public void simple() {
    ActorSystem<?> system = null;
    // #simple
    Source<String, NotUsed> untrustedSource = Source.repeat("element");

    CompletionStage<List<String>> elements =
        untrustedSource.limit(10000).runWith(Sink.seq(), system);
    // #simple
  }
}
