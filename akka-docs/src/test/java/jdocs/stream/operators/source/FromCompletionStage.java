/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

// #sourceFromCompletionStage
import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.stream.javadsl.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

// #sourceFromCompletionStage

class FromCompletionStage {

  public static void sourceFromCompletionStage() {
    // Use one ActorSystem per application
    ActorSystem system = null;

    // #sourceFromCompletionStage
    CompletionStage<Integer> stage = CompletableFuture.completedFuture(10);

    Source<Integer, NotUsed> source = Source.completionStage(stage);

    Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(i -> System.out.println(i.toString()));

    source.runWith(sink, system); // 10
    // #sourceFromCompletionStage
  }
}
