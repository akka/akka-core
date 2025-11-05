/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

import akka.NotUsed;
import akka.actor.Cancellable;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class Tick {

  // not really a runnable example, these are just pretend
  private ActorSystem<Void> system = null;
  private ActorRef<MyActor.Command> myActor = null;

  static class MyActor {
    interface Command {}

    static class Query implements Command {
      public final ActorRef<Response> replyTo;

      public Query(ActorRef<Response> replyTo) {
        this.replyTo = replyTo;
      }
    }

    static class Response {
      public final String text;

      public Response(String text) {
        this.text = text;
      }
    }
  }

  void simple() {
    // #simple
    Source.tick(
            Duration.ofSeconds(1), // delay of first tick
            Duration.ofSeconds(1), // delay of subsequent ticks
            "tick" // element emitted each tick
            )
        .runForeach(System.out::println, system);
    // #simple
  }

  void pollSomething() {
    // #poll-actor
    Source<String, Cancellable> periodicActorResponse =
        Source.tick(Duration.ofSeconds(1), Duration.ofSeconds(1), "tick")
            .mapAsync(
                1,
                notUsed -> {
                  CompletionStage<MyActor.Response> response =
                      AskPattern.ask(
                          myActor, MyActor.Query::new, Duration.ofSeconds(3), system.scheduler());
                  return response;
                })
            .map(response -> response.text);
    // #poll-actor

    // #zip-latest
    Flow<Integer, Pair<Integer, String>, NotUsed> zipWithLatestResponse =
        Flow.of(Integer.class).zipLatest(periodicActorResponse);
    // #zip-latest
  }
}
