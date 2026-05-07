/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.FlowMonitor;
import akka.stream.FlowMonitorState;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** */
public class Monitor {

  // #monitor
  private static <T> void printMonitorState(FlowMonitorState.StreamState<T> state) {
    if (state == FlowMonitorState.finished()) {
      System.out.println("Stream is initialized but hasn't processed any element");
    } else if (state instanceof FlowMonitorState.Received) {
      FlowMonitorState.Received msg = (FlowMonitorState.Received) state;
      System.out.println("Last message received: " + msg.msg());
    } else if (state instanceof FlowMonitorState.Failed) {
      Throwable cause = ((FlowMonitorState.Failed) state).cause();
      System.out.println("Stream failed with cause: " + cause.getMessage());
    } else {
      System.out.println("Stream completed already");
    }
  }
  // #monitor

  public static void main(String[] args)
      throws InterruptedException, TimeoutException, ExecutionException {
    ActorSystem actorSystem = ActorSystem.create("25fps-stream");

    // #monitor
    Source<Integer, FlowMonitor<Integer>> monitoredSource =
        Source.fromIterator(() -> Arrays.asList(0, 1, 2, 3, 4, 5).iterator())
            .throttle(5, Duration.ofSeconds(1))
            .monitorMat(Keep.right());

    Pair<FlowMonitor<Integer>, CompletionStage<Done>> run =
        monitoredSource.toMat(Sink.foreach(System.out::println), Keep.both()).run(actorSystem);

    FlowMonitor<Integer> monitor = run.first();

    // If we peek in the monitor too early, it's possible it was not initialized yet.
    printMonitorState(monitor.state());

    // Periodically check the monitor
    Source.tick(Duration.ofMillis(200), Duration.ofMillis(400), "")
        .runForeach(__ -> printMonitorState(monitor.state()), actorSystem);
    // #monitor

    run.second().toCompletableFuture().whenComplete((x, t) -> actorSystem.terminate());
  }
}
