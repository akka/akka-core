/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.sourceorflow;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.pattern.Patterns;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import docs.stream.operators.sourceorflow.CommonMapAsync;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import scala.jdk.javaapi.FutureConverters;

public class MapAsyncs {

  private static final Random random = new Random();

  // dummy objects for our pretend Kafka...
  private Object settings = new Object();
  private Object subscription = new Object();

  // #mapasync-strict-order
  // #mapasync-concurrent
  // #mapasyncunordered

  private final Source<Event, NotUsed> events =
      Consumer.plainSource(settings, subscription).throttle(1, Duration.ofMillis(50));
  // #mapasync-strict-order
  // #mapasync-concurrent
  // #mapasyncunordered

  private final ActorSystem system = ActorSystem.create("mapAsync-operator-examples");

  public MapAsyncs() {}

  // #mapasync-strict-order
  // #mapasync-concurrent
  // #mapasyncunordered

  public CompletionStage<Integer> eventHandler(Event in) throws InterruptedException {
    System.out.println("Processing event number " + in + "...");
    // ...
    // #mapasync-strict-order
    // #mapasync-concurrent
    // #mapasyncunordered
    CompletionStage<Integer> cs;
    if (random.nextInt(5) == 0) {
      cs =
          Patterns.after(
              Duration.ofMillis(500),
              system,
              () -> CompletableFuture.completedFuture(in.sequenceNumber()));
    } else {
      cs = CompletableFuture.completedFuture(in.sequenceNumber());
    }
    return cs.thenApply(
        i -> {
          System.out.println("Completed processing " + i.intValue());
          return i;
        });
    // #mapasync-strict-order
    // #mapasync-concurrent
    // #mapasyncunordered
  }
  // #mapasync-strict-order
  // #mapasync-concurrent
  // #mapasyncunordered

  private void runStrictOrder() {
    // #mapasync-strict-order

    events
        .mapAsync(1, this::eventHandler)
        .map(in -> "`mapSync` emitted event number " + in.intValue())
        .runWith(Sink.foreach(str -> System.out.println(str)), system);
    // #mapasync-strict-order
  }

  private void run() {
    // #mapasync-concurrent

    events
        .mapAsync(10, this::eventHandler)
        .map(in -> "`mapSync` emitted event number " + in.intValue())
        .runWith(Sink.foreach(str -> System.out.println(str)), system);
    // #mapasync-concurrent
  }

  private void runUnordered() {
    // #mapasyncunordered

    events
        .mapAsyncUnordered(10, this::eventHandler)
        .map(in -> "`mapSync` emitted event number " + in.intValue())
        .runWith(Sink.foreach(str -> System.out.println(str)), system);
    // #mapasyncunordered
  }

  private void runPartitioned() {
    // pretending this is an Alpakka Kafka-style CommitterSettings
    String commitSettings = "`mapAsyncPartitioned`";

    // #mapAsyncPartitioned
    Function<EntityEvent, Integer> partitioner =
        (event) -> {
          Integer partition = Integer.valueOf(event.entityId);
          System.out.println("Assigned event " + event + " to partition " + event.entityId);
          return partition;
        };

    Consumer.committableSource(settings, subscription)
        .take(1000)
        .statefulMap(
            () -> Integer.valueOf(0),
            (count, event) -> {
              System.out.println(
                  "Received event " + event + " at offset " + count + " from message broker");
              return Pair.create(count + 1, event);
            },
            (count) -> Optional.empty())
        .mapAsyncPartitioned(
            10, // parallelism
            1, // perPartition
            partitioner,
            (event, partition) -> {
              System.out.println("Processing event " + event + " from partition " + partition);

              // processing result is "partition-sequenceNr"
              // public CompletionStage<String> processEvent(EntityEvent event, Integer partition)
              CompletionStage<String> cs = processEvent(event, partition);

              return cs.thenApply(
                  s -> {
                    System.out.println("Completed processing " + s);
                    return s;
                  });
            })
        // for the purpose of this example, will print every element, prepended with
        // "`mapAsyncPartitioned` emitted "
        .runWith(Committer.sink(commitSettings), system);
    // #mapAsyncPartitioned
  }

  public static void main(String[] args) {
    MapAsyncs mapAsyncs = new MapAsyncs();
    if (args[0].equals("partitioned")) {
      mapAsyncs.runPartitioned();
    } else if (args[0].equals("strict")) {
      mapAsyncs.runStrictOrder();
    } else if (args[0].equals("unordered")) {
      mapAsyncs.runUnordered();
    } else {
      mapAsyncs.run();
    }
  }

  static interface Event {
    public int sequenceNumber();
  }

  static class PlainEvent implements Event {
    public final int _sequenceNumber;

    public PlainEvent(int sequenceNumber) {
      this._sequenceNumber = sequenceNumber;
    }

    public int sequenceNumber() {
      return _sequenceNumber;
    }

    @Override
    public String toString() {
      return "Event(" + _sequenceNumber + ')';
    }
  }

  static class EntityEvent implements Event {
    public final int entityId;
    public final int _sequenceNumber;

    public EntityEvent(int entityId, int sequenceNumber) {
      this.entityId = entityId;
      this._sequenceNumber = sequenceNumber;
    }

    public int sequenceNumber() {
      return _sequenceNumber;
    }

    @Override
    public String toString() {
      return "EntityEvent(" + entityId + ", " + _sequenceNumber + ")";
    }
  }

  // Pretend to be asking an entity from an event... response is sometimes delayed by up to a second
  public CompletionStage<String> processEvent(EntityEvent event, Integer partition) {
    CompletableFuture<String> cf =
        CompletableFuture.completedFuture(partition.toString() + "-" + event.sequenceNumber());

    if (random.nextBoolean()) {
      return Patterns.after(Duration.ofMillis(random.nextInt(1000)), system, () -> cf);
    } else {
      return cf;
    }
  }

  private static class Consumer {
    // almost but not quite, like Alpakka Kafka...
    public static Source<EntityEvent, NotUsed> committableSource(
        Object settings, Object subscription) {
      return CommonMapAsync.consumer()
          .committableSource(settings, subscription)
          .asJava()
          .map(scalaEvent -> new EntityEvent(scalaEvent.entityId(), scalaEvent.sequenceNumber()));
    }

    // likewise ape the Alpakka Kafka API
    public static Source<Event, NotUsed> plainSource(Object settings, Object subscription) {
      return CommonMapAsync.consumer()
          .plainSource(settings, subscription)
          .asJava()
          .map(scalaEvent -> new PlainEvent(scalaEvent.sequenceNumber()));
    }
  }

  private static class Committer {
    public static Sink<String, CompletionStage<Done>> sink(String prependTo) {
      return CommonMapAsync.committer()
          .sink(prependTo)
          .asJava()
          .mapMaterializedValue(FutureConverters::asJava);
    }
  }
}
