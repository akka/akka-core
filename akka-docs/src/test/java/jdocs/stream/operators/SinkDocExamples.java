/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators;

import akka.NotUsed;
import akka.actor.ActorSystem;

import akka.stream.javadsl.AsPublisher;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
// #takeLast-operator-example
import akka.japi.Pair;
import org.reactivestreams.Publisher;
// #takeLast-operator-example
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class SinkDocExamples {

  private static final ActorSystem system = ActorSystem.create("SourceFromExample");

  static void reduceExample() {

    // #reduce-operator-example
    Source<Integer, NotUsed> ints = Source.from(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    CompletionStage<Integer> sum = ints.runWith(Sink.reduce((a, b) -> a + b), system);
    sum.thenAccept(System.out::println);
    // 55
    // #reduce-operator-example
  }

  static void seqExample() {
    // #seq-operator-example
    Source<Integer, NotUsed> ints = Source.from(Arrays.asList(1, 2, 3));
    CompletionStage<List<Integer>> result = ints.runWith(Sink.seq(), system);
    result.thenAccept(list -> list.forEach(System.out::println));
    // 1
    // 2
    // 3
    // #seq-operator-example
  }

  static void takeLastExample() {
    // #takeLast-operator-example
    // pair of (Name, GPA)
    List<Pair<String, Double>> sortedStudents =
        Arrays.asList(
            new Pair<>("Benita", 2.1),
            new Pair<>("Adrian", 3.1),
            new Pair<>("Alexis", 4.0),
            new Pair<>("Kendra", 4.2),
            new Pair<>("Jerrie", 4.3),
            new Pair<>("Alison", 4.7));

    Source<Pair<String, Double>, NotUsed> studentSource = Source.from(sortedStudents);

    CompletionStage<List<Pair<String, Double>>> topThree =
        studentSource.runWith(Sink.takeLast(3), system);

    topThree.thenAccept(
        result -> {
          System.out.println("#### Top students ####");
          for (int i = result.size() - 1; i >= 0; i--) {
            Pair<String, Double> s = result.get(i);
            System.out.println("Name: " + s.first() + ", " + "GPA: " + s.second());
          }
        });
    /*
      #### Top students ####
      Name: Alison, GPA: 4.7
      Name: Jerrie, GPA: 4.3
      Name: Kendra, GPA: 4.2
    */
    // #takeLast-operator-example
  }

  static void headExample() {
    // #head-operator-example
    Source<Integer, NotUsed> source = Source.from(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    CompletionStage<Integer> result = source.runWith(Sink.head(), system);
    result.thenAccept(System.out::println);
    // 1
    // #head-operator-example
  }

  static void lastExample() {
    // #last-operator-example
    Source<Integer, NotUsed> source = Source.from(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    CompletionStage<Integer> result = source.runWith(Sink.last(), system);
    result.thenAccept(System.out::println);
    // 10
    // #last-operator-example
  }

  static void lastOptionExample() {
    // #lastOption-operator-example
    Source<Integer, NotUsed> source = Source.empty();
    CompletionStage<Optional<Integer>> result = source.runWith(Sink.lastOption(), system);
    result.thenAccept(System.out::println);
    // Optional.empty
    // #lastOption-operator-example
  }

  static void foldExample() {
    // #fold
    Source<Integer, NotUsed> source = Source.range(1, 100);
    CompletionStage<Integer> sum =
        source.runWith(Sink.fold(0, (res, element) -> res + element), system);
    sum.thenAccept(System.out::println);
    // #fold
  }

  static NotUsed cancelledExample() {
    // #cancelled
    Source<Integer, NotUsed> source = Source.range(1, 5);
    NotUsed sum = source.runWith(Sink.cancelled(), system);
    return sum;
    // #cancelled
  }

  static void headOptionExample() {
    // #headoption
    Source<Integer, NotUsed> source = Source.empty();
    CompletionStage<Optional<Integer>> result = source.runWith(Sink.headOption(), system);
    result.thenAccept(System.out::println);
    // Optional.empty
    // #headoption
  }

  static void ignoreExample() {
    // #ignore
    Source<String, NotUsed> lines = readLinesFromFile();
    Source<UUID, NotUsed> databaseIds = lines.mapAsync(1, line -> saveLineToDatabase(line));
    databaseIds.mapAsync(1, uuid -> writeIdToFile(uuid)).runWith(Sink.ignore(), system);
    // #ignore
  }

  static void asPublisherExample() {
    // #asPublisher
    Source<Integer, NotUsed> source = Source.range(1, 5);

    Publisher<Integer> publisherFalse =
        source.runWith(Sink.asPublisher(AsPublisher.WITHOUT_FANOUT), system);
    CompletionStage<Integer> resultFromFirstSubscriberFalse =
        Source.fromPublisher(publisherFalse)
            .runWith(Sink.fold(0, (acc, element) -> acc + element), system);
    CompletionStage<Integer> resultFromSecondSubscriberFalse =
        Source.fromPublisher(publisherFalse)
            .runWith(Sink.fold(1, (acc, element) -> acc * element), system);

    resultFromFirstSubscriberFalse.thenAccept(System.out::println); // 15
    resultFromSecondSubscriberFalse.thenAccept(
        System.out
            ::println); // No output, because the source was not able to subscribe to the publisher.
    // #asPublisher
  }

  private static Source<String, NotUsed> readLinesFromFile() {
    return Source.empty();
  }

  private static CompletionStage<UUID> saveLineToDatabase(String line) {
    return CompletableFuture.completedFuture(UUID.randomUUID());
  }

  private static CompletionStage<UUID> writeIdToFile(UUID uuid) {
    return CompletableFuture.completedFuture(uuid);
  }
}
