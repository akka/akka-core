/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.LogMarker;
import akka.japi.Pair;
import akka.japi.function.Function2;
import akka.japi.pf.PFBuilder;
import akka.stream.Attributes;
import akka.stream.javadsl.Flow;

// #zip
// #zip-with
// #zip-with-index
// #or-else
// #prepend
// #prependLazy
// #concat
// #concatLazy
// #concatAllLazy
// #interleave
// #interleaveAll
// #merge
// #merge-sorted
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Sink;

import java.util.*;

// #merge-sorted
// #merge
// #interleave
// #interleaveAll
// #concat
// #concatLazy
// #concatAllLazy
// #prepend
// #prependLazy
// #or-else
// #zip-with-index
// #zip-with
// #zip

// #log
import akka.event.LogMarker;
import akka.stream.Attributes;

// #log

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

class SourceOrFlow {
  private static ActorSystem system = null;

  void logExample() {
    Flow.of(String.class)
        // #log
        .log("myStream")
        .addAttributes(
            Attributes.createLogLevels(
                Attributes.logLevelOff(), // onElement
                Attributes.logLevelInfo(), // onFinish
                Attributes.logLevelError())) // onFailure
    // #log
    ;
  }

  void logWithMarkerExample() {
    Flow.of(String.class)
        // #logWithMarker
        .logWithMarker(
            "myStream", (e) -> LogMarker.create("myMarker", Collections.singletonMap("element", e)))
        .addAttributes(
            Attributes.createLogLevels(
                Attributes.logLevelOff(), // onElement
                Attributes.logLevelInfo(), // onFinish
                Attributes.logLevelError())) // onFailure
    // #logWithMarker
    ;
  }

  void zipWithIndexExample() {
    // #zip-with-index
    Source.from(Arrays.asList("apple", "orange", "banana"))
        .zipWithIndex()
        .runForeach(System.out::println, system);
    // this will print ('apple', 0), ('orange', 1), ('banana', 2)
    // #zip-with-index
  }

  void zipExample() {
    // #zip
    Source<String, NotUsed> sourceFruits = Source.from(Arrays.asList("apple", "orange", "banana"));
    Source<String, NotUsed> sourceFirstLetters = Source.from(Arrays.asList("A", "O", "B"));
    sourceFruits.zip(sourceFirstLetters).runForeach(System.out::println, system);
    // this will print ('apple', 'A'), ('orange', 'O'), ('banana', 'B')

    // #zip
  }

  void zipWithExample() {
    // #zip-with
    Source<String, NotUsed> sourceCount = Source.from(Arrays.asList("one", "two", "three"));
    Source<String, NotUsed> sourceFruits = Source.from(Arrays.asList("apple", "orange", "banana"));
    sourceCount
        .zipWith(
            sourceFruits,
            (Function2<String, String, String>) (countStr, fruitName) -> countStr + " " + fruitName)
        .runForeach(System.out::println, system);
    // this will print 'one apple', 'two orange', 'three banana'

    // #zip-with
  }

  void prependExample() {
    // #prepend
    Source<String, NotUsed> ladies = Source.from(Arrays.asList("Emma", "Emily"));
    Source<String, NotUsed> gentlemen = Source.from(Arrays.asList("Liam", "William"));
    gentlemen.prepend(ladies).runForeach(System.out::println, system);
    // this will print "Emma", "Emily", "Liam", "William"

    // #prepend
  }

  void prependLazyExample() {
    // #prepend
    Source<String, NotUsed> ladies = Source.from(Arrays.asList("Emma", "Emily"));
    Source<String, NotUsed> gentlemen = Source.from(Arrays.asList("Liam", "William"));
    gentlemen.prependLazy(ladies).runForeach(System.out::println, system);
    // this will print "Emma", "Emily", "Liam", "William"

    // #prepend
  }

  void concatExample() {
    // #concat
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3, 4));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(10, 20, 30, 40));
    sourceA.concat(sourceB).runForeach(System.out::println, system);
    // prints 1, 2, 3, 4, 10, 20, 30, 40

    // #concat
  }

  void concatLazyExample() {
    // #concatLazy
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3, 4));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(10, 20, 30, 40));
    sourceA.concatLazy(sourceB).runForeach(System.out::println, system);
    // prints 1, 2, 3, 4, 10, 20, 30, 40

    // #concatLazy
  }

  void concatAllLazyExample() {
    // #concatAllLazy
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(4, 5, 6));
    Source<Integer, NotUsed> sourceC = Source.from(Arrays.asList(7, 8, 9));
    sourceA
        .concatAllLazy(sourceB, sourceC)
        .fold(new StringJoiner(","), (joiner, input) -> joiner.add(String.valueOf(input)))
        .runForeach(System.out::println, system);
    // prints 1,2,3,4,5,6,7,8,9
    // #concatAllLazy
  }

  void interleaveExample() {
    // #interleave
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3, 4));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(10, 20, 30, 40));
    sourceA.interleave(sourceB, 2).runForeach(System.out::println, system);
    // prints 1, 2, 10, 20, 3, 4, 30, 40

    // #interleave
  }

  void interleaveAllExample() {
    // #interleaveAll
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 7, 8));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(3, 4, 9));
    Source<Integer, NotUsed> sourceC = Source.from(Arrays.asList(5, 6));
    sourceA
        .interleaveAll(Arrays.asList(sourceB, sourceC), 2, false)
        .fold(new StringJoiner(","), (joiner, input) -> joiner.add(String.valueOf(input)))
        .runForeach(System.out::println, system);
    // prints 1,2,3,4,5,6,7,8,9
    // #interleaveAll
  }

  void mergeExample() {
    // #merge
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3, 4));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(10, 20, 30, 40));
    sourceA.merge(sourceB).runForeach(System.out::println, system);
    // merging is not deterministic, can for example print 1, 2, 3, 4, 10, 20, 30, 40

    // #merge
  }

  void mergeAllExample() {
    // #merge-all
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(4, 5, 6));
    Source<Integer, NotUsed> sourceC = Source.from(Arrays.asList(7, 8, 9, 10));
    sourceA
        .mergeAll(Arrays.asList(sourceB, sourceC), false)
        .runForeach(System.out::println, system);
    // merging is not deterministic, can for example print 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    // #merge-all
  }

  void mergePreferredExample() {
    // #mergePreferred
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3, 4));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(10, 20, 30, 40));

    sourceA.mergePreferred(sourceB, false, false).runForeach(System.out::println, system);
    // prints 1, 10, ... since both sources have their first element ready and the left source is
    // preferred

    sourceA.mergePreferred(sourceB, true, false).runForeach(System.out::println, system);
    // prints 10, 1, ... since both sources have their first element ready and the right source is
    // preferred
    // #mergePreferred
  }

  void mergePrioritizedExample() {
    // #mergePrioritized
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3, 4));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(10, 20, 30, 40));

    sourceA.mergePrioritized(sourceB, 99, 1, false).runForeach(System.out::println, system);
    // prints e.g. 1, 10, 2, 3, 4, 20, 30, 40 since both sources have their first element ready and
    // the left source has higher priority – if both sources have elements ready, sourceA has a
    // 99% chance of being picked next while sourceB has a 1% chance
    // #mergePrioritized
  }

  void mergePrioritizedNExample() {
    // #mergePrioritizedN
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3, 4));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(10, 20, 30, 40));
    Source<Integer, NotUsed> sourceC = Source.from(Arrays.asList(100, 200, 300, 400));
    List<Pair<Source<Integer, ?>, Integer>> sourcesAndPriorities =
        Arrays.asList(new Pair<>(sourceA, 9900), new Pair<>(sourceB, 99), new Pair<>(sourceC, 1));
    Source.mergePrioritizedN(sourcesAndPriorities, false).runForeach(System.out::println, system);
    // prints e.g. 1, 100, 2, 3, 4, 10, 20, 30, 40, 200, 300, 400  since both sources have their
    // first element ready and
    // the left sourceA has higher priority - if both sources have elements ready, sourceA has a 99%
    // chance of being picked next
    // while sourceB has a 0.99% chance and sourceC has a 0.01% chance
    // #mergePrioritizedN
  }

  void mergeSortedExample() {
    // #merge-sorted
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 3, 5, 7));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(2, 4, 6, 8));
    sourceA
        .mergeSorted(sourceB, Comparator.<Integer>naturalOrder())
        .runForeach(System.out::println, system);
    // prints 1, 2, 3, 4, 5, 6, 7, 8

    Source<Integer, NotUsed> sourceC = Source.from(Arrays.asList(20, 1, 1, 1));
    sourceA
        .mergeSorted(sourceC, Comparator.<Integer>naturalOrder())
        .runForeach(System.out::println, system);
    // prints 1, 3, 5, 7, 20, 1, 1, 1
    // #merge-sorted
  }

  void orElseExample() {
    // #or-else
    Source<String, NotUsed> source1 = Source.from(Arrays.asList("First source"));
    Source<String, NotUsed> source2 = Source.from(Arrays.asList("Second source"));
    Source<String, NotUsed> emptySource = Source.empty();

    source1.orElse(source2).runForeach(System.out::println, system);
    // this will print "First source"

    emptySource.orElse(source2).runForeach(System.out::println, system);
    // this will print "Second source"

    // #or-else
  }

  void conflateExample() {
    // #conflate
    Source.cycle(() -> Arrays.asList(1, 10, 100).iterator())
        .throttle(10, Duration.ofSeconds(1)) // fast upstream
        .conflate((Integer acc, Integer el) -> acc + el)
        .throttle(1, Duration.ofSeconds(1)); // slow downstream
    // #conflate
  }

  void scanExample() {
    // #scan
    Source<Integer, NotUsed> source = Source.range(1, 5);
    source.scan(0, (acc, x) -> acc + x).runForeach(System.out::println, system);
    // 0  (= 0)
    // 1  (= 0 + 1)
    // 3  (= 0 + 1 + 2)
    // 6  (= 0 + 1 + 2 + 3)
    // 10 (= 0 + 1 + 2 + 3 + 4)
    // 15 (= 0 + 1 + 2 + 3 + 4 + 5)
    // #scan
  }

  // #scan-async
  CompletionStage<Integer> asyncFunction(int acc, int next) {
    return CompletableFuture.supplyAsync(() -> acc + next);
  }
  // #scan-async

  void scanAsyncExample() {
    // #scan-async
    Source<Integer, NotUsed> source = Source.range(1, 5);
    source.scanAsync(0, (acc, x) -> asyncFunction(acc, x)).runForeach(System.out::println, system);
    // 0  (= 0)
    // 1  (= 0 + 1)
    // 3  (= 0 + 1 + 2)
    // 6  (= 0 + 1 + 2 + 3)
    // 10 (= 0 + 1 + 2 + 3 + 4)
    // 15 (= 0 + 1 + 2 + 3 + 4 + 5)
    // #scan-async
  }

  static // #conflateWithSeed-type
  class Summed {

    private final Integer el;

    public Summed(Integer el) {
      this.el = el;
    }

    public Summed sum(Summed other) {
      return new Summed(this.el + other.el);
    }
  }
  // #conflateWithSeed-type

  void conflateWithSeedExample() {
    // #conflateWithSeed

    Source.cycle(() -> Arrays.asList(1, 10, 100).iterator())
        .throttle(10, Duration.ofSeconds(1)) // fast upstream
        .conflateWithSeed(Summed::new, (Summed acc, Integer el) -> acc.sum(new Summed(el)))
        .throttle(1, Duration.ofSeconds(1)); // slow downstream
    // #conflateWithSeed
  }

  // #collect-elements
  static interface Message {}

  static class Ping implements Message {
    final int id;

    Ping(int id) {
      this.id = id;
    }
  }

  static class Pong {
    final int id;

    Pong(int id) {
      this.id = id;
    }
  }
  // #collect-elements

  void collectExample() {
    // #collect
    Flow<Message, Pong, NotUsed> flow =
        Flow.of(Message.class)
            .collect(
                new PFBuilder<Message, Pong>()
                    .match(Ping.class, p -> p.id != 0, p -> new Pong(p.id))
                    .build());
    // #collect
  }

  void collectTypeExample() {
    // #collectType
    Flow<Message, Pong, NotUsed> flow =
        Flow.of(Message.class)
            .collectType(Ping.class)
            .filter(p -> p.id != 0)
            .map(p -> new Pong(p.id));
    // #collectType
  }

  void groupedExample() {
    // #grouped
    Source.from(Arrays.asList(1, 2, 3, 4, 5, 6, 7))
        .grouped(3)
        .runForeach(System.out::println, system);
    // [1, 2, 3]
    // [4, 5, 6]
    // [7]

    Source.from(Arrays.asList(1, 2, 3, 4, 5, 6, 7))
        .grouped(3)
        .map(g -> g.stream().reduce(0, Integer::sum))
        .runForeach(System.out::println, system);
    // 6   (= 1 + 2 + 3)
    // 15  (= 4 + 5 + 6)
    // 7   (= 7)
    // #grouped
  }

  void groupedWeightedExample() {
    // #groupedWeighted
    Source.from(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4), Arrays.asList(5, 6)))
        .groupedWeighted(4, x -> (long) x.size())
        .runForeach(System.out::println, system);
    // [[1, 2], [3, 4]]
    // [[5, 6]]

    Source.from(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4), Arrays.asList(5, 6)))
        .groupedWeighted(3, x -> (long) x.size())
        .runForeach(System.out::println, system);
    // [[1, 2], [3, 4]]
    // [[5, 6]]
    // #groupedWeighted
  }

  static
  // #fold // #foldAsync
  class Histogram {
    final long low;
    final long high;

    private Histogram(long low, long high) {
      this.low = low;
      this.high = high;
    }

    // Immutable start value
    public static Histogram INSTANCE = new Histogram(0L, 0L);

    // #foldAsync

    public Histogram add(int number) {
      if (number < 100) {
        return new Histogram(low + 1L, high);
      } else {
        return new Histogram(low, high + 1L);
      }
    }
    // #fold

    // #foldAsync
    public CompletionStage<Histogram> addAsync(Integer n) {
      if (n < 100) {
        return CompletableFuture.supplyAsync(() -> new Histogram(low + 1L, high));
      } else {
        return CompletableFuture.supplyAsync(() -> new Histogram(low, high + 1L));
      }
    }
    // #fold
  }
  // #fold // #foldAsync

  void foldExample() {
    // #fold

    // Folding over the numbers from 1 to 150:
    Source.range(1, 150)
        .fold(Histogram.INSTANCE, Histogram::add)
        .runForeach(h -> System.out.println("Histogram(" + h.low + ", " + h.high + ")"), system);

    // Prints: Histogram(99, 51)
    // #fold
  }

  void foldAsyncExample() {
    // #foldAsync

    // Folding over the numbers from 1 to 150:
    Source.range(1, 150)
        .foldAsync(Histogram.INSTANCE, Histogram::addAsync)
        .runForeach(h -> System.out.println("Histogram(" + h.low + ", " + h.high + ")"), system);

    // Prints: Histogram(99, 51)
    // #foldAsync
  }

  void takeExample() {
    // #take
    Source.from(Arrays.asList(1, 2, 3, 4, 5)).take(3).runForeach(System.out::println, system);
    // this will print:
    // 1
    // 2
    // 3
    // #take
  }

  void takeWhileExample() {
    // #take-while
    Source.from(Arrays.asList(1, 2, 3, 4, 5))
        .takeWhile(i -> i < 3)
        .runForeach(System.out::println, system);
    // this will print:
    // 1
    // 2
    // #take-while
  }

  void filterExample() {
    // #filter
    Source<String, NotUsed> words =
        Source.from(
            Arrays.asList(
                ("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt "
                        + "ut labore et dolore magna aliqua")
                    .split(" ")));

    Source<String, NotUsed> longWords = words.filter(w -> w.length() > 6);

    longWords.runForeach(System.out::println, system);
    // consectetur
    // adipiscing
    // eiusmod
    // incididunt
    // #filter
  }

  void filterNotExample() {
    // #filterNot
    Source<String, NotUsed> words =
        Source.from(
            Arrays.asList(
                ("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt "
                        + "ut labore et dolore magna aliqua")
                    .split(" ")));

    Source<String, NotUsed> longWords = words.filterNot(w -> w.length() <= 6);

    longWords.runForeach(System.out::println, system);
    // consectetur
    // adipiscing
    // eiusmod
    // incididunt
    // #filterNot
  }

  void dropExample() {
    // #drop
    Source<Integer, NotUsed> fiveIntegers = Source.from(Arrays.asList(1, 2, 3, 4, 5));
    Source<Integer, NotUsed> droppedThreeInts = fiveIntegers.drop(3);

    droppedThreeInts.runForeach(System.out::println, system);
    // 4
    // 5
    // #drop
  }

  void dropWhileExample() {
    // #dropWhile
    Source<Integer, NotUsed> droppedWhileNegative =
        Source.from(Arrays.asList(-3, -2, -1, 0, 1, 2, 3, -1)).dropWhile(integer -> integer < 0);

    droppedWhileNegative.runForeach(System.out::println, system);
    // 1
    // 2
    // 3
    // -1
    // #dropWhile
  }

  static void reduceExample() {
    // #reduceExample
    Source<Integer, NotUsed> source = Source.range(1, 100).reduce((acc, element) -> acc + element);
    CompletionStage<Integer> result = source.runWith(Sink.head(), system);
    result.thenAccept(System.out::println);
    // 5050
    // #reduceExample
  }

  void watchExample() {
    // #watch
    final ActorRef ref = someActor();
    Flow<String, String, NotUsed> flow =
        Flow.of(String.class)
            .watch(ref)
            .recover(akka.stream.WatchedActorTerminatedException.class, () -> ref + " terminated");
    // #watch
  }

  void groupByExample() {
    // #groupBy
    Source.range(1, 10)
        .groupBy(2, i -> i % 2 == 0) // create two sub-streams with odd and even numbers
        .reduce(Integer::sum) // for each sub-stream, sum its elements
        .mergeSubstreams() // merge back into a stream
        .runForeach(System.out::println, system);
    // 25
    // 30
    // #groupBy
  }

  void groupByWithAsyncExample() {
    // #groupByWithAsync
    Source.range(1, 10)
            .groupBy(2, i -> i % 2 == 0) // create two sub-streams with odd and even numbers
            .via(Flow.of(Integer.class).map(elem-> 1).reduce(Integer::sum).async()) // for each sub-stream, sum its elements
            .mergeSubstreams() // merge back into a stream
            .runForeach(System.out::println, system);
    // 25
    // 30
    // #groupByWithAsync
  }

  void watchTerminationExample() {
    // #watchTermination
    Source.range(1, 5)
        .watchTermination(
            (prevMatValue, completionStage) -> {
              completionStage.whenComplete(
                  (done, exc) -> {
                    if (done != null)
                      System.out.println("The stream materialized " + prevMatValue.toString());
                    else System.out.println(exc.getMessage());
                  });
              return prevMatValue;
            })
        .runForeach(System.out::println, system);

    /*
    Prints:
    1
    2
    3
    4
    5
    The stream materialized NotUsed
     */

    Source.range(1, 5)
        .watchTermination(
            (prevMatValue, completionStage) -> {
              // this function will be run when the stream terminates
              // the CompletionStage provided as a second parameter indicates whether
              // the stream completed successfully or failed
              completionStage.whenComplete(
                  (done, exc) -> {
                    if (done != null)
                      System.out.println("The stream materialized " + prevMatValue.toString());
                    else System.out.println(exc.getMessage());
                  });
              return prevMatValue;
            })
        .runForeach(
            element -> {
              if (element == 3) throw new Exception("Boom");
              else System.out.println(element);
            },
            system);
    /*
    Prints:
    1
    2
    Boom
     */
    // #watchTermination
  }

  static CompletionStage<Done> completionTimeoutExample() {
    // #completionTimeout
    Source<Integer, NotUsed> source = Source.range(1, 100000).map(number -> number * number);
    CompletionStage<Done> result = source.completionTimeout(Duration.ofMillis(10)).run(system);
    return result;
    // #completionTimeout
  }

  private ActorRef someActor() {
    return null;
  }
}
