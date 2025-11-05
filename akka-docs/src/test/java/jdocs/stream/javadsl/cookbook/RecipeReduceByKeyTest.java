/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.japi.function.Function2;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecipeReduceByKeyTest extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeReduceByKey");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void work() throws Exception {
    new TestKit(system) {
      {
        final Source<String, NotUsed> words =
            Source.from(Arrays.asList("hello", "world", "and", "hello", "akka"));

        // #word-count
        final int MAXIMUM_DISTINCT_WORDS = 1000;

        final Source<Pair<String, Integer>, NotUsed> counts =
            words
                // split the words into separate streams first
                .groupBy(MAXIMUM_DISTINCT_WORDS, i -> i)
                // transform each element to pair with number of words in it
                .map(i -> new Pair<>(i, 1))
                // add counting logic to the streams
                .reduce((left, right) -> new Pair<>(left.first(), left.second() + right.second()))
                // get a stream of word counts
                .mergeSubstreams();
        // #word-count

        final CompletionStage<List<Pair<String, Integer>>> f =
            counts.grouped(10).runWith(Sink.head(), system);
        final Set<Pair<String, Integer>> result =
            f.toCompletableFuture().get(3, TimeUnit.SECONDS).stream().collect(Collectors.toSet());
        final Set<Pair<String, Integer>> expected = new HashSet<>();
        expected.add(new Pair<>("hello", 2));
        expected.add(new Pair<>("world", 1));
        expected.add(new Pair<>("and", 1));
        expected.add(new Pair<>("akka", 1));
        Assert.assertEquals(expected, result);
      }
    };
  }

  // #reduce-by-key-general
  public static <In, K, Out> Flow<In, Pair<K, Out>, NotUsed> reduceByKey(
      int maximumGroupSize,
      Function<In, K> groupKey,
      Function<In, Out> map,
      Function2<Out, Out, Out> reduce) {

    return Flow.<In>create()
        .groupBy(maximumGroupSize, groupKey)
        .map(i -> new Pair<>(groupKey.apply(i), map.apply(i)))
        .reduce(
            (left, right) -> new Pair<>(left.first(), reduce.apply(left.second(), right.second())))
        .mergeSubstreams();
  }
  // #reduce-by-key-general

  @Test
  public void workGeneralised() throws Exception {
    new TestKit(system) {
      {
        final Source<String, NotUsed> words =
            Source.from(Arrays.asList("hello", "world", "and", "hello", "akka"));

        // #reduce-by-key-general2
        final int MAXIMUM_DISTINCT_WORDS = 1000;

        Source<Pair<String, Integer>, NotUsed> counts =
            words.via(
                reduceByKey(
                    MAXIMUM_DISTINCT_WORDS,
                    word -> word,
                    word -> 1,
                    (left, right) -> left + right));

        // #reduce-by-key-general2
        final CompletionStage<List<Pair<String, Integer>>> f =
            counts.grouped(10).runWith(Sink.head(), system);
        final Set<Pair<String, Integer>> result =
            f.toCompletableFuture().get(3, TimeUnit.SECONDS).stream().collect(Collectors.toSet());
        final Set<Pair<String, Integer>> expected = new HashSet<>();
        expected.add(new Pair<>("hello", 2));
        expected.add(new Pair<>("world", 1));
        expected.add(new Pair<>("and", 1));
        expected.add(new Pair<>("akka", 1));
        Assert.assertEquals(expected, result);
      }
    };
  }
}
