/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecipeSplitter {
  private static ActorSystem system;

  @Test
  public void simpleSplit() throws ExecutionException, InterruptedException {
    // #Simple-Split
    // Sample Source
    Source<String, NotUsed> source = Source.from(Arrays.asList("1-2-3", "2-3", "3-4"));

    CompletionStage<List<Integer>> ret =
        source
            .map(s -> Arrays.asList(s.split("-")))
            .mapConcat(f -> f)
            // Sub-streams logic
            .map(s -> Integer.valueOf(s))
            .runWith(Sink.seq(), system);

    // Verify results
    List<Integer> list = ret.toCompletableFuture().get();
    assert list.equals(Arrays.asList(1, 2, 3, 2, 3, 3, 4));
    // #Simple-Split
  }

  @Test
  public void splitAggregate() throws ExecutionException, InterruptedException {
    // #Aggregate-Split
    // Sample Source
    Source<String, NotUsed> source = Source.from(Arrays.asList("1-2-3", "2-3", "3-4"));

    CompletionStage<List<Integer>> ret =
        source
            .map(s -> Arrays.asList(s.split("-")))
            // split all messages into sub-streams
            .splitWhen(a -> true)
            // now split each collection
            .mapConcat(f -> f)
            // Sub-streams logic
            .map(s -> Integer.valueOf(s))
            // aggregate each sub-stream
            .reduce((a, b) -> a + b)
            // and merge back the result into the original stream
            .mergeSubstreams()
            .runWith(Sink.seq(), system);

    // Verify results
    List<Integer> list = ret.toCompletableFuture().get();
    assert list.equals(Arrays.asList(6, 5, 7));
    // #Aggregate-Split
  }

  @BeforeClass
  public static void setup() throws Exception {
    system = ActorSystem.create();
  }

  @AfterClass
  public static void teardown() throws Exception {
    TestKit.shutdownActorSystem(system);
  }
}
