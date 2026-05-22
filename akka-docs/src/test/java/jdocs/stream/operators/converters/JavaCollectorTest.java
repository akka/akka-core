/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.converters;

import static org.junit.Assert.assertEquals;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.testkit.javadsl.TestKit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JavaCollectorTest extends AbstractJavaTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("JavaCollectorTest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void demonstrateJavaCollector()
      throws InterruptedException, ExecutionException, TimeoutException {
    // #javaCollector
    Source<String, NotUsed> source = Source.from(Arrays.asList("one", "two", "three"));

    Sink<String, CompletionStage<List<String>>> sink =
        StreamConverters.javaCollector(Collectors::toList);

    CompletionStage<List<String>> result = source.runWith(sink, system);
    // #javaCollector
    assertEquals(
        Arrays.asList("one", "two", "three"),
        result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }
}
