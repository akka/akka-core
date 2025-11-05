/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.converters;

import akka.NotUsed;
import akka.actor.ActorSystem;
// #import
import akka.japi.function.Creator;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.StreamConverters;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.BaseStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
// #import
import akka.testkit.javadsl.TestKit;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import akka.stream.javadsl.Source;

import static org.junit.Assert.assertEquals;

/** */
public class StreamConvertersToJava extends AbstractJavaTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("StreamConvertersToJava");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void demonstrateConverterToJava8Stream() {
    // #asJavaStream

    Source<Integer, NotUsed> source = Source.range(0, 9).filter(i -> i % 2 == 0);

    Sink<Integer, java.util.stream.Stream<Integer>> sink = StreamConverters.<Integer>asJavaStream();

    Stream<Integer> jStream = source.runWith(sink, system);

    // #asJavaStream
    assertEquals(5, jStream.count());
  }

  @Test
  public void demonstrateCreatingASourceFromJava8Stream()
      throws InterruptedException, ExecutionException, TimeoutException {
    // #fromJavaStream

    Creator<BaseStream<Integer, IntStream>> creator = () -> IntStream.rangeClosed(0, 9);
    Source<Integer, NotUsed> source = StreamConverters.fromJavaStream(creator);

    Sink<Integer, CompletionStage<Integer>> sink = Sink.last();

    CompletionStage<Integer> integerCompletionStage = source.runWith(sink, system);
    // #fromJavaStream
    assertEquals(
        9, integerCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS).intValue());
  }
}
