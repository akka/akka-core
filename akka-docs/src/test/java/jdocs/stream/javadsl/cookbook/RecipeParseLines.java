/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.FramingTruncation;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecipeParseLines extends RecipeTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeParseLines");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void parseLines() throws Exception {
    final Source<ByteString, NotUsed> rawData =
        Source.from(
            Arrays.asList(
                ByteString.fromString("Hello World"),
                ByteString.fromString("\r"),
                ByteString.fromString("!\r"),
                ByteString.fromString("\nHello Akka!\r\nHello Streams!"),
                ByteString.fromString("\r\n\r\n")));

    // #parse-lines
    final Source<String, NotUsed> lines =
        rawData
            .via(Framing.delimiter(ByteString.fromString("\r\n"), 100, FramingTruncation.ALLOW))
            .map(b -> b.utf8String());
    // #parse-lines
    lines.limit(10).runWith(Sink.seq(), system).toCompletableFuture().get(1, TimeUnit.SECONDS);
  }
}
