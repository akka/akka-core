/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import static akka.util.ByteString.emptyByteString;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Compression;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecipeDecompress extends RecipeTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeDecompress");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void parseLines() throws Exception {
    final Source<ByteString, NotUsed> dataStream =
        Source.single(ByteString.fromString("Hello World"));

    final Source<ByteString, NotUsed> compressedStream = dataStream.via(Compression.gzip());

    // #decompress-gzip
    final Source<ByteString, NotUsed> decompressedStream =
        compressedStream.via(Compression.gunzip(100));
    // #decompress-gzip

    ByteString decompressedData =
        decompressedStream
            .runFold(emptyByteString(), ByteString::concat, system)
            .toCompletableFuture()
            .get(1, TimeUnit.SECONDS);
    String decompressedString = decompressedData.utf8String();
    Assert.assertEquals("Hello World", decompressedString);
  }
}
