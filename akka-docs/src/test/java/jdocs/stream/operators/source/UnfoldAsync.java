/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.source;

import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.japi.Pair;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

interface UnfoldAsync {

  // #unfoldAsync-actor-protocol
  class DataActor {
    interface Command {}

    static final class FetchChunk implements Command {
      public final long offset;
      public final ActorRef<Chunk> replyTo;

      public FetchChunk(long offset, ActorRef<Chunk> replyTo) {
        this.offset = offset;
        this.replyTo = replyTo;
      }
    }

    static final class Chunk {
      public final ByteString bytes;

      public Chunk(ByteString bytes) {
        this.bytes = bytes;
      }
    }
    // #unfoldAsync-actor-protocol
  }

  default void unfoldAsyncSample() {
    ActorSystem<Void> system = null;
    // #unfoldAsync
    ActorRef<DataActor.Command> dataActor = null; // let's say we got it from somewhere

    Duration askTimeout = Duration.ofSeconds(3);
    long startOffset = 0L;
    Source<ByteString, NotUsed> byteSource =
        Source.unfoldAsync(
            startOffset,
            currentOffset -> {
              // ask for next chunk
              CompletionStage<DataActor.Chunk> nextChunkCS =
                  AskPattern.ask(
                      dataActor,
                      (ActorRef<DataActor.Chunk> ref) ->
                          new DataActor.FetchChunk(currentOffset, ref),
                      askTimeout,
                      system.scheduler());

              return nextChunkCS.thenApply(
                  chunk -> {
                    ByteString bytes = chunk.bytes;
                    if (bytes.isEmpty()) return Optional.empty();
                    else return Optional.of(Pair.create(currentOffset + bytes.size(), bytes));
                  });
            });
    // #unfoldAsync
  }
}
