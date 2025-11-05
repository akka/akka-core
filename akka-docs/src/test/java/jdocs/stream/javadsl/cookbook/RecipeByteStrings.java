/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import static akka.util.ByteString.emptyByteString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.stage.*;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

public class RecipeByteStrings extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeByteStrings");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  final Source<ByteString, NotUsed> rawBytes =
      Source.from(
          Arrays.asList(
              ByteString.fromArray(new byte[] {1, 2}),
              ByteString.fromArray(new byte[] {3}),
              ByteString.fromArray(new byte[] {4, 5, 6}),
              ByteString.fromArray(new byte[] {7, 8, 9})));

  @Test
  public void chunker() throws Exception {
    new TestKit(system) {
      final int CHUNK_LIMIT = 2;

      // #bytestring-chunker
      class Chunker extends GraphStage<FlowShape<ByteString, ByteString>> {

        private final int chunkSize;

        public Inlet<ByteString> in = Inlet.<ByteString>create("Chunker.in");
        public Outlet<ByteString> out = Outlet.<ByteString>create("Chunker.out");
        private FlowShape<ByteString, ByteString> shape = FlowShape.of(in, out);

        public Chunker(int chunkSize) {
          this.chunkSize = chunkSize;
        }

        @Override
        public FlowShape<ByteString, ByteString> shape() {
          return shape;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) {
          return new GraphStageLogic(shape) {
            private ByteString buffer = emptyByteString();

            {
              setHandler(
                  out,
                  new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                      emitChunk();
                    }
                  });

              setHandler(
                  in,
                  new AbstractInHandler() {

                    @Override
                    public void onPush() throws Exception {
                      ByteString elem = grab(in);
                      buffer = buffer.concat(elem);
                      emitChunk();
                    }

                    @Override
                    public void onUpstreamFinish() throws Exception {
                      if (buffer.isEmpty()) completeStage();
                      else {
                        // There are elements left in buffer, so
                        // we keep accepting downstream pulls and push from buffer until emptied.
                        //
                        // It might be though, that the upstream finished while it was pulled, in
                        // which
                        // case we will not get an onPull from the downstream, because we already
                        // had one.
                        // In that case we need to emit from the buffer.
                        if (isAvailable(out)) emitChunk();
                      }
                    }
                  });
            }

            private void emitChunk() {
              if (buffer.isEmpty()) {
                if (isClosed(in)) completeStage();
                else pull(in);
              } else {
                Tuple2<ByteString, ByteString> split = buffer.splitAt(chunkSize);
                ByteString chunk = split._1();
                buffer = split._2();
                push(out, chunk);
              }
            }
          };
        }
      }
      // #bytestring-chunker

      {
        // #bytestring-chunker2
        Source<ByteString, NotUsed> chunksStream = rawBytes.via(new Chunker(CHUNK_LIMIT));
        // #bytestring-chunker2

        CompletionStage<List<ByteString>> chunksFuture =
            chunksStream.limit(10).runWith(Sink.seq(), system);

        List<ByteString> chunks = chunksFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);

        for (ByteString chunk : chunks) {
          assertTrue(chunk.size() <= 2);
        }

        ByteString sum = emptyByteString();
        for (ByteString chunk : chunks) {
          sum = sum.concat(chunk);
        }
        assertEquals(sum, ByteString.fromArray(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}));
      }
    };
  }

  @Test
  public void limiterShouldWork() throws Exception {
    new TestKit(system) {
      final int SIZE_LIMIT = 9;

      // #bytes-limiter
      class ByteLimiter extends GraphStage<FlowShape<ByteString, ByteString>> {

        final long maximumBytes;

        public Inlet<ByteString> in = Inlet.<ByteString>create("ByteLimiter.in");
        public Outlet<ByteString> out = Outlet.<ByteString>create("ByteLimiter.out");
        private FlowShape<ByteString, ByteString> shape = FlowShape.of(in, out);

        public ByteLimiter(long maximumBytes) {
          this.maximumBytes = maximumBytes;
        }

        @Override
        public FlowShape<ByteString, ByteString> shape() {
          return shape;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) {
          return new GraphStageLogic(shape) {
            private int count = 0;

            {
              setHandler(
                  out,
                  new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                      pull(in);
                    }
                  });
              setHandler(
                  in,
                  new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                      ByteString chunk = grab(in);
                      count += chunk.size();
                      if (count > maximumBytes) {
                        failStage(new IllegalStateException("Too much bytes"));
                      } else {
                        push(out, chunk);
                      }
                    }
                  });
            }
          };
        }
      }
      // #bytes-limiter

      {
        // #bytes-limiter2
        Flow<ByteString, ByteString, NotUsed> limiter =
            Flow.of(ByteString.class).via(new ByteLimiter(SIZE_LIMIT));
        // #bytes-limiter2

        final Source<ByteString, NotUsed> bytes1 =
            Source.from(
                Arrays.asList(
                    ByteString.fromArray(new byte[] {1, 2}),
                    ByteString.fromArray(new byte[] {3}),
                    ByteString.fromArray(new byte[] {4, 5, 6}),
                    ByteString.fromArray(new byte[] {7, 8, 9})));

        final Source<ByteString, NotUsed> bytes2 =
            Source.from(
                Arrays.asList(
                    ByteString.fromArray(new byte[] {1, 2}),
                    ByteString.fromArray(new byte[] {3}),
                    ByteString.fromArray(new byte[] {4, 5, 6}),
                    ByteString.fromArray(new byte[] {7, 8, 9, 10})));

        List<ByteString> got =
            bytes1
                .via(limiter)
                .limit(10)
                .runWith(Sink.seq(), system)
                .toCompletableFuture()
                .get(3, TimeUnit.SECONDS);
        ByteString acc = emptyByteString();
        for (ByteString b : got) {
          acc = acc.concat(b);
        }
        assertEquals(acc, ByteString.fromArray(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}));

        boolean thrown = false;
        try {
          bytes2
              .via(limiter)
              .limit(10)
              .runWith(Sink.seq(), system)
              .toCompletableFuture()
              .get(3, TimeUnit.SECONDS);
        } catch (ExecutionException ex) {
          assertEquals(ex.getCause().getClass(), IllegalStateException.class);
          thrown = true;
        }
        assertTrue("Expected IllegalStateException to be thrown", thrown);
      }
    };
  }

  @Test
  public void compacting() throws Exception {
    new TestKit(system) {
      {
        final Source<ByteString, NotUsed> rawBytes =
            Source.from(
                Arrays.asList(
                    ByteString.fromArray(new byte[] {1, 2}),
                    ByteString.fromArray(new byte[] {3}),
                    ByteString.fromArray(new byte[] {4, 5, 6}),
                    ByteString.fromArray(new byte[] {7, 8, 9})));

        // #compacting-bytestrings
        Source<ByteString, NotUsed> compacted = rawBytes.map(ByteString::compact);
        // #compacting-bytestrings

        List<ByteString> got =
            compacted
                .limit(10)
                .runWith(Sink.seq(), system)
                .toCompletableFuture()
                .get(3, TimeUnit.SECONDS);

        for (ByteString byteString : got) {
          assertTrue(byteString.isCompact());
        }
      }
    };
  }
}
