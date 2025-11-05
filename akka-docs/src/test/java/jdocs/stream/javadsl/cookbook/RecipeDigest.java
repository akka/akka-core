/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import static org.junit.Assert.assertEquals;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.stage.*;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecipeDigest extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeDigest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  // #calculating-digest
  class DigestCalculator extends GraphStage<FlowShape<ByteString, ByteString>> {
    private final String algorithm;
    public Inlet<ByteString> in = Inlet.create("DigestCalculator.in");
    public Outlet<ByteString> out = Outlet.create("DigestCalculator.out");
    private FlowShape<ByteString, ByteString> shape = FlowShape.of(in, out);

    public DigestCalculator(String algorithm) {
      this.algorithm = algorithm;
    }

    @Override
    public FlowShape<ByteString, ByteString> shape() {
      return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
      return new GraphStageLogic(shape) {
        final MessageDigest digest;

        {
          try {
            digest = MessageDigest.getInstance(algorithm);
          } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
          }

          setHandler(
              out,
              new AbstractOutHandler() {
                @Override
                public void onPull() {
                  pull(in);
                }
              });

          setHandler(
              in,
              new AbstractInHandler() {
                @Override
                public void onPush() {
                  ByteString chunk = grab(in);
                  digest.update(chunk.toArray());
                  pull(in);
                }

                @Override
                public void onUpstreamFinish() {
                  // If the stream is finished, we need to emit the digest
                  // before completing
                  emit(out, ByteString.fromArray(digest.digest()));
                  completeStage();
                }
              });
        }
      };
    }
  }
  // #calculating-digest

  @Test
  public void work() throws Exception {
    new TestKit(system) {
      {
        Source<ByteString, NotUsed> data = Source.single(ByteString.fromString("abc"));

        // #calculating-digest2
        final Source<ByteString, NotUsed> digest = data.via(new DigestCalculator("SHA-256"));
        // #calculating-digest2

        ByteString got =
            digest.runWith(Sink.head(), system).toCompletableFuture().get(3, TimeUnit.SECONDS);

        assertEquals(
            ByteString.fromInts(
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad),
            got);
      }
    };
  }
}
