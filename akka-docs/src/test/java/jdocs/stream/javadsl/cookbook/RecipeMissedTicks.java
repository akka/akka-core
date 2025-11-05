/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import akka.testkit.TestLatch;
import akka.testkit.javadsl.TestKit;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;

public class RecipeMissedTicks extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeMissedTicks");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void work() throws Exception {
    new TestKit(system) {
      class Tick {}

      final Tick Tick = new Tick();

      {
        final Source<Tick, TestPublisher.Probe<Tick>> tickStream = TestSource.probe(system);
        final Sink<Integer, TestSubscriber.Probe<Integer>> sink = TestSink.probe(system);

        @SuppressWarnings("unused")
        // #missed-ticks
        final Flow<Tick, Integer, NotUsed> missedTicks =
            Flow.of(Tick.class).conflateWithSeed(tick -> 0, (missed, tick) -> missed + 1);
        // #missed-ticks
        final TestLatch latch = new TestLatch(3, system);
        final Flow<Tick, Integer, NotUsed> realMissedTicks =
            Flow.of(Tick.class)
                .conflateWithSeed(
                    tick -> 0,
                    (missed, tick) -> {
                      latch.countDown();
                      return missed + 1;
                    });

        Pair<TestPublisher.Probe<Tick>, TestSubscriber.Probe<Integer>> pubSub =
            tickStream.via(realMissedTicks).toMat(sink, Keep.both()).run(system);
        TestPublisher.Probe<Tick> pub = pubSub.first();
        TestSubscriber.Probe<Integer> sub = pubSub.second();

        pub.sendNext(Tick);
        pub.sendNext(Tick);
        pub.sendNext(Tick);
        pub.sendNext(Tick);

        scala.concurrent.duration.FiniteDuration timeout =
            scala.concurrent.duration.FiniteDuration.create(200, TimeUnit.MILLISECONDS);

        Await.ready(latch, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS));

        sub.request(1);
        sub.expectNext(3);
        sub.request(1);
        sub.expectNoMessage(timeout);

        pub.sendNext(Tick);
        sub.expectNext(0);

        pub.sendComplete();
        sub.request(1);
        sub.expectComplete();
      }
    };
  }
}
