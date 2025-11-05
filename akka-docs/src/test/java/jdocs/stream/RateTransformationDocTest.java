/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream;

import static org.junit.Assert.assertEquals;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.tuple.Tuple3;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import scala.util.Random;

public class RateTransformationDocTest extends AbstractJavaTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RateTransformationDocTest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  final Random r = new Random();

  @Test
  public void conflateShouldSummarize() throws Exception {
    // #conflate-summarize
    final Flow<Double, Tuple3<Double, Double, Integer>, NotUsed> statsFlow =
        Flow.of(Double.class)
            .conflateWithSeed(
                elem -> Collections.singletonList(elem),
                (acc, elem) -> {
                  return Stream.concat(acc.stream(), Collections.singletonList(elem).stream())
                      .collect(Collectors.toList());
                })
            .map(
                s -> {
                  final Double mean = s.stream().mapToDouble(d -> d).sum() / s.size();
                  final DoubleStream se = s.stream().mapToDouble(x -> Math.pow(x - mean, 2));
                  final Double stdDev = Math.sqrt(se.sum() / s.size());
                  return new Tuple3<>(stdDev, mean, s.size());
                });
    // #conflate-summarize

    final CompletionStage<List<Tuple3<Double, Double, Integer>>> fut =
        Source.repeat(0)
            .map(i -> r.nextGaussian())
            .via(statsFlow)
            .grouped(10)
            .runWith(Sink.head(), system);

    fut.toCompletableFuture().get(1, TimeUnit.SECONDS);
  }

  @Test
  public void conflateShouldSample() throws Exception {
    // #conflate-sample
    final Double p = 0.01;
    final Flow<Double, Double, NotUsed> sampleFlow =
        Flow.of(Double.class)
            .conflateWithSeed(
                elem -> Collections.singletonList(elem),
                (acc, elem) -> {
                  if (r.nextDouble() < p) {
                    return Stream.concat(acc.stream(), Collections.singletonList(elem).stream())
                        .collect(Collectors.toList());
                  }
                  return acc;
                })
            .mapConcat(d -> d);
    // #conflate-sample

    final CompletionStage<Double> fut =
        Source.from(new ArrayList<Double>(Collections.nCopies(1000, 1.0)))
            .via(sampleFlow)
            .runWith(Sink.fold(0.0, (agg, next) -> agg + next), system);

    final Double count = fut.toCompletableFuture().get(1, TimeUnit.SECONDS);
  }

  @Test
  public void extrapolateShouldRepeatLast() throws Exception {
    // #extrapolate-last
    final Flow<Double, Double, NotUsed> lastFlow =
        Flow.of(Double.class).extrapolate(in -> Stream.iterate(in, i -> i).iterator());
    // #extrapolate-last

    final Pair<TestPublisher.Probe<Double>, CompletionStage<List<Double>>> probeFut =
        TestSource.<Double>probe(system)
            .via(lastFlow)
            .grouped(10)
            .toMat(Sink.head(), Keep.both())
            .run(system);

    final TestPublisher.Probe<Double> probe = probeFut.first();
    final CompletionStage<List<Double>> fut = probeFut.second();
    probe.sendNext(1.0);
    final List<Double> extrapolated = fut.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(extrapolated.size(), 10);
    assertEquals(extrapolated.stream().mapToDouble(d -> d).sum(), 10, 0.1);
  }

  @Test
  public void extrapolateShouldSeedFirst() throws Exception {
    // #extrapolate-seed
    Double initial = 2.0;
    final Flow<Double, Double, NotUsed> lastFlow =
        Flow.of(Double.class).extrapolate(in -> Stream.iterate(in, i -> i).iterator(), initial);
    // #extrapolate-seed

    final CompletionStage<List<Double>> fut =
        TestSource.<Double>probe(system)
            .via(lastFlow)
            .grouped(10)
            .toMat(Sink.head(), Keep.right())
            .run(system);

    final List<Double> extrapolated = fut.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(extrapolated.size(), 10);
    assertEquals(extrapolated.stream().mapToDouble(d -> d).sum(), 10 * initial, 0.1);
  }

  @Test
  public void extrapolateShouldTrackDrift() throws Exception {
    @SuppressWarnings("unused")
    // #extrapolate-drift
    final Flow<Double, Pair<Double, Integer>, NotUsed> driftFlow =
        Flow.of(Double.class)
            .map(d -> new Pair<>(d, 0))
            .extrapolate(
                d -> Stream.iterate(1, i -> i + 1).map(i -> new Pair<>(d.first(), i)).iterator());
    // #extrapolate-drift
    final TestLatch latch = new TestLatch(2, system);
    final Flow<Double, Pair<Double, Integer>, NotUsed> realDriftFlow =
        Flow.of(Double.class)
            .map(
                d -> {
                  latch.countDown();
                  return new Pair<>(d, 0);
                })
            .extrapolate(
                d -> {
                  latch.countDown();
                  return Stream.iterate(1, i -> i + 1)
                      .map(i -> new Pair<>(d.first(), i))
                      .iterator();
                });

    final Pair<TestPublisher.Probe<Double>, TestSubscriber.Probe<Pair<Double, Integer>>> pubSub =
        TestSource.<Double>probe(system)
            .via(realDriftFlow)
            .toMat(TestSink.<Pair<Double, Integer>>probe(system), Keep.both())
            .run(system);

    final TestPublisher.Probe<Double> pub = pubSub.first();
    final TestSubscriber.Probe<Pair<Double, Integer>> sub = pubSub.second();

    sub.request(1);
    pub.sendNext(1.0);
    sub.expectNext(new Pair<>(1.0, 0));

    sub.requestNext(new Pair<>(1.0, 1));
    sub.requestNext(new Pair<>(1.0, 2));

    pub.sendNext(2.0);
    Await.ready(latch, Duration.create(1, TimeUnit.SECONDS));
    sub.requestNext(new Pair<>(2.0, 0));
  }

  @Test
  public void expandShouldTrackDrift() throws Exception {
    @SuppressWarnings("unused")
    // #expand-drift
    final Flow<Double, Pair<Double, Integer>, NotUsed> driftFlow =
        Flow.of(Double.class)
            .expand(d -> Stream.iterate(0, i -> i + 1).map(i -> new Pair<>(d, i)).iterator());
    // #expand-drift
    final TestLatch latch = new TestLatch(2, system);
    final Flow<Double, Pair<Double, Integer>, NotUsed> realDriftFlow =
        Flow.of(Double.class)
            .expand(
                d -> {
                  latch.countDown();
                  return Stream.iterate(0, i -> i + 1).map(i -> new Pair<>(d, i)).iterator();
                });

    final Pair<TestPublisher.Probe<Double>, TestSubscriber.Probe<Pair<Double, Integer>>> pubSub =
        TestSource.<Double>probe(system)
            .via(realDriftFlow)
            .toMat(TestSink.<Pair<Double, Integer>>probe(system), Keep.both())
            .run(system);

    final TestPublisher.Probe<Double> pub = pubSub.first();
    final TestSubscriber.Probe<Pair<Double, Integer>> sub = pubSub.second();

    sub.request(1);
    pub.sendNext(1.0);
    sub.expectNext(new Pair<>(1.0, 0));

    sub.requestNext(new Pair<>(1.0, 1));
    sub.requestNext(new Pair<>(1.0, 2));

    pub.sendNext(2.0);
    Await.ready(latch, Duration.create(1, TimeUnit.SECONDS));
    sub.requestNext(new Pair<>(2.0, 0));
  }
}
