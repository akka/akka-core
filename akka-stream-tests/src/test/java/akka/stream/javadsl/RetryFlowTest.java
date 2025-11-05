/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static org.junit.Assert.assertEquals;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.StreamTest;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import java.time.Duration;
import java.util.Optional;
import org.junit.ClassRule;
import org.junit.Test;
import scala.util.Failure;
import scala.util.Success;
import scala.util.Try;

public class RetryFlowTest extends StreamTest {
  public RetryFlowTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("RetryFlowTest", AkkaSpec.testConf());

  public static
  // #withBackoff-signature
  <In, Out, Mat> Flow<In, Out, Mat> withBackoff(
      Duration minBackoff,
      Duration maxBackoff,
      double randomFactor,
      int maxRetries,
      Flow<In, Out, Mat> flow,
      akka.japi.function.Function2<In, Out, Optional<In>> decideRetry)
        // #withBackoff-signature
      {
    return RetryFlow.<In, Out, Mat>withBackoff(
        minBackoff, maxBackoff, randomFactor, maxRetries, flow, decideRetry);
  }

  public static
  // #signature
  <In, InCtx, Out, OutCtx, Mat> FlowWithContext<In, InCtx, Out, OutCtx, Mat> withBackoffAndContext(
      Duration minBackoff,
      Duration maxBackoff,
      double randomFactor,
      int maxRetries,
      FlowWithContext<In, InCtx, Out, OutCtx, Mat> flow,
      akka.japi.function.Function2<Pair<In, InCtx>, Pair<Out, OutCtx>, Optional<Pair<In, InCtx>>>
          decideRetry)
        // #signature
      {
    return RetryFlow.<In, InCtx, Out, OutCtx, Mat>withBackoffAndContext(
        minBackoff, maxBackoff, randomFactor, maxRetries, flow, decideRetry);
  }

  @Test
  public void withBackoffShouldRetry() {
    final Duration minBackoff = Duration.ofMillis(10);
    final Duration maxBackoff = Duration.ofSeconds(5);
    final double randomFactor = 0d;
    final int maxRetries = 3;

    // #withBackoff-demo
    Flow<Integer, Integer, NotUsed> flow = // ...
        // the wrapped flow
        // #withBackoff-demo
        Flow.fromFunction(in -> in / 2);

    // #withBackoff-demo

    Flow<Integer, Integer, NotUsed> retryFlow =
        RetryFlow.withBackoff(
            minBackoff,
            maxBackoff,
            randomFactor,
            maxRetries,
            flow,
            (in, out) -> {
              if (out > 0) return Optional.of(out);
              else return Optional.empty();
            });
    // #withBackoff-demo

    final Pair<TestPublisher.Probe<Integer>, TestSubscriber.Probe<Integer>> probes =
        TestSource.<Integer>probe(system)
            .via(retryFlow)
            .toMat(TestSink.probe(system), Keep.both())
            .run(system);

    final TestPublisher.Probe<Integer> source = probes.first();
    final TestSubscriber.Probe<Integer> sink = probes.second();

    sink.request(4);

    source.sendNext(5);
    assertEquals(0, sink.expectNext().intValue());

    source.sendComplete();
    sink.expectComplete();
  }

  @Test
  public void withBackoffAndContextShouldRetry() {
    final Duration minBackoff = Duration.ofMillis(10);
    final Duration maxBackoff = Duration.ofSeconds(5);
    final double randomFactor = 0d;
    final int maxRetries = 3;

    class SomeContext {}

    // #retry-success
    FlowWithContext<Integer, SomeContext, Integer, SomeContext, NotUsed> flow = // ...
        // the wrapped flow
        // #retry-success
        FlowWithContext.fromPairs(
            Flow.fromFunction(
                in -> {
                  final Integer request = in.first();
                  return Pair.create(request / 2, in.second());
                }));

    // #retry-success

    FlowWithContext<Integer, SomeContext, Integer, SomeContext, NotUsed> retryFlow =
        RetryFlow.withBackoffAndContext(
            minBackoff,
            maxBackoff,
            randomFactor,
            maxRetries,
            flow,
            (in, out) -> {
              Integer value = out.first();
              SomeContext context = out.second();
              if (value > 0) {
                return Optional.of(Pair.create(value, context));
              } else {
                return Optional.empty();
              }
            });
    // #retry-success

    final Pair<TestPublisher.Probe<Integer>, TestSubscriber.Probe<Pair<Integer, SomeContext>>>
        probes =
            TestSource.<Integer>probe(system)
                .map(i -> Pair.create(i, new SomeContext()))
                .via(retryFlow)
                .toMat(TestSink.probe(system), Keep.both())
                .run(system);

    final TestPublisher.Probe<Integer> source = probes.first();
    final TestSubscriber.Probe<Pair<Integer, SomeContext>> sink = probes.second();

    sink.request(4);

    source.sendNext(5);
    assertEquals(0, sink.expectNext().first().intValue());

    source.sendComplete();
    sink.expectComplete();
  }

  @Test
  public void retryFailedResponses() {
    final Duration minBackoff = Duration.ofMillis(10);
    final Duration maxBackoff = Duration.ofSeconds(5);
    final double randomFactor = 0d;
    final int maxRetries = 3;
    final FlowWithContext<Integer, Integer, Try<Integer>, Integer, NotUsed> failEvenValuesFlow =
        FlowWithContext.fromPairs(
            Flow.fromFunction(
                in -> {
                  final Integer request = in.first();
                  if (request % 2 == 0)
                    return Pair.create(Failure.apply(new Error("Failed response")), in.second());
                  else return Pair.create(Success.apply(request), in.second());
                }));

    final FlowWithContext<Integer, Integer, Try<Integer>, Integer, NotUsed> retryFlow =
        RetryFlow.withBackoffAndContext(
            minBackoff,
            maxBackoff,
            randomFactor,
            maxRetries,
            failEvenValuesFlow,
            (in, out) -> {
              if (out.first().isFailure()) {
                return Optional.of(Pair.create(in.first() + 1, out.second()));
              } else {
                return Optional.empty();
              }
            });

    final Pair<TestPublisher.Probe<Integer>, TestSubscriber.Probe<Pair<Try<Integer>, Integer>>>
        probes =
            TestSource.<Integer>probe(system)
                .map(i -> Pair.create(i, i))
                .via(retryFlow)
                .toMat(TestSink.probe(system), Keep.both())
                .run(system);

    final TestPublisher.Probe<Integer> source = probes.first();
    final TestSubscriber.Probe<Pair<Try<Integer>, Integer>> sink = probes.second();

    sink.request(1);
    source.sendNext(8);

    Pair<Try<Integer>, Integer> response = sink.expectNext();
    assertEquals(9, response.first().get().intValue());
    assertEquals(8, response.second().intValue());

    source.sendComplete();
    sink.expectComplete();
  }

  @Test
  public void supportFlowWithContext() {
    final Duration minBackoff = Duration.ofMillis(10);
    final Duration maxBackoff = Duration.ofSeconds(5);
    final double randomFactor = 0d;
    final int maxRetries = 5;
    final FlowWithContext<Integer, Integer, Try<Integer>, Integer, NotUsed> flow =
        Flow.<Integer>create()
            .<Integer, Integer, Integer>asFlowWithContext((el, ctx) -> el, ctx -> ctx)
            .map(
                i -> {
                  if (i > 0) return Failure.apply(new RuntimeException("i is larger than 0"));
                  else return Success.apply(i);
                });

    final Pair<TestPublisher.Probe<Integer>, TestSubscriber.Probe<Pair<Try<Integer>, Integer>>>
        probes =
            TestSource.<Integer>probe(system)
                .asSourceWithContext(ctx -> ctx)
                .via(
                    RetryFlow.withBackoffAndContext(
                        minBackoff,
                        maxBackoff,
                        randomFactor,
                        maxRetries,
                        flow,
                        (in, out) -> {
                          if (out.first().isFailure()) {
                            if (out.second() > 0) {
                              return Optional.of(Pair.create(out.second() / 2, out.second() / 2));
                            }
                          }
                          return Optional.empty();
                        }))
                .toMat(TestSink.probe(system), Keep.both())
                .run(system);

    final TestPublisher.Probe<Integer> source = probes.first();
    final TestSubscriber.Probe<Pair<Try<Integer>, Integer>> sink = probes.second();

    sink.request(1);
    source.sendNext(8);

    Pair<Try<Integer>, Integer> response = sink.expectNext();
    assertEquals(0, response.first().get().intValue());
    assertEquals(0, response.second().intValue());

    source.sendComplete();
    sink.expectComplete();
  }
}
