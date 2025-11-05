/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import static junit.framework.TestCase.assertTrue;

import akka.NotUsed;
import akka.actor.*;
import akka.pattern.Patterns;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecipeGlobalRateLimit extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeGlobalRateLimit");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  public
  // #global-limiter-actor
  static class Limiter extends AbstractActor {

    public static class WantToPass {}

    public static final WantToPass WANT_TO_PASS = new WantToPass();

    public static class MayPass {}

    public static final MayPass MAY_PASS = new MayPass();

    public static class ReplenishTokens {}

    public static final ReplenishTokens REPLENISH_TOKENS = new ReplenishTokens();

    private final int maxAvailableTokens;
    private final Duration tokenRefreshPeriod;
    private final int tokenRefreshAmount;

    private final List<ActorRef> waitQueue = new ArrayList<>();
    private final Cancellable replenishTimer;

    private int permitTokens;

    public static Props props(
        int maxAvailableTokens, Duration tokenRefreshPeriod, int tokenRefreshAmount) {
      return Props.create(
          Limiter.class, maxAvailableTokens, tokenRefreshPeriod, tokenRefreshAmount);
    }

    private Limiter(int maxAvailableTokens, Duration tokenRefreshPeriod, int tokenRefreshAmount) {
      this.maxAvailableTokens = maxAvailableTokens;
      this.tokenRefreshPeriod = tokenRefreshPeriod;
      this.tokenRefreshAmount = tokenRefreshAmount;
      this.permitTokens = maxAvailableTokens;

      this.replenishTimer =
          system
              .scheduler()
              .scheduleWithFixedDelay(
                  this.tokenRefreshPeriod,
                  this.tokenRefreshPeriod,
                  getSelf(),
                  REPLENISH_TOKENS,
                  getContext().getSystem().dispatcher(),
                  getSelf());
    }

    @Override
    public Receive createReceive() {
      return open();
    }

    private Receive open() {
      return receiveBuilder()
          .match(
              ReplenishTokens.class,
              rt -> {
                permitTokens = Math.min(permitTokens + tokenRefreshAmount, maxAvailableTokens);
              })
          .match(
              WantToPass.class,
              wtp -> {
                permitTokens -= 1;
                getSender().tell(MAY_PASS, getSelf());
                if (permitTokens == 0) {
                  getContext().become(closed());
                }
              })
          .build();
    }

    private Receive closed() {
      return receiveBuilder()
          .match(
              ReplenishTokens.class,
              rt -> {
                permitTokens = Math.min(permitTokens + tokenRefreshAmount, maxAvailableTokens);
                releaseWaiting();
              })
          .match(
              WantToPass.class,
              wtp -> {
                waitQueue.add(getSender());
              })
          .build();
    }

    private void releaseWaiting() {
      final List<ActorRef> toBeReleased = new ArrayList<>(permitTokens);
      for (Iterator<ActorRef> it = waitQueue.iterator(); permitTokens > 0 && it.hasNext(); ) {
        toBeReleased.add(it.next());
        it.remove();
        permitTokens--;
      }

      toBeReleased.stream().forEach(ref -> ref.tell(MAY_PASS, getSelf()));
      if (permitTokens > 0) {
        getContext().become(open());
      }
    }

    @Override
    public void postStop() {
      replenishTimer.cancel();
      waitQueue.stream()
          .forEach(
              ref -> {
                ref.tell(
                    new Status.Failure(new IllegalStateException("limiter stopped")), getSelf());
              });
    }
  }
  // #global-limiter-actor

  @Test
  public void work() throws Exception {
    new TestKit(system) {
      // #global-limiter-flow
      public <T> Flow<T, T, NotUsed> limitGlobal(ActorRef limiter, Duration maxAllowedWait) {
        final int parallelism = 4;
        final Flow<T, T, NotUsed> f = Flow.create();

        return f.mapAsync(
            parallelism,
            element -> {
              final CompletionStage<Object> limiterTriggerFuture =
                  Patterns.ask(limiter, Limiter.WANT_TO_PASS, maxAllowedWait);
              return limiterTriggerFuture.thenApplyAsync(response -> element, system.dispatcher());
            });
      }
      // #global-limiter-flow

      {
        // Use a large period and emulate the timer by hand instead
        ActorRef limiter = system.actorOf(Limiter.props(2, Duration.ofDays(100), 1), "limiter");

        final Iterator<String> e1 =
            new Iterator<String>() {
              @Override
              public boolean hasNext() {
                return true;
              }

              @Override
              public String next() {
                return "E1";
              }
            };
        final Iterator<String> e2 =
            new Iterator<String>() {
              @Override
              public boolean hasNext() {
                return true;
              }

              @Override
              public String next() {
                return "E2";
              }
            };

        final Duration twoSeconds = dilated(Duration.ofSeconds(2));

        final Sink<String, TestSubscriber.Probe<String>> sink = TestSink.probe(system);
        final TestSubscriber.Probe<String> probe =
            RunnableGraph.<TestSubscriber.Probe<String>>fromGraph(
                    GraphDSL.create(
                        sink,
                        (builder, s) -> {
                          final int inputPorts = 2;
                          final UniformFanInShape<String, String> merge =
                              builder.add(Merge.create(inputPorts));

                          final SourceShape<String> source1 =
                              builder.add(
                                  Source.<String>fromIterator(() -> e1)
                                      .via(limitGlobal(limiter, twoSeconds)));
                          final SourceShape<String> source2 =
                              builder.add(
                                  Source.<String>fromIterator(() -> e2)
                                      .via(limitGlobal(limiter, twoSeconds)));

                          builder.from(source1).toFanIn(merge);
                          builder.from(source2).toFanIn(merge);
                          builder.from(merge).to(s);
                          return ClosedShape.getInstance();
                        }))
                .run(system);

        probe.expectSubscription().request(1000);

        Duration fiveHundredMillis = Duration.ofMillis(500);

        assertTrue(probe.expectNext().startsWith("E"));
        assertTrue(probe.expectNext().startsWith("E"));
        probe.expectNoMessage(fiveHundredMillis);

        limiter.tell(Limiter.REPLENISH_TOKENS, getTestActor());
        assertTrue(probe.expectNext().startsWith("E"));
        probe.expectNoMessage(fiveHundredMillis);

        final Set<String> resultSet = new HashSet<>();
        for (int i = 0; i < 100; i++) {
          limiter.tell(Limiter.REPLENISH_TOKENS, getTestActor());
          resultSet.add(probe.expectNext());
        }

        assertTrue(resultSet.contains("E1"));
        assertTrue(resultSet.contains("E2"));

        probe.expectError();
      }
    };
  }
}
