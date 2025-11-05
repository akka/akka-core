/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import akka.Done;
import akka.NotUsed;
import akka.actor.*;
import akka.japi.Pair;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.stream.testkit.*;
import akka.stream.testkit.javadsl.*;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import jdocs.AbstractJavaTest;
import org.junit.*;

public class StreamTestKitDocTest extends AbstractJavaTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("StreamTestKitDocTest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void strictCollection() throws Exception {
    // #strict-collection
    final Sink<Integer, CompletionStage<Integer>> sinkUnderTest =
        Flow.of(Integer.class)
            .map(i -> i * 2)
            .toMat(Sink.fold(0, (agg, next) -> agg + next), Keep.right());

    final CompletionStage<Integer> future =
        Source.from(Arrays.asList(1, 2, 3, 4)).runWith(sinkUnderTest, system);
    final Integer result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(20, result.intValue());
    // #strict-collection
  }

  @Test
  public void groupedPartOfInfiniteStream() throws Exception {
    // #grouped-infinite
    final Source<Integer, NotUsed> sourceUnderTest = Source.repeat(1).map(i -> i * 2);

    final CompletionStage<List<Integer>> future =
        sourceUnderTest.take(10).runWith(Sink.seq(), system);
    final List<Integer> result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(Collections.nCopies(10, 2), result);
    // #grouped-infinite
  }

  @Test
  public void foldedStream() throws Exception {
    // #folded-stream
    final Flow<Integer, Integer, NotUsed> flowUnderTest =
        Flow.of(Integer.class).takeWhile(i -> i < 5);

    final CompletionStage<Integer> future =
        Source.from(Arrays.asList(1, 2, 3, 4, 5, 6))
            .via(flowUnderTest)
            .runWith(Sink.fold(0, (agg, next) -> agg + next), system);
    final Integer result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(10, result.intValue());
    // #folded-stream
  }

  @Test
  public void pipeToTestProbe() throws Exception {
    // #pipeto-testprobe
    final Source<List<Integer>, NotUsed> sourceUnderTest =
        Source.from(Arrays.asList(1, 2, 3, 4)).grouped(2);

    final TestKit probe = new TestKit(system);
    final CompletionStage<List<List<Integer>>> future =
        sourceUnderTest.grouped(2).runWith(Sink.head(), system);
    akka.pattern.Patterns.pipe(future, system.dispatcher()).to(probe.getRef());
    probe.expectMsg(Duration.ofSeconds(3), Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)));
    // #pipeto-testprobe
  }

  public enum Tick {
    TOCK,
    COMPLETED
  };

  @Test
  public void sinkActorRef() throws Exception {
    // #sink-actorref
    final Source<Tick, Cancellable> sourceUnderTest =
        Source.tick(Duration.ZERO, Duration.ofMillis(200), Tick.TOCK);

    final TestKit probe = new TestKit(system);
    final Cancellable cancellable =
        sourceUnderTest.to(Sink.actorRef(probe.getRef(), Tick.COMPLETED)).run(system);
    probe.expectMsg(Duration.ofSeconds(3), Tick.TOCK);
    probe.expectNoMessage(Duration.ofMillis(100));
    probe.expectMsg(Duration.ofSeconds(3), Tick.TOCK);
    cancellable.cancel();
    probe.expectMsg(Duration.ofSeconds(3), Tick.COMPLETED);
    // #sink-actorref
  }

  @Test
  public void sourceActorRef() throws Exception {
    // #source-actorref
    final Sink<Integer, CompletionStage<String>> sinkUnderTest =
        Flow.of(Integer.class)
            .map(i -> i.toString())
            .toMat(Sink.fold("", (agg, next) -> agg + next), Keep.right());

    final Pair<ActorRef, CompletionStage<String>> refAndCompletionStage =
        Source.<Integer>actorRef(
                elem -> {
                  // complete stream immediately if we send it Done
                  if (elem == Done.done()) return Optional.of(CompletionStrategy.immediately());
                  else return Optional.empty();
                },
                // never fail the stream because of a message
                elem -> Optional.empty(),
                8,
                OverflowStrategy.fail())
            .toMat(sinkUnderTest, Keep.both())
            .run(system);
    final ActorRef ref = refAndCompletionStage.first();
    final CompletionStage<String> future = refAndCompletionStage.second();

    ref.tell(1, ActorRef.noSender());
    ref.tell(2, ActorRef.noSender());
    ref.tell(3, ActorRef.noSender());
    ref.tell(Done.getInstance(), ActorRef.noSender());

    final String result = future.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals("123", result);
    // #source-actorref
  }

  @Test
  public void testSinkProbe() {
    // #test-sink-probe
    final Source<Integer, NotUsed> sourceUnderTest =
        Source.from(Arrays.asList(1, 2, 3, 4)).filter(elem -> elem % 2 == 0).map(elem -> elem * 2);

    sourceUnderTest
        .runWith(TestSink.probe(system), system)
        .request(2)
        .expectNext(4, 8)
        .expectComplete();
    // #test-sink-probe
  }

  @Test
  public void testSourceProbe() {
    // #test-source-probe
    final Sink<Integer, NotUsed> sinkUnderTest = Sink.cancelled();

    TestSource.<Integer>probe(system)
        .toMat(sinkUnderTest, Keep.left())
        .run(system)
        .expectCancellation();
    // #test-source-probe
  }

  @Test
  public void injectingFailure() throws Exception {
    // #injecting-failure
    final Sink<Integer, CompletionStage<Integer>> sinkUnderTest = Sink.head();

    final Pair<TestPublisher.Probe<Integer>, CompletionStage<Integer>> probeAndCompletionStage =
        TestSource.<Integer>probe(system).toMat(sinkUnderTest, Keep.both()).run(system);
    final TestPublisher.Probe<Integer> probe = probeAndCompletionStage.first();
    final CompletionStage<Integer> future = probeAndCompletionStage.second();
    probe.sendError(new Exception("boom"));

    ExecutionException exception =
        Assert.assertThrows(
            ExecutionException.class, () -> future.toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertEquals("boom", exception.getCause().getMessage());
    // #injecting-failure
  }

  @Test
  public void testSourceAndTestSink() throws Exception {
    // #test-source-and-sink
    final Flow<Integer, Integer, NotUsed> flowUnderTest =
        Flow.of(Integer.class)
            .mapAsyncUnordered(
                2,
                sleep ->
                    akka.pattern.Patterns.after(
                        Duration.ofMillis(10),
                        system.scheduler(),
                        system.dispatcher(),
                        () -> CompletableFuture.completedFuture(sleep)));

    final Pair<TestPublisher.Probe<Integer>, TestSubscriber.Probe<Integer>> pubAndSub =
        TestSource.<Integer>probe(system)
            .via(flowUnderTest)
            .toMat(TestSink.<Integer>probe(system), Keep.both())
            .run(system);
    final TestPublisher.Probe<Integer> pub = pubAndSub.first();
    final TestSubscriber.Probe<Integer> sub = pubAndSub.second();

    sub.request(3);
    pub.sendNext(3);
    pub.sendNext(2);
    pub.sendNext(1);
    sub.expectNextUnordered(1, 2, 3);

    pub.sendError(new Exception("Power surge in the linear subroutine C-47!"));
    final Throwable ex = sub.expectError();
    assertTrue(ex.getMessage().contains("C-47"));
    // #test-source-and-sink
  }
}
