/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static org.junit.Assert.*;

import akka.Done;
import akka.NotUsed;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.stream.*;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import akka.testkit.javadsl.TestKit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

public class SinkTest extends StreamTest {
  public SinkTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("SinkTest", AkkaSpec.testConf());

  @Test
  public void mustBeAbleToUseFanoutPublisher() throws Exception {
    final Sink<Object, Publisher<Object>> pubSink = Sink.asPublisher(AsPublisher.WITH_FANOUT);
    @SuppressWarnings("unused")
    final Publisher<Object> publisher = Source.from(new ArrayList<>()).runWith(pubSink, system);
  }

  @Test
  public void mustBeAbleToUseFuture() throws Exception {
    final Sink<Integer, CompletionStage<Integer>> futSink = Sink.head();
    final List<Integer> list = Collections.singletonList(1);
    final CompletionStage<Integer> future = Source.from(list).runWith(futSink, system);
    assertEquals(1, future.toCompletableFuture().get(1, TimeUnit.SECONDS).intValue());
  }

  @Test
  public void mustBeAbleToUseFold() throws Exception {
    Sink<Integer, CompletionStage<Integer>> foldSink = Sink.fold(0, (arg1, arg2) -> arg1 + arg2);
    @SuppressWarnings("unused")
    CompletionStage<Integer> integerFuture =
        Source.from(new ArrayList<Integer>()).runWith(foldSink, system);
  }

  @Test
  public void mustBeAbleToUseActorRefSink() throws Exception {
    final TestKit probe = new TestKit(system);
    final Sink<Integer, ?> actorRefSink = Sink.actorRef(probe.getRef(), "done");
    Source.from(Arrays.asList(1, 2, 3)).runWith(actorRefSink, system);
    probe.expectMsgEquals(1);
    probe.expectMsgEquals(2);
    probe.expectMsgEquals(3);
    probe.expectMsgEquals("done");
  }

  @Test
  public void mustBeAbleToUseCollector() throws Exception {
    final List<Integer> list = Arrays.asList(1, 2, 3);
    final Sink<Integer, CompletionStage<List<Integer>>> collectorSink =
        StreamConverters.javaCollector(Collectors::toList);
    CompletionStage<List<Integer>> result = Source.from(list).runWith(collectorSink, system);
    assertEquals(list, result.toCompletableFuture().get(1, TimeUnit.SECONDS));
  }

  @Test
  public void mustBeAbleToUseCollectorOnSink() throws Exception {
    // #collect-to-list
    final List<Integer> list = Arrays.asList(1, 2, 3);
    CompletionStage<List<Integer>> result =
        Source.from(list).runWith(Sink.collect(Collectors.toList()), system);
    // #collect-to-list
    assertEquals(list, result.toCompletableFuture().get(1, TimeUnit.SECONDS));
  }

  @Test
  public void mustBeAbleToCombine() throws Exception {
    final TestKit probe1 = new TestKit(system);
    final TestKit probe2 = new TestKit(system);

    final Sink<Integer, ?> sink1 = Sink.actorRef(probe1.getRef(), "done1");
    final Sink<Integer, ?> sink2 = Sink.actorRef(probe2.getRef(), "done2");

    final Sink<Integer, ?> sink =
        Sink.combine(
            sink1,
            sink2,
            new ArrayList<Sink<Integer, ?>>(),
            new Function<Integer, Graph<UniformFanOutShape<Integer, Integer>, NotUsed>>() {
              public Graph<UniformFanOutShape<Integer, Integer>, NotUsed> apply(Integer elem) {
                return Broadcast.create(elem);
              }
            });

    Source.from(Arrays.asList(0, 1)).runWith(sink, system);

    probe1.expectMsgEquals(0);
    probe2.expectMsgEquals(0);
    probe1.expectMsgEquals(1);
    probe2.expectMsgEquals(1);

    probe1.expectMsgEquals("done1");
    probe2.expectMsgEquals("done2");
  }

  @Test
  public void mustBeAbleToUseCombineMat() {
    final Sink<Integer, TestSubscriber.Probe<Integer>> sink1 = TestSink.create(system);
    final Sink<Integer, TestSubscriber.Probe<Integer>> sink2 = TestSink.create(system);
    final Sink<Integer, Pair<TestSubscriber.Probe<Integer>, TestSubscriber.Probe<Integer>>> sink =
        Sink.combineMat(sink1, sink2, Broadcast::create, Keep.both());

    final Pair<TestSubscriber.Probe<Integer>, TestSubscriber.Probe<Integer>> subscribers =
        Source.from(Arrays.asList(0, 1)).runWith(sink, system);
    final TestSubscriber.Probe<Integer> subscriber1 = subscribers.first();
    final TestSubscriber.Probe<Integer> subscriber2 = subscribers.second();
    final Subscription sub1 = subscriber1.expectSubscription();
    final Subscription sub2 = subscriber2.expectSubscription();
    sub1.request(2);
    sub2.request(2);
    subscriber1.expectNext(0, 1).expectComplete();
    subscriber2.expectNext(0, 1).expectComplete();
  }

  @Test
  public void mustBeAbleToUseCombineMany() throws Exception {
    final Sink<Long, CompletionStage<Long>> firstSink = Sink.head();
    final Sink<Long, CompletionStage<Long>> secondSink = Sink.head();
    final Sink<Long, CompletionStage<Long>> thirdSink = Sink.head();

    final Sink<Long, List<CompletionStage<Long>>> combineSink =
        Sink.combine(Arrays.asList(firstSink, secondSink, thirdSink), Broadcast::create);
    final List<CompletionStage<Long>> results =
        Source.single(1L).toMat(combineSink, Keep.right()).run(system);
    for (CompletionStage<Long> result : results) {
      final long value = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
      assertEquals(1L, value);
    }
  }

  @Test
  public void mustBeAbleToUseContramap() throws Exception {
    List<Integer> out =
        Source.range(0, 2)
            .toMat(Sink.<Integer>seq().contramap(x -> x + 1), Keep.right())
            .run(system)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

    assertEquals(Arrays.asList(1, 2, 3), out);
  }

  @Test
  public void mustBeAbleToUsePreMaterialize() throws Exception {
    Pair<CompletionStage<String>, Sink<String, NotUsed>> pair =
        Sink.<String>head().preMaterialize(system);

    CompletableFuture<String> future = pair.first().toCompletableFuture();
    assertFalse(future.isDone()); // not yet, only once actually source attached

    String element = "element";
    Source.single(element).runWith(pair.second(), system);

    String got = future.get(3, TimeUnit.SECONDS); // should complete nicely
    assertEquals(element, got);
    assertTrue(future.isDone());
  }

  public void mustSuitablyOverrideAttributeHandlingMethods() {
    @SuppressWarnings("unused")
    final Sink<Integer, CompletionStage<Integer>> s =
        Sink.<Integer>head()
            .withAttributes(Attributes.name(""))
            .addAttributes(Attributes.asyncBoundary())
            .named("");
  }

  @Test
  public void mustBeAbleToConvertToJavaInJava() {
    final akka.stream.scaladsl.Sink<Integer, NotUsed> scalaSink =
        akka.stream.scaladsl.Sink.cancelled();
    Sink<Integer, NotUsed> javaSink = scalaSink.asJava();
  }

  @Test
  public void sinkForeachMustBeDocumented()
      throws InterruptedException, ExecutionException, TimeoutException {
    // #foreach
    Sink<Integer, CompletionStage<Done>> printlnSink = Sink.foreach(System.out::println);
    CompletionStage<Done> cs = Source.from(Arrays.asList(1, 2, 3, 4)).runWith(printlnSink, system);
    Done done = cs.toCompletableFuture().get(100, TimeUnit.MILLISECONDS);
    // will print
    // 1
    // 2
    // 3
    // 4
    // #foreach
    assertEquals(Done.done(), done);
  }
}
