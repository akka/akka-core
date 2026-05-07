/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Status;
import akka.japi.Pair;
import akka.japi.Util;
import akka.japi.function.*;
import akka.japi.pf.PFBuilder;
// #imports
import akka.stream.*;
// #imports
import akka.stream.scaladsl.FlowSpec;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import akka.testkit.javadsl.TestKit;
import akka.util.ConstantFun;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import scala.util.Try;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static akka.NotUsed.notUsed;
import static akka.stream.testkit.StreamTestKit.PublisherProbeSubscription;
import static akka.stream.testkit.TestPublisher.ManualProbe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class SourceTest extends StreamTest {
  public SourceTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("SourceTest", AkkaSpec.testConf());

  interface Fruit {}

  static class Apple implements Fruit {};

  static class Orange implements Fruit {};

  public void compileOnlyUpcast() {
    Source<Apple, NotUsed> apples = null;
    Source<Orange, NotUsed> oranges = null;
    Source<Fruit, NotUsed> appleFruits = Source.upcast(apples);
    Source<Fruit, NotUsed> orangeFruits = Source.upcast(oranges);

    Source<Fruit, NotUsed> fruits = appleFruits.merge(orangeFruits);
  }

  @Test
  public void mustBeAbleToUseSimpleOperators() {
    final TestKit probe = new TestKit(system);
    final String[] lookup = {"a", "b", "c", "d", "e", "f"};
    final java.lang.Iterable<Integer> input = Arrays.asList(0, 1, 2, 3, 4, 5);
    final Source<Integer, NotUsed> ints = Source.from(input);

    ints.drop(2)
        .take(3)
        .takeWithin(Duration.ofSeconds(10))
        .map(elem -> lookup[elem])
        .filter(elem -> !elem.equals("c"))
        .grouped(2)
        .mapConcat(elem -> elem)
        .groupedWithin(100, Duration.ofMillis(50))
        .mapConcat(elem -> elem)
        .runFold("", (acc, elem) -> acc + elem, system)
        .thenAccept(elem -> probe.getRef().tell(elem, ActorRef.noSender()));

    probe.expectMsgEquals("de");
  }

  @Test
  public void mustBeAbleToUseVoidTypeInForeach() {
    final TestKit probe = new TestKit(system);
    final java.lang.Iterable<String> input = Arrays.asList("a", "b", "c");
    Source<String, NotUsed> ints = Source.from(input);

    final CompletionStage<Done> completion =
        ints.runForeach(elem -> probe.getRef().tell(elem, ActorRef.noSender()), system);

    completion.thenAccept(elem -> probe.getRef().tell(String.valueOf(elem), ActorRef.noSender()));

    probe.expectMsgEquals("a");
    probe.expectMsgEquals("b");
    probe.expectMsgEquals("c");
    probe.expectMsgEquals("Done");
  }

  @Test
  public void mustBeAbleToUseVia() {
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> input = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7);
    // duplicate each element, stop after 4 elements, and emit sum to the end
    Source.from(input)
        .via(
            new GraphStage<FlowShape<Integer, Integer>>() {
              public final Inlet<Integer> in = Inlet.create("in");
              public final Outlet<Integer> out = Outlet.create("out");

              @Override
              public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
                return new GraphStageLogic(shape()) {
                  int sum = 0;
                  int count = 0;

                  {
                    setHandler(
                        in,
                        new AbstractInHandler() {
                          @Override
                          public void onPush() {
                            final Integer element = grab(in);
                            sum += element;
                            count += 1;
                            if (count == 4) {
                              emitMultiple(
                                  out,
                                  Arrays.asList(element, element, sum).iterator(),
                                  () -> completeStage());
                            } else {
                              emitMultiple(out, Arrays.asList(element, element).iterator());
                            }
                          }
                        });
                    setHandler(
                        out,
                        new AbstractOutHandler() {
                          @Override
                          public void onPull() throws Exception {
                            pull(in);
                          }
                        });
                  }
                };
              }

              @Override
              public FlowShape<Integer, Integer> shape() {
                return FlowShape.of(in, out);
              }
            })
        .runForeach(
            (Procedure<Integer>) elem -> probe.getRef().tell(elem, ActorRef.noSender()), system);

    probe.expectMsgEquals(0);
    probe.expectMsgEquals(0);
    probe.expectMsgEquals(1);
    probe.expectMsgEquals(1);
    probe.expectMsgEquals(2);
    probe.expectMsgEquals(2);
    probe.expectMsgEquals(3);
    probe.expectMsgEquals(3);
    probe.expectMsgEquals(6);
  }

  @Test
  public void mustBeAbleToUseGroupBy() throws Exception {
    final Iterable<String> input = Arrays.asList("Aaa", "Abb", "Bcc", "Cdd", "Cee");
    final Source<List<String>, NotUsed> source =
        Source.from(input)
            .groupBy(
                3,
                new Function<String, String>() {
                  public String apply(String elem) {
                    return elem.substring(0, 1);
                  }
                })
            .grouped(10)
            .mergeSubstreams();

    final CompletionStage<List<List<String>>> future =
        source.grouped(10).runWith(Sink.head(), system);
    final List<List<String>> result =
        future.toCompletableFuture().get(1, TimeUnit.SECONDS).stream()
            .sorted(Comparator.comparingInt(list -> list.get(0).charAt(0)))
            .collect(Collectors.toList());

    assertEquals(
        Arrays.asList(
            Arrays.asList("Aaa", "Abb"), Arrays.asList("Bcc"), Arrays.asList("Cdd", "Cee")),
        result);
  }

  @Test
  public void mustBeAbleToUseSplitWhen() throws Exception {
    final Iterable<String> input = Arrays.asList("A", "B", "C", ".", "D", ".", "E", "F");
    final Source<List<String>, NotUsed> source =
        Source.from(input)
            .splitWhen(
                new Predicate<String>() {
                  public boolean test(String elem) {
                    return elem.equals(".");
                  }
                })
            .grouped(10)
            .concatSubstreams();

    final CompletionStage<List<List<String>>> future =
        source.grouped(10).runWith(Sink.head(), system);
    final List<List<String>> result = future.toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertEquals(
        Arrays.asList(
            Arrays.asList("A", "B", "C"), Arrays.asList(".", "D"), Arrays.asList(".", "E", "F")),
        result);
  }

  @Test
  public void mustBeAbleToUseSplitAfter() throws Exception {
    final Iterable<String> input = Arrays.asList("A", "B", "C", ".", "D", ".", "E", "F");
    final Source<List<String>, NotUsed> source =
        Source.from(input)
            .splitAfter(
                new Predicate<String>() {
                  public boolean test(String elem) {
                    return elem.equals(".");
                  }
                })
            .grouped(10)
            .concatSubstreams();

    final CompletionStage<List<List<String>>> future =
        source.grouped(10).runWith(Sink.head(), system);
    final List<List<String>> result = future.toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertEquals(
        Arrays.asList(
            Arrays.asList("A", "B", "C", "."), Arrays.asList("D", "."), Arrays.asList("E", "F")),
        result);
  }

  @Test
  public void mustBeAbleToUseConcat() {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<String> input2 = Arrays.asList("D", "E", "F");

    final Source<String, NotUsed> in1 = Source.from(input1);
    final Source<String, NotUsed> in2 = Source.from(input2);

    in1.concat(in2)
        .runForeach(
            new Procedure<String>() {
              public void apply(String elem) {
                probe.getRef().tell(elem, ActorRef.noSender());
              }
            },
            system);

    List<Object> output = probe.receiveN(6);
    assertEquals(Arrays.asList("A", "B", "C", "D", "E", "F"), output);
  }

  @Test
  public void mustBeAbleToConcatEmptySource() {
    Source.from(Arrays.asList("A", "B", "C"))
        .concat(Source.empty())
        .runWith(TestSink.probe(system), system)
        .ensureSubscription()
        .request(3)
        .expectNext("A", "B", "C")
        .expectComplete();
  }

  @Test
  public void mustBeAbleToUseConcatAll() {
    final Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3));
    final Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(4, 5, 6));
    final Source<Integer, NotUsed> sourceC = Source.from(Arrays.asList(7, 8, 9));
    final TestSubscriber.Probe<Integer> sub =
        sourceA.concatAllLazy(sourceB, sourceC).runWith(TestSink.probe(system), system);
    sub.expectSubscription().request(9);
    sub.expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9).expectComplete();
  }

  @Test
  public void mustBeAbleToUsePrepend() {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<String> input2 = Arrays.asList("D", "E", "F");

    final Source<String, NotUsed> in1 = Source.from(input1);
    final Source<String, NotUsed> in2 = Source.from(input2);

    in2.prepend(in1)
        .runForeach(
            new Procedure<String>() {
              public void apply(String elem) {
                probe.getRef().tell(elem, ActorRef.noSender());
              }
            },
            system);

    List<Object> output = probe.receiveN(6);
    assertEquals(Arrays.asList("A", "B", "C", "D", "E", "F"), output);
  }

  @Test
  public void mustBeAbleToUseCallableInput() {
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> input1 = Arrays.asList(4, 3, 2, 1, 0);
    final Creator<Iterator<Integer>> input =
        new Creator<Iterator<Integer>>() {
          @Override
          public Iterator<Integer> create() {
            return input1.iterator();
          }
        };
    Source.fromIterator(input)
        .runForeach(
            new Procedure<Integer>() {
              public void apply(Integer elem) {
                probe.getRef().tell(elem, ActorRef.noSender());
              }
            },
            system);

    List<Object> output = probe.receiveN(5);
    assertEquals(Arrays.asList(4, 3, 2, 1, 0), output);
    probe.expectNoMessage(Duration.ofMillis(500));
  }

  @Test
  public void mustBeAbleToUseOnCompleteSuccess() {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input = Arrays.asList("A", "B", "C");

    Source.from(input)
        .runWith(
            Sink.onComplete(
                new Procedure<Try<Done>>() {
                  @Override
                  public void apply(Try<Done> param) throws Exception {
                    probe.getRef().tell(param.get(), ActorRef.noSender());
                  }
                }),
            system);

    probe.expectMsgClass(Done.class);
  }

  @Test
  public void mustBeAbleToUseOnCompleteError() {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input = Arrays.asList("A", "B", "C");

    Source.from(input)
        .<String>map(
            in -> {
              throw new RuntimeException("simulated err");
            })
        .runWith(Sink.head(), system)
        .whenComplete(
            (s, ex) -> {
              if (ex == null) {
                probe.getRef().tell("done", ActorRef.noSender());
              } else {
                probe.getRef().tell(ex.getMessage(), ActorRef.noSender());
              }
            });

    probe.expectMsgEquals("simulated err");
  }

  @Test
  public void mustBeAbleToUseToFuture() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input = Arrays.asList("A", "B", "C");
    CompletionStage<String> future = Source.from(input).runWith(Sink.head(), system);
    String result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals("A", result);
  }

  @Test
  public void mustBeAbleToUseSingle() throws Exception {
    // #source-single
    CompletionStage<List<String>> future = Source.single("A").runWith(Sink.seq(), system);
    CompletableFuture<List<String>> completableFuture = future.toCompletableFuture();
    completableFuture.thenAccept(result -> System.out.printf("collected elements: %s\n", result));
    // result list will contain exactly one element "A"

    // #source-single
    // DO NOT use get() directly in your production code!
    List<String> result = completableFuture.get();
    assertEquals(1, result.size());
    assertEquals("A", result.get(0));
  }

  @Test
  public void mustBeAbleToUsePrefixAndTail() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6);
    CompletionStage<Pair<List<Integer>, Source<Integer, NotUsed>>> future =
        Source.from(input).prefixAndTail(3).runWith(Sink.head(), system);
    Pair<List<Integer>, Source<Integer, NotUsed>> result =
        future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(Arrays.asList(1, 2, 3), result.first());

    CompletionStage<List<Integer>> tailFuture =
        result.second().limit(4).runWith(Sink.seq(), system);
    List<Integer> tailResult = tailFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(Arrays.asList(4, 5, 6), tailResult);
  }

  @Test
  public void mustBeAbleToUseConcatAllWithSources() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> input1 = Arrays.asList(1, 2, 3);
    final Iterable<Integer> input2 = Arrays.asList(4, 5);

    final List<Source<Integer, NotUsed>> mainInputs = new ArrayList<>();
    mainInputs.add(Source.from(input1));
    mainInputs.add(Source.from(input2));

    CompletionStage<List<Integer>> future =
        Source.from(mainInputs)
            .flatMapConcat(ConstantFun.javaIdentityFunction())
            .grouped(6)
            .runWith(Sink.head(), system);

    List<Integer> result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);

    assertEquals(Arrays.asList(1, 2, 3, 4, 5), result);
  }

  @Test
  public void mustBeAbleToUseFlatMapMerge() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> input1 = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    final Iterable<Integer> input2 = Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
    final Iterable<Integer> input3 = Arrays.asList(20, 21, 22, 23, 24, 25, 26, 27, 28, 29);
    final Iterable<Integer> input4 = Arrays.asList(30, 31, 32, 33, 34, 35, 36, 37, 38, 39);

    final List<Source<Integer, NotUsed>> mainInputs = new ArrayList<>();
    mainInputs.add(Source.from(input1));
    mainInputs.add(Source.from(input2));
    mainInputs.add(Source.from(input3));
    mainInputs.add(Source.from(input4));

    CompletionStage<List<Integer>> future =
        Source.from(mainInputs)
            .flatMapMerge(3, ConstantFun.javaIdentityFunction())
            .grouped(60)
            .runWith(Sink.head(), system);

    List<Integer> result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    final Set<Integer> set = new HashSet<>(result);
    final Set<Integer> expected = new HashSet<>();
    for (int i = 0; i < 40; ++i) {
      expected.add(i);
    }

    assertEquals(expected, set);
  }

  @Test
  public void mustBeAbleToUseBuffer() throws Exception {
    final TestKit probe = new TestKit(system);
    final List<String> input = Arrays.asList("A", "B", "C");
    final CompletionStage<List<String>> future =
        Source.from(input)
            .buffer(2, OverflowStrategy.backpressure())
            .grouped(4)
            .runWith(Sink.head(), system);

    List<String> result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(input, result);
  }

  @Test
  public void mustBeAbleToUseConflate() throws Exception {
    final TestKit probe = new TestKit(system);
    final List<String> input = Arrays.asList("A", "B", "C");
    CompletionStage<String> future =
        Source.from(input)
            .conflateWithSeed(s -> s, (aggr, in) -> aggr + in)
            .runFold("", (aggr, in) -> aggr + in, system);
    String result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals("ABC", result);

    final Flow<String, String, NotUsed> flow2 = Flow.of(String.class).conflate((a, b) -> a + b);

    CompletionStage<String> future2 =
        Source.from(input)
            .conflate((String a, String b) -> a + b)
            .runFold("", (a, b) -> a + b, system);
    String result2 = future2.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals("ABC", result2);
  }

  @Test
  public void mustBeAbleToUseExpand() throws Exception {
    final TestKit probe = new TestKit(system);
    final List<String> input = Arrays.asList("A", "B", "C");
    CompletionStage<String> future =
        Source.from(input)
            .expand(in -> Stream.iterate(in, i -> i).iterator())
            .runWith(Sink.head(), system);
    String result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals("A", result);
  }

  @Test
  public void mustProduceTicks() throws Exception {
    final TestKit probe = new TestKit(system);
    Source<String, Cancellable> tickSource =
        Source.tick(Duration.ofSeconds(1), Duration.ofMillis(500), "tick");
    @SuppressWarnings("unused")
    Cancellable cancellable =
        tickSource
            .to(
                Sink.foreach(
                    new Procedure<String>() {
                      public void apply(String elem) {
                        probe.getRef().tell(elem, ActorRef.noSender());
                      }
                    }))
            .run(system);
    probe.expectNoMessage(Duration.ofMillis(600));
    probe.expectMsgEquals("tick");
    probe.expectNoMessage(Duration.ofMillis(200));
    probe.expectMsgEquals("tick");
    probe.expectNoMessage(Duration.ofMillis(200));
    cancellable.cancel();
  }

  @Test
  @SuppressWarnings("unused")
  public void mustCompileMethodsWithJavaDuration() {
    Source<NotUsed, Cancellable> tickSource =
        Source.tick(Duration.ofSeconds(1), Duration.ofMillis(500), notUsed());
  }

  @Test
  public void mustBeAbleToUseMapFuture() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input = Arrays.asList("a", "b", "c");
    Source.from(input)
        .mapAsync(4, elem -> CompletableFuture.completedFuture(elem.toUpperCase()))
        .runForeach(elem -> probe.getRef().tell(elem, ActorRef.noSender()), system);
    probe.expectMsgEquals("A");
    probe.expectMsgEquals("B");
    probe.expectMsgEquals("C");
  }

  @Test
  public void mustBeAbleToUseCollectType() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<FlowSpec.Apple> input = Collections.singletonList(new FlowSpec.Apple());
    final Source<FlowSpec.Apple, ?> appleSource = Source.from(input);
    final Source<FlowSpec.Fruit, ?> fruitSource = appleSource.collectType(FlowSpec.Fruit.class);
    fruitSource
        .collectType(FlowSpec.Apple.class)
        .collectType(FlowSpec.Apple.class)
        .runForeach(
            (elem) -> {
              probe.getRef().tell(elem, ActorRef.noSender());
            },
            system);
    probe.expectMsgAnyClassOf(FlowSpec.Apple.class);
  }

  @Test
  public void mustWorkFromFuture() throws Exception {
    final Iterable<String> input = Arrays.asList("A", "B", "C");
    CompletionStage<String> future1 = Source.from(input).runWith(Sink.head(), system);
    CompletionStage<String> future2 = Source.completionStage(future1).runWith(Sink.head(), system);
    String result = future2.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals("A", result);
  }

  @Test
  public void mustWorkFromFutureVoid() throws Exception {
    CompletionStage<Void> future = CompletableFuture.completedFuture(null);
    CompletionStage<List<Void>> future2 =
        Source.completionStage(future).runWith(Sink.seq(), system);
    List<Void> result = future2.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(0, result.size());
  }

  @Test
  public void mustWorkFromRange() throws Exception {
    CompletionStage<List<Integer>> f = Source.range(0, 10).grouped(20).runWith(Sink.head(), system);
    final List<Integer> result = f.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(11, result.size());
    Integer counter = 0;
    for (Integer i : result) assertEquals(i, counter++);
  }

  @Test
  public void mustWorkFromRangeWithStep() throws Exception {
    CompletionStage<List<Integer>> f =
        Source.range(0, 10, 2).grouped(20).runWith(Sink.head(), system);
    final List<Integer> result = f.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(6, result.size());
    Integer counter = 0;
    for (Integer i : result) {
      assertEquals(i, counter);
      counter += 2;
    }
  }

  @Test
  public void mustRepeat() throws Exception {
    final CompletionStage<List<Integer>> f =
        Source.repeat(42).grouped(10000).runWith(Sink.head(), system);
    final List<Integer> result = f.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(10000, result.size());
    for (Integer i : result) assertEquals(i, (Integer) 42);
  }

  @Test
  public void mustRepeatForDocs() throws Exception {
    // #repeat
    Source<Integer, NotUsed> source = Source.repeat(42);
    CompletionStage<Done> f = source.take(4).runWith(Sink.foreach(System.out::println), system);
    // 42
    // 42
    // 42
    // 42
    // #repeat
    final Done result = f.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(Done.done(), result);
  }

  @Test
  public void mustBeAbleToUseQueue() throws Exception {
    final Pair<BoundedSourceQueue<String>, CompletionStage<List<String>>> x =
        Flow.of(String.class).runWith(Source.queue(2), Sink.seq(), system);
    final BoundedSourceQueue<String> source = x.first();
    final CompletionStage<List<String>> result = x.second();
    source.offer("hello");
    source.offer("world");
    source.complete();
    assertEquals(
        Arrays.asList("hello", "world"), result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void mustBeAbleToUseActorRefSource() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<Integer, ActorRef> actorRefSource =
        Source.actorRef(
            msg -> Optional.empty(), msg -> Optional.empty(), 10, OverflowStrategy.fail());
    final ActorRef ref =
        actorRefSource
            .to(
                Sink.foreach(
                    new Procedure<Integer>() {
                      public void apply(Integer elem) {
                        probe.getRef().tell(elem, ActorRef.noSender());
                      }
                    }))
            .run(system);
    ref.tell(1, ActorRef.noSender());
    probe.expectMsgEquals(1);
    ref.tell(2, ActorRef.noSender());
    probe.expectMsgEquals(2);
    ref.tell(new Status.Success("ok"), ActorRef.noSender());
  }

  @Test
  public void mustBeAbleToUseStatefulMaponcat() throws Exception {
    final TestKit probe = new TestKit(system);
    final java.lang.Iterable<Integer> input = Arrays.asList(1, 2, 3, 4, 5);
    final Source<Integer, NotUsed> ints =
        Source.from(input)
            .statefulMapConcat(
                () -> {
                  int[] state = new int[] {0};
                  return (elem) -> {
                    List<Integer> list = new ArrayList<>(Collections.nCopies(state[0], elem));
                    state[0] = elem;
                    return list;
                  };
                });

    ints.runFold("", (acc, elem) -> acc + elem, system)
        .thenAccept(elem -> probe.getRef().tell(elem, ActorRef.noSender()));

    probe.expectMsgEquals("2334445555");
  }

  @Test
  public void mustBeAbleToUseStatefulMap() throws Exception {
    final java.lang.Iterable<Integer> input = Arrays.asList(1, 2, 3, 4, 5);
    final CompletionStage<String> grouped =
        Source.from(input)
            .statefulMap(
                () -> new ArrayList<Integer>(2),
                (buffer, elem) -> {
                  if (buffer.size() == 2) {
                    final ArrayList<Integer> group = new ArrayList<>(buffer);
                    buffer.clear();
                    buffer.add(elem);
                    return Pair.create(buffer, group);
                  } else {
                    buffer.add(elem);
                    return Pair.create(buffer, Collections.emptyList());
                  }
                },
                Optional::ofNullable)
            .filterNot(List::isEmpty)
            .map(String::valueOf)
            .runFold("", (acc, elem) -> acc + elem, system);
    Assert.assertEquals("[1, 2][3, 4][5]", grouped.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void mustBeAbleToUseStatefulMapAsDistinctUntilChanged() throws Exception {
    final java.lang.Iterable<Integer> input = Arrays.asList(1, 1, 1, 2, 3, 3, 3, 4, 5, 5, 5);
    final CompletionStage<String> result =
        Source.from(input)
            .statefulMap(
                Optional::<Integer>empty,
                (buffer, elem) -> {
                  if (buffer.isPresent()) {
                    if (buffer.get().equals(elem)) {
                      return Pair.create(buffer, Optional.<Integer>empty());
                    } else {
                      return Pair.create(Optional.of(elem), Optional.of(elem));
                    }
                  } else {
                    return Pair.create(Optional.of(elem), Optional.of(elem));
                  }
                },
                last -> Optional.empty())
            .filter(Optional::isPresent)
            .map(Optional::get)
            .runFold("", (acc, elem) -> acc + elem, system);
    Assert.assertEquals("12345", result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void mustBeAbleToUseMapWithResource() {
    Source.from(Arrays.asList("1", "2", "3"))
        .mapWithResource(
            () -> "resource",
            (resource, elem) -> elem,
            (resource) -> {
              return Optional.of("end");
            })
        .runWith(TestSink.create(system), system)
        .request(4)
        .expectNext("1", "2", "3", "end")
        .expectComplete();
  }

  @Test
  public void mustBeAbleToUseIntersperse() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<String, NotUsed> source =
        Source.from(Arrays.asList("0", "1", "2", "3")).intersperse("[", ",", "]");

    final CompletionStage<Done> future =
        source.runWith(
            Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    probe.expectMsgEquals("[");
    probe.expectMsgEquals("0");
    probe.expectMsgEquals(",");
    probe.expectMsgEquals("1");
    probe.expectMsgEquals(",");
    probe.expectMsgEquals("2");
    probe.expectMsgEquals(",");
    probe.expectMsgEquals("3");
    probe.expectMsgEquals("]");
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToUseIntersperseAndConcat() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<String, NotUsed> source =
        Source.from(Arrays.asList("0", "1", "2", "3")).intersperse(",");

    final CompletionStage<Done> future =
        Source.single(">> ")
            .concat(source)
            .runWith(Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    probe.expectMsgEquals(">> ");
    probe.expectMsgEquals("0");
    probe.expectMsgEquals(",");
    probe.expectMsgEquals("1");
    probe.expectMsgEquals(",");
    probe.expectMsgEquals("2");
    probe.expectMsgEquals(",");
    probe.expectMsgEquals("3");
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToUseInterleaveAll() {
    Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 7, 8));
    Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(3, 4, 9));
    Source<Integer, NotUsed> sourceC = Source.from(Arrays.asList(5, 6));
    final TestSubscriber.Probe<Integer> sub =
        sourceA
            .interleaveAll(Arrays.asList(sourceB, sourceC), 2, false)
            .runWith(TestSink.probe(system), system);
    sub.expectSubscription().request(9);
    sub.expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9).expectComplete();
  }

  @Test
  public void mustBeAbleToUseDropWhile() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<Integer, NotUsed> source =
        Source.from(Arrays.asList(0, 1, 2, 3))
            .dropWhile(
                new Predicate<Integer>() {
                  public boolean test(Integer elem) {
                    return elem < 2;
                  }
                });

    final CompletionStage<Done> future =
        source.runWith(
            Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    probe.expectMsgEquals(2);
    probe.expectMsgEquals(3);
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToUseTakeWhile() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<Integer, NotUsed> source =
        Source.from(Arrays.asList(0, 1, 2, 3))
            .takeWhile(
                new Predicate<Integer>() {
                  public boolean test(Integer elem) {
                    return elem < 2;
                  }
                });

    final CompletionStage<Done> future =
        source.runWith(
            Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    probe.expectMsgEquals(0);
    probe.expectMsgEquals(1);

    Duration duration = Duration.ofMillis(200);

    probe.expectNoMessage(duration);
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToRecover() throws Exception {
    final ManualProbe<Integer> publisherProbe = TestPublisher.manualProbe(true, system);
    final TestKit probe = new TestKit(system);

    final Source<Integer, NotUsed> source =
        Source.fromPublisher(publisherProbe)
            .map(
                elem -> {
                  if (elem == 1) throw new RuntimeException("ex");
                  else return elem;
                })
            .recoverWithRetries(
                1,
                new PFBuilder<Throwable, Source<Integer, NotUsed>>()
                    .matchAny(ex -> Source.single(0))
                    .build());

    final CompletionStage<Done> future =
        source.runWith(
            Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);
    final PublisherProbeSubscription<Integer> s = publisherProbe.expectSubscription();
    s.sendNext(0);
    probe.expectMsgEquals(0);
    s.sendNext(1);
    probe.expectMsgEquals(0);

    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToCombine() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<Integer, NotUsed> source1 = Source.from(Arrays.asList(0, 1));
    final Source<Integer, NotUsed> source2 = Source.from(Arrays.asList(2, 3));

    final Source<Integer, NotUsed> source =
        Source.combine(source1, source2, new ArrayList<>(), width -> Merge.create(width));

    final CompletionStage<Done> future =
        source.runWith(
            Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    probe.expectMsgAllOf(0, 1, 2, 3);

    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToCombineMat() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<Integer, BoundedSourceQueue<Integer>> source1 = Source.queue(2);
    final Source<Integer, NotUsed> source2 = Source.from(Arrays.asList(2, 3));

    // compiler to check the correct materialized value of type = BoundedSourceQueue<Integer>
    // available
    final Source<Integer, BoundedSourceQueue<Integer>> combined =
        Source.combineMat(
            source1,
            source2,
            width -> Concat.create(width),
            Keep.left()); // Keep.left() (i.e. preserve queueSource's materialized value)

    BoundedSourceQueue<Integer> queue =
        combined
            .toMat(
                Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), Keep.left())
            .run(system);

    queue.offer(0);
    queue.offer(1);
    queue.complete(); // complete queueSource so that combined with `Concat` pulls elements from
    // queueSource

    // elements from source1 (i.e. first of combined source) come first, then source2 elements, due
    // to `Concat`
    probe.expectMsgAllOf(0, 1, 2, 3);
  }

  @Test
  public void mustBeAbleToCombineN() throws Exception {
    final Source<Integer, NotUsed> source1 = Source.single(1);
    final Source<Integer, NotUsed> source2 = Source.single(2);
    final Source<Integer, NotUsed> source3 = Source.single(3);
    final List<Source<Integer, NotUsed>> sources = Arrays.asList(source1, source2, source3);
    final CompletionStage<Integer> result =
        Source.combine(sources, Concat::create)
            .runWith(Sink.collect(Collectors.toList()), system)
            .thenApply(list -> list.stream().mapToInt(l -> l).sum());
    assertEquals(6, result.toCompletableFuture().get(3, TimeUnit.SECONDS).intValue());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mustBeAbleToZipN() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<Integer, NotUsed> source1 = Source.from(Arrays.asList(0, 1));
    final Source<Integer, NotUsed> source2 = Source.from(Arrays.asList(2, 3));

    final List<Source<Integer, ?>> sources = Arrays.asList(source1, source2);

    final Source<List<Integer>, ?> source = Source.zipN(sources);

    final CompletionStage<Done> future =
        source.runWith(
            Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    probe.expectMsgAllOf(Arrays.asList(0, 2), Arrays.asList(1, 3));

    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToZipWithN() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<Integer, NotUsed> source1 = Source.from(Arrays.asList(0, 1));
    final Source<Integer, NotUsed> source2 = Source.from(Arrays.asList(2, 3));

    final List<Source<Integer, ?>> sources = Arrays.asList(source1, source2);

    final Source<Boolean, ?> source =
        Source.zipWithN(list -> Boolean.valueOf(list.contains(0)), sources);

    final CompletionStage<Done> future =
        source.runWith(
            Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    probe.expectMsgAllOf(Boolean.TRUE, Boolean.FALSE);

    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToZipAll() {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 =
        Arrays.asList("A", "B", "C", "D", "new kid on the block1", "second newbie");
    final Iterable<Integer> input2 = Arrays.asList(1, 2, 3, 4);

    Source<String, NotUsed> src1 = Source.from(input1);
    Source<Integer, NotUsed> src2 = Source.from(input2);
    Sink<Pair<String, Integer>, CompletionStage<Done>> sink =
        Sink.foreach(
            new Procedure<Pair<String, Integer>>() {
              @Override
              public void apply(Pair<String, Integer> param) throws Exception {
                probe.getRef().tell(param, ActorRef.noSender());
              }
            });
    Source<Pair<String, Integer>, NotUsed> zippedSrc = src1.zipAll(src2, "MISSING", -1);
    zippedSrc.runWith(sink, system);

    List<Object> output = probe.receiveN(6);
    List<Pair<String, Integer>> expected =
        Arrays.asList(
            new Pair<>("A", 1),
            new Pair<>("B", 2),
            new Pair<>("C", 3),
            new Pair<>("D", 4),
            new Pair<>("new kid on the block1", -1),
            new Pair<>("second newbie", -1));
    assertEquals(expected, output);
  }

  @Test
  public void createEmptySource() throws Exception {
    List<Integer> actual =
        Source.empty(Integer.class).runWith(Sink.seq(), system).toCompletableFuture().get();
    assertThat(actual, is(Collections.emptyList()));
  }

  @Test
  public void cycleSourceMustGenerateSameSequenceInRepeatedFashion() throws Exception {
    // #cycle
    final Source<Integer, NotUsed> source = Source.cycle(() -> Arrays.asList(1, 2, 3).iterator());
    CompletionStage<List<Integer>> result = source.grouped(9).runWith(Sink.head(), system);
    List<Integer> emittedValues = result.toCompletableFuture().get();
    assertThat(emittedValues, is(Arrays.asList(1, 2, 3, 1, 2, 3, 1, 2, 3)));
    // #cycle
  }

  @Test
  public void cycleSourceMustThrow() {
    ExecutionException exception =
        Assert.assertThrows(
            "CompletableFuture.get() should throw ExecutionException",
            ExecutionException.class,
            () -> {
              // #cycle-error
              Iterator<Integer> emptyIterator = Collections.<Integer>emptyList().iterator();
              Source.cycle(() -> emptyIterator)
                  .runWith(Sink.head(), system)
                  // stream will be terminated with IllegalArgumentException
                  // #cycle-error
                  .toCompletableFuture()
                  .get();
            });
    assertEquals(
        "The cause of ExecutionException should be IllegalArgumentException",
        IllegalArgumentException.class,
        exception.getCause().getClass());
  }

  @Test
  public void mustBeAbleToUseMerge() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<String> input2 = Arrays.asList("D", "E", "F");

    Source.from(input1)
        .merge(Source.from(input2))
        .runForeach(
            new Procedure<String>() {
              public void apply(String elem) {
                probe.getRef().tell(elem, ActorRef.noSender());
              }
            },
            system);

    probe.expectMsgAllOf("A", "B", "C", "D", "E", "F");
  }

  @Test
  public void mustBeAbleToUseZipWith() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<String> input2 = Arrays.asList("D", "E", "F");

    Source.from(input1)
        .zipWith(
            Source.from(input2),
            new Function2<String, String, String>() {
              public String apply(String s1, String s2) {
                return s1 + "-" + s2;
              }
            })
        .runForeach(
            new Procedure<String>() {
              public void apply(String elem) {
                probe.getRef().tell(elem, ActorRef.noSender());
              }
            },
            system);

    probe.expectMsgEquals("A-D");
    probe.expectMsgEquals("B-E");
    probe.expectMsgEquals("C-F");
  }

  @Test
  public void mustBeAbleToUseZip() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<String> input2 = Arrays.asList("D", "E", "F");

    Source.from(input1)
        .zip(Source.from(input2))
        .runForeach(
            new Procedure<Pair<String, String>>() {
              public void apply(Pair<String, String> elem) {
                probe.getRef().tell(elem, ActorRef.noSender());
              }
            },
            system);

    probe.expectMsgEquals(new Pair<>("A", "D"));
    probe.expectMsgEquals(new Pair<>("B", "E"));
    probe.expectMsgEquals(new Pair<>("C", "F"));
  }

  @Test
  public void mustBeAbleToUseMerge2() {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<String> input2 = Arrays.asList("D", "E", "F");

    Source.from(input1)
        .merge(Source.from(input2))
        .runForeach(
            new Procedure<String>() {
              public void apply(String elem) {
                probe.getRef().tell(elem, ActorRef.noSender());
              }
            },
            system);

    probe.expectMsgAllOf("A", "B", "C", "D", "E", "F");
  }

  @Test
  public void mustBeAbleToUseMerge3() {
    final Source<Integer, NotUsed> sourceA = Source.from(Arrays.asList(1, 2, 3));
    final Source<Integer, NotUsed> sourceB = Source.from(Arrays.asList(4, 5, 6));
    final Source<Integer, NotUsed> sourceC = Source.from(Arrays.asList(7, 8, 9));
    final TestSubscriber.Probe<Integer> sub =
        sourceA
            .mergeAll(Arrays.asList(sourceB, sourceC), false)
            .runWith(TestSink.probe(system), system);
    sub.expectSubscription().request(9);
    sub.expectNextUnorderedN(Util.immutableSeq(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9)))
        .expectComplete();
  }

  @Test
  public void mustBeAbleToUseInitialTimeout() {
    ExecutionException exception =
        Assert.assertThrows(
            "CompletableFuture.get() should throw ExecutionException",
            ExecutionException.class,
            () ->
                Source.maybe()
                    .initialTimeout(Duration.ofSeconds(1))
                    .runWith(Sink.head(), system)
                    .toCompletableFuture()
                    .get(3, TimeUnit.SECONDS));
    assertTrue(
        "The cause of ExecutionException should be TimeoutException",
        TimeoutException.class.isAssignableFrom(exception.getCause().getClass()));
  }

  @Test
  public void mustBeAbleToUseCompletionTimeout() {
    ExecutionException exception =
        Assert.assertThrows(
            "CompletableFuture.get() should throw ExecutionException",
            ExecutionException.class,
            () ->
                Source.maybe()
                    .completionTimeout(Duration.ofSeconds(1))
                    .runWith(Sink.head(), system)
                    .toCompletableFuture()
                    .get(3, TimeUnit.SECONDS));
    assertTrue(
        "The cause of ExecutionException should be TimeoutException",
        TimeoutException.class.isAssignableFrom(exception.getCause().getClass()));
  }

  @Test
  public void mustBeAbleToUseIdleTimeout() {
    ExecutionException exception =
        Assert.assertThrows(
            "CompletableFuture.get() should throw ExecutionException",
            ExecutionException.class,
            () ->
                Source.maybe()
                    .idleTimeout(Duration.ofSeconds(1))
                    .runWith(Sink.head(), system)
                    .toCompletableFuture()
                    .get(3, TimeUnit.SECONDS));
    assertTrue(
        "The cause of ExecutionException should be TimeoutException",
        TimeoutException.class.isAssignableFrom(exception.getCause().getClass()));
  }

  @Test
  public void mustBeAbleToUseIdleInject() throws Exception {
    Integer result =
        Source.<Integer>maybe()
            .keepAlive(Duration.ofSeconds(1), () -> 0)
            .takeWithin(Duration.ofMillis(1500))
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

    assertEquals((Object) 0, result);
  }

  public void mustSuitablyOverrideAttributeHandlingMethods() {
    @SuppressWarnings("unused")
    final Source<Integer, NotUsed> f =
        Source.single(42)
            .withAttributes(Attributes.name(""))
            .addAttributes(Attributes.asyncBoundary())
            .named("");
  }

  @Test
  public void mustBeAbleToUseThrottle() throws Exception {
    Integer result =
        Source.from(Arrays.asList(0, 1, 2))
            .throttle(10, Duration.ofSeconds(1), 10, ThrottleMode.shaping())
            .throttle(10, Duration.ofSeconds(1), 10, ThrottleMode.enforcing())
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

    assertEquals((Object) 0, result);
  }

  @Test
  public void mustBeAbleToUseAlsoTo() {
    final Source<Integer, NotUsed> f = Source.<Integer>empty().alsoTo(Sink.ignore());
    final Source<Integer, String> f2 =
        Source.<Integer>empty().alsoToMat(Sink.ignore(), (i, n) -> "foo");
  }

  @Test
  public void mustBeAbleToUseAlsoToAll() {
    final Source<Integer, NotUsed> f =
        Source.<Integer>empty().alsoToAll(Sink.ignore(), Sink.ignore());
  }

  @Test
  public void mustBeAbleToUseDivertTo() {
    final Source<Integer, NotUsed> f = Source.<Integer>empty().divertTo(Sink.ignore(), e -> true);
    final Source<Integer, String> f2 =
        Source.<Integer>empty().divertToMat(Sink.ignore(), e -> true, (i, n) -> "foo");
  }

  @Test
  public void mustBeAbleToUsePreMaterialize() {
    final Pair<NotUsed, Source<Integer, NotUsed>> p =
        Source.<Integer>empty().preMaterialize(system);
  }

  @Test
  public void mustBeAbleToConvertToJavaInJava() {
    final akka.stream.scaladsl.Source<Integer, NotUsed> scalaSource =
        akka.stream.scaladsl.Source.empty();
    Source<Integer, NotUsed> javaSource = scalaSource.asJava();
  }

  @Test
  public void mustProperlyIterate() throws Exception {
    final Creator<Iterator<Boolean>> input = () -> Iterables.cycle(false, true).iterator();

    final CompletableFuture<List<Boolean>> future =
        Source.fromIterator(input).grouped(10).runWith(Sink.head(), system).toCompletableFuture();

    assertArrayEquals(
        new Boolean[] {false, true, false, true, false, true, false, true, false, true},
        future.get(1, TimeUnit.SECONDS).toArray());
  }

  @Test
  public void mustRunSourceAndIgnoreElementsItOutputsAndOnlySignalTheCompletion() throws Exception {
    final Iterator<Integer> iterator = IntStream.range(1, 10).iterator();
    final Creator<Iterator<Integer>> input = () -> iterator;
    final Done completion =
        Source.fromIterator(input)
            .map(it -> it * 10)
            .run(system)
            .toCompletableFuture()
            .get(1, TimeUnit.SECONDS);
    assertEquals(Done.getInstance(), completion);
  }

  @Test
  public void mustRunSourceAndIgnoreElementsItOutputsAndOnlySignalTheCompletionWithMaterializer()
      throws Exception {
    final Materializer materializer = Materializer.createMaterializer(system);
    final Iterator<Integer> iterator = IntStream.range(1, 10).iterator();
    final Creator<Iterator<Integer>> input = () -> iterator;
    final Done completion =
        Source.fromIterator(input)
            .map(it -> it * 10)
            .run(materializer)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);
    assertEquals(Done.getInstance(), completion);
  }

  @Test
  public void mustGenerateAFiniteFibonacciSequenceAsynchronously() {
    final List<Integer> resultList =
        Source.unfoldAsync(
                Pair.create(0, 1),
                (pair) -> {
                  if (pair.first() > 10000000) {
                    return CompletableFuture.completedFuture(Optional.empty());
                  } else {
                    return CompletableFuture.supplyAsync(
                        () ->
                            Optional.of(
                                Pair.create(
                                    Pair.create(pair.second(), pair.first() + pair.second()),
                                    pair.first())),
                        system.dispatcher());
                  }
                })
            .runFold(
                new ArrayList<Integer>(),
                (list, next) -> {
                  list.add(next);
                  return list;
                },
                system)
            .toCompletableFuture()
            .join();
    assertEquals(
        Arrays.asList(
            0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181,
            6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811, 514229, 832040,
            1346269, 2178309, 3524578, 5702887, 9227465),
        resultList);
  }

  @Test
  public void flattenOptional() throws Exception {
    // #flattenOptional
    final CompletionStage<List<Integer>> resultList =
        Source.range(1, 10)
            .map(x -> Optional.of(x).filter(n -> n % 2 == 0))
            .via(Flow.flattenOptional())
            .runWith(Sink.seq(), system);
    // #flattenOptional
    Assert.assertEquals(
        Arrays.asList(2, 4, 6, 8, 10), resultList.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void flattenOptionalOptional() throws Exception {
    final List<Integer> resultList =
        Source.range(1, 10)
            .map(x -> Optional.of(x).filter(n -> n % 2 == 0))
            .map(Optional::ofNullable)
            .via(Flow.flattenOptional())
            .via(Flow.flattenOptional())
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);
    Assert.assertEquals(Arrays.asList(2, 4, 6, 8, 10), resultList);
  }
}
