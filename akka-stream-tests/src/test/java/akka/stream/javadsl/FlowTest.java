/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static akka.Done.done;
import static akka.stream.testkit.StreamTestKit.PublisherProbeSubscription;
import static org.junit.Assert.*;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.japi.JavaPartialFunction;
import akka.japi.Pair;
import akka.japi.function.*;
import akka.japi.pf.PFBuilder;
import akka.stream.*;
import akka.stream.javadsl.GraphDSL.Builder;
import akka.stream.scaladsl.FlowSpec;
import akka.stream.stage.*;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import akka.testkit.javadsl.TestKit;
import akka.util.ConstantFun;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.reactivestreams.Publisher;

@SuppressWarnings("serial")
public class FlowTest extends StreamTest {
  public FlowTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("FlowTest", AkkaSpec.testConf());

  interface Fruit {}

  static class Apple implements Fruit {};

  static class Orange implements Fruit {};

  public void compileOnlyUpcast() {
    Flow<Apple, Apple, NotUsed> appleFlow = null;
    Flow<Apple, Fruit, NotUsed> appleFruitFlow = Flow.upcast(appleFlow);

    Flow<Apple, Fruit, NotUsed> fruitFlow = appleFruitFlow.intersperse(new Orange());
  }

  @Test
  public void mustBeAbleToUseSimpleOperators() {
    final TestKit probe = new TestKit(system);
    final String[] lookup = {"a", "b", "c", "d", "e", "f"};
    final java.lang.Iterable<Integer> input = Arrays.asList(0, 1, 2, 3, 4, 5);
    final Source<Integer, NotUsed> ints = Source.from(input);
    final Flow<Integer, String, NotUsed> flow1 =
        Flow.of(Integer.class)
            .drop(2)
            .take(3)
            .takeWithin(Duration.ofSeconds(10))
            .map(
                new Function<Integer, String>() {
                  public String apply(Integer elem) {
                    return lookup[elem];
                  }
                })
            .filter(
                new Predicate<String>() {
                  public boolean test(String elem) {
                    return !elem.equals("c");
                  }
                });
    final Flow<String, String, NotUsed> flow2 =
        Flow.of(String.class)
            .grouped(2)
            .mapConcat(
                new Function<java.util.List<String>, java.lang.Iterable<String>>() {
                  public java.util.List<String> apply(java.util.List<String> elem) {
                    return elem;
                  }
                })
            .groupedWithin(100, Duration.ofMillis(50))
            .mapConcat(
                new Function<java.util.List<String>, java.lang.Iterable<String>>() {
                  public java.util.List<String> apply(java.util.List<String> elem) {
                    return elem;
                  }
                });

    ints.via(flow1.via(flow2))
        .runFold("", (acc, elem) -> acc + elem, system)
        .thenAccept(elem -> probe.getRef().tell(elem, ActorRef.noSender()));

    probe.expectMsgEquals("de");
  }

  @Test
  public void mustBeAbleToUseContraMap() {
    final Source<String, NotUsed> source = Source.from(Arrays.asList("1", "2", "3"));
    final Flow<Integer, String, NotUsed> flow = Flow.fromFunction(String::valueOf);
    source
        .via(flow.contramap(Integer::valueOf))
        .runWith(TestSink.create(system), system)
        .request(3)
        .expectNext("1")
        .expectNext("2")
        .expectNext("3")
        .expectComplete();
  }

  @Test
  public void mustBeAbleToUseDropWhile() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<Integer, NotUsed> source = Source.from(Arrays.asList(0, 1, 2, 3));
    final Flow<Integer, Integer, NotUsed> flow = Flow.of(Integer.class).dropWhile(elem -> elem < 2);

    final CompletionStage<Done> future =
        source
            .via(flow)
            .runWith(Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    probe.expectMsgEquals(2);
    probe.expectMsgEquals(3);
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToUseStatefulMaponcat() throws Exception {
    final TestKit probe = new TestKit(system);
    final java.lang.Iterable<Integer> input = Arrays.asList(1, 2, 3, 4, 5);
    final Source<Integer, NotUsed> ints = Source.from(input);
    final Flow<Integer, Integer, NotUsed> flow =
        Flow.of(Integer.class)
            .statefulMapConcat(
                () -> {
                  int[] state = new int[] {0};
                  return (elem) -> {
                    List<Integer> list = new ArrayList<>(Collections.nCopies(state[0], elem));
                    state[0] = elem;
                    return list;
                  };
                });

    ints.via(flow)
        .runFold("", (acc, elem) -> acc + elem, system)
        .thenAccept(elem -> probe.getRef().tell(elem, ActorRef.noSender()));

    probe.expectMsgEquals("2334445555");
  }

  @Test
  public void mustBeAbleToUseStatefulMap() throws Exception {
    final java.lang.Iterable<Integer> input = Arrays.asList(1, 2, 3, 4, 5);
    final Source<Integer, NotUsed> source = Source.from(input);
    final Flow<Integer, String, NotUsed> flow =
        Flow.of(Integer.class)
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
            .map(String::valueOf);

    final CompletionStage<String> grouped =
        source.via(flow).runFold("", (acc, elem) -> acc + elem, system);
    Assert.assertEquals("[1, 2][3, 4][5]", grouped.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void mustBeAbleToUseMapWithResource() {
    Source.from(Arrays.asList("1", "2", "3"))
        .via(
            Flow.of(String.class)
                .mapWithResource(
                    () -> "resource",
                    (resource, elem) -> elem,
                    (resource) -> {
                      return Optional.of("end");
                    }))
        .runWith(TestSink.create(system), system)
        .request(4)
        .expectNext("1", "2", "3", "end")
        .expectComplete();
  }

  @Test
  public void mustBeAbleToUseIntersperse() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<String, NotUsed> source = Source.from(Arrays.asList("0", "1", "2", "3"));
    final Flow<String, String, NotUsed> flow = Flow.of(String.class).intersperse("[", ",", "]");

    final CompletionStage<Done> future =
        source
            .via(flow)
            .runWith(Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

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
    final Source<String, NotUsed> source = Source.from(Arrays.asList("0", "1", "2", "3"));
    final Flow<String, String, NotUsed> flow = Flow.of(String.class).intersperse(",");

    final CompletionStage<Done> future =
        Source.single(">> ")
            .concat(source.via(flow))
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
  public void mustBeAbleToUseTakeWhile() throws Exception {
    final TestKit probe = new TestKit(system);
    final Source<Integer, NotUsed> source = Source.from(Arrays.asList(0, 1, 2, 3));
    final Flow<Integer, Integer, NotUsed> flow =
        Flow.of(Integer.class)
            .takeWhile(
                new Predicate<Integer>() {
                  public boolean test(Integer elem) {
                    return elem < 2;
                  }
                });

    final CompletionStage<Done> future =
        source
            .via(flow)
            .runWith(Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    probe.expectMsgEquals(0);
    probe.expectMsgEquals(1);
    probe.expectNoMessage(Duration.ofMillis(200));
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToUseVia() {
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> input = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7);
    // duplicate each element, stop after 4 elements, and emit sum to the end
    final Flow<Integer, Integer, NotUsed> flow =
        Flow.of(Integer.class)
            .via(
                new GraphStage<FlowShape<Integer, Integer>>() {

                  public final Inlet<Integer> in = Inlet.create("in");
                  public final Outlet<Integer> out = Outlet.create("out");

                  @Override
                  public GraphStageLogic createLogic(Attributes inheritedAttributes)
                      throws Exception {
                    return new GraphStageLogic(shape()) {
                      int sum = 0;
                      int count = 0;

                      {
                        setHandler(
                            in,
                            new AbstractInHandler() {
                              @Override
                              public void onPush() throws Exception {
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
                });
    Source.from(input)
        .via(flow)
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
    final Flow<String, List<String>, NotUsed> flow =
        Flow.of(String.class)
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
        Source.from(input).via(flow).limit(10).runWith(Sink.seq(), system);
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
    final Flow<String, List<String>, NotUsed> flow =
        Flow.of(String.class)
            .splitWhen(
                new Predicate<String>() {
                  public boolean test(String elem) {
                    return elem.equals(".");
                  }
                })
            .grouped(10)
            .concatSubstreams();

    final CompletionStage<List<List<String>>> future =
        Source.from(input).via(flow).limit(10).runWith(Sink.seq(), system);
    final List<List<String>> result = future.toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertEquals(
        Arrays.asList(
            Arrays.asList("A", "B", "C"), Arrays.asList(".", "D"), Arrays.asList(".", "E", "F")),
        result);
  }

  @Test
  public void mustBeAbleToUseSplitAfter() throws Exception {
    final Iterable<String> input = Arrays.asList("A", "B", "C", ".", "D", ".", "E", "F");
    final Flow<String, List<String>, NotUsed> flow =
        Flow.of(String.class)
            .splitAfter(
                new Predicate<String>() {
                  public boolean test(String elem) {
                    return elem.equals(".");
                  }
                })
            .grouped(10)
            .concatSubstreams();

    final CompletionStage<List<List<String>>> future =
        Source.from(input).via(flow).limit(10).runWith(Sink.seq(), system);
    final List<List<String>> result = future.toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertEquals(
        Arrays.asList(
            Arrays.asList("A", "B", "C", "."), Arrays.asList("D", "."), Arrays.asList("E", "F")),
        result);
  }

  public <T> GraphStage<FlowShape<T, T>> op() {
    return new GraphStage<FlowShape<T, T>>() {
      public final Inlet<T> in = Inlet.create("in");
      public final Outlet<T> out = Outlet.create("out");

      @Override
      public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
        return new GraphStageLogic(shape()) {
          {
            setHandler(
                in,
                new AbstractInHandler() {
                  @Override
                  public void onPush() throws Exception {
                    push(out, grab(in));
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
      public FlowShape<T, T> shape() {
        return FlowShape.of(in, out);
      }
    };
  }

  @Test
  public void mustBeAbleToUseMerge() throws Exception {
    final Flow<String, String, NotUsed> f1 =
        Flow.of(String.class).via(FlowTest.this.op()).named("f1");
    final Flow<String, String, NotUsed> f2 =
        Flow.of(String.class).via(FlowTest.this.op()).named("f2");
    @SuppressWarnings("unused")
    final Flow<String, String, NotUsed> f3 =
        Flow.of(String.class).via(FlowTest.this.op()).named("f3");

    final Source<String, NotUsed> in1 = Source.from(Arrays.asList("a", "b", "c"));
    final Source<String, NotUsed> in2 = Source.from(Arrays.asList("d", "e", "f"));

    final Sink<String, Publisher<String>> publisher = Sink.asPublisher(AsPublisher.WITHOUT_FANOUT);

    final Source<String, NotUsed> source =
        Source.fromGraph(
            GraphDSL.create(
                new Function<GraphDSL.Builder<NotUsed>, SourceShape<String>>() {
                  @Override
                  public SourceShape<String> apply(Builder<NotUsed> b) throws Exception {
                    final UniformFanInShape<String, String> merge = b.add(Merge.create(2));
                    b.from(b.add(in1)).via(b.add(f1)).toInlet(merge.in(0));
                    b.from(b.add(in2)).via(b.add(f2)).toInlet(merge.in(1));
                    return new SourceShape<>(merge.out());
                  }
                }));

    // collecting
    final Publisher<String> pub = source.runWith(publisher, system);
    final CompletionStage<List<String>> all =
        Source.fromPublisher(pub).limit(100).runWith(Sink.seq(), system);

    final List<String> result = all.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(
        new HashSet<Object>(Arrays.asList("a", "b", "c", "d", "e", "f")), new HashSet<>(result));
  }

  @Test
  public void mustBeAbleToUsefromSourceCompletionStage() throws Exception {
    final Flow<String, String, NotUsed> f1 =
        Flow.of(String.class).via(FlowTest.this.op()).named("f1");

    final Flow<String, String, NotUsed> f2 =
        Flow.of(String.class).via(FlowTest.this.op()).named("f2");

    @SuppressWarnings("unused")
    final Flow<String, String, NotUsed> f3 =
        Flow.of(String.class).via(FlowTest.this.op()).named("f3");

    final Source<String, NotUsed> in1 = Source.from(Arrays.asList("a", "b", "c"));
    final Source<String, NotUsed> in2 = Source.from(Arrays.asList("d", "e", "f"));

    final Sink<String, Publisher<String>> publisher = Sink.asPublisher(AsPublisher.WITHOUT_FANOUT);

    final Graph<SourceShape<String>, NotUsed> graph =
        Source.fromGraph(
            GraphDSL.create(
                new Function<GraphDSL.Builder<NotUsed>, SourceShape<String>>() {
                  @Override
                  public SourceShape<String> apply(Builder<NotUsed> b) throws Exception {
                    final UniformFanInShape<String, String> merge = b.add(Merge.create(2));
                    b.from(b.add(in1)).via(b.add(f1)).toInlet(merge.in(0));
                    b.from(b.add(in2)).via(b.add(f2)).toInlet(merge.in(1));
                    return new SourceShape<>(merge.out());
                  }
                }));

    final Supplier<Graph<SourceShape<String>, NotUsed>> fn =
        new Supplier<Graph<SourceShape<String>, NotUsed>>() {
          public Graph<SourceShape<String>, NotUsed> get() {
            return graph;
          }
        };

    final CompletionStage<Graph<SourceShape<String>, NotUsed>> stage =
        CompletableFuture.supplyAsync(fn);

    final Source<String, CompletionStage<NotUsed>> source =
        Source.completionStageSource(stage.thenApply(Source::fromGraph));

    // collecting
    final Publisher<String> pub = source.runWith(publisher, system);
    final CompletionStage<List<String>> all =
        Source.fromPublisher(pub).limit(100).runWith(Sink.seq(), system);

    final List<String> result = all.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(
        new HashSet<Object>(Arrays.asList("a", "b", "c", "d", "e", "f")), new HashSet<>(result));
  }

  @Test
  public void mustBeAbleToUseZip() {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<Integer> input2 = Arrays.asList(1, 2, 3);

    RunnableGraph.fromGraph(
            GraphDSL.create(
                new Function<Builder<NotUsed>, ClosedShape>() {
                  public ClosedShape apply(Builder<NotUsed> b) {
                    final Outlet<String> in1 = b.add(Source.from(input1)).out();
                    final Outlet<Integer> in2 = b.add(Source.from(input2)).out();
                    final FanInShape2<String, Integer, Pair<String, Integer>> zip =
                        b.add(Zip.create());
                    final SinkShape<Pair<String, Integer>> out =
                        b.add(
                            Sink.foreach(
                                new Procedure<Pair<String, Integer>>() {
                                  @Override
                                  public void apply(Pair<String, Integer> param) throws Exception {
                                    probe.getRef().tell(param, ActorRef.noSender());
                                  }
                                }));

                    b.from(in1).toInlet(zip.in0());
                    b.from(in2).toInlet(zip.in1());
                    b.from(zip.out()).to(out);
                    return ClosedShape.getInstance();
                  }
                }))
        .run(system);

    List<Object> output = probe.receiveN(3);
    List<Pair<String, Integer>> expected =
        Arrays.asList(new Pair<>("A", 1), new Pair<>("B", 2), new Pair<>("C", 3));
    assertEquals(expected, output);
  }

  @Test
  public void mustBeAbleToUseZipAll() {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
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
    Flow<String, Pair<String, Integer>, NotUsed> fl =
        Flow.<String>create().zipAll(src2, "MISSING", -1);
    src1.via(fl).runWith(sink, system);

    List<Object> output = probe.receiveN(4);
    List<Pair<String, Integer>> expected =
        Arrays.asList(
            new Pair<>("A", 1), new Pair<>("B", 2), new Pair<>("C", 3), new Pair<>("MISSING", 4));
    assertEquals(expected, output);
  }

  @Test
  public void mustBeAbleToUseConcat() {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<String> input2 = Arrays.asList("D", "E", "F");

    final Source<String, NotUsed> in1 = Source.from(input1);
    final Source<String, NotUsed> in2 = Source.from(input2);
    final Flow<String, String, NotUsed> flow = Flow.of(String.class);
    in1.via(flow.concat(in2))
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
  public void mustBeAbleToUsePrepend() {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<String> input2 = Arrays.asList("D", "E", "F");

    final Source<String, NotUsed> in1 = Source.from(input1);
    final Source<String, NotUsed> in2 = Source.from(input2);
    final Flow<String, String, NotUsed> flow = Flow.of(String.class);
    in2.via(flow.prepend(in1))
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
  public void mustBeAbleToUsePrefixAndTail() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6);
    final Flow<Integer, Pair<List<Integer>, Source<Integer, NotUsed>>, NotUsed> flow =
        Flow.of(Integer.class).prefixAndTail(3);
    CompletionStage<Pair<List<Integer>, Source<Integer, NotUsed>>> future =
        Source.from(input).via(flow).runWith(Sink.head(), system);
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

    final Flow<Source<Integer, NotUsed>, List<Integer>, NotUsed> flow =
        Flow.<Source<Integer, NotUsed>>create()
            .flatMapConcat(ConstantFun.javaIdentityFunction())
            .grouped(6);
    CompletionStage<List<Integer>> future =
        Source.from(mainInputs).via(flow).runWith(Sink.head(), system);

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

    final Flow<Source<Integer, NotUsed>, List<Integer>, NotUsed> flow =
        Flow.<Source<Integer, NotUsed>>create()
            .flatMapMerge(3, ConstantFun.javaIdentityFunction())
            .grouped(60);
    CompletionStage<List<Integer>> future =
        Source.from(mainInputs).via(flow).runWith(Sink.head(), system);

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
    final Flow<String, List<String>, NotUsed> flow =
        Flow.of(String.class).buffer(2, OverflowStrategy.backpressure()).grouped(4);
    final CompletionStage<List<String>> future =
        Source.from(input).via(flow).runWith(Sink.head(), system);

    List<String> result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(input, result);
  }

  @Test
  public void mustBeAbleToUseWatchTermination() throws Exception {
    final List<String> input = Arrays.asList("A", "B", "C");
    CompletionStage<Done> future =
        Source.from(input).watchTermination(Keep.right()).to(Sink.ignore()).run(system);

    assertEquals(done(), future.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void mustBeAbleToUseConflate() throws Exception {
    final TestKit probe = new TestKit(system);
    final List<String> input = Arrays.asList("A", "B", "C");
    final Flow<String, String, NotUsed> flow =
        Flow.of(String.class)
            .conflateWithSeed(
                new Function<String, String>() {
                  @Override
                  public String apply(String s) throws Exception {
                    return s;
                  }
                },
                new Function2<String, String, String>() {
                  @Override
                  public String apply(String aggr, String in) throws Exception {
                    return aggr + in;
                  }
                });
    CompletionStage<String> future =
        Source.from(input).via(flow).runFold("", (aggr, in) -> aggr + in, system);
    String result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals("ABC", result);

    final Flow<String, String, NotUsed> flow2 = Flow.of(String.class).conflate((a, b) -> a + b);

    CompletionStage<String> future2 =
        Source.from(input).via(flow2).runFold("", (a, b) -> a + b, system);
    String result2 = future2.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals("ABC", result2);
  }

  @Test
  public void mustBeAbleToUseBatch() throws Exception {
    final TestKit probe = new TestKit(system);
    final List<String> input = Arrays.asList("A", "B", "C");
    final Flow<String, String, NotUsed> flow =
        Flow.of(String.class)
            .batch(
                3L,
                new Function<String, String>() {
                  @Override
                  public String apply(String s) throws Exception {
                    return s;
                  }
                },
                new Function2<String, String, String>() {
                  @Override
                  public String apply(String aggr, String in) throws Exception {
                    return aggr + in;
                  }
                });
    CompletionStage<String> future =
        Source.from(input).via(flow).runFold("", (aggr, in) -> aggr + in, system);
    String result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals("ABC", result);
  }

  @Test
  public void mustBeAbleToUseBatchWeighted() throws Exception {
    final TestKit probe = new TestKit(system);
    final List<String> input = Arrays.asList("A", "B", "C");
    final Flow<String, String, NotUsed> flow =
        Flow.of(String.class)
            .batchWeighted(
                3L,
                new Function<String, java.lang.Long>() {
                  @Override
                  public java.lang.Long apply(String s) throws Exception {
                    return 1L;
                  }
                },
                new Function<String, String>() {
                  @Override
                  public String apply(String s) throws Exception {
                    return s;
                  }
                },
                new Function2<String, String, String>() {
                  @Override
                  public String apply(String aggr, String in) throws Exception {
                    return aggr + in;
                  }
                });
    CompletionStage<String> future =
        Source.from(input).via(flow).runFold("", (aggr, in) -> aggr + in, system);
    String result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals("ABC", result);
  }

  @Test
  public void mustBeAbleToUseExpand() throws Exception {
    final TestKit probe = new TestKit(system);
    final List<String> input = Arrays.asList("A", "B", "C");
    final Flow<String, String, NotUsed> flow =
        Flow.of(String.class).expand(in -> Stream.iterate(in, i -> i).iterator());
    final Sink<String, CompletionStage<String>> sink = Sink.head();
    CompletionStage<String> future = Source.from(input).via(flow).runWith(sink, system);
    String result = future.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals("A", result);
  }

  @Test
  public void mustBeAbleToUseMapAsync() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input = Arrays.asList("a", "b", "c");
    final Flow<String, String, NotUsed> flow =
        Flow.of(String.class)
            .mapAsync(4, elem -> CompletableFuture.completedFuture(elem.toUpperCase()));
    Source.from(input)
        .via(flow)
        .runForeach(
            new Procedure<String>() {
              public void apply(String elem) {
                probe.getRef().tell(elem, ActorRef.noSender());
              }
            },
            system);
    probe.expectMsgEquals("A");
    probe.expectMsgEquals("B");
    probe.expectMsgEquals("C");
  }

  @Test
  public void mustBeAbleToUseMapAsyncForFutureWithNullResult() throws Exception {
    final Iterable<Integer> input = Arrays.asList(1, 2, 3);
    Flow<Integer, Void, NotUsed> flow =
        Flow.of(Integer.class).mapAsync(1, x -> CompletableFuture.completedFuture(null));
    List<Void> result =
        Source.from(input)
            .via(flow)
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

    assertEquals(0, result.size());
  }

  @Test
  public void mustBeAbleToUseCollectType() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<FlowSpec.Fruit> input =
        Arrays.asList(new FlowSpec.Apple(), new FlowSpec.Orange());

    Source.from(input)
        .via(Flow.of(FlowSpec.Fruit.class).collectType(FlowSpec.Apple.class))
        .runForeach((apple) -> probe.getRef().tell(apple, ActorRef.noSender()), system);
    probe.expectMsgAnyClassOf(FlowSpec.Apple.class);
  }

  @Test
  public void mustBeAbleToRecover() throws Exception {
    final TestPublisher.ManualProbe<Integer> publisherProbe =
        TestPublisher.manualProbe(true, system);
    final TestKit probe = new TestKit(system);

    final Source<Integer, NotUsed> source = Source.fromPublisher(publisherProbe);
    final Flow<Integer, Integer, NotUsed> flow =
        Flow.of(Integer.class)
            .map(
                elem -> {
                  if (elem == 2) throw new RuntimeException("ex");
                  else return elem;
                })
            .recoverWithRetries(
                1,
                new JavaPartialFunction<Throwable, Graph<SourceShape<Integer>, NotUsed>>() {
                  public Graph<SourceShape<Integer>, NotUsed> apply(
                      Throwable elem, boolean isCheck) {
                    if (isCheck) return Source.empty();
                    return Source.single(0);
                  }
                });

    final CompletionStage<Done> future =
        source
            .via(flow)
            .runWith(Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    final PublisherProbeSubscription<Integer> s = publisherProbe.expectSubscription();

    s.sendNext(0);
    probe.expectMsgEquals(0);
    s.sendNext(1);
    probe.expectMsgEquals(1);
    s.sendNext(2);
    probe.expectMsgEquals(0);
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToRecoverClass() throws Exception {
    final TestPublisher.ManualProbe<Integer> publisherProbe =
        TestPublisher.manualProbe(true, system);
    final TestKit probe = new TestKit(system);

    final Source<Integer, NotUsed> source = Source.fromPublisher(publisherProbe);
    final Flow<Integer, Integer, NotUsed> flow =
        Flow.of(Integer.class)
            .map(
                elem -> {
                  if (elem == 2) throw new RuntimeException("ex");
                  else return elem;
                })
            .recoverWithRetries(1, RuntimeException.class, () -> Source.single(0));

    final CompletionStage<Done> future =
        source
            .via(flow)
            .runWith(Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    final PublisherProbeSubscription<Integer> s = publisherProbe.expectSubscription();

    s.sendNext(0);
    probe.expectMsgEquals(0);
    s.sendNext(1);
    probe.expectMsgEquals(1);
    s.sendNext(2);
    probe.expectMsgEquals(0);
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToRecoverWithSource() throws Exception {
    final TestPublisher.ManualProbe<Integer> publisherProbe =
        TestPublisher.manualProbe(true, system);
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> recover = Arrays.asList(55, 0);

    final Source<Integer, NotUsed> source = Source.fromPublisher(publisherProbe);
    final Flow<Integer, Integer, NotUsed> flow =
        Flow.of(Integer.class)
            .map(
                elem -> {
                  if (elem == 2) throw new RuntimeException("ex");
                  else return elem;
                })
            .recoverWith(
                new JavaPartialFunction<Throwable, Source<Integer, NotUsed>>() {
                  public Source<Integer, NotUsed> apply(Throwable elem, boolean isCheck) {
                    if (isCheck) return null;
                    return Source.from(recover);
                  }
                });

    final CompletionStage<Done> future =
        source
            .via(flow)
            .runWith(Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    final PublisherProbeSubscription<Integer> s = publisherProbe.expectSubscription();

    s.sendNext(0);
    probe.expectMsgEquals(0);
    s.sendNext(1);
    probe.expectMsgEquals(1);
    s.sendNext(2);
    probe.expectMsgEquals(55);
    probe.expectMsgEquals(0);
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToRecoverWithClass() throws Exception {
    final TestPublisher.ManualProbe<Integer> publisherProbe =
        TestPublisher.manualProbe(true, system);
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> recover = Arrays.asList(55, 0);
    final int maxRetries = 10;

    final Source<Integer, NotUsed> source = Source.fromPublisher(publisherProbe);
    final Flow<Integer, Integer, NotUsed> flow =
        Flow.of(Integer.class)
            .map(
                elem -> {
                  if (elem == 2) throw new RuntimeException("ex");
                  else return elem;
                })
            .recoverWithRetries(maxRetries, RuntimeException.class, () -> Source.from(recover));

    final CompletionStage<Done> future =
        source
            .via(flow)
            .runWith(Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    final PublisherProbeSubscription<Integer> s = publisherProbe.expectSubscription();

    s.sendNext(0);
    probe.expectMsgEquals(0);
    s.sendNext(1);
    probe.expectMsgEquals(1);
    s.sendNext(2);
    probe.expectMsgEquals(55);
    probe.expectMsgEquals(0);
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToRecoverWithRetries() throws Exception {
    final TestPublisher.ManualProbe<Integer> publisherProbe =
        TestPublisher.manualProbe(true, system);
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> recover = Arrays.asList(55, 0);

    final Source<Integer, NotUsed> source = Source.fromPublisher(publisherProbe);
    final Flow<Integer, Integer, NotUsed> flow =
        Flow.of(Integer.class)
            .map(
                elem -> {
                  if (elem == 2) throw new RuntimeException("ex");
                  else return elem;
                })
            .recoverWithRetries(
                3,
                new PFBuilder<Throwable, Graph<SourceShape<Integer>, NotUsed>>()
                    .match(RuntimeException.class, ex -> Source.from(recover))
                    .build());

    final CompletionStage<Done> future =
        source
            .via(flow)
            .runWith(Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    final PublisherProbeSubscription<Integer> s = publisherProbe.expectSubscription();

    s.sendNext(0);
    probe.expectMsgEquals(0);
    s.sendNext(1);
    probe.expectMsgEquals(1);
    s.sendNext(2);
    probe.expectMsgEquals(55);
    probe.expectMsgEquals(0);
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToRecoverWithRetriesClass() throws Exception {
    final TestPublisher.ManualProbe<Integer> publisherProbe =
        TestPublisher.manualProbe(true, system);
    final TestKit probe = new TestKit(system);
    final Iterable<Integer> recover = Arrays.asList(55, 0);

    final Source<Integer, NotUsed> source = Source.fromPublisher(publisherProbe);
    final Flow<Integer, Integer, NotUsed> flow =
        Flow.of(Integer.class)
            .map(
                elem -> {
                  if (elem == 2) throw new RuntimeException("ex");
                  else return elem;
                })
            .recoverWithRetries(3, RuntimeException.class, () -> Source.from(recover));

    final CompletionStage<Done> future =
        source
            .via(flow)
            .runWith(Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), system);

    final PublisherProbeSubscription<Integer> s = publisherProbe.expectSubscription();

    s.sendNext(0);
    probe.expectMsgEquals(0);
    s.sendNext(1);
    probe.expectMsgEquals(1);
    s.sendNext(2);
    probe.expectMsgEquals(55);
    probe.expectMsgEquals(0);
    future.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void mustBeAbleToMapErrorClass() {
    final String head = "foo";
    final Source<Optional<String>, NotUsed> source =
        Source.from(Arrays.asList(Optional.of(head), Optional.empty()));
    final IllegalArgumentException boom = new IllegalArgumentException("boom");
    final Flow<Optional<String>, String, NotUsed> flow =
        Flow.<Optional<String>, String>fromFunction(Optional::get)
            .mapError(NoSuchElementException.class, (NoSuchElementException e) -> boom);

    source
        .via(flow)
        .runWith(TestSink.create(system), system)
        .request(2)
        .expectNext(head)
        .expectError(boom);
  }

  @Test
  public void mustBeAbleToMapErrorClassExactly() {
    final Source<String, NotUsed> source = Source.single("foo");
    final Flow<String, Character, NotUsed> flow =
        Flow.<String, Character>fromFunction(str -> str.charAt(-1))
            .mapError(NoSuchElementException.class, IllegalArgumentException::new);

    final Throwable actual =
        source.via(flow).runWith(TestSink.probe(system), system).request(1).expectError();
    org.junit.Assert.assertTrue(actual instanceof IndexOutOfBoundsException);
  }

  @Test
  public void mustBeAbleToMapErrorSuperClass() {
    final String head = "foo";
    final Source<Optional<String>, NotUsed> source =
        Source.from(Arrays.asList(Optional.of(head), Optional.empty()));
    final IllegalArgumentException boom = new IllegalArgumentException("boom");
    final Flow<Optional<String>, String, NotUsed> flow =
        Flow.<Optional<String>, String>fromFunction(Optional::get)
            .mapError(RuntimeException.class, (RuntimeException e) -> boom);

    source
        .via(flow)
        .runWith(TestSink.probe(system), system)
        .request(2)
        .expectNext(head)
        .expectError(boom);
  }

  @Test
  public void mustBeAbleToMaterializeIdentityWithJavaFlow() throws Exception {
    final TestKit probe = new TestKit(system);
    final List<String> input = Arrays.asList("A", "B", "C");

    Flow<String, String, NotUsed> otherFlow = Flow.of(String.class);

    Flow<String, String, NotUsed> myFlow = Flow.of(String.class).via(otherFlow);
    Source.from(input)
        .via(myFlow)
        .runWith(
            Sink.foreach(
                new Procedure<String>() { // Scala Future
                  public void apply(String elem) {
                    probe.getRef().tell(elem, ActorRef.noSender());
                  }
                }),
            system);

    probe.expectMsgAllOf("A", "B", "C");
  }

  @Test
  public void mustBeAbleToMaterializeIdentityToJavaSink() throws Exception {
    final TestKit probe = new TestKit(system);
    final List<String> input = Arrays.asList("A", "B", "C");
    Flow<String, String, NotUsed> otherFlow = Flow.of(String.class);

    Sink<String, NotUsed> sink =
        Flow.of(String.class)
            .to(
                otherFlow.to(
                    Sink.foreach(
                        new Procedure<String>() { // Scala Future
                          public void apply(String elem) {
                            probe.getRef().tell(elem, ActorRef.noSender());
                          }
                        })));

    Source.from(input).to(sink).run(system);
    probe.expectMsgAllOf("A", "B", "C");
  }

  @Test
  public void mustBeAbleToBroadcastEagerCancel() throws Exception {
    final Sink<String, NotUsed> sink =
        Sink.fromGraph(
            GraphDSL.create(
                new Function<GraphDSL.Builder<NotUsed>, SinkShape<String>>() {
                  @Override
                  public SinkShape<String> apply(Builder<NotUsed> b) throws Exception {
                    final UniformFanOutShape<String, String> broadcast =
                        b.add(Broadcast.create(2, true));
                    final SinkShape<String> out1 = b.add(Sink.cancelled());
                    final SinkShape<String> out2 = b.add(Sink.ignore());
                    b.from(broadcast.out(0)).to(out1);
                    b.from(broadcast.out(1)).to(out2);
                    return new SinkShape<>(broadcast.in());
                  }
                }));

    final TestKit probe = new TestKit(system);
    @SuppressWarnings("deprecation")
    Source<String, ActorRef> source =
        Source.actorRef(
            msg -> Optional.empty(), msg -> Optional.empty(), 1, OverflowStrategy.dropNew());
    final ActorRef actor = source.toMat(sink, Keep.left()).run(system);
    probe.watch(actor);
    probe.expectTerminated(actor);
  }

  @Test
  public void mustBeAbleToUseZipWith() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<String> input2 = Arrays.asList("D", "E", "F");

    Source.from(input1)
        .via(
            Flow.of(String.class)
                .zipWith(
                    Source.from(input2),
                    new Function2<String, String, String>() {
                      public String apply(String s1, String s2) {
                        return s1 + "-" + s2;
                      }
                    }))
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
  public void mustBeAbleToUseZip2() throws Exception {
    final TestKit probe = new TestKit(system);
    final Iterable<String> input1 = Arrays.asList("A", "B", "C");
    final Iterable<String> input2 = Arrays.asList("D", "E", "F");

    Source.from(input1)
        .via(Flow.of(String.class).zip(Source.from(input2)))
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
        .via(Flow.of(String.class).merge(Source.from(input2)))
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
  public void mustBeAbleToUseInitialTimeout() {
    ExecutionException executionException =
        Assert.assertThrows(
            ExecutionException.class,
            () ->
                Source.<Integer>maybe()
                    .via(Flow.of(Integer.class).initialTimeout(Duration.ofSeconds(1)))
                    .runWith(Sink.head(), system)
                    .toCompletableFuture()
                    .get(3, TimeUnit.SECONDS));
    assertTrue(
        "A TimeoutException was expected",
        TimeoutException.class.isAssignableFrom(executionException.getCause().getClass()));
  }

  @Test
  public void mustBeAbleToUseCompletionTimeout() {
    ExecutionException executionException =
        Assert.assertThrows(
            ExecutionException.class,
            () ->
                Source.<Integer>maybe()
                    .via(Flow.of(Integer.class).completionTimeout(Duration.ofSeconds(1)))
                    .runWith(Sink.head(), system)
                    .toCompletableFuture()
                    .get(3, TimeUnit.SECONDS));
    assertTrue(
        "A TimeoutException was expected",
        TimeoutException.class.isAssignableFrom(executionException.getCause().getClass()));
  }

  @Test
  public void mustBeAbleToUseIdleTimeout() {
    ExecutionException executionException =
        Assert.assertThrows(
            ExecutionException.class,
            () ->
                Source.<Integer>maybe()
                    .via(Flow.of(Integer.class).idleTimeout(Duration.ofSeconds(1)))
                    .runWith(Sink.head(), system)
                    .toCompletableFuture()
                    .get(3, TimeUnit.SECONDS));
    assertTrue(
        "A TimeoutException was expected",
        TimeoutException.class.isAssignableFrom(executionException.getCause().getClass()));
  }

  @Test
  public void mustBeAbleToUseKeepAlive() throws Exception {
    Integer result =
        Source.<Integer>maybe()
            .via(
                Flow.of(Integer.class).keepAlive(Duration.ofSeconds(1), (Creator<Integer>) () -> 0))
            .takeWithin(Duration.ofMillis(1500))
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

    assertEquals((Object) 0, result);
  }

  @Test
  public void shouldBePossibleToCreateFromFunction() throws Exception {
    List<Integer> out =
        Source.range(0, 2)
            .via(Flow.fromFunction((Integer x) -> x + 1))
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

    assertEquals(Arrays.asList(1, 2, 3), out);
  }

  @Test
  public void mustSuitablyOverrideAttributeHandlingMethods() {
    @SuppressWarnings("unused")
    final Flow<Integer, Integer, NotUsed> f =
        Flow.of(Integer.class)
            .withAttributes(Attributes.name(""))
            .addAttributes(Attributes.asyncBoundary())
            .named("");
  }

  @Test
  public void mustBeAbleToUseAlsoTo() {
    final Flow<Integer, Integer, NotUsed> f = Flow.of(Integer.class).alsoTo(Sink.ignore());
    final Flow<Integer, Integer, String> f2 =
        Flow.of(Integer.class).alsoToMat(Sink.ignore(), (i, n) -> "foo");
  }

  @Test
  public void mustBeAbleToUseAlsoToAll() {
    final Flow<Integer, Integer, NotUsed> f =
        Flow.of(Integer.class).alsoToAll(Sink.ignore(), Sink.ignore());
  }

  @Test
  public void mustBeAbleToUseDivertTo() {
    final Flow<Integer, Integer, NotUsed> f =
        Flow.of(Integer.class).divertTo(Sink.ignore(), e -> true);
    final Flow<Integer, Integer, String> f2 =
        Flow.of(Integer.class).divertToMat(Sink.ignore(), e -> true, (i, n) -> "foo");
  }

  @Test
  public void mustBeAbleToUseLazyInit() throws Exception {
    final CompletionStage<Flow<Integer, Integer, NotUsed>> future = new CompletableFuture<>();
    future.toCompletableFuture().complete(Flow.fromFunction((id) -> id));
    Integer result =
        Source.range(1, 10)
            .via(Flow.lazyCompletionStageFlow(() -> future))
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

    assertEquals((Object) 1, result);
  }

  @Test
  public void mustBeAbleToConvertToJavaInJava() {
    final akka.stream.scaladsl.Flow<Integer, Integer, NotUsed> scalaFlow =
        akka.stream.scaladsl.Flow.apply();
    Flow<Integer, Integer, NotUsed> javaFlow = scalaFlow.asJava();
  }
}
