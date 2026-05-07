/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.*;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class GraphDslTest extends StreamTest {
  public GraphDslTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("GraphDslTest", AkkaSpec.testConf());

  @Test
  public void demonstrateBuildSimpleGraph() throws Exception {
    // #simple-graph-dsl
    final Source<Integer, NotUsed> in = Source.from(Arrays.asList(1, 2, 3, 4, 5));
    final Sink<List<String>, CompletionStage<List<String>>> sink = Sink.head();
    final Flow<Integer, Integer, NotUsed> f1 = Flow.of(Integer.class).map(elem -> elem + 10);
    final Flow<Integer, Integer, NotUsed> f2 = Flow.of(Integer.class).map(elem -> elem + 20);
    final Flow<Integer, String, NotUsed> f3 = Flow.of(Integer.class).map(elem -> elem.toString());
    final Flow<Integer, Integer, NotUsed> f4 = Flow.of(Integer.class).map(elem -> elem + 30);

    final RunnableGraph<CompletionStage<List<String>>> result =
        RunnableGraph.fromGraph(
            GraphDSL // create() function binds sink, out which is sink's out port and builder DSL
                .create( // we need to reference out's shape in the builder DSL below (in to()
                // function)
                sink, // previously created sink (Sink)
                (builder, out) -> { // variables: builder (GraphDSL.Builder) and out (SinkShape)
                  final UniformFanOutShape<Integer, Integer> bcast =
                      builder.add(Broadcast.create(2));
                  final UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(2));

                  final Outlet<Integer> source = builder.add(in).out();
                  builder
                      .from(source)
                      .via(builder.add(f1))
                      .viaFanOut(bcast)
                      .via(builder.add(f2))
                      .viaFanIn(merge)
                      .via(builder.add(f3.grouped(1000)))
                      .to(out); // to() expects a SinkShape
                  builder.from(bcast).via(builder.add(f4)).toFanIn(merge);
                  return ClosedShape.getInstance();
                }));
    // #simple-graph-dsl
    final List<String> list = result.run(system).toCompletableFuture().get(3, TimeUnit.SECONDS);
    final String[] res = list.toArray(new String[] {});
    Arrays.sort(res, null);
    assertArrayEquals(
        new String[] {"31", "32", "33", "34", "35", "41", "42", "43", "44", "45"}, res);
  }

  @Test
  @SuppressWarnings("unused")
  public void demonstrateConnectErrors() {
    IllegalStateException exception =
        Assert.assertThrows(
            "expected IllegalStateException",
            IllegalStateException.class,
            () -> {
              // #simple-graph
              final RunnableGraph<NotUsed> g =
                  RunnableGraph.fromGraph(
                      GraphDSL.create(
                          (b) -> {
                            final SourceShape<Integer> source1 =
                                b.add(Source.from(Arrays.asList(1, 2, 3, 4, 5)));
                            final SourceShape<Integer> source2 =
                                b.add(Source.from(Arrays.asList(1, 2, 3, 4, 5)));
                            final FanInShape2<Integer, Integer, Pair<Integer, Integer>> zip =
                                b.add(Zip.create());
                            b.from(source1).toInlet(zip.in0());
                            b.from(source2).toInlet(zip.in1());
                            return ClosedShape.getInstance();
                          }));
              // unconnected zip.out (!) => "The inlets [] and outlets [] must correspond to the
              // inlets []
              // and outlets [ZipWith2.out]"
              // #simple-graph
            });
    assertNotNull(exception.getMessage());
    assertTrue(exception.getMessage().contains("ZipWith2.out"));
  }

  @Test
  public void demonstrateReusingFlowInGraph() throws Exception {
    // #graph-dsl-reusing-a-flow
    final Sink<Integer, CompletionStage<Integer>> topHeadSink = Sink.head();
    final Sink<Integer, CompletionStage<Integer>> bottomHeadSink = Sink.head();
    final Flow<Integer, Integer, NotUsed> sharedDoubler =
        Flow.of(Integer.class).map(elem -> elem * 2);

    final RunnableGraph<Pair<CompletionStage<Integer>, CompletionStage<Integer>>> g =
        RunnableGraph.fromGraph(
            GraphDSL.create(
                topHeadSink, // import this sink into the graph
                bottomHeadSink, // and this as well
                Keep.both(),
                (b, top, bottom) -> {
                  final UniformFanOutShape<Integer, Integer> bcast = b.add(Broadcast.create(2));

                  b.from(b.add(Source.single(1)))
                      .viaFanOut(bcast)
                      .via(b.add(sharedDoubler))
                      .to(top);
                  b.from(bcast).via(b.add(sharedDoubler)).to(bottom);
                  return ClosedShape.getInstance();
                }));
    // #graph-dsl-reusing-a-flow
    final Pair<CompletionStage<Integer>, CompletionStage<Integer>> pair = g.run(system);
    assertEquals(Integer.valueOf(2), pair.first().toCompletableFuture().get(3, TimeUnit.SECONDS));
    assertEquals(Integer.valueOf(2), pair.second().toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void demonstrateMatValue() throws Exception {
    // #graph-dsl-matvalue
    final Sink<Integer, CompletionStage<Integer>> foldSink =
        Sink.fold(
            0,
            (a, b) -> {
              return a + b;
            });

    final Flow<CompletionStage<Integer>, Integer, NotUsed> flatten =
        Flow.<CompletionStage<Integer>>create().mapAsync(4, x -> x);

    final Flow<Integer, Integer, CompletionStage<Integer>> foldingFlow =
        Flow.fromGraph(
            GraphDSL.create(
                foldSink,
                (b, fold) -> {
                  return FlowShape.of(
                      fold.in(), b.from(b.materializedValue()).via(b.add(flatten)).out());
                }));
    // #graph-dsl-matvalue

    // #graph-dsl-matvalue-cycle
    // This cannot produce any value:
    final Source<Integer, CompletionStage<Integer>> cyclicSource =
        Source.fromGraph(
            GraphDSL.create(
                foldSink,
                (b, fold) -> {
                  // - Fold cannot complete until its upstream mapAsync completes
                  // - mapAsync cannot complete until the materialized Future produced by
                  //   fold completes
                  // As a result this Source will never emit anything, and its materialited
                  // Future will never complete
                  b.from(b.materializedValue()).via(b.add(flatten)).to(fold);
                  return SourceShape.of(b.from(b.materializedValue()).via(b.add(flatten)).out());
                }));

    // #graph-dsl-matvalue-cycle
  }

  @Test
  public void beAbleToConstructClosedGraphFromList() throws Exception {
    // #graph-from-list
    // create the source
    final Source<String, NotUsed> in = Source.from(Arrays.asList("ax", "bx", "cx"));
    // generate the sinks from code
    List<String> prefixes = Arrays.asList("a", "b", "c");
    final List<Sink<String, CompletionStage<String>>> list = new ArrayList<>();
    for (String prefix : prefixes) {
      final Sink<String, CompletionStage<String>> sink =
          Flow.of(String.class)
              .filter(str -> str.startsWith(prefix))
              .toMat(Sink.head(), Keep.right());
      list.add(sink);
    }

    final RunnableGraph<List<CompletionStage<String>>> g =
        RunnableGraph.fromGraph(
            GraphDSL.create(
                list,
                (GraphDSL.Builder<List<CompletionStage<String>>> builder,
                    List<SinkShape<String>> outs) -> {
                  final UniformFanOutShape<String, String> bcast =
                      builder.add(Broadcast.create(outs.size()));

                  final Outlet<String> source = builder.add(in).out();
                  builder.from(source).viaFanOut(bcast);

                  for (SinkShape<String> sink : outs) {
                    builder.from(bcast).to(sink);
                  }

                  return ClosedShape.getInstance();
                }));
    List<CompletionStage<String>> result = g.run(system);
    // #graph-from-list

    assertEquals(3, result.size());
    assertEquals("ax", result.get(0).toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertEquals("bx", result.get(1).toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertEquals("cx", result.get(2).toCompletableFuture().get(1, TimeUnit.SECONDS));
  }

  @Test
  public void canUseMapMaterializedValueOnGraphs() {
    Graph<SourceShape<Object>, NotUsed> srcGraph = Source.empty();
    // FIXME https://github.com/akka/akka-core/issues/30487
    // it looks like the Graph.mapMaterializedValue static forwarder is no longer
    // generated on Scala 3
    //    Graph<SourceShape<Object>, Pair<NotUsed, NotUsed>> mappedMatValueSrcGraph =
    //        Graph.mapMaterializedValue(srcGraph, notUsed -> new Pair<>(notUsed, notUsed));
    //    Sink<Object, CompletionStage<Done>> snk = Sink.ignore();
    //    Pair<NotUsed, NotUsed> pair =
    // Source.fromGraph(mappedMatValueSrcGraph).to(snk).run(system);
    //    assertEquals(pair, new Pair<>(NotUsed.getInstance(), NotUsed.getInstance()));
  }
}
