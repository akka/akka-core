/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.stream.scaladsl.MergePreferred.MergePreferredShape;
import akka.testkit.javadsl.TestKit;
import java.util.Arrays;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphCyclesDocTest extends AbstractJavaTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("GraphCyclesDocTest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  static final SilenceSystemOut.System System = SilenceSystemOut.get();

  final Source<Integer, NotUsed> source = Source.from(Arrays.asList(1, 2, 3, 4, 5));

  @Test
  public void demonstrateDeadlockedCycle() {
    // #deadlocked
    // WARNING! The graph below deadlocks!
    final Flow<Integer, Integer, NotUsed> printFlow =
        Flow.of(Integer.class)
            .map(
                s -> {
                  System.out.println(s);
                  return s;
                });

    RunnableGraph.fromGraph(
        GraphDSL.create(
            b -> {
              final UniformFanInShape<Integer, Integer> merge = b.add(Merge.create(2));
              final UniformFanOutShape<Integer, Integer> bcast = b.add(Broadcast.create(2));
              final Outlet<Integer> src = b.add(source).out();
              final FlowShape<Integer, Integer> printer = b.add(printFlow);
              final SinkShape<Integer> ignore = b.add(Sink.ignore());

              b.from(src).viaFanIn(merge).via(printer).viaFanOut(bcast).to(ignore);
              b.to(merge).fromFanOut(bcast);
              return ClosedShape.getInstance();
            }));
    // #deadlocked
  }

  @Test
  public void demonstrateUnfairCycle() {
    final Flow<Integer, Integer, NotUsed> printFlow =
        Flow.of(Integer.class)
            .map(
                s -> {
                  System.out.println(s);
                  return s;
                });
    // #unfair
    // WARNING! The graph below stops consuming from "source" after a few steps
    RunnableGraph.fromGraph(
        GraphDSL.create(
            b -> {
              final MergePreferredShape<Integer> merge = b.add(MergePreferred.create(1));
              final UniformFanOutShape<Integer, Integer> bcast = b.add(Broadcast.create(2));
              final Outlet<Integer> src = b.add(source).out();
              final FlowShape<Integer, Integer> printer = b.add(printFlow);
              final SinkShape<Integer> ignore = b.add(Sink.ignore());

              b.from(src).viaFanIn(merge).via(printer).viaFanOut(bcast).to(ignore);
              b.to(merge.preferred()).fromFanOut(bcast);
              return ClosedShape.getInstance();
            }));
    // #unfair
  }

  @Test
  public void demonstrateDroppingCycle() {
    final Flow<Integer, Integer, NotUsed> printFlow =
        Flow.of(Integer.class)
            .map(
                s -> {
                  System.out.println(s);
                  return s;
                });
    // #dropping
    RunnableGraph.fromGraph(
        GraphDSL.create(
            b -> {
              final UniformFanInShape<Integer, Integer> merge = b.add(Merge.create(2));
              final UniformFanOutShape<Integer, Integer> bcast = b.add(Broadcast.create(2));
              final FlowShape<Integer, Integer> droppyFlow =
                  b.add(Flow.of(Integer.class).buffer(10, OverflowStrategy.dropHead()));
              final Outlet<Integer> src = b.add(source).out();
              final FlowShape<Integer, Integer> printer = b.add(printFlow);
              final SinkShape<Integer> ignore = b.add(Sink.ignore());

              b.from(src).viaFanIn(merge).via(printer).viaFanOut(bcast).to(ignore);
              b.to(merge).via(droppyFlow).fromFanOut(bcast);
              return ClosedShape.getInstance();
            }));
    // #dropping
  }

  @Test
  public void demonstrateZippingCycle() {
    final Flow<Integer, Integer, NotUsed> printFlow =
        Flow.of(Integer.class)
            .map(
                s -> {
                  System.out.println(s);
                  return s;
                });
    // #zipping-dead
    // WARNING! The graph below never processes any elements
    RunnableGraph.fromGraph(
        GraphDSL.create(
            b -> {
              final FanInShape2<Integer, Integer, Integer> zip =
                  b.add(ZipWith.create((Integer left, Integer right) -> left));
              final UniformFanOutShape<Integer, Integer> bcast = b.add(Broadcast.create(2));
              final FlowShape<Integer, Integer> printer = b.add(printFlow);
              final SinkShape<Integer> ignore = b.add(Sink.ignore());

              b.from(b.add(source)).toInlet(zip.in0());
              b.from(zip.out()).via(printer).viaFanOut(bcast).to(ignore);
              b.to(zip.in1()).fromFanOut(bcast);
              return ClosedShape.getInstance();
            }));
    // #zipping-dead
  }

  @Test
  public void demonstrateLiveZippingCycle() {
    final Flow<Integer, Integer, NotUsed> printFlow =
        Flow.of(Integer.class)
            .map(
                s -> {
                  System.out.println(s);
                  return s;
                });
    // #zipping-live
    RunnableGraph.fromGraph(
        GraphDSL.create(
            b -> {
              final FanInShape2<Integer, Integer, Integer> zip =
                  b.add(ZipWith.create((Integer left, Integer right) -> left));
              final UniformFanOutShape<Integer, Integer> bcast = b.add(Broadcast.create(2));
              final UniformFanInShape<Integer, Integer> concat = b.add(Concat.create());
              final FlowShape<Integer, Integer> printer = b.add(printFlow);
              final SinkShape<Integer> ignore = b.add(Sink.ignore());

              b.from(b.add(source)).toInlet(zip.in0());
              b.from(zip.out()).via(printer).viaFanOut(bcast).to(ignore);
              b.to(zip.in1()).viaFanIn(concat).from(b.add(Source.single(1)));
              b.to(concat).fromFanOut(bcast);
              return ClosedShape.getInstance();
            }));
    // #zipping-live
  }
}
