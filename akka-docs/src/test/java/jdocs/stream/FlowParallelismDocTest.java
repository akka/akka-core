/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.testkit.javadsl.TestKit;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class FlowParallelismDocTest extends AbstractJavaTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("FlowParallellismDocTest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  static class ScoopOfBatter {}

  static class HalfCookedPancake {}

  static class Pancake {}

  // #pipelining
  Flow<ScoopOfBatter, HalfCookedPancake, NotUsed> fryingPan1 =
      Flow.of(ScoopOfBatter.class).map(batter -> new HalfCookedPancake());

  Flow<HalfCookedPancake, Pancake, NotUsed> fryingPan2 =
      Flow.of(HalfCookedPancake.class).map(halfCooked -> new Pancake());
  // #pipelining

  @Test
  public void demonstratePipelining() {
    // #pipelining

    // With the two frying pans we can fully cook pancakes
    Flow<ScoopOfBatter, Pancake, NotUsed> pancakeChef = fryingPan1.async().via(fryingPan2.async());
    // #pipelining
  }

  @Test
  public void demonstrateParallelism() {
    // #parallelism
    Flow<ScoopOfBatter, Pancake, NotUsed> fryingPan =
        Flow.of(ScoopOfBatter.class).map(batter -> new Pancake());

    Flow<ScoopOfBatter, Pancake, NotUsed> pancakeChef =
        Flow.fromGraph(
            GraphDSL.create(
                b -> {
                  final UniformFanInShape<Pancake, Pancake> mergePancakes = b.add(Merge.create(2));
                  final UniformFanOutShape<ScoopOfBatter, ScoopOfBatter> dispatchBatter =
                      b.add(Balance.create(2));

                  // Using two frying pans in parallel, both fully cooking a pancake from the
                  // batter.
                  // We always put the next scoop of batter to the first frying pan that becomes
                  // available.
                  b.from(dispatchBatter.out(0))
                      .via(b.add(fryingPan.async()))
                      .toInlet(mergePancakes.in(0));
                  // Notice that we used the "fryingPan" flow without importing it via
                  // builder.add().
                  // Flows used this way are auto-imported, which in this case means that the two
                  // uses of "fryingPan" mean actually different stages in the graph.
                  b.from(dispatchBatter.out(1))
                      .via(b.add(fryingPan.async()))
                      .toInlet(mergePancakes.in(1));

                  return FlowShape.of(dispatchBatter.in(), mergePancakes.out());
                }));
    // #parallelism
  }

  @Test
  public void parallelPipeline() {
    // #parallel-pipeline
    Flow<ScoopOfBatter, Pancake, NotUsed> pancakeChef =
        Flow.fromGraph(
            GraphDSL.create(
                b -> {
                  final UniformFanInShape<Pancake, Pancake> mergePancakes = b.add(Merge.create(2));
                  final UniformFanOutShape<ScoopOfBatter, ScoopOfBatter> dispatchBatter =
                      b.add(Balance.create(2));

                  // Using two pipelines, having two frying pans each, in total using
                  // four frying pans
                  b.from(dispatchBatter.out(0))
                      .via(b.add(fryingPan1.async()))
                      .via(b.add(fryingPan2.async()))
                      .toInlet(mergePancakes.in(0));

                  b.from(dispatchBatter.out(1))
                      .via(b.add(fryingPan1.async()))
                      .via(b.add(fryingPan2.async()))
                      .toInlet(mergePancakes.in(1));

                  return FlowShape.of(dispatchBatter.in(), mergePancakes.out());
                }));
    // #parallel-pipeline
  }

  @Test
  public void pipelinedParallel() {
    // #pipelined-parallel
    Flow<ScoopOfBatter, HalfCookedPancake, NotUsed> pancakeChefs1 =
        Flow.fromGraph(
            GraphDSL.create(
                b -> {
                  final UniformFanInShape<HalfCookedPancake, HalfCookedPancake> mergeHalfCooked =
                      b.add(Merge.create(2));
                  final UniformFanOutShape<ScoopOfBatter, ScoopOfBatter> dispatchBatter =
                      b.add(Balance.create(2));

                  // Two chefs work with one frying pan for each, half-frying the pancakes then
                  // putting
                  // them into a common pool
                  b.from(dispatchBatter.out(0))
                      .via(b.add(fryingPan1.async()))
                      .toInlet(mergeHalfCooked.in(0));
                  b.from(dispatchBatter.out(1))
                      .via(b.add(fryingPan1.async()))
                      .toInlet(mergeHalfCooked.in(1));

                  return FlowShape.of(dispatchBatter.in(), mergeHalfCooked.out());
                }));

    Flow<HalfCookedPancake, Pancake, NotUsed> pancakeChefs2 =
        Flow.fromGraph(
            GraphDSL.create(
                b -> {
                  final UniformFanInShape<Pancake, Pancake> mergePancakes = b.add(Merge.create(2));
                  final UniformFanOutShape<HalfCookedPancake, HalfCookedPancake>
                      dispatchHalfCooked = b.add(Balance.create(2));

                  // Two chefs work with one frying pan for each, finishing the pancakes then
                  // putting
                  // them into a common pool
                  b.from(dispatchHalfCooked.out(0))
                      .via(b.add(fryingPan2.async()))
                      .toInlet(mergePancakes.in(0));
                  b.from(dispatchHalfCooked.out(1))
                      .via(b.add(fryingPan2.async()))
                      .toInlet(mergePancakes.in(1));

                  return FlowShape.of(dispatchHalfCooked.in(), mergePancakes.out());
                }));

    Flow<ScoopOfBatter, Pancake, NotUsed> kitchen = pancakeChefs1.via(pancakeChefs2);
    // #pipelined-parallel
  }
}
