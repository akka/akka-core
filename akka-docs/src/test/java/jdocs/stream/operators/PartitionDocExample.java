/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators;

import akka.actor.ActorSystem;

// #import
import akka.NotUsed;
import akka.stream.Attributes;
import akka.stream.ClosedShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Partition;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
// #import

public class PartitionDocExample {

  private final ActorSystem system = ActorSystem.create("PartitionDocExample");

  void partitionExample() {

    // #partition

    Source<Integer, NotUsed> source = Source.range(1, 10);

    Sink<Integer, NotUsed> even =
        Flow.of(Integer.class)
            .log("even")
            .withAttributes(Attributes.createLogLevels(Attributes.logLevelInfo()))
            .to(Sink.ignore());
    Sink<Integer, NotUsed> odd =
        Flow.of(Integer.class)
            .log("odd")
            .withAttributes(Attributes.createLogLevels(Attributes.logLevelInfo()))
            .to(Sink.ignore());

    RunnableGraph.fromGraph(
            GraphDSL.create(
                builder -> {
                  UniformFanOutShape<Integer, Integer> partition =
                      builder.add(
                          Partition.create(
                              Integer.class, 2, element -> (element % 2 == 0) ? 0 : 1));
                  builder.from(builder.add(source)).viaFanOut(partition);
                  builder.from(partition.out(0)).to(builder.add(even));
                  builder.from(partition.out(1)).to(builder.add(odd));
                  return ClosedShape.getInstance();
                }))
        .run(system);

    // #partition
  }
}
