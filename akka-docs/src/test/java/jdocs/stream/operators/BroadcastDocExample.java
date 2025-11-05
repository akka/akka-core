/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators;

import akka.actor.ActorSystem;

// #import
import akka.NotUsed;
import akka.japi.tuple.Tuple3;
import akka.stream.ClosedShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.util.concurrent.CompletionStage;
// #import

public class BroadcastDocExample {

  private final ActorSystem system = ActorSystem.create("BroadcastDocExample");

  void broadcastExample() {

    // #broadcast

    Source<Integer, NotUsed> source = Source.range(1, 10);

    Sink<Integer, CompletionStage<Integer>> countSink =
        Flow.of(Integer.class).toMat(Sink.fold(0, (acc, elem) -> acc + 1), Keep.right());
    Sink<Integer, CompletionStage<Integer>> minSink =
        Flow.of(Integer.class).toMat(Sink.fold(0, Math::min), Keep.right());
    Sink<Integer, CompletionStage<Integer>> maxSink =
        Flow.of(Integer.class).toMat(Sink.fold(0, Math::max), Keep.right());

    final Tuple3<CompletionStage<Integer>, CompletionStage<Integer>, CompletionStage<Integer>>
        result =
            RunnableGraph.fromGraph(
                    GraphDSL.create3(
                        countSink,
                        minSink,
                        maxSink,
                        Tuple3::create,
                        (builder, countS, minS, maxS) -> {
                          final UniformFanOutShape<Integer, Integer> broadcast =
                              builder.add(Broadcast.create(3));
                          builder.from(builder.add(source)).viaFanOut(broadcast);
                          builder.from(broadcast.out(0)).to(countS);
                          builder.from(broadcast.out(1)).to(minS);
                          builder.from(broadcast.out(2)).to(maxS);
                          return ClosedShape.getInstance();
                        }))
                .run(system);

    // #broadcast

    // #broadcast-async
    RunnableGraph.fromGraph(
        GraphDSL.create3(
            countSink.async(),
            minSink.async(),
            maxSink.async(),
            Tuple3::create,
            (builder, countS, minS, maxS) -> {
              final UniformFanOutShape<Integer, Integer> broadcast =
                  builder.add(Broadcast.create(3));
              builder.from(builder.add(source)).viaFanOut(broadcast);

              builder.from(broadcast.out(0)).to(countS);
              builder.from(broadcast.out(1)).to(minS);
              builder.from(broadcast.out(2)).to(maxS);
              return ClosedShape.getInstance();
            }));
    // #broadcast-async

  }
}
