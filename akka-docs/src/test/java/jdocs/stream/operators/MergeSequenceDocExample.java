/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators;

import akka.actor.ActorSystem;

// #import
import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.ClosedShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.MergeSequence;
import akka.stream.javadsl.Partition;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
// #import

public class MergeSequenceDocExample {

  private final ActorSystem system = ActorSystem.create("MergeSequenceDocExample");

  interface Message {}

  boolean shouldProcess(Message message) {
    return true;
  }

  Source<Message, NotUsed> createSubscription() {
    return null;
  }

  Flow<Pair<Message, Long>, Pair<Message, Long>, NotUsed> createMessageProcessor() {
    return null;
  }

  Sink<Message, NotUsed> createMessageAcknowledger() {
    return null;
  }

  void mergeSequenceExample() {

    // #merge-sequence

    Source<Message, NotUsed> subscription = createSubscription();
    Flow<Pair<Message, Long>, Pair<Message, Long>, NotUsed> messageProcessor =
        createMessageProcessor();
    Sink<Message, NotUsed> messageAcknowledger = createMessageAcknowledger();

    RunnableGraph.fromGraph(
            GraphDSL.create(
                builder -> {
                  // Partitions stream into messages that should or should not be processed
                  UniformFanOutShape<Pair<Message, Long>, Pair<Message, Long>> partition =
                      builder.add(
                          Partition.create(2, element -> shouldProcess(element.first()) ? 0 : 1));
                  // Merges stream by the index produced by zipWithIndex
                  UniformFanInShape<Pair<Message, Long>, Pair<Message, Long>> merge =
                      builder.add(MergeSequence.create(2, Pair::second));

                  builder.from(builder.add(subscription.zipWithIndex())).viaFanOut(partition);
                  // First goes through message processor
                  builder.from(partition.out(0)).via(builder.add(messageProcessor)).viaFanIn(merge);
                  // Second partition bypasses message processor
                  builder.from(partition.out(1)).viaFanIn(merge);

                  // Unwrap message index pairs and send to acknowledger
                  builder
                      .from(merge.out())
                      .to(
                          builder.add(
                              Flow.<Pair<Message, Long>>create()
                                  .map(Pair::first)
                                  .to(messageAcknowledger)));

                  return ClosedShape.getInstance();
                }))
        .run(system);

    // #merge-sequence
  }
}
