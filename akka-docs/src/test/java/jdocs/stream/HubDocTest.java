/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.japi.Pair;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.stream.javadsl.PartitionHub.ConsumerInfo;
import akka.testkit.javadsl.TestKit;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.ToLongBiFunction;
import jdocs.AbstractJavaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HubDocTest extends AbstractJavaTest {

  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("GraphDSLDocTest");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  @Test
  public void dynamicMerge() {
    // #merge-hub
    // A simple consumer that will print to the console for now
    Sink<String, CompletionStage<Done>> consumer = Sink.foreach(System.out::println);

    // Attach a MergeHub Source to the consumer. This will materialize to a
    // corresponding Sink.
    RunnableGraph<Sink<String, NotUsed>> runnableGraph = MergeHub.of(String.class, 16).to(consumer);

    // By running/materializing the consumer we get back a Sink, and hence
    // now have access to feed elements into it. This Sink can be materialized
    // any number of times, and every element that enters the Sink will
    // be consumed by our consumer.
    Sink<String, NotUsed> toConsumer = runnableGraph.run(system);

    Source.single("Hello!").runWith(toConsumer, system);
    Source.single("Hub!").runWith(toConsumer, system);
    // #merge-hub
  }

  @Test
  public void dynamicBroadcast() {
    // Used to be able to clean up the running stream
    Materializer materializer = Materializer.createMaterializer(system);

    // #broadcast-hub
    // A simple producer that publishes a new "message" every second
    Source<String, Cancellable> producer =
        Source.tick(Duration.ofSeconds(1), Duration.ofSeconds(1), "New message");

    // Attach a BroadcastHub Sink to the producer. This will materialize to a
    // corresponding Source.
    // (We need to use toMat and Keep.right since by default the materialized
    // value to the left is used)
    RunnableGraph<Source<String, NotUsed>> runnableGraph =
        producer.toMat(BroadcastHub.of(String.class, 256), Keep.right());

    // By running/materializing the producer, we get back a Source, which
    // gives us access to the elements published by the producer.
    Source<String, NotUsed> fromProducer = runnableGraph.run(materializer);

    // Print out messages from the producer in two independent consumers
    fromProducer.runForeach(msg -> System.out.println("consumer1: " + msg), materializer);
    fromProducer.runForeach(msg -> System.out.println("consumer2: " + msg), materializer);
    // #broadcast-hub

    // Cleanup
    materializer.shutdown();
  }

  @Test
  public void mergeBroadcastCombination() {
    // #pub-sub-1
    // Obtain a Sink and Source which will publish and receive from the "bus" respectively.
    Pair<Sink<String, NotUsed>, Source<String, NotUsed>> sinkAndSource =
        MergeHub.of(String.class, 16)
            .toMat(BroadcastHub.of(String.class, 256), Keep.both())
            .run(system);

    Sink<String, NotUsed> sink = sinkAndSource.first();
    Source<String, NotUsed> source = sinkAndSource.second();
    // #pub-sub-1

    // #pub-sub-2
    // Ensure that the Broadcast output is dropped if there are no listening parties.
    // If this dropping Sink is not attached, then the broadcast hub will not drop any
    // elements itself when there are no subscribers, backpressuring the producer instead.
    source.runWith(Sink.ignore(), system);
    // #pub-sub-2

    // #pub-sub-3
    // We create now a Flow that represents a publish-subscribe channel using the above
    // started stream as its "topic". We add two more features, external cancellation of
    // the registration and automatic cleanup for very slow subscribers.
    Flow<String, String, UniqueKillSwitch> busFlow =
        Flow.fromSinkAndSource(sink, source)
            .joinMat(KillSwitches.singleBidi(), Keep.right())
            .backpressureTimeout(Duration.ofSeconds(1));
    // #pub-sub-3

    // #pub-sub-4
    UniqueKillSwitch killSwitch =
        Source.repeat("Hello World!")
            .viaMat(busFlow, Keep.right())
            .to(Sink.foreach(System.out::println))
            .run(system);

    // Shut down externally
    killSwitch.shutdown();
    // #pub-sub-4
  }

  @Test
  public void dynamicPartition() {
    // Used to be able to clean up the running stream
    Materializer materializer = Materializer.createMaterializer(system);

    // #partition-hub
    // A simple producer that publishes a new "message-n" every second
    Source<String, Cancellable> producer =
        Source.tick(Duration.ofSeconds(1), Duration.ofSeconds(1), "message")
            .zipWith(Source.range(0, 100), (a, b) -> a + "-" + b);

    // Attach a PartitionHub Sink to the producer. This will materialize to a
    // corresponding Source.
    // (We need to use toMat and Keep.right since by default the materialized
    // value to the left is used)
    RunnableGraph<Source<String, NotUsed>> runnableGraph =
        producer.toMat(
            PartitionHub.of(String.class, (size, elem) -> Math.abs(elem.hashCode() % size), 2, 256),
            Keep.right());

    // By running/materializing the producer, we get back a Source, which
    // gives us access to the elements published by the producer.
    Source<String, NotUsed> fromProducer = runnableGraph.run(materializer);

    // Print out messages from the producer in two independent consumers
    fromProducer.runForeach(msg -> System.out.println("consumer1: " + msg), materializer);
    fromProducer.runForeach(msg -> System.out.println("consumer2: " + msg), materializer);
    // #partition-hub

    // Cleanup
    materializer.shutdown();
  }

  // #partition-hub-stateful-function
  // Using a class since variable must otherwise be final.
  // New instance is created for each materialization of the PartitionHub.
  static class RoundRobin<T> implements ToLongBiFunction<ConsumerInfo, T> {

    private long i = -1;

    @Override
    public long applyAsLong(ConsumerInfo info, T elem) {
      i++;
      return info.consumerIdByIdx((int) (i % info.size()));
    }
  }
  // #partition-hub-stateful-function

  @Test
  public void dynamicStatefulPartition() {
    // Used to be able to clean up the running stream
    Materializer materializer = Materializer.createMaterializer(system);

    // #partition-hub-stateful
    // A simple producer that publishes a new "message-n" every second
    Source<String, Cancellable> producer =
        Source.tick(Duration.ofSeconds(1), Duration.ofSeconds(1), "message")
            .zipWith(Source.range(0, 100), (a, b) -> a + "-" + b);

    // Attach a PartitionHub Sink to the producer. This will materialize to a
    // corresponding Source.
    // (We need to use toMat and Keep.right since by default the materialized
    // value to the left is used)
    RunnableGraph<Source<String, NotUsed>> runnableGraph =
        producer.toMat(
            PartitionHub.ofStateful(String.class, () -> new RoundRobin<String>(), 2, 256),
            Keep.right());

    // By running/materializing the producer, we get back a Source, which
    // gives us access to the elements published by the producer.
    Source<String, NotUsed> fromProducer = runnableGraph.run(materializer);

    // Print out messages from the producer in two independent consumers
    fromProducer.runForeach(msg -> System.out.println("consumer1: " + msg), materializer);
    fromProducer.runForeach(msg -> System.out.println("consumer2: " + msg), materializer);
    // #partition-hub-stateful

    // Cleanup
    materializer.shutdown();
  }

  @Test
  public void dynamicFastestPartition() {
    // Used to be able to clean up the running stream
    Materializer materializer = Materializer.createMaterializer(system);

    // #partition-hub-fastest
    Source<Integer, NotUsed> producer = Source.range(0, 100);

    // ConsumerInfo.queueSize is the approximate number of buffered elements for a consumer.
    // Note that this is a moving target since the elements are consumed concurrently.
    RunnableGraph<Source<Integer, NotUsed>> runnableGraph =
        producer.toMat(
            PartitionHub.ofStateful(
                Integer.class,
                () ->
                    (info, elem) -> {
                      final List<Object> ids = info.getConsumerIds();
                      int minValue = info.queueSize(0);
                      long fastest = info.consumerIdByIdx(0);
                      for (int i = 1; i < ids.size(); i++) {
                        int value = info.queueSize(i);
                        if (value < minValue) {
                          minValue = value;
                          fastest = info.consumerIdByIdx(i);
                        }
                      }
                      return fastest;
                    },
                2,
                8),
            Keep.right());

    Source<Integer, NotUsed> fromProducer = runnableGraph.run(materializer);

    fromProducer.runForeach(msg -> System.out.println("consumer1: " + msg), materializer);
    fromProducer
        .throttle(10, Duration.ofMillis(100))
        .runForeach(msg -> System.out.println("consumer2: " + msg), materializer);
    // #partition-hub-fastest

    // Cleanup
    materializer.shutdown();
  }
}
