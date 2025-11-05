/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.javadsl.cookbook;

import static org.junit.Assert.assertTrue;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.testkit.javadsl.TestKit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;

public class RecipeWorkerPool extends RecipeTest {
  static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("RecipeWorkerPool");
  }

  @AfterClass
  public static void tearDown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  // #worker-pool
  public static <In, Out> Flow<In, Out, NotUsed> balancer(
      Flow<In, Out, NotUsed> worker, int workerCount) {
    return Flow.fromGraph(
        GraphDSL.create(
            b -> {
              boolean waitForAllDownstreams = true;
              final UniformFanOutShape<In, In> balance =
                  b.add(Balance.<In>create(workerCount, waitForAllDownstreams));
              final UniformFanInShape<Out, Out> merge = b.add(Merge.<Out>create(workerCount));

              for (int i = 0; i < workerCount; i++) {
                b.from(balance.out(i)).via(b.add(worker.async())).toInlet(merge.in(i));
              }

              return FlowShape.of(balance.in(), merge.out());
            }));
  }
  // #worker-pool

  @Test
  public void workForVersion1() throws Exception {
    new TestKit(system) {
      {
        Source<Message, NotUsed> data =
            Source.from(Arrays.asList("1", "2", "3", "4", "5")).map(t -> new Message(t));

        Flow<Message, Message, NotUsed> worker =
            Flow.of(Message.class).map(m -> new Message(m.msg + " done"));

        // #worker-pool2
        Flow<Message, Message, NotUsed> balancer = balancer(worker, 3);
        Source<Message, NotUsed> processedJobs = data.via(balancer);
        // #worker-pool2

        FiniteDuration timeout = FiniteDuration.create(200, TimeUnit.MILLISECONDS);
        CompletionStage<List<String>> future =
            processedJobs.map(m -> m.msg).limit(10).runWith(Sink.seq(), system);
        List<String> got = future.toCompletableFuture().get(1, TimeUnit.SECONDS);
        assertTrue(got.contains("1 done"));
        assertTrue(got.contains("2 done"));
        assertTrue(got.contains("3 done"));
        assertTrue(got.contains("4 done"));
        assertTrue(got.contains("5 done"));
      }
    };
  }
}
