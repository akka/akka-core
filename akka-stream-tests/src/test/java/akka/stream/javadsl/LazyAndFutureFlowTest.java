/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static org.junit.Assert.assertEquals;

import akka.stream.StreamTest;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;

public class LazyAndFutureFlowTest extends StreamTest {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("LazyAndFutureFlowTest", AkkaSpec.testConf());

  public LazyAndFutureFlowTest() {
    super(actorSystemResource);
  }

  // note these are minimal happy path tests to cover API, more thorough tests are on the Scala side

  @Test
  public void completionStageFlow() throws Exception {
    CompletionStage<List<String>> result =
        Source.single("one")
            .via(
                Flow.completionStageFlow(
                    CompletableFuture.completedFuture(Flow.fromFunction(str -> str))))
            .runWith(Sink.seq(), system);

    assertEquals(Arrays.asList("one"), result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void lazyFlow() throws Exception {
    CompletionStage<List<String>> result =
        Source.single("one")
            .via(Flow.lazyFlow(() -> Flow.fromFunction(str -> str)))
            .runWith(Sink.seq(), system);

    assertEquals(Arrays.asList("one"), result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void lazyCompletionStageFlow() throws Exception {
    CompletionStage<List<String>> result =
        Source.single("one")
            .via(
                Flow.lazyCompletionStageFlow(
                    () -> CompletableFuture.completedFuture(Flow.fromFunction(str -> str))))
            .runWith(Sink.seq(), system);

    assertEquals(Arrays.asList("one"), result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }
}
