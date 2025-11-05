/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static org.junit.Assert.assertEquals;

import akka.NotUsed;
import akka.japi.Pair;
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
import scala.concurrent.Future;

public class LazyAndFutureSourcesTest extends StreamTest {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("LazyAndFutureSourcesTest", AkkaSpec.testConf());

  public LazyAndFutureSourcesTest() {
    super(actorSystemResource);
  }

  // note these are minimal happy path tests to cover API, more thorough tests are on the Scala side

  @Test
  public void future() throws Exception {
    CompletionStage<List<String>> result =
        Source.future(Future.successful("one")).runWith(Sink.seq(), system);

    assertEquals(Arrays.asList("one"), result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void completionStage() throws Exception {
    CompletionStage<String> one = CompletableFuture.completedFuture("one");
    CompletionStage<List<String>> result = Source.completionStage(one).runWith(Sink.seq(), system);

    assertEquals(Arrays.asList("one"), result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void completionStageSource() throws Exception {
    Pair<CompletionStage<NotUsed>, CompletionStage<List<String>>> result =
        Source.completionStageSource(CompletableFuture.completedFuture(Source.single("one")))
            .toMat(Sink.seq(), Keep.both())
            .run(system);

    CompletionStage<NotUsed> nestedMatVal = result.first();
    CompletionStage<List<String>> list = result.second();
    assertEquals(Arrays.asList("one"), list.toCompletableFuture().get(3, TimeUnit.SECONDS));
    // Future adaption to completionstage of matval means we cannot count on matval future being
    // completed just because stream is
    nestedMatVal.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void lazySingle() throws Exception {
    CompletionStage<List<String>> list = Source.lazySingle(() -> "one").runWith(Sink.seq(), system);

    assertEquals(Arrays.asList("one"), list.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void lazyCompletionStage() throws Exception {
    CompletionStage<List<String>> list =
        Source.lazyCompletionStage(() -> CompletableFuture.completedFuture("one"))
            .runWith(Sink.seq(), system);

    assertEquals(Arrays.asList("one"), list.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void lazySource() throws Exception {
    Pair<CompletionStage<NotUsed>, CompletionStage<List<String>>> result =
        Source.lazySource(() -> Source.single("one")).toMat(Sink.seq(), Keep.both()).run(system);

    CompletionStage<NotUsed> nestedMatVal = result.first();
    CompletionStage<List<String>> list = result.second();
    assertEquals(Arrays.asList("one"), list.toCompletableFuture().get(3, TimeUnit.SECONDS));
    // Future adaption to completionstage of matval means we cannot count on matval future being
    // completed just because stream is
    nestedMatVal.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void lazyCompletionStageSource() throws Exception {
    Pair<CompletionStage<NotUsed>, CompletionStage<List<String>>> result =
        Source.lazyCompletionStageSource(
                () -> CompletableFuture.completedFuture(Source.single("one")))
            .toMat(Sink.seq(), Keep.both())
            .run(system);

    CompletionStage<NotUsed> nestedMatVal = result.first();
    CompletionStage<List<String>> list = result.second();
    assertEquals(Arrays.asList("one"), list.toCompletableFuture().get(3, TimeUnit.SECONDS));
    // flatMap/thenCompose of matval means we cannot count on matval future being completed just
    // because stream is
    nestedMatVal.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }
}
