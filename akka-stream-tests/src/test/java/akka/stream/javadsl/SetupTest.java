/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl;

import static org.junit.Assert.assertEquals;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.StreamTest;
import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;

public class SetupTest extends StreamTest {
  public SetupTest() {
    super(actorSystemResource);
  }

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("SetupTest", AkkaSpec.testConf());

  @Test
  public void shouldExposeMaterializerAndAttributesToSource() throws Exception {
    final Source<Pair<Boolean, Boolean>, CompletionStage<NotUsed>> source =
        Source.fromMaterializer(
            (mat, attr) ->
                Source.single(Pair.create(mat.isShutdown(), attr.attributeList().isEmpty())));

    assertEquals(
        Pair.create(false, false),
        source.runWith(Sink.head(), system).toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void shouldExposeMaterializerAndAttributesToFlow() throws Exception {
    final Flow<Object, Pair<Boolean, Boolean>, CompletionStage<NotUsed>> flow =
        Flow.fromMaterializer(
            (mat, attr) ->
                Flow.fromSinkAndSource(
                    Sink.ignore(),
                    Source.single(Pair.create(mat.isShutdown(), attr.attributeList().isEmpty()))));

    assertEquals(
        Pair.create(false, false),
        Source.empty()
            .via(flow)
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void shouldExposeMaterializerAndAttributesToSink() throws Exception {
    Sink<Object, CompletionStage<CompletionStage<Pair<Boolean, Boolean>>>> sink =
        Sink.fromMaterializer(
            (mat, attr) ->
                Sink.fold(
                    Pair.create(mat.isShutdown(), attr.attributeList().isEmpty()), Keep.left()));

    assertEquals(
        Pair.create(false, false),
        Source.empty()
            .runWith(sink, system)
            .thenCompose(c -> c)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS));
  }
}
