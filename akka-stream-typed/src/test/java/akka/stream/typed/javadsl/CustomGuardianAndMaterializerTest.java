/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.typed.javadsl;

import static org.junit.Assert.assertEquals;

import akka.Done;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.AbruptStageTerminationException;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class CustomGuardianAndMaterializerTest extends JUnitSuite {

  @ClassRule public static final TestKitJunitResource testKit = new TestKitJunitResource();

  @Test
  public void useSystemWideMaterialiser() throws Exception {
    CompletionStage<String> result = Source.single("hello").runWith(Sink.head(), testKit.system());

    assertEquals("hello", result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  @Test
  public void createCustomSystemLevelMaterialiser() throws Exception {
    Materializer materializer = Materializer.createMaterializer(testKit.system());

    CompletionStage<String> result = Source.single("hello").runWith(Sink.head(), materializer);

    assertEquals("hello", result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }

  private static Behavior<String> actorStreamBehavior(ActorRef<Object> probe) {
    return Behaviors.setup(
        (context) -> {
          Materializer materializer = Materializer.createMaterializer(context);

          CompletionStage<Done> done = Source.repeat("hello").runWith(Sink.ignore(), materializer);
          done.whenComplete(
              (success, failure) -> {
                if (success != null) probe.tell(success);
                else probe.tell(failure);
              });

          return Behaviors.receive(String.class)
              .onMessageEquals("stop", () -> Behaviors.stopped())
              .build();
        });
  }

  @Test
  public void createCustomActorLevelMaterializer() throws Exception {
    TestProbe<Object> probe = testKit.createTestProbe();
    ActorRef<String> actor = testKit.spawn(actorStreamBehavior(probe.getRef()));

    actor.tell("stop");

    probe.expectMessageClass(AbruptStageTerminationException.class);
  }
}
