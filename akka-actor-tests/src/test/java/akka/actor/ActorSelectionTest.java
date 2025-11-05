/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor;

import static org.junit.Assert.assertEquals;

import akka.testkit.AkkaJUnitActorSystemResource;
import akka.testkit.AkkaSpec;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class ActorSelectionTest extends JUnitSuite {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("ActorSelectionTest", AkkaSpec.testConf());

  private final ActorSystem system = actorSystemResource.getSystem();

  @Test
  public void testResolveOne() throws Exception {
    ActorRef actorRef = system.actorOf(Props.create(JavaAPITestActor.class), "ref1");
    ActorSelection selection = system.actorSelection("user/ref1");
    Duration timeout = Duration.ofMillis(10);

    CompletionStage<ActorRef> cs = selection.resolveOne(timeout);

    ActorRef resolvedRef = cs.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(actorRef, resolvedRef);
  }
}
