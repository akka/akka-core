/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;

import akka.testkit.AkkaJUnitActorSystemResource;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class ActorSystemTest extends JUnitSuite {

  @Rule
  public final AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("ActorSystemTest");

  private ActorSystem system = null;

  @Before
  public void beforeEach() {
    system = actorSystemResource.getSystem();
  }

  @Test
  public void testGetWhenTerminated() throws Exception {
    system.terminate();
    final CompletionStage<Terminated> cs = system.getWhenTerminated();
    cs.toCompletableFuture().get(2, SECONDS);
  }

  @Test
  public void testGetWhenTerminatedWithoutTermination() {
    assertFalse(system.getWhenTerminated().toCompletableFuture().isDone());
  }
}
