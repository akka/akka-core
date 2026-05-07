/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;

import akka.Done;
import akka.actor.typed.javadsl.Behaviors;
import java.util.concurrent.CompletionStage;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

public class ActorSystemTest extends JUnitSuite {

  @Test
  public void testGetWhenTerminated() throws Exception {
    final ActorSystem<Void> system =
        ActorSystem.create(Behaviors.empty(), "GetWhenTerminatedSystem");
    system.terminate();
    final CompletionStage<Done> cs = system.getWhenTerminated();
    cs.toCompletableFuture().get(2, SECONDS);
  }

  @Test
  public void testGetWhenTerminatedWithoutTermination() {
    final ActorSystem<Void> system =
        ActorSystem.create(Behaviors.empty(), "GetWhenTerminatedWithoutTermination");
    assertFalse(system.getWhenTerminated().toCompletableFuture().isDone());
  }
}
