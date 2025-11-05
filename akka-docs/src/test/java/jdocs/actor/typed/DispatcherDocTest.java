/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor.typed;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import scala.concurrent.ExecutionContextExecutor;

@SuppressWarnings("unused")
public class DispatcherDocTest {

  private final ActorSystem<Void> system = null;
  private final ActorContext<Void> context = null;

  public void defineDispatcherInCode() {
    // #defining-dispatcher-in-code
    ActorRef<Integer> myActor =
        context.spawn(
            PrintActor.create(), "PrintActor", DispatcherSelector.fromConfig("my-dispatcher"));
    // #defining-dispatcher-in-code
  }

  public void defineFixedPoolSizeDispatcher() {
    // #defining-fixed-pool-size-dispatcher
    ActorRef<Integer> myActor =
        context.spawn(
            PrintActor.create(),
            "PrintActor",
            DispatcherSelector.fromConfig("blocking-io-dispatcher"));
    // #defining-fixed-pool-size-dispatcher
  }

  public void definePinnedDispatcher() {
    // #defining-pinned-dispatcher
    ActorRef<Integer> myActor =
        context.spawn(
            PrintActor.create(),
            "PrintActor",
            DispatcherSelector.fromConfig("my-pinned-dispatcher"));
    // #defining-pinned-dispatcher
  }

  public void compileLookup() {
    // #lookup
    // this is scala.concurrent.ExecutionContextExecutor, which implements
    // both scala.concurrent.ExecutionContext (for use with Futures, Scheduler, etc.)
    // and java.util.concurrent.Executor (for use with CompletableFuture etc.)
    final ExecutionContextExecutor ex =
        system.dispatchers().lookup(DispatcherSelector.fromConfig("my-dispatcher"));
    // #lookup
  }
}
