/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.*;

public class DispatchersDocTest {

  public static void spawnDispatchers(ActorContext<Integer> context, Behavior<String> behavior) {
    // #spawn-dispatcher
    context.spawn(behavior, "DefaultDispatcher");
    context.spawn(behavior, "ExplicitDefaultDispatcher", DispatcherSelector.defaultDispatcher());
    context.spawn(behavior, "BlockingDispatcher", DispatcherSelector.blocking());
    context.spawn(behavior, "ParentDispatcher", DispatcherSelector.sameAsParent());
    context.spawn(
        behavior, "DispatcherFromConfig", DispatcherSelector.fromConfig("your-dispatcher"));
    // #spawn-dispatcher
  }
}
