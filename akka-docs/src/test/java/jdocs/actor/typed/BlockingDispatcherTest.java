/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor.typed;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;

public class BlockingDispatcherTest {
  public static void main(String args[]) {
    // #blocking-main
    Behavior<Void> root =
        Behaviors.setup(
            context -> {
              for (int i = 0; i < 100; i++) {
                context.spawn(BlockingActor.create(), "BlockingActor-" + i).tell(i);
                context.spawn(PrintActor.create(), "PrintActor-" + i).tell(i);
              }
              return Behaviors.ignore();
            });
    // #blocking-main

    ActorSystem<Void> system = ActorSystem.<Void>create(root, "BlockingDispatcherTest");
  }
}
