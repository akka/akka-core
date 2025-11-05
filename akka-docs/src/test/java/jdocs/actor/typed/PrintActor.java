/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor.typed;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;

// #print-actor
class PrintActor extends AbstractBehavior<Integer> {

  public static Behavior<Integer> create() {
    return Behaviors.setup(PrintActor::new);
  }

  private PrintActor(ActorContext<Integer> context) {
    super(context);
  }

  @Override
  public Receive<Integer> createReceive() {
    return newReceiveBuilder()
        .onMessage(
            Integer.class,
            i -> {
              System.out.println("PrintActor: " + i);
              return Behaviors.same();
            })
        .build();
  }
}
// #print-actor
