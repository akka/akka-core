/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor;

// #sample-actor
import akka.actor.AbstractActor;

public class SampleActor extends AbstractActor {

  private Receive guarded =
      receiveBuilder()
          .match(
              String.class,
              s -> s.contains("guard"),
              s -> {
                getSender().tell("contains(guard): " + s, getSelf());
                getContext().unbecome();
              })
          .build();

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            Double.class,
            d -> {
              getSender().tell(d.isNaN() ? 0 : d, getSelf());
            })
        .match(
            Integer.class,
            i -> {
              getSender().tell(i * 10, getSelf());
            })
        .match(
            String.class,
            s -> s.startsWith("guard"),
            s -> {
              getSender().tell("startsWith(guard): " + s.toUpperCase(), getSelf());
              getContext().become(guarded, false);
            })
        .build();
  }
}
// #sample-actor
