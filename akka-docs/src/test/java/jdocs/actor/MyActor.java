/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor;

// #imports
import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

// #imports

// #my-actor
public class MyActor extends AbstractActor {
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            String.class,
            s -> {
              log.info("Received String message: {}", s);
              // #my-actor
              // #reply
              getSender().tell(s, getSelf());
              // #reply
              // #my-actor
            })
        .matchAny(o -> log.info("received unknown message"))
        .build();
  }
}
// #my-actor
