/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed.fromclassic;

// #hello-world-actor
import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

// #hello-world-actor

interface ClassicSample {

  // #hello-world-actor
  public class HelloWorld extends AbstractActor {

    public static final class Greet {
      public final String whom;

      public Greet(String whom) {
        this.whom = whom;
      }
    }

    public static final class Greeted {
      public final String whom;

      public Greeted(String whom) {
        this.whom = whom;
      }
    }

    public static Props props() {
      return Props.create(HelloWorld.class, HelloWorld::new);
    }

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    @Override
    public Receive createReceive() {
      return receiveBuilder().match(Greet.class, this::onGreet).build();
    }

    private void onGreet(Greet command) {
      log.info("Hello {}!", command.whom);
      getSender().tell(new Greeted(command.whom), getSelf());
    }
  }
  // #hello-world-actor

}
