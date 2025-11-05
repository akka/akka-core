/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.circuitbreaker;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ReceiveTimeout;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.CircuitBreaker;
import java.time.Duration;

public class TellPatternJavaActor extends AbstractActor {

  private final ActorRef target;
  private final CircuitBreaker breaker;
  private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  public TellPatternJavaActor(ActorRef targetActor) {
    this.target = targetActor;
    this.breaker =
        new CircuitBreaker(
                getContext().getDispatcher(),
                getContext().getSystem().getScheduler(),
                5,
                Duration.ofSeconds(10),
                Duration.ofMinutes(1))
            .addOnOpenListener(this::notifyMeOnOpen);
  }

  public void notifyMeOnOpen() {
    log.warning("My CircuitBreaker is now open, and will not close for one minute");
  }

  // #circuit-breaker-tell-pattern
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            String.class,
            payload -> "call".equals(payload) && breaker.isClosed(),
            payload -> target.tell("message", self()))
        .matchEquals("response", payload -> breaker.succeed())
        .match(Throwable.class, t -> breaker.fail())
        .match(ReceiveTimeout.class, t -> breaker.fail())
        .build();
  }
  // #circuit-breaker-tell-pattern

}
