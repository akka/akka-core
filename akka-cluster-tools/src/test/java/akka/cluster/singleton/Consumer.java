/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.cluster.singleton.TestSingletonMessages.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class Consumer extends AbstractActor {

  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

  ActorRef queue;
  ActorRef delegateTo;
  int current = 0;
  boolean stoppedBeforeUnregistration = true;

  public Consumer(ActorRef _queue, ActorRef _delegateTo) {
    queue = _queue;
    delegateTo = _delegateTo;
  }

  @Override
  public void preStart() {
    queue.tell(TestSingletonMessages.registerConsumer(), getSelf());
  }

  @Override
  public void postStop() {
    if (stoppedBeforeUnregistration) log.warning("Stopped before unregistration");
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            Integer.class,
            n -> {
              if (n <= current) getContext().stop(self());
              else {
                current = n;
                delegateTo.tell(n, getSelf());
              }
            })
        .match(RegistrationOk.class, message -> delegateTo.tell(message, getSelf()))
        .match(UnexpectedRegistration.class, message -> delegateTo.tell(message, getSelf()))
        .match(GetCurrent.class, message -> getSender().tell(current, getSelf()))
        // #consumer-end
        .match(End.class, message -> queue.tell(UnregisterConsumer.class, getSelf()))
        .match(
            UnregistrationOk.class,
            message -> {
              stoppedBeforeUnregistration = false;
              getContext().stop(getSelf());
            })
        .match(Ping.class, message -> getSender().tell(TestSingletonMessages.pong(), getSelf()))
        // #consumer-end
        .build();
  }
}
