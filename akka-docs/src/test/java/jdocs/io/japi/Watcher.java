/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.io.japi;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Terminated;
import java.util.concurrent.CountDownLatch;

public class Watcher extends AbstractActor {

  public static class Watch {
    final ActorRef target;

    public Watch(ActorRef target) {
      this.target = target;
    }
  }

  final CountDownLatch latch;

  public Watcher(CountDownLatch latch) {
    this.latch = latch;
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            Watch.class,
            msg -> {
              getContext().watch(msg.target);
            })
        .match(
            Terminated.class,
            msg -> {
              latch.countDown();
              if (latch.getCount() == 0) getContext().stop(getSelf());
            })
        .build();
  }
}
