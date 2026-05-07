/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.pattern;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.Props;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class SupervisedAskSpec {

  public Object execute(
      Class<? extends AbstractActor> someActor,
      Object message,
      Duration timeout,
      ActorRefFactory actorSystem)
      throws Exception {
    // example usage
    try {
      ActorRef supervisorCreator = SupervisedAsk.createSupervisorCreator(actorSystem);
      CompletionStage<Object> finished =
          SupervisedAsk.askOf(supervisorCreator, Props.create(someActor), message, timeout);
      return finished.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      // exception propagated by supervision
      throw e;
    }
  }
}
