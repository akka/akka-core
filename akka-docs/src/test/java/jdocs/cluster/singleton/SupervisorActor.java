/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.cluster.singleton;

// #singleton-supervisor-actor
import akka.actor.AbstractActor;
import akka.actor.AbstractActor.Receive;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;

public class SupervisorActor extends AbstractActor {
  final Props childProps;
  final SupervisorStrategy supervisorStrategy;
  final ActorRef child;

  SupervisorActor(Props childProps, SupervisorStrategy supervisorStrategy) {
    this.childProps = childProps;
    this.supervisorStrategy = supervisorStrategy;
    this.child = getContext().actorOf(childProps, "supervised-child");
  }

  @Override
  public SupervisorStrategy supervisorStrategy() {
    return supervisorStrategy;
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder().matchAny(msg -> child.forward(msg, getContext())).build();
  }
}
// #singleton-supervisor-actor
