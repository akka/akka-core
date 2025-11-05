/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.typed.tutorial_1;

/*
//#print-refs
package com.example;

//#print-refs
*/

import akka.actor.typed.PreRestart;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.PostStop;

// #print-refs
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

class PrintMyActorRefActor extends AbstractBehavior<String> {

  static Behavior<String> create() {
    return Behaviors.setup(PrintMyActorRefActor::new);
  }

  private PrintMyActorRefActor(ActorContext<String> context) {
    super(context);
  }

  @Override
  public Receive<String> createReceive() {
    return newReceiveBuilder().onMessageEquals("printit", this::printIt).build();
  }

  private Behavior<String> printIt() {
    ActorRef<String> secondRef = getContext().spawn(Behaviors.empty(), "second-actor");
    System.out.println("Second: " + secondRef);
    return this;
  }
}
// #print-refs

// #start-stop
class StartStopActor1 extends AbstractBehavior<String> {

  static Behavior<String> create() {
    return Behaviors.setup(StartStopActor1::new);
  }

  private StartStopActor1(ActorContext<String> context) {
    super(context);
    System.out.println("first started");

    context.spawn(StartStopActor2.create(), "second");
  }

  @Override
  public Receive<String> createReceive() {
    return newReceiveBuilder()
        .onMessageEquals("stop", Behaviors::stopped)
        .onSignal(PostStop.class, signal -> onPostStop())
        .build();
  }

  private Behavior<String> onPostStop() {
    System.out.println("first stopped");
    return this;
  }
}

class StartStopActor2 extends AbstractBehavior<String> {

  static Behavior<String> create() {
    return Behaviors.setup(StartStopActor2::new);
  }

  private StartStopActor2(ActorContext<String> context) {
    super(context);
    System.out.println("second started");
  }

  @Override
  public Receive<String> createReceive() {
    return newReceiveBuilder().onSignal(PostStop.class, signal -> onPostStop()).build();
  }

  private Behavior<String> onPostStop() {
    System.out.println("second stopped");
    return this;
  }
}
// #start-stop

// #supervise
class SupervisingActor extends AbstractBehavior<String> {

  static Behavior<String> create() {
    return Behaviors.setup(SupervisingActor::new);
  }

  private final ActorRef<String> child;

  private SupervisingActor(ActorContext<String> context) {
    super(context);
    child =
        context.spawn(
            Behaviors.supervise(SupervisedActor.create()).onFailure(SupervisorStrategy.restart()),
            "supervised-actor");
  }

  @Override
  public Receive<String> createReceive() {
    return newReceiveBuilder().onMessageEquals("failChild", this::onFailChild).build();
  }

  private Behavior<String> onFailChild() {
    child.tell("fail");
    return this;
  }
}

class SupervisedActor extends AbstractBehavior<String> {

  static Behavior<String> create() {
    return Behaviors.setup(SupervisedActor::new);
  }

  private SupervisedActor(ActorContext<String> context) {
    super(context);
    System.out.println("supervised actor started");
  }

  @Override
  public Receive<String> createReceive() {
    return newReceiveBuilder()
        .onMessageEquals("fail", this::fail)
        .onSignal(PreRestart.class, signal -> preRestart())
        .onSignal(PostStop.class, signal -> postStop())
        .build();
  }

  private Behavior<String> fail() {
    System.out.println("supervised actor fails now");
    throw new RuntimeException("I failed!");
  }

  private Behavior<String> preRestart() {
    System.out.println("supervised will be restarted");
    return this;
  }

  private Behavior<String> postStop() {
    System.out.println("supervised stopped");
    return this;
  }
}
// #supervise

// #print-refs

class Main extends AbstractBehavior<String> {

  static Behavior<String> create() {
    return Behaviors.setup(Main::new);
  }

  private Main(ActorContext<String> context) {
    super(context);
  }

  @Override
  public Receive<String> createReceive() {
    return newReceiveBuilder().onMessageEquals("start", this::start).build();
  }

  private Behavior<String> start() {
    ActorRef<String> firstRef = getContext().spawn(PrintMyActorRefActor.create(), "first-actor");

    System.out.println("First: " + firstRef);
    firstRef.tell("printit");
    return Behaviors.same();
  }
}

public class ActorHierarchyExperiments {
  public static void main(String[] args) {
    ActorRef<String> testSystem = ActorSystem.create(Main.create(), "testSystem");
    testSystem.tell("start");
  }
}
// #print-refs

class StartingActorHierarchyActors {

  public void showStartAndStopActors() {
    Behaviors.setup(
        context -> {
          // #start-stop-main
          ActorRef<String> first = context.spawn(StartStopActor1.create(), "first");
          first.tell("stop");
          // #start-stop-main
          return Behaviors.empty();
        });
  }

  public void showSuperviseActors() {
    Behaviors.setup(
        context -> {
          // #supervise-main
          ActorRef<String> supervisingActor =
              context.spawn(SupervisingActor.create(), "supervising-actor");
          supervisingActor.tell("failChild");
          // #supervise-main
          return Behaviors.empty();
        });
  }
}
