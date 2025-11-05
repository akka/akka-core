/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// #logMessages
import org.slf4j.event.Level;

// #logMessages

// #test-logging
import akka.actor.testkit.typed.javadsl.LoggingTestKit;

// #test-logging

public interface LoggingDocExamples {

  // #context-log
  public class MyLoggingBehavior extends AbstractBehavior<String> {

    public static Behavior<String> create() {
      return Behaviors.setup(MyLoggingBehavior::new);
    }

    private MyLoggingBehavior(ActorContext<String> context) {
      super(context);
    }

    @Override
    public Receive<String> createReceive() {
      return newReceiveBuilder().onMessage(String.class, this::onReceive).build();
    }

    private Behavior<String> onReceive(String message) {
      getContext().getLog().info("Received message: {}", message);
      return this;
    }
  }
  // #context-log

  // #logger-name
  public class BackendManager extends AbstractBehavior<String> {

    public static Behavior<String> create() {
      return Behaviors.setup(
          context -> {
            context.setLoggerName(BackendManager.class);
            context.getLog().info("Starting up");
            return new BackendManager(context);
          });
    }

    private BackendManager(ActorContext<String> context) {
      super(context);
    }

    @Override
    public Receive<String> createReceive() {
      return newReceiveBuilder().onMessage(String.class, this::onReceive).build();
    }

    private Behavior<String> onReceive(String message) {
      getContext().getLog().debug("Received message: {}", message);
      return this;
    }
  }
  // #logger-name

  // #logger-factory
  class BackendTask {
    private final Logger log = LoggerFactory.getLogger(getClass());

    void run() {
      CompletableFuture<String> task =
          CompletableFuture.supplyAsync(
              () -> {
                // some work
                return "result";
              });
      task.whenComplete(
          (result, exc) -> {
            if (exc == null) log.error("Task failed", exc);
            else log.info("Task completed: {}", result);
          });
    }
  }
  // #logger-factory

  static void logMessages() {
    // #logMessages
    Behaviors.logMessages(LogOptions.create().withLevel(Level.TRACE), BackendManager.create());
    // #logMessages
  }

  public class BackendManager2 extends AbstractBehavior<BackendManager2.Command> {

    interface Command {
      String identifier();
    }

    public static Behavior<Command> create() {
      return Behaviors.empty();
    }

    public BackendManager2(ActorContext<Command> context) {
      super(context);
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder().build();
    }
  }

  static void withMdc() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "notUsed");

    // #withMdc
    Map<String, String> staticMdc = new HashMap<>();
    staticMdc.put("startTime", String.valueOf(system.startTime()));

    Behaviors.withMdc(
        BackendManager2.Command.class,
        staticMdc,
        message -> {
          Map<String, String> msgMdc = new HashMap<>();
          msgMdc.put("identifier", message.identifier());
          msgMdc.put("upTime", String.valueOf(system.uptime()));
          return msgMdc;
        },
        BackendManager2.create());
    // #withMdc
  }

  static class Message {
    final String s;

    public Message(String s) {
      this.s = s;
    }
  }

  static void logging() {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "notUsed");
    ActorRef<Message> ref = null;

    // #test-logging
    LoggingTestKit.info("Received message")
        .expect(
            system,
            () -> {
              ref.tell(new Message("hello"));
              return null;
            });
    // #test-logging

    // #test-logging-criteria
    LoggingTestKit.error(IllegalArgumentException.class)
        .withMessageRegex(".*was rejected.*expecting ascii input.*")
        .withCustom(
            event ->
                event.getMarker().isPresent()
                    && event.getMarker().get().getName().equals("validation"))
        .withOccurrences(2)
        .expect(
            system,
            () -> {
              ref.tell(new Message("hellö"));
              ref.tell(new Message("hejdå"));
              return null;
            });
    // #test-logging-criteria

  }

  static void tagsExample() {
    ActorContext<Object> context = null;
    Behavior<Object> myBehavior = Behaviors.empty();
    // #tags
    context.spawn(myBehavior, "MyActor", ActorTags.create("processing"));
    // #tags
  }
}
