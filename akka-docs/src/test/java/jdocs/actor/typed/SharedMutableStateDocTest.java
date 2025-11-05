/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor.typed;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Receive;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

interface SharedMutableStateDocTest {

  static CompletableFuture<String> expensiveCalculation() {
    throw new UnsupportedOperationException("just a sample signature");
  }

  class Query {
    public final ActorRef<String> replyTo;

    public Query(ActorRef<String> replyTo) {
      this.replyTo = replyTo;
    }
  }

  // #mutable-state
  class MyActor extends AbstractBehavior<MyActor.Command> {

    interface Command {}

    class Message implements Command {
      public final ActorRef<Object> otherActor;

      public Message(ActorRef<Object> replyTo) {
        this.otherActor = replyTo;
      }
    }

    class UpdateState implements Command {
      public final String newState;

      public UpdateState(String newState) {
        this.newState = newState;
      }
    }

    private String state = "";
    private Set<String> mySet = new HashSet<>();

    public MyActor(ActorContext<Command> context) {
      super(context);
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(Message.class, this::onMessage)
          .onMessage(UpdateState.class, this::onUpdateState)
          .build();
    }

    private Behavior<Command> onMessage(Message message) {
      // Very bad: shared mutable object allows
      // the other actor to mutate your own state,
      // or worse, you might get weird race conditions
      message.otherActor.tell(mySet);

      // Example of incorrect approach
      // Very bad: shared mutable state will cause your
      // application to break in weird ways
      CompletableFuture.runAsync(
          () -> {
            state = "This will race";
          });

      // Example of incorrect approach
      // Very bad: shared mutable state will cause your
      // application to break in weird ways
      expensiveCalculation()
          .whenComplete(
              (result, failure) -> {
                if (result != null) state = "new state: " + result;
              });

      // Example of correct approach
      // Turn the future result into a message that is sent to
      // self when future completes
      CompletableFuture<String> futureResult = expensiveCalculation();
      getContext()
          .pipeToSelf(
              futureResult,
              (result, failure) -> {
                if (result != null) return new UpdateState(result);
                else throw new RuntimeException(failure);
              });

      // Another example of incorrect approach
      // mutating actor state from ask future callback
      CompletionStage<String> response =
          AskPattern.ask(
              message.otherActor,
              Query::new,
              Duration.ofSeconds(3),
              getContext().getSystem().scheduler());
      response.whenComplete(
          (result, failure) -> {
            if (result != null) state = "new state: " + result;
          });

      // use context.ask instead, turns the completion
      // into a message sent to self
      getContext()
          .ask(
              String.class,
              message.otherActor,
              Duration.ofSeconds(3),
              Query::new,
              (result, failure) -> {
                if (result != null) return new UpdateState(result);
                else throw new RuntimeException(failure);
              });
      return this;
    }

    private Behavior<Command> onUpdateState(UpdateState command) {
      // safe as long as `newState` is immutable, if it is mutable we'd need to
      // make a defensive copy
      this.state = command.newState;
      return this;
    }
  }
  // #mutable-state
}
