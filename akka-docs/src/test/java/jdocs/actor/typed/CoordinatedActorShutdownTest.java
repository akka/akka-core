/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.actor.typed;

import akka.Done;
import akka.actor.Cancellable;
import akka.actor.CoordinatedShutdown;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
// #coordinated-shutdown-addTask
import static akka.actor.typed.javadsl.AskPattern.ask;

// #coordinated-shutdown-addTask

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class CoordinatedActorShutdownTest {

  // #coordinated-shutdown-addTask
  public static class MyActor extends AbstractBehavior<MyActor.Messages> {
    interface Messages {}

    // ...

    static final class Stop implements Messages {
      final ActorRef<Done> replyTo;

      Stop(ActorRef<Done> replyTo) {
        this.replyTo = replyTo;
      }
    }
    // #coordinated-shutdown-addTask

    public static Behavior<Messages> create() {
      return Behaviors.setup(MyActor::new);
    }

    private MyActor(ActorContext<Messages> context) {
      super(context);
    }

    // #coordinated-shutdown-addTask
    @Override
    public Receive<Messages> createReceive() {
      return newReceiveBuilder().onMessage(Stop.class, this::stop).build();
    }

    private Behavior<Messages> stop(Stop stop) {
      // shut down the actor internal
      // ...
      stop.replyTo.tell(Done.done());
      return Behaviors.stopped();
    }
  }

  // #coordinated-shutdown-addTask

  public static class Root extends AbstractBehavior<Void> {

    public static Behavior<Void> create() {
      return Behaviors.setup(
          context -> {
            ActorRef<MyActor.Messages> myActor = context.spawn(MyActor.create(), "my-actor");
            ActorSystem<Void> system = context.getSystem();
            // #coordinated-shutdown-addTask
            CoordinatedShutdown.get(system)
                .addTask(
                    CoordinatedShutdown.PhaseBeforeServiceUnbind(),
                    "someTaskName",
                    () ->
                        ask(myActor, MyActor.Stop::new, Duration.ofSeconds(5), system.scheduler()));
            // #coordinated-shutdown-addTask
            return Behaviors.empty();
          });
    }

    private Root(ActorContext<Void> context) {
      super(context);
    }

    @Override
    public Receive<Void> createReceive() {
      return newReceiveBuilder().build();
    }
  }

  private CompletionStage<Done> cleanup() {
    return CompletableFuture.completedFuture(Done.done());
  }

  public void mount() {
    ActorSystem<Void> system = ActorSystem.create(Root.create(), "main");

    // #coordinated-shutdown-cancellable
    Cancellable cancellable =
        CoordinatedShutdown.get(system)
            .addCancellableTask(
                CoordinatedShutdown.PhaseBeforeServiceUnbind(), "someTaskCleanup", () -> cleanup());
    // much later...
    cancellable.cancel();
    // #coordinated-shutdown-cancellable

    // #coordinated-shutdown-jvm-hook
    CoordinatedShutdown.get(system)
        .addJvmShutdownHook(() -> System.out.println("custom JVM shutdown hook..."));
    // #coordinated-shutdown-jvm-hook

    // don't run this
    if (false) {
      // #coordinated-shutdown-run
      // shut down with `ActorSystemTerminateReason`
      system.terminate();

      // or define a specific reason
      class UserInitiatedShutdown implements CoordinatedShutdown.Reason {
        @Override
        public String toString() {
          return "UserInitiatedShutdown";
        }
      }

      CompletionStage<Done> done =
          CoordinatedShutdown.get(system).runAll(new UserInitiatedShutdown());
      // #coordinated-shutdown-run
    }
  }
}
