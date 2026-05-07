/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.typed;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.time.Duration;

// #import
import akka.cluster.typed.ClusterSingleton;
import akka.cluster.typed.ClusterSingletonSettings;
import akka.cluster.typed.SingletonActor;

// #import

public interface SingletonCompileOnlyTest {

  // #counter
  public class Counter extends AbstractBehavior<Counter.Command> {

    public interface Command {}

    public enum Increment implements Command {
      INSTANCE
    }

    public static class GetValue implements Command {
      private final ActorRef<Integer> replyTo;

      public GetValue(ActorRef<Integer> replyTo) {
        this.replyTo = replyTo;
      }
    }

    public enum GoodByeCounter implements Command {
      INSTANCE
    }

    public static Behavior<Command> create() {
      return Behaviors.setup(Counter::new);
    }

    private int value = 0;

    private Counter(ActorContext<Command> context) {
      super(context);
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(Increment.class, msg -> onIncrement())
          .onMessage(GetValue.class, this::onGetValue)
          .onMessage(GoodByeCounter.class, msg -> onGoodByCounter())
          .build();
    }

    private Behavior<Command> onIncrement() {
      value++;
      return this;
    }

    private Behavior<Command> onGetValue(GetValue msg) {
      msg.replyTo.tell(value);
      return this;
    }

    private Behavior<Command> onGoodByCounter() {
      // Possible async action then stop
      return this;
    }
  }
  // #counter

  ActorSystem system = ActorSystem.create(Behaviors.empty(), "SingletonExample");

  public static void example() {

    // #singleton
    ClusterSingleton singleton = ClusterSingleton.get(system);
    // Start if needed and provide a proxy to a named singleton
    ActorRef<Counter.Command> proxy =
        singleton.init(SingletonActor.of(Counter.create(), "GlobalCounter"));

    proxy.tell(Counter.Increment.INSTANCE);
    // #singleton

  }

  public static void customStopMessage() {
    ClusterSingleton singleton = ClusterSingleton.get(system);
    // #stop-message
    SingletonActor<Counter.Command> counterSingleton =
        SingletonActor.of(Counter.create(), "GlobalCounter")
            .withStopMessage(Counter.GoodByeCounter.INSTANCE);
    ActorRef<Counter.Command> proxy = singleton.init(counterSingleton);
    // #stop-message
    proxy.tell(Counter.Increment.INSTANCE); // avoid unused warning
  }

  public static void backoff() {
    // #backoff
    ClusterSingleton singleton = ClusterSingleton.get(system);
    ActorRef<Counter.Command> proxy =
        singleton.init(
            SingletonActor.of(
                Behaviors.supervise(Counter.create())
                    .onFailure(
                        SupervisorStrategy.restartWithBackoff(
                            Duration.ofSeconds(1), Duration.ofSeconds(10), 0.2)),
                "GlobalCounter"));
    // #backoff
    proxy.tell(Counter.Increment.INSTANCE); // avoid unused warning
  }

  public static void dcProxy() {
    // #create-singleton-proxy-dc
    ActorRef<Counter.Command> singletonProxy =
        ClusterSingleton.get(system)
            .init(
                SingletonActor.of(Counter.create(), "GlobalCounter")
                    .withSettings(ClusterSingletonSettings.create(system).withDataCenter("B")));
    // #create-singleton-proxy-dc

  }
}
