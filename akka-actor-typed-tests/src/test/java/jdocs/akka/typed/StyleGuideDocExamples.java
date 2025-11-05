/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

// #oo-style
// #fun-style
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
// #fun-style
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.Receive;
// #oo-style

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.Done;
import java.time.Duration;

interface StyleGuideDocExamples {

  interface FunctionalStyle {

    // #fun-style

    public class Counter {
      public interface Command {}

      public enum Increment implements Command {
        INSTANCE
      }

      public static class GetValue implements Command {
        public final ActorRef<Value> replyTo;

        public GetValue(ActorRef<Value> replyTo) {
          this.replyTo = replyTo;
        }
      }

      public static class Value {
        public final int value;

        public Value(int value) {
          this.value = value;
        }
      }

      public static Behavior<Command> create() {
        return Behaviors.setup(context -> counter(context, 0));
      }

      private static Behavior<Command> counter(final ActorContext<Command> context, final int n) {

        return Behaviors.receive(Command.class)
            .onMessage(Increment.class, notUsed -> onIncrement(context, n))
            .onMessage(GetValue.class, command -> onGetValue(n, command))
            .build();
      }

      private static Behavior<Command> onIncrement(ActorContext<Command> context, int n) {
        int newValue = n + 1;
        context.getLog().debug("Incremented counter to [{}]", newValue);
        return counter(context, newValue);
      }

      private static Behavior<Command> onGetValue(int n, GetValue command) {
        command.replyTo.tell(new Value(n));
        return Behaviors.same();
      }
    }
    // #fun-style
  }

  interface OOStyle {

    // #oo-style

    // #messages
    public class Counter extends AbstractBehavior<Counter.Command> {

      public interface Command {}

      // #message-enum
      public enum Increment implements Command {
        INSTANCE
      }
      // #message-enum

      public static class GetValue implements Command {
        public final ActorRef<Value> replyTo;

        public GetValue(ActorRef<Value> replyTo) {
          this.replyTo = replyTo;
        }
      }

      public static class Value {
        public final int value;

        public Value(int value) {
          this.value = value;
        }
      }
      // #messages

      public static Behavior<Command> create() {
        return Behaviors.setup(Counter::new);
      }

      private int n;

      private Counter(ActorContext<Command> context) {
        super(context);
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder()
            // #message-enum-match
            .onMessage(Increment.class, notUsed -> onIncrement())
            // #message-enum-match
            .onMessage(GetValue.class, this::onGetValue)
            .build();
      }

      private Behavior<Command> onIncrement() {
        n++;
        getContext().getLog().debug("Incremented counter to [{}]", n);
        return this;
      }

      private Behavior<Command> onGetValue(GetValue command) {
        command.replyTo.tell(new Value(n));
        return this;
      }
      // #messages
    }
    // #messages
    // #oo-style

  }

  interface FunctionalStyleSetupParams1 {
    // #fun-style-setup-params1

    // this is an anti-example, better solutions exists
    public class Counter {
      public interface Command {}

      public static class IncrementRepeatedly implements Command {
        public final Duration interval;

        public IncrementRepeatedly(Duration interval) {
          this.interval = interval;
        }
      }

      public enum Increment implements Command {
        INSTANCE
      }

      public static class GetValue implements Command {
        public final ActorRef<Value> replyTo;

        public GetValue(ActorRef<Value> replyTo) {
          this.replyTo = replyTo;
        }
      }

      public static class Value {
        public final int value;

        public Value(int value) {
          this.value = value;
        }
      }

      public static Behavior<Command> create(String name) {
        return Behaviors.setup(
            context -> Behaviors.withTimers(timers -> counter(name, context, timers, 0)));
      }

      private static Behavior<Command> counter(
          final String name,
          final ActorContext<Command> context,
          final TimerScheduler<Command> timers,
          final int n) {

        return Behaviors.receive(Command.class)
            .onMessage(
                IncrementRepeatedly.class,
                command -> onIncrementRepeatedly(name, context, timers, n, command))
            .onMessage(Increment.class, notUsed -> onIncrement(name, context, timers, n))
            .onMessage(GetValue.class, command -> onGetValue(n, command))
            .build();
      }

      private static Behavior<Command> onIncrementRepeatedly(
          String name,
          ActorContext<Command> context,
          TimerScheduler<Command> timers,
          int n,
          IncrementRepeatedly command) {
        context
            .getLog()
            .debug(
                "[{}] Starting repeated increments with interval [{}], current count is [{}]",
                name,
                command.interval,
                n);
        timers.startTimerWithFixedDelay(Increment.INSTANCE, command.interval);
        return Behaviors.same();
      }

      private static Behavior<Command> onIncrement(
          String name, ActorContext<Command> context, TimerScheduler<Command> timers, int n) {
        int newValue = n + 1;
        context.getLog().debug("[{}] Incremented counter to [{}]", name, newValue);
        return counter(name, context, timers, newValue);
      }

      private static Behavior<Command> onGetValue(int n, GetValue command) {
        command.replyTo.tell(new Value(n));
        return Behaviors.same();
      }
    }
    // #fun-style-setup-params1
  }

  interface FunctionalStyleSetupParams2 {
    // #fun-style-setup-params2

    // this is better than previous example, but even better solution exists
    public class Counter {
      // messages omitted for brevity, same messages as above example
      // #fun-style-setup-params2
      public interface Command {}

      public static class IncrementRepeatedly implements Command {
        public final Duration interval;

        public IncrementRepeatedly(Duration interval) {
          this.interval = interval;
        }
      }

      public enum Increment implements Command {
        INSTANCE
      }

      public static class GetValue implements Command {
        public final ActorRef<Value> replyTo;

        public GetValue(ActorRef<Value> replyTo) {
          this.replyTo = replyTo;
        }
      }

      public static class Value {
        public final int value;

        public Value(int value) {
          this.value = value;
        }
      }
      // #fun-style-setup-params2

      private static class Setup {
        final String name;
        final ActorContext<Command> context;
        final TimerScheduler<Command> timers;

        private Setup(String name, ActorContext<Command> context, TimerScheduler<Command> timers) {
          this.name = name;
          this.context = context;
          this.timers = timers;
        }
      }

      public static Behavior<Command> create(String name) {
        return Behaviors.setup(
            context ->
                Behaviors.withTimers(timers -> counter(new Setup(name, context, timers), 0)));
      }

      private static Behavior<Command> counter(final Setup setup, final int n) {

        return Behaviors.receive(Command.class)
            .onMessage(
                IncrementRepeatedly.class, command -> onIncrementRepeatedly(setup, n, command))
            .onMessage(Increment.class, notUsed -> onIncrement(setup, n))
            .onMessage(GetValue.class, command -> onGetValue(n, command))
            .build();
      }

      private static Behavior<Command> onIncrementRepeatedly(
          Setup setup, int n, IncrementRepeatedly command) {
        setup
            .context
            .getLog()
            .debug(
                "[{}] Starting repeated increments with interval [{}], current count is [{}]",
                setup.name,
                command.interval,
                n);
        setup.timers.startTimerWithFixedDelay(Increment.INSTANCE, command.interval);
        return Behaviors.same();
      }

      private static Behavior<Command> onIncrement(Setup setup, int n) {
        int newValue = n + 1;
        setup.context.getLog().debug("[{}] Incremented counter to [{}]", setup.name, newValue);
        return counter(setup, newValue);
      }

      private static Behavior<Command> onGetValue(int n, GetValue command) {
        command.replyTo.tell(new Value(n));
        return Behaviors.same();
      }
    }
    // #fun-style-setup-params2
  }

  interface FunctionalStyleSetupParams3 {
    // #fun-style-setup-params3

    // this is better than previous examples
    public class Counter {
      // messages omitted for brevity, same messages as above example
      // #fun-style-setup-params3
      public interface Command {}

      public static class IncrementRepeatedly implements Command {
        public final Duration interval;

        public IncrementRepeatedly(Duration interval) {
          this.interval = interval;
        }
      }

      public enum Increment implements Command {
        INSTANCE
      }

      public static class GetValue implements Command {
        public final ActorRef<Value> replyTo;

        public GetValue(ActorRef<Value> replyTo) {
          this.replyTo = replyTo;
        }
      }

      public static class Value {
        public final int value;

        public Value(int value) {
          this.value = value;
        }
      }
      // #fun-style-setup-params3

      public static Behavior<Command> create(String name) {
        return Behaviors.setup(
            context ->
                Behaviors.withTimers(timers -> new Counter(name, context, timers).counter(0)));
      }

      private final String name;
      private final ActorContext<Command> context;
      private final TimerScheduler<Command> timers;

      private Counter(String name, ActorContext<Command> context, TimerScheduler<Command> timers) {
        this.name = name;
        this.context = context;
        this.timers = timers;
      }

      private Behavior<Command> counter(final int n) {
        return Behaviors.receive(Command.class)
            .onMessage(IncrementRepeatedly.class, command -> onIncrementRepeatedly(n, command))
            .onMessage(Increment.class, notUsed -> onIncrement(n))
            .onMessage(GetValue.class, command -> onGetValue(n, command))
            .build();
      }

      private Behavior<Command> onIncrementRepeatedly(int n, IncrementRepeatedly command) {
        context
            .getLog()
            .debug(
                "[{}] Starting repeated increments with interval [{}], current count is [{}]",
                name,
                command.interval,
                n);
        timers.startTimerWithFixedDelay(Increment.INSTANCE, command.interval);
        return Behaviors.same();
      }

      private Behavior<Command> onIncrement(int n) {
        int newValue = n + 1;
        context.getLog().debug("[{}] Incremented counter to [{}]", name, newValue);
        return counter(newValue);
      }

      private Behavior<Command> onGetValue(int n, GetValue command) {
        command.replyTo.tell(new Value(n));
        return Behaviors.same();
      }
    }
    // #fun-style-setup-params3
  }

  interface FactoryMethod {
    // #behavior-factory-method
    public class CountDown extends AbstractBehavior<CountDown.Command> {

      public interface Command {}

      public enum Down implements Command {
        INSTANCE
      }

      // factory for the initial `Behavior`
      public static Behavior<Command> create(int countDownFrom, ActorRef<Done> notifyWhenZero) {
        return Behaviors.setup(context -> new CountDown(context, countDownFrom, notifyWhenZero));
      }

      private final ActorRef<Done> notifyWhenZero;
      private int remaining;

      private CountDown(
          ActorContext<Command> context, int countDownFrom, ActorRef<Done> notifyWhenZero) {
        super(context);
        this.remaining = countDownFrom;
        this.notifyWhenZero = notifyWhenZero;
      }

      @Override
      public Receive<Command> createReceive() {
        return newReceiveBuilder().onMessage(Down.class, notUsed -> onDown()).build();
      }

      private Behavior<Command> onDown() {
        remaining--;
        if (remaining == 0) {
          notifyWhenZero.tell(Done.getInstance());
          return Behaviors.stopped();
        } else {
          return this;
        }
      }
    }
    // #behavior-factory-method

    public class Usage {
      private ActorContext<?> context = null;
      private ActorRef<Done> doneRef = null;

      {
        // #behavior-factory-method-spawn
        ActorRef<CountDown.Command> countDown =
            context.spawn(CountDown.create(100, doneRef), "countDown");
        // #behavior-factory-method-spawn

        // #message-prefix-in-tell
        countDown.tell(CountDown.Down.INSTANCE);
        // #message-prefix-in-tell
      }
    }
  }

  interface Messages {
    // #message-protocol
    interface CounterProtocol {
      interface Command {}

      public static class Increment implements Command {
        public final int delta;
        private final ActorRef<OperationResult> replyTo;

        public Increment(int delta, ActorRef<OperationResult> replyTo) {
          this.delta = delta;
          this.replyTo = replyTo;
        }
      }

      public static class Decrement implements Command {
        public final int delta;
        private final ActorRef<OperationResult> replyTo;

        public Decrement(int delta, ActorRef<OperationResult> replyTo) {
          this.delta = delta;
          this.replyTo = replyTo;
        }
      }

      interface OperationResult {}

      enum Confirmed implements OperationResult {
        INSTANCE
      }

      public static class Rejected implements OperationResult {
        public final String reason;

        public Rejected(String reason) {
          this.reason = reason;
        }
      }
    }
    // #message-protocol
  }

  interface PublicVsPrivateMessages1 {
    // #on-message-lambda-anti
    // this is an anti-pattern, don't use lambdas with a large block of code
    // #on-message-lambda-anti
    // #public-private-messages-1
    public class Counter extends AbstractBehavior<Counter.Command> {

      public interface Command {}

      public enum Increment implements Command {
        INSTANCE
      }

      public static class GetValue implements Command {
        public final ActorRef<Value> replyTo;

        public GetValue(ActorRef<Value> replyTo) {
          this.replyTo = replyTo;
        }
      }

      public static class Value {
        public final int value;

        public Value(int value) {
          this.value = value;
        }
      }

      // Tick is private so can't be sent from the outside
      private enum Tick implements Command {
        INSTANCE
      }

      public static Behavior<Command> create(String name, Duration tickInterval) {
        return Behaviors.setup(
            context ->
                Behaviors.withTimers(
                    timers -> {
                      timers.startTimerWithFixedDelay(Tick.INSTANCE, tickInterval);
                      return new Counter(name, context);
                    }));
      }

      private final String name;
      private int count;

      private Counter(String name, ActorContext<Command> context) {
        super(context);
        this.name = name;
      }

      // #on-message-lambda
      // #on-message-method-ref
      @Override
      // #on-message-lambda-anti
      public Receive<Command> createReceive() {
        // #on-message-lambda-anti
        return newReceiveBuilder()
            // #on-message-method-ref
            .onMessage(Increment.class, notUsed -> onIncrement())
            // #on-message-lambda
            .onMessage(Tick.class, notUsed -> onTick())
            // #on-message-method-ref
            .onMessage(GetValue.class, this::onGetValue)
            // #on-message-lambda
            .build();
      }

      // #on-message-lambda
      // #on-message-method-ref

      // #on-message-lambda
      private Behavior<Command> onIncrement() {
        count++;
        getContext().getLog().debug("[{}] Incremented counter to [{}]", name, count);
        return this;
      }
      // #on-message-lambda

      private Behavior<Command> onTick() {
        count++;
        getContext()
            .getLog()
            .debug("[{}] Incremented counter by background tick to [{}]", name, count);
        return this;
      }

      // #on-message-method-ref
      private Behavior<Command> onGetValue(GetValue command) {
        command.replyTo.tell(new Value(count));
        return this;
      }
      // #on-message-method-ref

      // #public-private-messages-1
      // anti-pattern, don't do like this
      public Receive<Command> createReceiveAnti() {
        // #on-message-lambda-anti
        return newReceiveBuilder()
            .onMessage(
                Increment.class,
                notUsed -> {
                  count++;
                  getContext().getLog().debug("[{}] Incremented counter to [{}]", name, count);
                  return this;
                })
            .onMessage(
                Tick.class,
                notUsed -> {
                  count++;
                  getContext()
                      .getLog()
                      .debug("[{}] Incremented counter by background tick to [{}]", name, count);
                  return this;
                })
            .onMessage(
                GetValue.class,
                command -> {
                  command.replyTo.tell(new Value(count));
                  return this;
                })
            .build();
      }
      // #on-message-lambda-anti
      // #public-private-messages-1
    }
    // #public-private-messages-1

  }

  interface PublicVsPrivateMessages2 {
    // #public-private-messages-2
    // above example is preferred, but this is possible and not wrong
    public class Counter extends AbstractBehavior<Counter.Message> {

      // The type of all public and private messages the Counter actor handles
      public interface Message {}

      /** Counter's public message protocol type. */
      public interface Command extends Message {}

      public enum Increment implements Command {
        INSTANCE
      }

      public static class GetValue implements Command {
        public final ActorRef<Value> replyTo;

        public GetValue(ActorRef<Value> replyTo) {
          this.replyTo = replyTo;
        }
      }

      public static class Value {
        public final int value;

        public Value(int value) {
          this.value = value;
        }
      }

      // The type of the Counter actor's internal messages.
      interface PrivateCommand extends Message {}

      // Tick is a private command so can't be sent to an ActorRef<Command>
      enum Tick implements PrivateCommand {
        INSTANCE
      }

      public static Behavior<Command> create(String name, Duration tickInterval) {
        return Behaviors.setup(
                (ActorContext<Message> context) ->
                    Behaviors.withTimers(
                        timers -> {
                          timers.startTimerWithFixedDelay(Tick.INSTANCE, tickInterval);
                          return new Counter(name, context);
                        }))
            .narrow(); // note narrow here
      }

      private final String name;
      private int count;

      private Counter(String name, ActorContext<Message> context) {
        super(context);
        this.name = name;
      }

      @Override
      public Receive<Message> createReceive() {
        return newReceiveBuilder()
            .onMessage(Increment.class, notUsed -> onIncrement())
            .onMessage(Tick.class, notUsed -> onTick())
            .onMessage(GetValue.class, this::onGetValue)
            .build();
      }

      private Behavior<Message> onIncrement() {
        count++;
        getContext().getLog().debug("[{}] Incremented counter to [{}]", name, count);
        return this;
      }

      private Behavior<Message> onTick() {
        count++;
        getContext()
            .getLog()
            .debug("[{}] Incremented counter by background tick to [{}]", name, count);
        return this;
      }

      private Behavior<Message> onGetValue(GetValue command) {
        command.replyTo.tell(new Value(count));
        return this;
      }
    }
    // #public-private-messages-2
  }

  interface NestingSample1 {
    interface Command {}

    // #nesting
    public static Behavior<Command> apply() {
      return Behaviors.setup(
          context ->
              Behaviors.withStash(
                  100,
                  stash ->
                      Behaviors.withTimers(
                          timers -> {
                            context.getLog().debug("Starting up");

                            // behavior using context, stash and timers ...
                            // #nesting
                            return Behaviors.empty();
                            // #nesting
                          })));
    }
    // #nesting
  }

  interface NestingSample2 {
    interface Command {}

    // #nesting-supervise
    public static Behavior<Command> create() {
      return Behaviors.setup(
          context -> {
            // only run on initial actor start, not on crash-restart
            context.getLog().info("Starting");

            return Behaviors.<Command>supervise(
                    Behaviors.withStash(
                        100,
                        stash -> {
                          // every time the actor crashes and restarts a new stash is created
                          // (previous stash is lost)
                          context.getLog().debug("Starting up with stash");
                          // Behaviors.receiveMessage { ... }
                          // #nesting-supervise
                          return Behaviors.empty();
                          // #nesting-supervise
                        }))
                .onFailure(RuntimeException.class, SupervisorStrategy.restart());
          });
    }
    // #nesting-supervise
  }
}
