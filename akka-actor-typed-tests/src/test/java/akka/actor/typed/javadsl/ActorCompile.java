/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.javadsl;

import static akka.actor.typed.javadsl.Behaviors.*;

import akka.actor.typed.*;
import akka.actor.typed.TypedActorContext;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

@SuppressWarnings("unused")
public class ActorCompile {

  interface MyMsg {}

  static class MyMsgA implements MyMsg {
    final ActorRef<String> replyTo;

    public MyMsgA(ActorRef<String> replyTo) {
      this.replyTo = replyTo;
    }
  }

  static class MyMsgB implements MyMsg {
    final String greeting;

    public MyMsgB(String greeting) {
      this.greeting = greeting;
    }
  }

  Behavior<MyMsg> actor1 =
      Behaviors.receive((context, message) -> stopped(), (context, signal) -> same());
  Behavior<MyMsg> actor2 = Behaviors.receive((context, message) -> unhandled());
  Behavior<MyMsg> actor4 = empty();
  Behavior<MyMsg> actor5 = ignore();
  Behavior<MyMsg> actor6 =
      intercept(
          () ->
              new BehaviorInterceptor<MyMsg, MyMsg>(MyMsg.class) {
                @Override
                public Behavior<MyMsg> aroundReceive(
                    TypedActorContext<MyMsg> context, MyMsg message, ReceiveTarget<MyMsg> target) {
                  return target.apply(context, message);
                }

                @Override
                public Behavior<MyMsg> aroundSignal(
                    TypedActorContext<MyMsg> context, Signal signal, SignalTarget<MyMsg> target) {
                  return target.apply(context, signal);
                }
              },
          actor5);
  Behavior<MyMsgA> actor7 = actor6.narrow();
  Behavior<MyMsg> actor8 =
      setup(
          context -> {
            final ActorRef<MyMsg> self = context.getSelf();
            return monitor(MyMsg.class, self, ignore());
          });

  Behavior<MyMsgA> actor9a =
      transformMessages(MyMsgA.class, actor7, pf -> pf.match(MyMsgA.class, x -> x));
  Behavior<MyMsg> actor9b =
      transformMessages(MyMsg.class, actor7, pf -> pf.match(MyMsgA.class, x -> x));

  // this is the example from the Javadoc
  Behavior<String> s =
      Behaviors.receive(
          (ctx, msg) -> {
            return Behaviors.same();
          });
  Behavior<Number> n =
      Behaviors.transformMessages(
          Number.class,
          s,
          pf ->
              pf.match(BigInteger.class, i -> "BigInteger(" + i + ")")
                  .match(BigDecimal.class, d -> "BigDecimal(" + d + ")")
          // drop all other kinds of Number
          );

  Behavior<MyMsg> actor10 =
      Behaviors.receive((context, message) -> stopped(() -> {}), (context, signal) -> same());

  ActorSystem<MyMsg> system = ActorSystem.create(actor1, "Sys");

  {
    ActorRef<MyMsg> recipient = null;

    CompletionStage<String> reply =
        AskPattern.ask(
            recipient, replyTo -> new MyMsgA(replyTo), Duration.ofSeconds(3), system.scheduler());

    AskPattern.ask(
            recipient,
            (ActorRef<String> replyTo) -> new MyMsgA(replyTo),
            Duration.ofSeconds(3),
            system.scheduler())
        .thenApply(rsp -> rsp.toUpperCase());
  }

  {
    Behaviors.<MyMsg>receive(
        (context, message) -> {
          if (message instanceof MyMsgA) {
            return Behaviors.receive(
                (ctx2, msg2) -> {
                  if (msg2 instanceof MyMsgB) {
                    ((MyMsgA) message).replyTo.tell(((MyMsgB) msg2).greeting);

                    ActorRef<String> adapter =
                        ctx2.messageAdapter(String.class, s -> new MyMsgB(s.toUpperCase()));
                  }
                  return same();
                });
          } else return unhandled();
        });
  }

  {
    Behavior<MyMsg> b =
        Behaviors.withTimers(
            timers -> {
              timers.startTimerWithFixedDelay("key", new MyMsgB("tick"), Duration.ofSeconds(1));
              return Behaviors.ignore();
            });
  }

  {
    Behavior<MyMsg> b =
        Behaviors.withTimers(
            timers -> {
              timers.startTimerWithFixedDelay(new MyMsgB("tick"), Duration.ofSeconds(1));
              return Behaviors.ignore();
            });
  }

  static class MyBehavior extends ExtensibleBehavior<MyMsg> {

    @Override
    public Behavior<MyMsg> receiveSignal(TypedActorContext<MyMsg> context, Signal message)
        throws Exception {
      return this;
    }

    @Override
    public Behavior<MyMsg> receive(TypedActorContext<MyMsg> context, MyMsg message)
        throws Exception {
      ActorRef<String> adapter =
          context.asJava().messageAdapter(String.class, s -> new MyMsgB(s.toUpperCase()));
      return this;
    }
  }

  // SupervisorStrategy
  {
    SupervisorStrategy strategy1 = SupervisorStrategy.restart();
    SupervisorStrategy strategy2 = SupervisorStrategy.restart().withLoggingEnabled(false);
    SupervisorStrategy strategy3 = SupervisorStrategy.resume();
    SupervisorStrategy strategy4 = SupervisorStrategy.restart().withLimit(3, Duration.ofSeconds(1));

    SupervisorStrategy strategy5 =
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(10), 0.1);

    BackoffSupervisorStrategy strategy6 =
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(10), 0.1);
    SupervisorStrategy strategy7 = strategy6.withResetBackoffAfter(Duration.ofSeconds(2));

    Behavior<MyMsg> behv =
        Behaviors.supervise(
                Behaviors.supervise(Behaviors.<MyMsg>ignore())
                    .onFailure(IllegalStateException.class, strategy6))
            .onFailure(RuntimeException.class, strategy1);
    // or using flattern API:
      Behavior<MyMsg> flatternBehv =
          Behaviors.supervise(Behaviors.<MyMsg>ignore())
              .whenFailure(IllegalStateException.class, strategy6)
              .whenFailure(RuntimeException.class, strategy1);
  }

  // actor context
  {
    final ActorRef<Object> otherActor = null;
    Behavior<String> behavior =
        Behaviors.setup(
            context -> {
              context.ask(
                  String.class,
                  otherActor,
                  Duration.ofSeconds(10),
                  (ActorRef<String> respRef) -> new Object(),
                  (String res, Throwable failure) -> {
                    // checked exception should be ok
                    if (failure != null) throw new Exception(failure);
                    else return "success";
                  });

              ActorRef<Integer> adapter =
                  context.messageAdapter(
                      Integer.class,
                      (number) -> {
                        // checked exception should be ok
                        if (number < 10) throw new Exception("too small number");
                        else return number.toString();
                      });

              return Behaviors.empty();
            });
  }

  // stash buffer
  {
    Behavior<String> behavior =
        Behaviors.withStash(
            5,
            stash -> {
              stash.forEach(
                  msg -> {
                    // checked is ok
                    throw new Exception("checked");
                  });

              return Behaviors.empty();
            });
  }
}
