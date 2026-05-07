/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.circuitbreaker;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.CircuitBreaker;
import akka.pattern.StatusReply;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

public class CircuitBreakerDocTest {

  // note: config sample is in Scala CircuitBreakerDocSpec

  interface ThirdPartyWebService {
    CompletionStage<Done> call(String id, String value);
  }

  static // #circuit-breaker-usage
  class DataAccess extends AbstractBehavior<DataAccess.Command> {

    public interface Command {}

    public static class Handle implements Command {
      final String value;
      final ActorRef<StatusReply<Done>> replyTo;

      public Handle(String value, ActorRef<StatusReply<Done>> replyTo) {
        this.value = value;
        this.replyTo = replyTo;
      }
    }

    private final class HandleFailed implements Command {
      final Throwable failure;
      final ActorRef<StatusReply<Done>> replyTo;

      public HandleFailed(Throwable failure, ActorRef<StatusReply<Done>> replyTo) {
        this.failure = failure;
        this.replyTo = replyTo;
      }
    }

    private final class HandleSuceeded implements Command {
      final ActorRef<StatusReply<Done>> replyTo;

      public HandleSuceeded(ActorRef<StatusReply<Done>> replyTo) {
        this.replyTo = replyTo;
      }
    }

    private final class CircuitBreakerStateChange implements Command {
      final String newState;

      public CircuitBreakerStateChange(String newState) {
        this.newState = newState;
      }
    }

    public static Behavior<Command> create(String id, ThirdPartyWebService service) {
      return Behaviors.setup(
          context -> {
            // #circuit-breaker-initialization
            CircuitBreaker circuitBreaker =
                CircuitBreaker.lookup("data-access", context.getSystem());
            // #circuit-breaker-initialization
            return new DataAccess(context, id, service, circuitBreaker);
          });
    }

    private final String id;
    private final ThirdPartyWebService service;
    private final CircuitBreaker circuitBreaker;

    public DataAccess(
        ActorContext<Command> context,
        String id,
        ThirdPartyWebService service,
        CircuitBreaker circuitBreaker) {
      super(context);
      this.id = id;
      this.service = service;
      this.circuitBreaker = circuitBreaker;
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(Handle.class, this::onHandle)
          .onMessage(HandleSuceeded.class, this::onHandleSucceeded)
          .onMessage(HandleFailed.class, this::onHandleFailed)
          .build();
    }

    private Behavior<Command> onHandle(Handle handle) {
      CompletionStage<Done> futureResult =
          circuitBreaker.callWithCircuitBreakerCS(() -> service.call(id, handle.value));
      getContext()
          .pipeToSelf(
              futureResult,
              (done, throwable) -> {
                if (throwable != null) {
                  return new HandleFailed(throwable, handle.replyTo);
                } else {
                  return new HandleSuceeded(handle.replyTo);
                }
              });
      return this;
    }

    private Behavior<Command> onHandleSucceeded(HandleSuceeded handleSuceeded) {
      handleSuceeded.replyTo.tell(StatusReply.ack());
      return this;
    }

    private Behavior<Command> onHandleFailed(HandleFailed handleFailed) {
      getContext().getLog().warn("Failed to call web service", handleFailed.failure);
      handleFailed.replyTo.tell(StatusReply.error("Dependency service not available"));
      return this;
    }

    // #circuit-breaker-usage
    public int luckyNumber() {
      // #even-no-as-failure
      BiFunction<Optional<Integer>, Optional<Throwable>, Boolean> evenNoAsFailure =
          (result, err) -> (result.isPresent() && result.get() % 2 == 0);

      // this will return 8888 and increase failure count at the same time
      return circuitBreaker.callWithSyncCircuitBreaker(() -> 8888, evenNoAsFailure);
      // #even-no-as-failure
    }
    // #circuit-breaker-usage
  }
  // #circuit-breaker-usage

  public static class OtherActor {
    public interface Command {}

    public static class Call implements Command {
      public final String payload;
      public final ActorRef<StatusReply<Done>> replyTo;

      public Call(String payload, ActorRef<StatusReply<Done>> replyTo) {
        this.payload = payload;
        this.replyTo = replyTo;
      }
    }
  }

  public // #circuit-breaker-tell-pattern
  static class CircuitBreakingIntermediateActor
      extends AbstractBehavior<CircuitBreakingIntermediateActor.Command> {

    public interface Command {}

    public static class Call implements Command {
      final String payload;
      final ActorRef<StatusReply<Done>> replyTo;

      public Call(String payload, ActorRef<StatusReply<Done>> replyTo) {
        this.payload = payload;
        this.replyTo = replyTo;
      }
    }

    private class OtherActorReply implements Command {
      final Optional<Throwable> failure;
      final ActorRef<StatusReply<Done>> originalReplyTo;

      public OtherActorReply(
          Optional<Throwable> failure, ActorRef<StatusReply<Done>> originalReplyTo) {
        this.failure = failure;
        this.originalReplyTo = originalReplyTo;
      }
    }

    private class BreakerOpen implements Command {}

    private final ActorRef<OtherActor.Command> target;
    private final CircuitBreaker breaker;

    public CircuitBreakingIntermediateActor(
        ActorContext<Command> context, ActorRef<OtherActor.Command> targetActor) {
      super(context);
      this.target = targetActor;
      // #manual-construction
      breaker =
          CircuitBreaker.create(
                  getContext().getSystem().classicSystem().getScheduler(),
                  // maxFailures
                  5,
                  // callTimeout
                  Duration.ofSeconds(10),
                  // resetTimeout
                  Duration.ofMinutes(1))
              .addOnOpenListener(() -> context.getSelf().tell(new BreakerOpen()));
      // #manual-construction
    }

    @Override
    public Receive<Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(Call.class, this::onCall)
          .onMessage(OtherActorReply.class, this::onOtherActorReply)
          .onMessage(BreakerOpen.class, this::breakerOpened)
          .build();
    }

    private Behavior<Command> onCall(Call call) {
      if (breaker.isClosed() || breaker.isHalfOpen()) {
        getContext()
            .askWithStatus(
                Done.class,
                target,
                Duration.ofSeconds(11),
                (replyTo) -> new OtherActor.Call(call.payload, replyTo),
                (done, failure) -> new OtherActorReply(Optional.ofNullable(failure), call.replyTo));
      } else {
        call.replyTo.tell(StatusReply.error("Service unavailable"));
      }
      return this;
    }

    private Behavior<Command> onOtherActorReply(OtherActorReply otherActorReply) {
      if (otherActorReply.failure.isPresent()) {
        breaker.fail();
        getContext().getLog().warn("Service failure", otherActorReply.failure.get());
        otherActorReply.originalReplyTo.tell(StatusReply.error("Service unavailable"));
      } else {
        breaker.succeed();
        otherActorReply.originalReplyTo.tell(StatusReply.ack());
      }
      return this;
    }

    private Behavior<Command> breakerOpened(BreakerOpen breakerOpen) {
      getContext().getLog().warn("Circuit breaker open");
      return this;
    }
  }
  // #circuit-breaker-tell-pattern
}
