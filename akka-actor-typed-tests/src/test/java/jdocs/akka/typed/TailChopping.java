/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.typed;

// #behavior
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import java.time.Duration;
import java.util.function.BiFunction;

public class TailChopping<Reply> extends AbstractBehavior<TailChopping.Command> {

  interface Command {}

  private enum RequestTimeout implements Command {
    INSTANCE
  }

  private enum FinalTimeout implements Command {
    INSTANCE
  }

  private class WrappedReply implements Command {
    final Reply reply;

    private WrappedReply(Reply reply) {
      this.reply = reply;
    }
  }

  public static <R> Behavior<Command> create(
      Class<R> replyClass,
      BiFunction<Integer, ActorRef<R>, Boolean> sendRequest,
      Duration nextRequestAfter,
      ActorRef<R> replyTo,
      Duration finalTimeout,
      R timeoutReply) {
    return Behaviors.setup(
        context ->
            Behaviors.withTimers(
                timers ->
                    new TailChopping<R>(
                        replyClass,
                        context,
                        timers,
                        sendRequest,
                        nextRequestAfter,
                        replyTo,
                        finalTimeout,
                        timeoutReply)));
  }

  private final TimerScheduler<Command> timers;
  private final BiFunction<Integer, ActorRef<Reply>, Boolean> sendRequest;
  private final Duration nextRequestAfter;
  private final ActorRef<Reply> replyTo;
  private final Duration finalTimeout;
  private final Reply timeoutReply;
  private final ActorRef<Reply> replyAdapter;

  private int requestCount = 0;

  private TailChopping(
      Class<Reply> replyClass,
      ActorContext<Command> context,
      TimerScheduler<Command> timers,
      BiFunction<Integer, ActorRef<Reply>, Boolean> sendRequest,
      Duration nextRequestAfter,
      ActorRef<Reply> replyTo,
      Duration finalTimeout,
      Reply timeoutReply) {
    super(context);
    this.timers = timers;
    this.sendRequest = sendRequest;
    this.nextRequestAfter = nextRequestAfter;
    this.replyTo = replyTo;
    this.finalTimeout = finalTimeout;
    this.timeoutReply = timeoutReply;

    replyAdapter = context.messageAdapter(replyClass, WrappedReply::new);

    sendNextRequest();
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(WrappedReply.class, this::onReply)
        .onMessage(RequestTimeout.class, notUsed -> onRequestTimeout())
        .onMessage(FinalTimeout.class, notUsed -> onFinalTimeout())
        .build();
  }

  private Behavior<Command> onReply(WrappedReply wrappedReply) {
    Reply reply = wrappedReply.reply;
    replyTo.tell(reply);
    return Behaviors.stopped();
  }

  private Behavior<Command> onRequestTimeout() {
    sendNextRequest();
    return this;
  }

  private Behavior<Command> onFinalTimeout() {
    replyTo.tell(timeoutReply);
    return Behaviors.stopped();
  }

  private void sendNextRequest() {
    requestCount++;
    if (sendRequest.apply(requestCount, replyAdapter)) {
      timers.startSingleTimer(RequestTimeout.INSTANCE, RequestTimeout.INSTANCE, nextRequestAfter);
    } else {
      timers.startSingleTimer(FinalTimeout.INSTANCE, FinalTimeout.INSTANCE, finalTimeout);
    }
  }
}
// #behavior
