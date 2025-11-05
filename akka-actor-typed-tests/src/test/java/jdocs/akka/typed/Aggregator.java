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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class Aggregator<Reply, Aggregate> extends AbstractBehavior<Aggregator.Command> {

  interface Command {}

  private enum ReceiveTimeout implements Command {
    INSTANCE
  }

  private class WrappedReply implements Command {
    final Reply reply;

    private WrappedReply(Reply reply) {
      this.reply = reply;
    }
  }

  public static <R, A> Behavior<Command> create(
      Class<R> replyClass,
      Consumer<ActorRef<R>> sendRequests,
      int expectedReplies,
      ActorRef<A> replyTo,
      Function<List<R>, A> aggregateReplies,
      Duration timeout) {
    return Behaviors.setup(
        context ->
            new Aggregator<R, A>(
                replyClass,
                context,
                sendRequests,
                expectedReplies,
                replyTo,
                aggregateReplies,
                timeout));
  }

  private final int expectedReplies;
  private final ActorRef<Aggregate> replyTo;
  private final Function<List<Reply>, Aggregate> aggregateReplies;
  private final List<Reply> replies = new ArrayList<>();

  private Aggregator(
      Class<Reply> replyClass,
      ActorContext<Command> context,
      Consumer<ActorRef<Reply>> sendRequests,
      int expectedReplies,
      ActorRef<Aggregate> replyTo,
      Function<List<Reply>, Aggregate> aggregateReplies,
      Duration timeout) {
    super(context);
    this.expectedReplies = expectedReplies;
    this.replyTo = replyTo;
    this.aggregateReplies = aggregateReplies;

    context.setReceiveTimeout(timeout, ReceiveTimeout.INSTANCE);

    ActorRef<Reply> replyAdapter = context.messageAdapter(replyClass, WrappedReply::new);
    sendRequests.accept(replyAdapter);
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(WrappedReply.class, this::onReply)
        .onMessage(ReceiveTimeout.class, notUsed -> onReceiveTimeout())
        .build();
  }

  private Behavior<Command> onReply(WrappedReply wrappedReply) {
    Reply reply = wrappedReply.reply;
    replies.add(reply);
    if (replies.size() == expectedReplies) {
      Aggregate result = aggregateReplies.apply(replies);
      replyTo.tell(result);
      return Behaviors.stopped();
    } else {
      return this;
    }
  }

  private Behavior<Command> onReceiveTimeout() {
    Aggregate result = aggregateReplies.apply(replies);
    replyTo.tell(result);
    return Behaviors.stopped();
  }
}
// #behavior
