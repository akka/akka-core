/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
// #ask-actor
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.pattern.StatusReply;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.typed.javadsl.ActorFlow;

// #ask-actor
import java.time.Duration;

public class ActorFlowCompileTest {

  final ActorSystem<String> system = null;

  static
  // #ask-actor
  class Asking {
    final String payload;
    final ActorRef<Reply> replyTo;

    public Asking(String payload, ActorRef<Reply> replyTo) {
      this.payload = payload;
      this.replyTo = replyTo;
    }
  }

  static class AskingWithStatus {
    final String payload;
    final ActorRef<StatusReply<String>> replyTo;

    public AskingWithStatus(String payload, ActorRef<StatusReply<String>> replyTo) {
      this.payload = payload;
      this.replyTo = replyTo;
    }
  }

  // #ask-actor
  static
  // #ask-actor
  class Reply {
    public final String msg;

    public Reply(String msg) {
      this.msg = msg;
    }
  }

  // #ask-actor

  {
    // #ask
    final ActorRef<Asking> actorRef = // ???
        // #ask
        null;

    // #ask
    final ActorRef<AskingWithStatus> actorWithStatusRef = // ???
        // #ask
        null;

    // #ask
    Duration timeout = Duration.ofSeconds(1);

    // method reference notation
    Flow<String, Reply, NotUsed> askFlow = ActorFlow.ask(actorRef, timeout, Asking::new);

    // explicit creation of the sent message
    Flow<String, Reply, NotUsed> askFlowExplicit =
        ActorFlow.ask(actorRef, timeout, (msg, replyTo) -> new Asking(msg, replyTo));

    Flow<String, String, NotUsed> askFlowExplicitWithStatus =
        ActorFlow.askWithStatus(
            actorWithStatusRef, timeout, (msg, replyTo) -> new AskingWithStatus(msg, replyTo));

    Source.repeat("hello").via(askFlow).map(reply -> reply.msg).runWith(Sink.seq(), system);
    // #ask

    // #askWithContext

    // method reference notation
    Flow<akka.japi.Pair<String, Long>, akka.japi.Pair<Reply, Long>, NotUsed> askFlowWithContext =
        ActorFlow.askWithContext(actorRef, timeout, Asking::new);

    // explicit creation of the sent message
    Flow<akka.japi.Pair<String, Long>, akka.japi.Pair<Reply, Long>, NotUsed>
        askFlowExplicitWithContext =
            ActorFlow.askWithContext(actorRef, timeout, (msg, replyTo) -> new Asking(msg, replyTo));

    Flow<akka.japi.Pair<String, Long>, akka.japi.Pair<String, Long>, NotUsed>
        askFlowExplicitWithStatusAndContext =
            ActorFlow.askWithStatusAndContext(
                actorWithStatusRef, timeout, (msg, replyTo) -> new AskingWithStatus(msg, replyTo));

    Source.repeat("hello")
        .zipWithIndex()
        .via(askFlowWithContext)
        .map(pair -> pair.first().msg)
        .runWith(Sink.seq(), system);
    // #askWithContext
  }
}
