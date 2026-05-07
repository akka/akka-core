/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.stream.typed;

// #actor-sink-ref-with-backpressure
import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.typed.javadsl.ActorSink;

// #actor-sink-ref-with-backpressure

public class ActorSinkWithAckExample {

  // #actor-sink-ref-with-backpressure

  enum Ack {
    INSTANCE;
  }

  interface Protocol {}

  class Init implements Protocol {
    private final ActorRef<Ack> ack;

    public Init(ActorRef<Ack> ack) {
      this.ack = ack;
    }
  }

  class Message implements Protocol {
    private final ActorRef<Ack> ackTo;
    private final String msg;

    public Message(ActorRef<Ack> ackTo, String msg) {
      this.ackTo = ackTo;
      this.msg = msg;
    }
  }

  class Complete implements Protocol {}

  class Fail implements Protocol {
    private final Throwable ex;

    public Fail(Throwable ex) {
      this.ex = ex;
    }
  }
  // #actor-sink-ref-with-backpressure

  final ActorSystem<Void> system = null;

  {
    // #actor-sink-ref-with-backpressure

    final ActorRef<Protocol> actorRef = // spawned actor
        null; // #hidden

    final Complete completeMessage = new Complete();

    final Sink<String, NotUsed> sink =
        ActorSink.actorRefWithBackpressure(
            actorRef,
            (responseActorRef, element) -> new Message(responseActorRef, element),
            (responseActorRef) -> new Init(responseActorRef),
            Ack.INSTANCE,
            completeMessage,
            (exception) -> new Fail(exception));

    Source.single("msg1").runWith(sink, system);
    // #actor-sink-ref-with-backpressure
  }
}
