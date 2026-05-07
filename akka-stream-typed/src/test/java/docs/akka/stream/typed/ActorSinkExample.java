/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.stream.typed;

// #actor-sink-ref
import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.typed.javadsl.ActorSink;

// #actor-sink-ref

public class ActorSinkExample {

  // #actor-sink-ref

  interface Protocol {}

  class Message implements Protocol {
    private final String msg;

    public Message(String msg) {
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
  // #actor-sink-ref

  final ActorSystem<Void> system = null;

  {
    // #actor-sink-ref

    final ActorRef<Protocol> actor = null;

    final Sink<Protocol, NotUsed> sink = ActorSink.actorRef(actor, new Complete(), Fail::new);

    Source.<Protocol>single(new Message("msg1")).runWith(sink, system);
    // #actor-sink-ref
  }
}
