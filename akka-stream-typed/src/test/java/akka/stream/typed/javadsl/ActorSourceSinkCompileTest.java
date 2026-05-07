/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.typed.javadsl;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.util.Optional;

public class ActorSourceSinkCompileTest {

  interface Protocol {}

  class Init implements Protocol {}

  class Msg implements Protocol {}

  class Complete implements Protocol {}

  class Failure implements Protocol {
    public Exception ex;
  }

  {
    final ActorSystem<String> system = null;
  }

  {
    final ActorRef<String> ref = null;

    Source.<String>queue(10)
        .map(s -> s + "!")
        .to(ActorSink.actorRef(ref, "DONE", ex -> "FAILED: " + ex.getMessage()));
  }

  {
    final ActorRef<Protocol> ref = null;

    Source.<String>queue(10)
        .to(
            ActorSink.actorRefWithBackpressure(
                ref,
                (sender, msg) -> new Init(),
                (sender) -> new Msg(),
                "ACK",
                new Complete(),
                (f) -> new Failure()));
  }

  {
    ActorSource.actorRef(
            (m) -> m == "complete", (m) -> Optional.empty(), 10, OverflowStrategy.dropBuffer())
        .to(Sink.seq());
  }

  {
    ActorSource.actorRef(
            (m) -> false,
            (m) -> (m instanceof Failure) ? Optional.of(((Failure) m).ex) : Optional.empty(),
            10,
            OverflowStrategy.dropBuffer())
        .to(Sink.seq());
  }
}
