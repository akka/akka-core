/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.stream.operators.flow;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.japi.Pair;
import akka.stream.javadsl.*;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.util.ByteString;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class FromSinkAndSource {

  void halfClosedTcpServer() {
    ActorSystem system = null;
    // #halfClosedTcpServer
    // close in immediately
    Sink<ByteString, NotUsed> sink = Sink.cancelled();
    // periodic tick out
    Source<ByteString, Cancellable> source =
        Source.tick(Duration.ofSeconds(1), Duration.ofSeconds(1), "tick")
            .map(tick -> ByteString.fromString(System.currentTimeMillis() + "\n"));

    Flow<ByteString, ByteString, NotUsed> serverFlow = Flow.fromSinkAndSource(sink, source);

    Source<Tcp.IncomingConnection, CompletionStage<Tcp.ServerBinding>> connectionStream =
        Tcp.get(system)
            .bind(
                "127.0.0.1", // interface
                9999, // port
                100, // backlog
                Collections.emptyList(), // socket options
                true, // Important: half close enabled
                Optional.empty() // idle timeout
                );

    connectionStream.runForeach(
        incomingConnection -> incomingConnection.handleWith(serverFlow, system), system);
    // #halfClosedTcpServer
  }

  void chat() {
    ActorSystem system = null;
    // #chat
    Pair<Sink<String, NotUsed>, Source<String, NotUsed>> pair =
        MergeHub.of(String.class).toMat(BroadcastHub.of(String.class), Keep.both()).run(system);
    Sink<String, NotUsed> sink = pair.first();
    Source<String, NotUsed> source = pair.second();

    Flow<ByteString, ByteString, NotUsed> framing =
        Framing.delimiter(ByteString.fromString("\n"), 1024);

    Sink<ByteString, NotUsed> sinkWithFraming =
        framing.map(bytes -> bytes.utf8String()).to(pair.first());
    Source<ByteString, NotUsed> sourceWithFraming =
        source.map(text -> ByteString.fromString(text + "\n"));

    Flow<ByteString, ByteString, NotUsed> serverFlow =
        Flow.fromSinkAndSource(sinkWithFraming, sourceWithFraming);

    Tcp.get(system)
        .bind("127.0.0.1", 9999)
        .runForeach(
            incomingConnection -> incomingConnection.handleWith(serverFlow, system), system);
    // #chat
  }

  <In, Out> void myApiThatTakesAFlow(Flow<In, Out, NotUsed> flow) {
    throw new UnsupportedOperationException();
  }

  void testing() {
    ActorSystem system = null;
    // #testing
    TestSubscriber.Probe<String> inProbe = TestSubscriber.probe(system);
    TestPublisher.Probe<String> outProbe = TestPublisher.probe(0, system);
    Flow<String, String, NotUsed> testFlow =
        Flow.fromSinkAndSource(Sink.fromSubscriber(inProbe), Source.fromPublisher(outProbe));

    myApiThatTakesAFlow(testFlow);
    inProbe.expectNext("first");
    outProbe.expectRequest();
    outProbe.sendError(new RuntimeException("test error"));
    // ...
    // #testing
  }
}
