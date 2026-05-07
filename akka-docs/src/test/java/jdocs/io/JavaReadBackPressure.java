/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.io;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.io.Inet;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.util.ByteString;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class JavaReadBackPressure {

  public static class Listener extends AbstractActor {
    ActorRef tcp;
    ActorRef listener;

    @Override
    // #pull-accepting
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Tcp.Bound.class,
              x -> {
                listener = getSender();
                // Accept connections one by one
                listener.tell(TcpMessage.resumeAccepting(1), getSelf());
              })
          .match(
              Tcp.Connected.class,
              x -> {
                ActorRef handler = getContext().actorOf(Props.create(PullEcho.class, getSender()));
                getSender().tell(TcpMessage.register(handler), getSelf());
                // Resume accepting connections
                listener.tell(TcpMessage.resumeAccepting(1), getSelf());
              })
          .build();
    }
    // #pull-accepting

    @Override
    public void preStart() throws Exception {
      // #pull-mode-bind
      tcp = Tcp.get(getContext().getSystem()).manager();
      final List<Inet.SocketOption> options = new ArrayList<Inet.SocketOption>();
      tcp.tell(
          TcpMessage.bind(getSelf(), new InetSocketAddress("localhost", 0), 100, options, true),
          getSelf());
      // #pull-mode-bind
    }

    private void demonstrateConnect() {
      // #pull-mode-connect
      final List<Inet.SocketOption> options = new ArrayList<Inet.SocketOption>();
      Duration timeout = null;
      tcp.tell(
          TcpMessage.connect(
              new InetSocketAddress("localhost", 3000), null, options, timeout, true),
          getSelf());
      // #pull-mode-connect
    }
  }

  public static class Ack implements Tcp.Event {}

  public static class PullEcho extends AbstractActor {
    final ActorRef connection;

    public PullEcho(ActorRef connection) {
      this.connection = connection;
    }

    // #pull-reading-echo
    @Override
    public void preStart() throws Exception {
      connection.tell(TcpMessage.resumeReading(), getSelf());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Tcp.Received.class,
              message -> {
                ByteString data = message.data();
                connection.tell(TcpMessage.write(data, new Ack()), getSelf());
              })
          .match(
              Ack.class,
              message -> {
                connection.tell(TcpMessage.resumeReading(), getSelf());
              })
          .build();
    }
    // #pull-reading-echo
  }
}
