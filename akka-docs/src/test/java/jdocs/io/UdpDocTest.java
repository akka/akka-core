/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.io;

// #imports
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.io.Udp;
import akka.io.UdpConnected;
import akka.io.UdpConnectedMessage;
import akka.io.UdpMessage;
import akka.util.ByteString;
import java.net.InetSocketAddress;

// #imports

public class UdpDocTest {

  // #sender
  public static class SimpleSender extends AbstractActor {
    final InetSocketAddress remote;

    public SimpleSender(InetSocketAddress remote) {
      this.remote = remote;

      // request creation of a SimpleSender
      final ActorRef mgr = Udp.get(getContext().getSystem()).getManager();
      mgr.tell(UdpMessage.simpleSender(), getSelf());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Udp.SimpleSenderReady.class,
              message -> {
                getContext().become(ready(getSender()));
                // #sender
                getSender()
                    .tell(UdpMessage.send(ByteString.fromString("hello"), remote), getSelf());
                // #sender
              })
          .build();
    }

    private Receive ready(final ActorRef send) {
      return receiveBuilder()
          .match(
              String.class,
              message -> {
                send.tell(UdpMessage.send(ByteString.fromString(message), remote), getSelf());
                // #sender
                if (message.equals("world")) {
                  send.tell(PoisonPill.getInstance(), getSelf());
                }
                // #sender
              })
          .build();
    }
  }
  // #sender

  // #listener
  public static class Listener extends AbstractActor {
    final ActorRef nextActor;

    public Listener(ActorRef nextActor) {
      this.nextActor = nextActor;

      // request creation of a bound listen socket
      final ActorRef mgr = Udp.get(getContext().getSystem()).getManager();
      mgr.tell(UdpMessage.bind(getSelf(), new InetSocketAddress("localhost", 0)), getSelf());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Udp.Bound.class,
              bound -> {
                // #listener
                nextActor.tell(bound.localAddress(), getSender());
                // #listener
                getContext().become(ready(getSender()));
              })
          .build();
    }

    private Receive ready(final ActorRef socket) {
      return receiveBuilder()
          .match(
              Udp.Received.class,
              r -> {
                // echo server example: send back the data
                socket.tell(UdpMessage.send(r.data(), r.sender()), getSelf());
                // or do some processing and forward it on
                final Object processed = // parse data etc., e.g. using PipelineStage
                    // #listener
                    r.data().utf8String();
                // #listener
                nextActor.tell(processed, getSelf());
              })
          .matchEquals(
              UdpMessage.unbind(),
              message -> {
                socket.tell(message, getSelf());
              })
          .match(
              Udp.Unbound.class,
              message -> {
                getContext().stop(getSelf());
              })
          .build();
    }
  }
  // #listener

  // #connected
  public static class Connected extends AbstractActor {
    final InetSocketAddress remote;

    public Connected(InetSocketAddress remote) {
      this.remote = remote;

      // create a restricted a.k.a. “connected” socket
      final ActorRef mgr = UdpConnected.get(getContext().getSystem()).getManager();
      mgr.tell(UdpConnectedMessage.connect(getSelf(), remote), getSelf());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              UdpConnected.Connected.class,
              message -> {
                getContext().become(ready(getSender()));
                // #connected
                getSender()
                    .tell(UdpConnectedMessage.send(ByteString.fromString("hello")), getSelf());
                // #connected
              })
          .build();
    }

    private Receive ready(final ActorRef connection) {
      return receiveBuilder()
          .match(
              UdpConnected.Received.class,
              r -> {
                // process data, send it on, etc.
                // #connected
                if (r.data().utf8String().equals("hello")) {
                  connection.tell(
                      UdpConnectedMessage.send(ByteString.fromString("world")), getSelf());
                }
                // #connected
              })
          .match(
              String.class,
              str -> {
                connection.tell(UdpConnectedMessage.send(ByteString.fromString(str)), getSelf());
              })
          .matchEquals(
              UdpConnectedMessage.disconnect(),
              message -> {
                connection.tell(message, getSelf());
              })
          .match(
              UdpConnected.Disconnected.class,
              x -> {
                getContext().stop(getSelf());
              })
          .build();
    }
  }
  // #connected

}
