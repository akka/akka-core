/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.io;

import akka.japi.pf.ReceiveBuilder;
import org.junit.Test;

import akka.actor.ActorSystem;
import akka.actor.AbstractActor;
// #imports
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import akka.actor.ActorRef;
import akka.io.Inet;
import akka.io.UdpConnected;
import akka.io.UdpConnectedMessage;
import akka.io.UdpSO;
import akka.util.ByteString;

import static akka.util.ByteString.emptyByteString;

// #imports

public class UdpConnectedDocTest {

  public static class Demo extends AbstractActor {
    ActorRef connectionActor = null;
    ActorRef handler = getSelf();
    ActorSystem system = getContext().getSystem();

    @Override
    public Receive createReceive() {
      ReceiveBuilder builder = receiveBuilder();
      builder.matchEquals(
          "connect",
          message -> {
            // #manager
            final ActorRef udp = UdpConnected.get(system).manager();
            // #manager
            // #connect
            final InetSocketAddress remoteAddr = new InetSocketAddress("127.0.0.1", 12345);
            udp.tell(UdpConnectedMessage.connect(handler, remoteAddr), getSelf());
            // #connect
            // #connect-with-options
            final InetSocketAddress localAddr = new InetSocketAddress("127.0.0.1", 1234);
            final List<Inet.SocketOption> options = new ArrayList<Inet.SocketOption>();
            options.add(UdpSO.broadcast(true));
            udp.tell(
                UdpConnectedMessage.connect(handler, remoteAddr, localAddr, options), getSelf());
            // #connect-with-options
          });
      // #connected
      builder.match(
          UdpConnected.Connected.class,
          conn -> {
            connectionActor = getSender(); // Save the worker ref for later use
          });
      // #connected
      // #received
      builder
          .match(
              UdpConnected.Received.class,
              recv -> {
                final ByteString data = recv.data();
                // and do something with the received data ...
              })
          .match(
              UdpConnected.CommandFailed.class,
              failed -> {
                final UdpConnected.Command command = failed.cmd();
                // react to failed connect, etc.
              })
          .match(
              UdpConnected.Disconnected.class,
              x -> {
                // do something on disconnect
              });
      // #received
      builder.matchEquals(
          "send",
          x -> {
            ByteString data = emptyByteString();
            // #send
            connectionActor.tell(UdpConnectedMessage.send(data), getSelf());
            // #send
          });
      return builder.build();
    }
  }

  @Test
  public void demonstrateConnect() {}
}
