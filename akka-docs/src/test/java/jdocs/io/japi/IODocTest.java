/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.io.japi;

import akka.testkit.AkkaJUnitActorSystemResource;
import jdocs.AbstractJavaTest;
import akka.testkit.javadsl.TestKit;
import org.junit.ClassRule;
import org.junit.Test;

// #imports
import java.net.InetSocketAddress;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.AbstractActor;
import akka.io.Tcp;
import akka.io.Tcp.Bound;
import akka.io.Tcp.CommandFailed;
import akka.io.Tcp.Connected;
import akka.io.Tcp.ConnectionClosed;
import akka.io.Tcp.Received;
import akka.io.TcpMessage;
import akka.util.ByteString;
// #imports

import akka.testkit.AkkaSpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IODocTest extends AbstractJavaTest {

  public
  // #server
  static class Server extends AbstractActor {

    final ActorRef manager;

    public Server(ActorRef manager) {
      this.manager = manager;
    }

    public static Props props(ActorRef manager) {
      return Props.create(Server.class, manager);
    }

    @Override
    public void preStart() throws Exception {
      final ActorRef tcp = Tcp.get(getContext().getSystem()).manager();
      tcp.tell(TcpMessage.bind(getSelf(), new InetSocketAddress("localhost", 0), 100), getSelf());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Bound.class,
              msg -> {
                manager.tell(msg, getSelf());
              })
          .match(
              CommandFailed.class,
              msg -> {
                getContext().stop(getSelf());
              })
          .match(
              Connected.class,
              conn -> {
                manager.tell(conn, getSelf());
                final ActorRef handler =
                    getContext().actorOf(Props.create(SimplisticHandler.class));
                getSender().tell(TcpMessage.register(handler), getSelf());
              })
          .build();
    }
  }
  // #server

  public
  // #simplistic-handler
  static class SimplisticHandler extends AbstractActor {
    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              Received.class,
              msg -> {
                final ByteString data = msg.data();
                System.out.println(data);
                getSender().tell(TcpMessage.write(data), getSelf());
              })
          .match(
              ConnectionClosed.class,
              msg -> {
                getContext().stop(getSelf());
              })
          .build();
    }
  }
  // #simplistic-handler

  public
  // #client
  static class Client extends AbstractActor {

    final InetSocketAddress remote;
    final ActorRef listener;

    public static Props props(InetSocketAddress remote, ActorRef listener) {
      return Props.create(Client.class, remote, listener);
    }

    public Client(InetSocketAddress remote, ActorRef listener) {
      this.remote = remote;
      this.listener = listener;

      final ActorRef tcp = Tcp.get(getContext().getSystem()).manager();
      tcp.tell(TcpMessage.connect(remote), getSelf());
    }

    @Override
    public Receive createReceive() {
      return receiveBuilder()
          .match(
              CommandFailed.class,
              msg -> {
                listener.tell("failed", getSelf());
                getContext().stop(getSelf());
              })
          .match(
              Connected.class,
              msg -> {
                listener.tell(msg, getSelf());
                getSender().tell(TcpMessage.register(getSelf()), getSelf());
                getContext().become(connected(getSender()));
              })
          .build();
    }

    private Receive connected(final ActorRef connection) {
      return receiveBuilder()
          .match(
              ByteString.class,
              msg -> {
                connection.tell(TcpMessage.write((ByteString) msg), getSelf());
              })
          .match(
              CommandFailed.class,
              msg -> {
                // OS kernel socket buffer was full
              })
          .match(
              Received.class,
              msg -> {
                listener.tell(msg.data(), getSelf());
              })
          .matchEquals(
              "close",
              msg -> {
                connection.tell(TcpMessage.close(), getSelf());
              })
          .match(
              ConnectionClosed.class,
              msg -> {
                getContext().stop(getSelf());
              })
          .build();
    }
  }
  // #client

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource =
      new AkkaJUnitActorSystemResource("IODocTest", AkkaSpec.testConf());

  private final ActorSystem system = actorSystemResource.getSystem();

  @Test
  public void testConnection() {
    new TestKit(system) {
      {
        @SuppressWarnings("unused")
        final ActorRef server = system.actorOf(Server.props(getRef()), "server1");
        final InetSocketAddress listen = expectMsgClass(Bound.class).localAddress();
        final ActorRef client = system.actorOf(Client.props(listen, getRef()), "client1");

        final Connected c1 = expectMsgClass(Connected.class);
        final Connected c2 = expectMsgClass(Connected.class);
        assertTrue(c1.localAddress().equals(c2.remoteAddress()));
        assertTrue(c2.localAddress().equals(c1.remoteAddress()));

        client.tell(ByteString.fromString("hello"), getRef());
        final ByteString reply = expectMsgClass(ByteString.class);
        assertEquals("hello", reply.utf8String());

        watch(client);
        client.tell("close", getRef());
        expectTerminated(client);
      }
    };
  }
}
