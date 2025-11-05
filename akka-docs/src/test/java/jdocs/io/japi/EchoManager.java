/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.io.japi;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.io.Tcp.Bind;
import akka.io.Tcp.Bound;
import akka.io.Tcp.Connected;
import akka.io.TcpMessage;
import java.net.InetSocketAddress;

public class EchoManager extends AbstractActor {

  final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), getSelf());

  final Class<?> handlerClass;

  public EchoManager(Class<?> handlerClass) {
    this.handlerClass = handlerClass;
  }

  @Override
  public SupervisorStrategy supervisorStrategy() {
    return SupervisorStrategy.stoppingStrategy();
  }

  @Override
  public void preStart() throws Exception {
    // #manager
    final ActorRef tcpManager = Tcp.get(getContext().getSystem()).manager();
    // #manager
    tcpManager.tell(
        TcpMessage.bind(getSelf(), new InetSocketAddress("localhost", 0), 100), getSelf());
  }

  @Override
  public void postRestart(Throwable arg0) throws Exception {
    // do not restart
    getContext().stop(getSelf());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(
            Bound.class,
            msg -> {
              log.info("listening on [{}]", msg.localAddress());
            })
        .match(
            Tcp.CommandFailed.class,
            failed -> {
              if (failed.cmd() instanceof Bind) {
                log.warning("cannot bind to [{}]", ((Bind) failed.cmd()).localAddress());
                getContext().stop(getSelf());
              } else {
                log.warning("unknown command failed [{}]", failed.cmd());
              }
            })
        .match(
            Connected.class,
            conn -> {
              log.info("received connection from [{}]", conn.remoteAddress());
              final ActorRef connection = getSender();
              final ActorRef handler =
                  getContext()
                      .actorOf(Props.create(handlerClass, connection, conn.remoteAddress()));
              // #echo-manager
              connection.tell(
                  TcpMessage.register(
                      handler, true, // <-- keepOpenOnPeerClosed flag
                      true),
                  getSelf());
              // #echo-manager
            })
        .build();
  }
}
