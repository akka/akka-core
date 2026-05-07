/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import Tcp._

import akka.actor.{ ActorLogging, Props }

/**
 * INTERNAL API
 *
 * TcpManager is a facade for accepting commands ([[akka.io.Tcp.Command]]) to open client or server TCP connections.
 *
 * TcpManager is obtainable by calling {{{ IO(Tcp) }}} (see [[akka.io.IO]] and [[akka.io.Tcp]])
 *
 * == Bind ==
 *
 * To bind and listen to a local address, a [[akka.io.Tcp.Bind]] command must be sent to this actor. If the binding
 * was successful, the sender of the [[akka.io.Tcp.Bind]] will be notified with a [[akka.io.Tcp.Bound]]
 * message. The sender() of the [[akka.io.Tcp.Bound]] message is the Listener actor (an internal actor responsible for
 * listening to server events). To unbind the port an [[akka.io.Tcp.Unbind]] message must be sent to the Listener actor.
 *
 * If the bind request is rejected because the Tcp system is not able to register more channels (see the nr-of-selectors
 * and max-channels configuration options in the akka.io.tcp section of the configuration) the sender will be notified
 * with a [[akka.io.Tcp.CommandFailed]] message. This message contains the original command for reference.
 *
 * When an inbound TCP connection is established, the handler will be notified by a [[akka.io.Tcp.Connected]] message.
 * The sender of this message is the Connection actor (an internal actor representing the TCP connection). At this point
 * the procedure is the same as for outbound connections (see section below).
 *
 * == Connect ==
 *
 * To initiate a connection to a remote server, a [[akka.io.Tcp.Connect]] message must be sent to this actor. If the
 * connection succeeds, the sender() will be notified with a [[akka.io.Tcp.Connected]] message. The sender of the
 * [[akka.io.Tcp.Connected]] message is the Connection actor (an internal actor representing the TCP connection). Before
 * starting to use the connection, a handler must be registered to the Connection actor by sending a [[akka.io.Tcp.Register]]
 * command message. After a handler has been registered, all incoming data will be sent to the handler in the form of
 * [[akka.io.Tcp.Received]] messages. To write data to the connection, a [[akka.io.Tcp.Write]] message must be sent
 * to the Connection actor.
 *
 * If the connect request is rejected because the Tcp system is not able to register more channels (see the nr-of-selectors
 * and max-channels configuration options in the akka.io.tcp section of the configuration) the sender will be notified
 * with a [[akka.io.Tcp.CommandFailed]] message. This message contains the original command for reference.
 *
 */
private[io] class TcpManager(tcp: TcpExt)
    extends SelectionHandler.SelectorBasedManager(tcp.Settings, tcp.Settings.NrOfSelectors)
    with ActorLogging {

  def receive = workerForCommandHandler {
    case c: Connect =>
      val commander = sender() // cache because we create a function that will run asynchly
      (registry => Props(new TcpOutgoingConnection(tcp, registry, commander, c)))

    case b: Bind =>
      val commander = sender() // cache because we create a function that will run asynchly
      (registry => Props(new TcpListener(selectorPool, tcp, registry, commander, b)))
  }

}
