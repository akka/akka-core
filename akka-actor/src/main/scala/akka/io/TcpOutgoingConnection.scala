/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.net.{ ConnectException, InetSocketAddress }
import java.nio.channels.{ SelectionKey, SocketChannel }

import scala.concurrent.duration._
import scala.util.control.{ NoStackTrace, NonFatal }

import akka.actor.{ ActorRef, ReceiveTimeout }
import akka.actor.Status.Failure
import akka.annotation.InternalApi
import akka.io.SelectionHandler._
import akka.io.Tcp._
import akka.io.TcpConnection.CloseInformation
import akka.io.dns.DnsProtocol

/**
 * An actor handling the connection state machine for an outgoing connection
 * to be established.
 *
 * INTERNAL API
 */
private[io] class TcpOutgoingConnection(
    _tcp: TcpExt,
    channelRegistry: ChannelRegistry,
    commander: ActorRef,
    connect: Connect)
    extends TcpConnection(
      _tcp,
      SocketChannel.open().configureBlocking(false).asInstanceOf[SocketChannel],
      connect.pullMode) {

  import TcpOutgoingConnection._
  import connect._
  import context._

  signDeathPact(commander)

  options.foreach(_.beforeConnect(channel.socket))
  localAddress.foreach(channel.socket.bind)
  channelRegistry.register(channel, 0)
  timeout.foreach(context.setReceiveTimeout) //Initiate connection timeout if supplied

  private def stop(cause: Throwable): Unit =
    stopWith(CloseInformation(Set(commander), CommandFailed(connect).withCause(cause)), shouldAbort = true)

  private def reportConnectFailure(thunk: => Unit): Unit = {
    try {
      thunk
    } catch {
      case NonFatal(e) =>
        log.debug("Could not establish connection to [{}] due to {}", remoteAddress, e)
        stop(e)
    }
  }

  def receive: Receive = {
    case registration: ChannelRegistration =>
      setRegistration(registration)
      reportConnectFailure {
        if (remoteAddress.isUnresolved) {
          log.debug("Resolving {} before connecting", remoteAddress.getHostName)
          Dns.resolve(DnsProtocol.Resolve(remoteAddress.getHostName), system, self) match {
            case None =>
              context.become(resolving(registration))
            case Some(resolved) =>
              register(new InetSocketAddress(resolved.address(), remoteAddress.getPort), registration)
          }
        } else {
          register(remoteAddress, registration)
        }
      }
    case ReceiveTimeout =>
      connectionTimeout()
  }

  def resolving(registration: ChannelRegistration): Receive = {
    case resolved: DnsProtocol.Resolved =>
      reportConnectFailure {
        register(new InetSocketAddress(resolved.address(), remoteAddress.getPort), registration)
      }
    case ReceiveTimeout =>
      connectionTimeout()
    case Failure(ex) =>
      // async-dns responds with a Failure on DNS server lookup failure
      reportConnectFailure {
        throw new RuntimeException(ex)
      }
  }

  def register(address: InetSocketAddress, registration: ChannelRegistration): Unit = {
    reportConnectFailure {
      log.debug("Attempting connection to [{}]", address)
      if (channel.connect(address))
        completeConnect(registration, commander, options)
      else {
        registration.enableInterest(SelectionKey.OP_CONNECT)
        context.become(connecting(registration, tcp.Settings.FinishConnectRetries))
      }
    }
  }

  def connecting(registration: ChannelRegistration, remainingFinishConnectRetries: Int): Receive = {
    {
      case ChannelConnectable =>
        reportConnectFailure {
          if (channel.finishConnect()) {
            if (timeout.isDefined) context.setReceiveTimeout(Duration.Undefined) // Clear the timeout
            log.debug("Connection established to [{}]", remoteAddress)
            completeConnect(registration, commander, options)
          } else {
            if (remainingFinishConnectRetries > 0) {
              context.system.scheduler.scheduleOnce(1.millisecond) {
                channelRegistry.register(channel, SelectionKey.OP_CONNECT)
              }(context.dispatcher)
              context.become(connecting(registration, remainingFinishConnectRetries - 1))
            } else {
              log.debug(
                "Could not establish connection because finishConnect " +
                "never returned true (consider increasing akka.io.tcp.finish-connect-retries)")
              stop(FinishConnectNeverReturnedTrueException)
            }
          }
        }
      case ReceiveTimeout =>
        connectionTimeout()
    }
  }

  private def connectionTimeout(): Unit = {
    if (timeout.isDefined) context.setReceiveTimeout(Duration.Undefined) // Clear the timeout
    log.debug("Connect timeout expired, could not establish connection to [{}]", remoteAddress)
    stop(connectTimeoutExpired(timeout))
  }
}

@InternalApi
private[io] object TcpOutgoingConnection {
  val FinishConnectNeverReturnedTrueException =
    new ConnectException("Could not establish connection because finishConnect never returned true") with NoStackTrace

  def connectTimeoutExpired(timeout: Option[FiniteDuration]) =
    new ConnectException(s"Connect timeout of $timeout expired") with NoStackTrace
}
