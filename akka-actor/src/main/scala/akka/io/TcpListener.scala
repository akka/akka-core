/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.net.InetSocketAddress
import java.nio.channels.{ SelectionKey, ServerSocketChannel, SocketChannel }

import scala.annotation.tailrec
import scala.util.control.NonFatal

import akka.actor._
import akka.dispatch.{ RequiresMessageQueue, UnboundedMessageQueueSemantics }
import akka.io.SelectionHandler._
import akka.io.Tcp._

/**
 * INTERNAL API
 */
private[io] object TcpListener {

  final case class RegisterIncoming(channel: SocketChannel)
      extends HasFailureMessage
      with NoSerializationVerificationNeeded {
    def failureMessage = FailedRegisterIncoming(channel)
  }

  final case class FailedRegisterIncoming(channel: SocketChannel) extends NoSerializationVerificationNeeded

}

/**
 * INTERNAL API
 */
private[io] class TcpListener(
    selectorRouter: ActorRef,
    tcp: TcpExt,
    channelRegistry: ChannelRegistry,
    bindCommander: ActorRef,
    bind: Bind)
    extends Actor
    with ActorLogging
    with RequiresMessageQueue[UnboundedMessageQueueSemantics] {

  import TcpListener._
  import tcp.Settings._

  context.watch(bind.handler) // sign death pact

  val channel = ServerSocketChannel.open
  channel.configureBlocking(false)

  var acceptLimit = if (bind.pullMode) 0 else BatchAcceptLimit

  val localAddress =
    try {
      val socket = channel.socket
      bind.options.foreach(_.beforeServerSocketBind(socket))
      socket.bind(bind.localAddress, bind.backlog)
      val ret = socket.getLocalSocketAddress match {
        case isa: InetSocketAddress => isa
        case x                      => throw new IllegalArgumentException(s"bound to unknown SocketAddress [$x]")
      }
      channelRegistry.register(channel, if (bind.pullMode) 0 else SelectionKey.OP_ACCEPT)
      log.debug("Successfully bound to {}", ret)
      bind.options.foreach {
        case o: Inet.SocketOptionV2 => o.afterBind(channel.socket)
        case _                      =>
      }
      ret
    } catch {
      case NonFatal(e) =>
        val exception = if (e.isInstanceOf[java.net.BindException]) {
          val newException = new java.net.BindException(s"[${bind.localAddress}] ${e.getMessage}")
          newException.setStackTrace(e.getStackTrace)
          newException
        } else {
          e
        }
        bindCommander ! CommandFailed(bind).withCause(exception)
        log.error(exception, "Bind failed for TCP channel on endpoint [{}]", bind.localAddress)
        context.stop(self)
    }

  override def supervisorStrategy = SelectionHandler.connectionSupervisorStrategy

  def receive: Receive = {
    case registration: ChannelRegistration =>
      bindCommander ! Bound(channel.socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress])
      context.become(bound(registration))
  }

  def bound(registration: ChannelRegistration): Receive = {
    case ChannelAcceptable =>
      acceptLimit = acceptAllPending(registration, acceptLimit)
      if (acceptLimit > 0) registration.enableInterest(SelectionKey.OP_ACCEPT)

    case ResumeAccepting(batchSize) =>
      acceptLimit = batchSize
      registration.enableInterest(SelectionKey.OP_ACCEPT)

    case FailedRegisterIncoming(socketChannel) =>
      log.warning("Could not register incoming connection since selector capacity limit is reached, closing connection")
      try socketChannel.close()
      catch {
        case NonFatal(e) => log.debug("Error closing socket channel: {}", e)
      }

    case Unbind =>
      log.debug("Unbinding endpoint {}", localAddress)
      registration.cancelAndClose { () =>
        self ! Unbound
      }

      context.become(unregistering(sender()))
  }
  def unregistering(requester: ActorRef): Receive = {
    case Unbound =>
      requester ! Unbound
      log.debug("Unbound endpoint {}, stopping listener", localAddress)
      context.stop(self)
  }

  @tailrec final def acceptAllPending(registration: ChannelRegistration, limit: Int): Int = {
    val socketChannel =
      if (limit > 0) {
        try channel.accept()
        catch {
          case NonFatal(e) => { log.error(e, "Accept error: could not accept new connection"); null }
        }
      } else null
    if (socketChannel != null) {
      log.debug("New connection accepted")
      socketChannel.configureBlocking(false)
      def props(registry: ChannelRegistry) =
        Props(new TcpIncomingConnection(tcp, socketChannel, registry, bind.handler, bind.options, bind.pullMode))
      selectorRouter ! WorkerForCommand(RegisterIncoming(socketChannel), self, props)
      acceptAllPending(registration, limit - 1)
    } else if (bind.pullMode) limit
    else BatchAcceptLimit
  }

  override def postStop(): Unit = {
    try {
      if (channel.isOpen) {
        log.debug("Closing serverSocketChannel after being stopped")
        channel.close()
      }
    } catch {
      case NonFatal(e) => log.debug("Error closing ServerSocketChannel: {}", e)
    }
  }
}
