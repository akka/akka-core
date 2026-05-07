/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.net.{ InetSocketAddress, PortUnreachableException }
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.nio.channels.SelectionKey._

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.util.control.NonFatal

import akka.actor.{ Actor, ActorLogging, ActorRef }
import akka.actor.Status.Failure
import akka.dispatch.{ RequiresMessageQueue, UnboundedMessageQueueSemantics }
import akka.io.SelectionHandler._
import akka.io.UdpConnected._
import akka.io.dns.DnsProtocol
import akka.util.ByteString

/**
 * INTERNAL API
 */
private[io] class UdpConnection(
    udpConn: UdpConnectedExt,
    channelRegistry: ChannelRegistry,
    commander: ActorRef,
    connect: Connect)
    extends Actor
    with ActorLogging
    with RequiresMessageQueue[UnboundedMessageQueueSemantics] {

  import connect._
  import udpConn._
  import udpConn.settings._

  var pendingSend: (Send, ActorRef) = null
  def writePending = pendingSend ne null

  context.watch(handler) // sign death pact
  var channel: DatagramChannel = null

  if (remoteAddress.isUnresolved) {
    Dns.resolve(DnsProtocol.Resolve(remoteAddress.getHostName), context.system, self) match {
      case Some(r) =>
        reportConnectFailure {
          doConnect(new InetSocketAddress(r.address(), remoteAddress.getPort))
        }
      case None =>
        context.become(resolving())
    }
  } else {
    reportConnectFailure {
      doConnect(remoteAddress)
    }
  }

  def resolving(): Receive = {
    case r: DnsProtocol.Resolved =>
      reportConnectFailure {
        doConnect(new InetSocketAddress(r.address(), remoteAddress.getPort))
      }
    case Failure(ex) =>
      // async-dns responds with a Failure on DNS server lookup failure
      reportConnectFailure {
        throw new RuntimeException(ex)
      }
  }

  def doConnect(@nowarn("msg=never used") address: InetSocketAddress): Unit = {
    channel = DatagramChannel.open
    channel.configureBlocking(false)
    val socket = channel.socket
    options.foreach(_.beforeDatagramBind(socket))
    localAddress.foreach(socket.bind)
    channel.connect(remoteAddress)
    channelRegistry.register(channel, OP_READ)

    log.debug("Successfully connected to [{}]", remoteAddress)
  }

  def receive = {
    case registration: ChannelRegistration =>
      options.foreach {
        case v2: Inet.SocketOptionV2 => v2.afterConnect(channel.socket)
        case _                       =>
      }
      commander ! Connected
      context.become(connected(registration), discardOld = true)
  }

  def connected(registration: ChannelRegistration): Receive = {
    case SuspendReading  => registration.disableInterest(OP_READ)
    case ResumeReading   => registration.enableInterest(OP_READ)
    case ChannelReadable => doRead(registration, handler)

    case Disconnect =>
      log.debug("Closing UDP connection to [{}]", remoteAddress)
      channel.close()
      sender() ! Disconnected
      log.debug("Connection closed to [{}], stopping listener", remoteAddress)
      context.stop(self)

    case send: Send if writePending =>
      if (TraceLogging) log.debug("Dropping write because queue is full")
      sender() ! CommandFailed(send)

    case send: Send if send.payload.isEmpty =>
      if (send.wantsAck)
        sender() ! send.ack

    case send: Send =>
      pendingSend = (send, sender())
      registration.enableInterest(OP_WRITE)

    case ChannelWritable => doWrite()
  }

  def doRead(registration: ChannelRegistration, handler: ActorRef): Unit = {
    @tailrec def innerRead(readsLeft: Int, buffer: ByteBuffer): Unit = {
      buffer.clear()
      buffer.limit(DirectBufferSize)

      if (channel.read(buffer) > 0) {
        buffer.flip()
        handler ! Received(ByteString(buffer))
        innerRead(readsLeft - 1, buffer)
      }
    }
    val buffer = bufferPool.acquire()
    try innerRead(BatchReceiveLimit, buffer)
    catch {
      case _: PortUnreachableException =>
        if (TraceLogging) log.debug("Ignoring PortUnreachableException in doRead")
    } finally {
      registration.enableInterest(OP_READ)
      bufferPool.release(buffer)
    }
  }

  final def doWrite(): Unit = {
    val buffer = udpConn.bufferPool.acquire()
    try {
      val (send, commander) = pendingSend
      buffer.clear()
      send.payload.copyToBuffer(buffer)
      buffer.flip()
      val writtenBytes = channel.write(buffer)
      if (TraceLogging) log.debug("Wrote [{}] bytes to channel", writtenBytes)

      // Datagram channel either sends the whole message, or nothing
      if (writtenBytes == 0) commander ! CommandFailed(send)
      else if (send.wantsAck) commander ! send.ack
    } finally {
      udpConn.bufferPool.release(buffer)
      pendingSend = null
    }
  }

  override def postStop(): Unit =
    if (channel != null && channel.isOpen) {
      log.debug("Closing DatagramChannel after being stopped")
      try channel.close()
      catch {
        case NonFatal(e) => log.debug("Error closing DatagramChannel: {}", e)
      }
    }

  private def reportConnectFailure(thunk: => Unit): Unit = {
    try {
      thunk
    } catch {
      case NonFatal(e) =>
        log.debug(
          "Failure while connecting UDP channel to remote address [{}] local address [{}]: {}",
          remoteAddress,
          localAddress.getOrElse("undefined"),
          e)
        commander ! CommandFailed(connect)
        context.stop(self)
    }
  }
}
