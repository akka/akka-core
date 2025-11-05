/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.net.InetSocketAddress
import java.nio.channels.{ DatagramChannel, SelectionKey }

import scala.util.control.NonFatal

import akka.actor.{ Actor, ActorLogging, ActorRef }
import akka.io.SelectionHandler._
import akka.io.Udp.{ CommandFailed, Send }
import akka.io.dns.DnsProtocol

/**
 * INTERNAL API
 */
private[io] trait WithUdpSend {
  me: Actor with ActorLogging =>

  private var pendingSend: Send = null
  private var pendingCommander: ActorRef = null
  // If send fails first, we allow a second go after selected writable, but no more. This flag signals that
  // pending send was already tried once.
  private var retriedSend = false
  private def hasWritePending = pendingSend ne null

  def channel: DatagramChannel
  def udp: UdpExt
  val settings = udp.settings

  import settings._

  def sendHandlers(registration: ChannelRegistration): Receive = {
    case send: Send if hasWritePending =>
      if (TraceLogging) log.debug("Dropping write because queue is full")
      sender() ! CommandFailed(send)

    case send: Send if send.payload.isEmpty =>
      if (send.wantsAck)
        sender() ! send.ack

    case send: Send =>
      pendingSend = send
      pendingCommander = sender()
      if (send.target.isUnresolved) {
        Dns.resolve(DnsProtocol.Resolve(send.target.getHostName), context.system, self) match {
          case Some(r) =>
            try {
              pendingSend = pendingSend.copy(target = new InetSocketAddress(r.address(), pendingSend.target.getPort))
              doSend(registration)
            } catch {
              case NonFatal(e) =>
                sender() ! CommandFailed(send)
                log.debug("Failure while sending UDP datagram to remote address [{}]: {}", send.target, e)
                retriedSend = false
                pendingSend = null
                pendingCommander = null
            }
          case None =>
            sender() ! CommandFailed(send)
            log.debug("Name resolution failed for remote address [{}]", send.target)
            retriedSend = false
            pendingSend = null
            pendingCommander = null
        }
      } else {
        doSend(registration)
      }

    case ChannelWritable => if (hasWritePending) doSend(registration)
  }

  private def doSend(registration: ChannelRegistration): Unit = {
    val buffer = udp.bufferPool.acquire()
    try {
      buffer.clear()
      pendingSend.payload.copyToBuffer(buffer)
      buffer.flip()
      val writtenBytes = channel.send(buffer, pendingSend.target)
      if (TraceLogging) log.debug("Wrote [{}] bytes to channel", writtenBytes)

      // Datagram channel either sends the whole message, or nothing
      if (writtenBytes == 0) {
        if (retriedSend) {
          pendingCommander ! CommandFailed(pendingSend)
          retriedSend = false
          pendingSend = null
          pendingCommander = null
        } else {
          registration.enableInterest(SelectionKey.OP_WRITE)
          retriedSend = true
        }
      } else {
        if (pendingSend.wantsAck) pendingCommander ! pendingSend.ack
        retriedSend = false
        pendingSend = null
        pendingCommander = null
      }
    } finally {
      udp.bufferPool.release(buffer)
    }
  }
}
