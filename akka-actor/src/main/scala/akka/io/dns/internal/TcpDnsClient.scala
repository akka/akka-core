/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import java.net.InetSocketAddress
import akka.AkkaException
import akka.actor.Terminated
import akka.actor.{ Actor, ActorLogging, ActorRef, Stash }
import akka.annotation.InternalApi
import akka.io.Tcp
import akka.io.dns.internal.DnsClient.Answer
import akka.util.ByteString
import akka.event.LoggingAdapter

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TcpDnsClient(tcp: ActorRef, ns: InetSocketAddress, answerRecipient: ActorRef)
    extends Actor
    with ActorLogging
    with Stash {
  import TcpDnsClient._

  override def receive: Receive = idle

  val idle: Receive = {
    case _: Message =>
      stash()
      log.debug("Connecting to [{}]", ns)
      tcp ! Tcp.Connect(ns)
      context.become(connecting)
  }

  val connecting: Receive = {
    case failure @ Tcp.CommandFailed(_: Tcp.Connect) =>
      throwFailure(s"Failed to connect to TCP DNS server at [$ns]", failure.cause)
    case _: Tcp.Connected =>
      log.debug("Connected to TCP address [{}]", ns)
      val connection = sender()
      context.watch(connection)
      context.become(ready(connection, Vector.empty, ByteString.empty, waitingForWriteAck = false))
      connection ! Tcp.Register(self)
      unstashAll()
    case _: Message =>
      stash()
  }

  private def ready(
      connection: ActorRef,
      requestBuffer: Vector[Message],
      responseBuffer: ByteString,
      waitingForWriteAck: Boolean): Receive = {

    case msg: Message =>
      if (!waitingForWriteAck) {
        if (requestBuffer.nonEmpty)
          throwFailure(
            s"Unexpected state, not waiting for ack but request buffer is not empty, this is a bug${requestBufferExtraMsg(requestBuffer)}")
        sendWrite(connection, Vector.empty, responseBuffer, msg)
      } else if (requestBuffer.size < MaxRequestsBuffered) {
        // buffer, wait for ack
        context.become(ready(connection, requestBuffer :+ msg, responseBuffer, waitingForWriteAck))
      } else {
        val oldest = requestBuffer.head
        val newRequestBuffer = requestBuffer.tail :+ msg
        log.warning("Dropping oldest buffered DNS request [{}] due to TCP backpressure", oldest)
        context.become(ready(connection, newRequestBuffer, responseBuffer, waitingForWriteAck))
      }

    case Ack =>
      if (waitingForWriteAck) {
        if (requestBuffer.nonEmpty) {
          val bufferedRequest = requestBuffer.head
          val newRequestBuffer = requestBuffer.tail
          sendWrite(connection, newRequestBuffer, responseBuffer, bufferedRequest)
        } else {
          context.become(ready(connection, Vector.empty, responseBuffer, waitingForWriteAck = false))
        }
      } else {
        log.debug("Unexpected Ack in TCP DNS client")
      }

    case Tcp.Received(newData) =>
      onNewResponseData(connection, requestBuffer, waitingForWriteAck, responseBuffer ++ newData)

    case failure: Tcp.CommandFailed =>
      connection ! Tcp.Abort
      throwFailure(s"TCP command failed${requestBufferExtraMsg(requestBuffer)}", failure.cause)

    case Tcp.PeerClosed =>
      if (requestBuffer.nonEmpty) {
        log.warning("Connection closed, dropping {} buffered requests)", requestBuffer.size)
        // gracefully handled but we are still dropping requests
        answerRecipient ! DnsClient.TcpDropped
      }
      context.unwatch(connection)
      context.become(idle)

    case Tcp.ErrorClosed(cause) =>
      throwFailure(s"Connection closed with error $cause${requestBufferExtraMsg(requestBuffer)}")

    case Terminated(`connection`) =>
      throwFailure(s"Connection terminated unexpectedly${requestBufferExtraMsg(requestBuffer)}")
  }

  private def requestBufferExtraMsg(requestBuffer: Vector[Message]): String =
    if (requestBuffer.nonEmpty) s" (dropping ${requestBuffer.size} buffered requests)" else ""

  private def onNewResponseData(
      connection: ActorRef,
      requestBuffer: Vector[Message],
      waitingForWriteAck: Boolean,
      responseBuffer: ByteString): Unit = {
    // TCP DNS responses are prefixed by 2 bytes encoding the length of the response
    val prefixSize = 2
    if (responseBuffer.length < prefixSize)
      context.become(ready(connection, requestBuffer, responseBuffer, waitingForWriteAck))
    else {
      val expectedPayloadLength = decodeLength(responseBuffer)
      if (responseBuffer.drop(prefixSize).length < expectedPayloadLength)
        context.become(ready(connection, requestBuffer, responseBuffer, waitingForWriteAck))
      else {
        answerRecipient ! parseResponse(responseBuffer.drop(prefixSize))
        if (responseBuffer.length > prefixSize + expectedPayloadLength) {
          val newBuffer = responseBuffer.drop(prefixSize + expectedPayloadLength)
          // needs to happen right away to not risk re-order with additional replies
          onNewResponseData(connection, requestBuffer, waitingForWriteAck, newBuffer)
        } else {
          context.become(ready(connection, requestBuffer, ByteString.empty, waitingForWriteAck))
        }
      }
    }

  }

  private def sendWrite(
      connection: ActorRef,
      requestBuffer: Vector[Message],
      responseBuffer: ByteString,
      message: Message): Unit = {
    val bytes = message.write()
    connection ! Tcp.Write(encodeLength(bytes.length) ++ bytes, Ack)
    context.become(ready(connection, requestBuffer, responseBuffer, waitingForWriteAck = true))
  }

  private def parseResponse(data: ByteString) = {
    val msg = Message.parse(data)
    log.debug("Decoded TCP DNS response [{}]", msg)
    if (msg.flags.isTruncated) {
      log.warning("TCP DNS response truncated")
    }
    val (recs, additionalRecs) =
      if (msg.flags.responseCode == ResponseCode.SUCCESS) (msg.answerRecs, msg.additionalRecs) else (Nil, Nil)
    Answer(msg.id, recs, additionalRecs)
  }

  private def throwFailure(message: String, cause: Option[Throwable] = None): Nothing =
    TcpDnsClient.throwFailure(message, cause, log, answerRecipient)
}

/**
 * INTERNAL API
 */
@InternalApi
private[internal] object TcpDnsClient {

  private val MaxRequestsBuffered = 10

  private[internal] def encodeLength(length: Int): ByteString =
    ByteString((length / 256).toByte, length.toByte)

  private def decodeLength(data: ByteString): Int =
    ((data(0).toInt + 256) % 256) * 256 + ((data(1) + 256) % 256)

  private def throwFailure(
      message: String,
      cause: Option[Throwable],
      log: LoggingAdapter,
      reportTo: ActorRef): Nothing = {
    cause match {
      case None =>
        log.warning("TCP DNS client failed: {}", message)
        reportTo ! DnsClient.TcpDropped
        throw new AkkaException(message)

      case Some(throwable) =>
        log.warning(throwable, "TCP DNS client failed: {}", message)
        reportTo ! DnsClient.TcpDropped
        throw new AkkaException(message, throwable)
    }
  }

  private case object Ack extends Tcp.Event
}
