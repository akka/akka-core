/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import scala.annotation.nowarn
import scala.collection.immutable
import scala.util.control.NonFatal

import akka.actor._
import akka.dispatch.{ RequiresMessageQueue, UnboundedMessageQueueSemantics }
import akka.io.Inet.{ DatagramChannelCreator, SocketOption }
import akka.io.Udp._

/**
 * INTERNAL API
 */
@nowarn("msg=deprecated")
private[io] class UdpSender(
    val udp: UdpExt,
    channelRegistry: ChannelRegistry,
    commander: ActorRef,
    options: immutable.Traversable[SocketOption])
    extends Actor
    with ActorLogging
    with WithUdpSend
    with RequiresMessageQueue[UnboundedMessageQueueSemantics] {

  val channel = {
    val datagramChannel = options
      .collectFirst {
        case creator: DatagramChannelCreator => creator
      }
      .getOrElse(DatagramChannelCreator())
      .create()
    datagramChannel.configureBlocking(false)
    val socket = datagramChannel.socket
    options.foreach { _.beforeDatagramBind(socket) }

    datagramChannel
  }
  channelRegistry.register(channel, initialOps = 0)

  def receive: Receive = {
    case registration: ChannelRegistration =>
      options.foreach {
        case v2: Inet.SocketOptionV2 => v2.afterConnect(channel.socket)
        case _                       =>
      }
      commander ! SimpleSenderReady
      context.become(sendHandlers(registration))
  }

  override def postStop(): Unit = if (channel.isOpen) {
    log.debug("Closing DatagramChannel after being stopped")
    try channel.close()
    catch {
      case NonFatal(e) => log.debug("Error closing DatagramChannel: {}", e)
    }
  }
}
