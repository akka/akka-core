/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.pubsub

import akka.actor.ActorRef
import akka.util.{ MessageBuffer, MessageBufferMap }

private[pubsub] trait PerGroupingBuffer {

  private val buffers: MessageBufferMap[String] = new MessageBufferMap()

  def bufferOr(grouping: String, message: Any, originalSender: ActorRef)(action: => Unit): Unit = {
    if (!buffers.contains(grouping)) action
    else buffers.append(grouping, message, originalSender)
  }

  def recreateAndForwardMessagesIfNeeded(grouping: String, recipient: => ActorRef): Unit = {
    val buffer = buffers.getOrEmpty(grouping)
    if (buffer.nonEmpty) {
      forwardMessages(buffer, recipient)
    }
    buffers.remove(grouping)
  }

  def forwardMessages(grouping: String, recipient: ActorRef): Unit = {
    forwardMessages(buffers.getOrEmpty(grouping), recipient)
    buffers.remove(grouping)
  }

  private def forwardMessages(messages: MessageBuffer, recipient: ActorRef): Unit = {
    messages.foreach {
      case (message, originalSender) => recipient.tell(message, originalSender)
    }
  }

  def initializeGrouping(grouping: String): Unit = buffers.add(grouping)
}
