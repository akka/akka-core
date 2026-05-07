/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.annotation.tailrec
import scala.concurrent.duration.Duration

import com.typesafe.config.Config
import org.jctools.queues.MpscGrowableArrayQueue

import akka.dispatch.BoundedMessageQueueSemantics
import akka.dispatch.BoundedNodeMessageQueue
import akka.dispatch.Envelope
import akka.dispatch.MailboxType
import akka.dispatch.MessageQueue
import akka.dispatch.ProducesMessageQueue

case class JCToolsMailbox(val capacity: Int) extends MailboxType with ProducesMessageQueue[BoundedNodeMessageQueue] {

  def this(settings: ActorSystem.Settings, config: Config) = this(config.getInt("mailbox-capacity"))

  if (capacity < 0) throw new IllegalArgumentException("The capacity for JCToolsMailbox can not be negative")

  final override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue =
    new JCToolsMessageQueue(capacity)
}

class JCToolsMessageQueue(capacity: Int)
    extends MpscGrowableArrayQueue[Envelope](capacity)
    with MessageQueue
    with BoundedMessageQueueSemantics {
  final def pushTimeOut: Duration = Duration.Undefined

  final def enqueue(receiver: ActorRef, handle: Envelope): Unit =
    if (!offer(handle))
      receiver
        .asInstanceOf[InternalActorRef]
        .provider
        .deadLetters
        .tell(DeadLetter(handle.message, handle.sender, receiver), handle.sender)

  final def dequeue(): Envelope = poll()

  final def numberOfMessages: Int = size()

  final def hasMessages: Boolean = !isEmpty()

  @tailrec final def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
    val envelope = dequeue()
    if (envelope ne null) {
      deadLetters.enqueue(owner, envelope)
      cleanUp(owner, deadLetters)
    }
  }
}
