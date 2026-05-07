/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.dungeon

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.util.control.{ NoStackTrace, NonFatal }
import scala.util.control.Exception.Catcher

import akka.AkkaException
import akka.actor._
import akka.annotation.InternalApi
import akka.dispatch.{ Envelope, Mailbox }
import akka.dispatch.MailboxType
import akka.dispatch.ProducesMessageQueue
import akka.dispatch.UnboundedMailbox
import akka.dispatch.sysmsg._
import akka.event.Logging.Error
import akka.serialization.{ DisabledJavaSerializer, SerializationExtension, Serializers }
import akka.serialization.Serialization

@SerialVersionUID(1L)
final case class SerializationCheckFailedException private[dungeon] (msg: Object, cause: Throwable)
    extends AkkaException(
      s"Failed to serialize and deserialize message of type ${msg.getClass.getName} for testing. " +
      "To avoid this error, either disable 'akka.actor.serialize-messages', mark the message with 'akka.actor.NoSerializationVerificationNeeded', or configure serialization to support this message",
      cause)

/**
 * INTERNAL API
 */
@InternalApi
private[akka] trait Dispatch { this: ActorCell =>

  @nowarn @volatile private var _mailboxDoNotCallMeDirectly
      : Mailbox = _ //This must be volatile since it isn't protected by the mailbox status

  @nowarn private def _preventPrivateUnusedErasure = {
    _mailboxDoNotCallMeDirectly
  }

  @inline final def mailbox: Mailbox = {
    AbstractActorCell.mailboxHandle.getVolatile(this).asInstanceOf[Mailbox]
  }

  @tailrec final def swapMailbox(newMailbox: Mailbox): Mailbox = {
    val oldMailbox = mailbox
    if (!AbstractActorCell.mailboxHandle.compareAndSet(this, oldMailbox, newMailbox))
      swapMailbox(newMailbox)
    else oldMailbox
  }

  final def hasMessages: Boolean = mailbox.hasMessages

  final def numberOfMessages: Int = mailbox.numberOfMessages

  final def isTerminated: Boolean = mailbox.isClosed

  /**
   * Initialize this cell, i.e. set up mailboxes and supervision. The UID must be
   * reasonably different from the previous UID of a possible actor with the same path,
   * which can be achieved by using ThreadLocalRandom.current.nextInt().
   */
  final def init(sendSupervise: Boolean, mailboxType: MailboxType): this.type = {
    /*
     * Create the mailbox and enqueue the Create() message to ensure that
     * this is processed before anything else.
     */
    val mbox = dispatcher.createMailbox(this, mailboxType)

    /*
     * The mailboxType was calculated taking into account what the MailboxType
     * has promised to produce. If that was more than the default, then we need
     * to reverify here because the dispatcher may well have screwed it up.
     */
    // we need to delay the failure to the point of actor creation so we can handle
    // it properly in the normal way
    val actorClass = props.actorClass()
    val createMessage = mailboxType match {
      case _: ProducesMessageQueue[_] if system.mailboxes.hasRequiredType(actorClass) =>
        val req = system.mailboxes.getRequiredType(actorClass)
        if (req.isInstance(mbox.messageQueue)) Create(None)
        else {
          val gotType = if (mbox.messageQueue == null) "null" else mbox.messageQueue.getClass.getName
          Create(Some(ActorInitializationException(self, s"Actor [$self] requires mailbox type [$req] got [$gotType]")))
        }
      case _ => Create(None)
    }

    swapMailbox(mbox)
    mailbox.setActor(this)

    // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
    mailbox.systemEnqueue(self, createMessage)

    if (sendSupervise) {
      // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
      parent.sendSystemMessage(akka.dispatch.sysmsg.Supervise(self, async = false))
    }
    this
  }

  final def initWithFailure(failure: Throwable): this.type = {
    val mbox = dispatcher.createMailbox(this, new UnboundedMailbox)
    swapMailbox(mbox)
    mailbox.setActor(this)
    // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
    val createMessage = Create(Some(ActorInitializationException(self, "failure while creating ActorCell", failure)))
    mailbox.systemEnqueue(self, createMessage)
    this
  }

  /**
   * Start this cell, i.e. attach it to the dispatcher.
   */
  def start(): this.type = {
    // This call is expected to start off the actor by scheduling its mailbox.
    dispatcher.attach(this)
    this
  }

  private def handleException: Catcher[Unit] = {
    case e: InterruptedException =>
      system.eventStream.publish(Error(e, self.path.toString, clazz(actor), "interrupted during message send"))
      Thread.currentThread().interrupt()
    case NonFatal(e) =>
      val message = e match {
        case n: NoStackTrace => "swallowing exception during message send: " + n.getMessage
        case _               => "swallowing exception during message send" // stack trace includes message
      }
      system.eventStream.publish(Error(e, self.path.toString, clazz(actor), message))
  }

  // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
  final def suspend(): Unit =
    try dispatcher.systemDispatch(this, Suspend())
    catch handleException

  // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
  final def resume(causedByFailure: Throwable): Unit =
    try dispatcher.systemDispatch(this, Resume(causedByFailure))
    catch handleException

  // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
  final def restart(cause: Throwable): Unit =
    try dispatcher.systemDispatch(this, Recreate(cause))
    catch handleException

  // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
  final def stop(): Unit =
    try dispatcher.systemDispatch(this, Terminate())
    catch handleException

  def sendMessage(msg: Envelope): Unit =
    try {
      val msgToDispatch =
        if (system.settings.SerializeAllMessages) serializeAndDeserialize(msg)
        else msg

      dispatcher.dispatch(this, msgToDispatch)
    } catch handleException

  private def serializeAndDeserialize(envelope: Envelope): Envelope = {

    val unwrappedMessage =
      (envelope.message match {
        case DeadLetter(wrapped, _, _) => wrapped
        case other                     => other
      }).asInstanceOf[AnyRef]

    unwrappedMessage match {
      case _: NoSerializationVerificationNeeded => envelope
      case msg =>
        if (system.settings.NoSerializationVerificationNeededClassPrefix.exists(msg.getClass.getName.startsWith))
          envelope
        else {
          val deserializedMsg = try {
            serializeAndDeserializePayload(msg)
          } catch {
            case NonFatal(e) => throw SerializationCheckFailedException(msg, e)
          }
          envelope.message match {
            case dl: DeadLetter => envelope.copy(message = dl.copy(message = deserializedMsg))
            case _              => envelope.copy(message = deserializedMsg)
          }
        }
    }
  }

  private def serializeAndDeserializePayload(obj: AnyRef): AnyRef = {
    val s = SerializationExtension(system)
    val serializer = s.findSerializerFor(obj)
    if (serializer.isInstanceOf[DisabledJavaSerializer] && !s.shouldWarnAboutJavaSerializer(obj.getClass, serializer))
      obj // skip check for known "local" messages
    else {
      val oldInfo = Serialization.currentTransportInformation.value
      try {
        if (oldInfo eq null)
          Serialization.currentTransportInformation.value = system.provider.serializationInformation
        val bytes = serializer.toBinary(obj)
        val ms = Serializers.manifestFor(serializer, obj)
        s.deserialize(bytes, serializer.identifier, ms).get
      } finally Serialization.currentTransportInformation.value = oldInfo
    }
  }

  override def sendSystemMessage(message: SystemMessage): Unit =
    try dispatcher.systemDispatch(this, message)
    catch handleException

}
