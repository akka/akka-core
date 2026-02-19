/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import scala.collection.immutable
import scala.util.control.NoStackTrace
import akka.AkkaException
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.dispatch.{
  DequeBasedMessageQueueSemantics,
  Envelope,
  RequiresMessageQueue,
  UnboundedDequeBasedMessageQueueSemantics
}

/**
 *  The `Stash` trait enables an actor to temporarily stash away messages that can not or
 *  should not be handled using the actor's current behavior.
 *  <p/>
 *  Example:
 *  <pre>
 *    class ActorWithProtocol extends Actor with Stash {
 *      def receive = {
 *        case "open" =>
 *          unstashAll()
 *          context.become({
 *            case "write" => // do writing...
 *            case "close" =>
 *              unstashAll()
 *              context.unbecome()
 *            case msg => stash()
 *          }, discardOld = false)
 *        case "done" => // done
 *        case msg    => stash()
 *      }
 *    }
 *  </pre>
 *
 *  Note that the `Stash` trait can only be used together with actors that have a deque-based
 *  mailbox. By default Stash based actors request a Deque based mailbox since the stash
 *  trait extends `RequiresMessageQueue[DequeBasedMessageQueueSemantics]`.
 *  You can override the default mailbox provided when `DequeBasedMessageQueueSemantics` are requested via config:
 *  <pre>
 *    akka.actor.mailbox.requirements {
 *      "akka.dispatch.BoundedDequeBasedMessageQueueSemantics" = your-custom-mailbox
 *    }
 *  </pre>
 *  Alternatively, you can add your own requirement marker to the actor and configure a mailbox type to be used
 *  for your marker.
 *
 *  For a `Stash` that also enforces unboundedness of the deque see [[akka.actor.UnboundedStash]]. For a `Stash`
 *  that does not enforce any mailbox type see [[akka.actor.UnrestrictedStash]].
 *
 *  Note that the `Stash` trait must be mixed into (a subclass of) the `Actor` trait before
 *  any trait/class that overrides the `preRestart` callback. This means it's not possible to write
 *  `Actor with MyActor with Stash` if `MyActor` overrides `preRestart`.
 */
trait Stash extends UnrestrictedStash with RequiresMessageQueue[DequeBasedMessageQueueSemantics]

/**
 * The `UnboundedStash` trait is a version of [[akka.actor.Stash]] that enforces an unbounded stash for you actor.
 */
trait UnboundedStash extends UnrestrictedStash with RequiresMessageQueue[UnboundedDequeBasedMessageQueueSemantics]

/**
 * A version of [[akka.actor.Stash]] that does not enforce any mailbox type. The proper mailbox has to be configured
 * manually, and the mailbox should extend the [[akka.dispatch.DequeBasedMessageQueueSemantics]] marker trait.
 */
trait UnrestrictedStash extends Actor with StashSupport {

  /**
   *  Overridden callback. Prepends all messages in the stash to the mailbox,
   *  clears the stash, stops all children and invokes the postStop() callback.
   */
  @throws(classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    try unstashAll()
    finally super.preRestart(reason, message)
  }

  /**
   *  Overridden callback. Prepends all messages in the stash to the mailbox and clears the stash.
   *  Must be called when overriding this method, otherwise stashed messages won't be propagated to DeadLetters
   *  when actor stops.
   */
  @throws(classOf[Exception])
  override def postStop(): Unit =
    try unstashAll()
    finally super.postStop()
}

/**
 * INTERNAL API.
 *
 * A factory for creating stashes for an actor instance.
 *
 * @see [[StashSupport]]
 */
@InternalApi
private[akka] trait StashFactory { this: Actor =>
  private[akka] def createStash()(implicit ctx: ActorContext, ref: ActorRef): StashSupport = new StashSupport {
    def context: ActorContext = ctx
    def self: ActorRef = ref
  }
}

/**
 * INTERNAL API.
 *
 * Support trait for implementing a stash for an actor instance. A default stash per actor (= user stash)
 * is maintained by [[UnrestrictedStash]] by extending this trait. Actors that explicitly need other stashes
 * (optionally in addition to and isolated from the user stash) can create new stashes via [[StashFactory]].
 */
private[akka] trait StashSupport {

  /**
   * INTERNAL API.
   *
   * Context of the actor that uses this stash.
   */
  private[akka] def context: ActorContext

  /**
   * INTERNAL API.
   *
   * Self reference of the actor that uses this stash.
   */
  private[akka] def self: ActorRef

  /* The private stash of the actor. It is only accessible using `stash()` and
   * `unstashAll()`.
   */
  private var theStash = Vector.empty[Envelope]

  private def actorCell = context.asInstanceOf[ActorCell]

  /* The capacity of the stash. Configured in the actor's mailbox or dispatcher config.
   */
  private val capacity: Int =
    context.system.mailboxes.stashCapacity(context.props.dispatcher, context.props.mailbox)

  /**
   * INTERNAL API.
   *
   * The actor's deque-based message queue.
   * `mailbox.queue` is the underlying `Deque`.
   */
  private[akka] val mailbox: DequeBasedMessageQueueSemantics = {
    actorCell.mailbox.messageQueue match {
      case queue: DequeBasedMessageQueueSemantics => queue
      case other =>
        throw ActorInitializationException(
          self,
          s"DequeBasedMailbox required, got: ${other.getClass.getName}\n" +
          """An (unbounded) deque-based mailbox can be configured as follows:
          |  my-custom-mailbox {
          |    mailbox-type = "akka.dispatch.UnboundedDequeBasedMailbox"
          |  }
          |""".stripMargin)
    }
  }

  /**
   *  Adds the current message (the message that the actor received last) to the
   *  actor's stash.
   *
   *  @throws StashOverflowException in case of a stash capacity violation
   *  @throws IllegalStateException  if the same message is stashed more than once
   */
  def stash(): Unit = {
    val currMsg = actorCell.currentMessage
    if (theStash.nonEmpty && (currMsg eq theStash.last))
      throw new IllegalStateException(s"Can't stash the same message $currMsg more than once")
    if (capacity <= 0 || theStash.size < capacity) theStash :+= currMsg
    else
      throw new StashOverflowException(
        s"Couldn't enqueue message ${currMsg.message.getClass.getName} from ${currMsg.sender} to stash of $self")
  }

  /**
   * INTERNAL API.
   *
   * Adds the current message to the head of the actor's stash (instead of the tail).
   * When `unstashAll()` is called, messages at the head are processed first.
   * This allows priority handling of certain messages (e.g., `Terminated`)
   * while still going through the stash mechanism's correct `terminatedQueuedFor` handling.
   */
  private[akka] def stashAtHead(): Unit = {
    val currMsg = actorCell.currentMessage
    if (theStash.nonEmpty && (currMsg eq theStash.head))
      throw new IllegalStateException(s"Can't stash the same message $currMsg more than once")
    if (capacity <= 0 || theStash.size < capacity) theStash = currMsg +: theStash
    else
      throw new StashOverflowException(
        s"Couldn't enqueue message ${currMsg.message.getClass.getName} from ${currMsg.sender} to stash of $self")
  }

  /**
   * Prepends `others` to this stash. This method is optimized for a large stash and
   * small `others`.
   */
  private[akka] def prepend(others: immutable.Seq[Envelope]): Unit =
    theStash = others.foldRight(theStash)((e, s) => e +: s)

  /**
   *  Prepends the oldest message in the stash to the mailbox, and then removes that
   *  message from the stash.
   *
   *  Messages from the stash are enqueued to the mailbox until the capacity of the
   *  mailbox (if any) has been reached. In case a bounded mailbox overflows, a
   *  `MessageQueueAppendFailedException` is thrown.
   *
   *  The unstashed message is guaranteed to be removed from the stash regardless
   *  if the `unstash()` call successfully returns or throws an exception.
   */
  private[akka] def unstash(): Unit =
    if (theStash.nonEmpty) try {
      enqueueFirst(theStash.head)
    } finally {
      theStash = theStash.tail
    }

  /**
   *  Prepends all messages in the stash to the mailbox, and then clears the stash.
   *
   *  Messages from the stash are enqueued to the mailbox until the capacity of the
   *  mailbox (if any) has been reached. In case a bounded mailbox overflows, a
   *  `MessageQueueAppendFailedException` is thrown.
   *
   *  The stash is guaranteed to be empty after calling `unstashAll()`.
   */
  def unstashAll(): Unit = unstashAll(_ => true)

  /**
   * INTERNAL API.
   *
   *  Prepends selected messages in the stash, applying `filterPredicate`,  to the
   *  mailbox, and then clears the stash.
   *
   *  Messages from the stash are enqueued to the mailbox until the capacity of the
   *  mailbox (if any) has been reached. In case a bounded mailbox overflows, a
   *  `MessageQueueAppendFailedException` is thrown.
   *
   *  The stash is guaranteed to be empty after calling `unstashAll(Any => Boolean)`.
   *
   *  @param filterPredicate only stashed messages selected by this predicate are
   *                         prepended to the mailbox.
   */
  @InternalStableApi
  private[akka] def unstashAll(filterPredicate: Any => Boolean): Unit = {
    try {
      val i = theStash.reverseIterator.filter(envelope => filterPredicate(envelope.message))
      while (i.hasNext) enqueueFirst(i.next())
    } finally {
      theStash = Vector.empty[Envelope]
    }
  }

  /**
   * INTERNAL API.
   *
   * Clears the stash and and returns all envelopes that have not been unstashed.
   */
  @InternalStableApi
  private[akka] def clearStash(): Vector[Envelope] = {
    val stashed = theStash
    theStash = Vector.empty[Envelope]
    stashed
  }

  /**
   * Enqueues `envelope` at the first position in the mailbox. If the message contained in
   * the envelope is a `Terminated` message, it will be ensured that it can be re-received
   * by the actor.
   */
  private def enqueueFirst(envelope: Envelope): Unit = {
    mailbox.enqueueFirst(self, envelope)
    envelope.message match {
      case Terminated(ref) => actorCell.terminatedQueuedFor(ref, None)
      case _               =>
    }
  }
}

/**
 * Is thrown when the size of the Stash exceeds the capacity of the Stash
 */
class StashOverflowException(message: String, cause: Throwable = null)
    extends AkkaException(message, cause)
    with NoStackTrace
