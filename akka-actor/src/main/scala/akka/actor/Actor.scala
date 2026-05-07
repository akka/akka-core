/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.Optional

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.beans.BeanProperty
import scala.util.control.NoStackTrace

import akka.AkkaException
import akka.annotation.InternalApi
import akka.event.LoggingAdapter

/**
 * INTERNAL API
 *
 * Marker trait to show which Messages are automatically handled by Akka
 */
private[akka] trait AutoReceivedMessage extends Serializable

/**
 * Marker trait to indicate that a message might be potentially harmful,
 * this is used to block messages coming in over remoting.
 */
trait PossiblyHarmful

/**
 * Marker trait to signal that this class should not be verified for serializability.
 */
trait NoSerializationVerificationNeeded

abstract class PoisonPill extends AutoReceivedMessage with PossiblyHarmful with DeadLetterSuppression

/**
 * A message all Actors will understand, that when processed will terminate the Actor permanently.
 */
@SerialVersionUID(1L)
case object PoisonPill extends PoisonPill {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

abstract class Kill extends AutoReceivedMessage with PossiblyHarmful

/**
 * A message all Actors will understand, that when processed will make the Actor throw an ActorKilledException,
 * which will trigger supervision.
 */
@SerialVersionUID(1L)
case object Kill extends Kill {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

/**
 * A message all Actors will understand, that when processed will reply with
 * [[akka.actor.ActorIdentity]] containing the `ActorRef`. The `messageId`
 * is returned in the `ActorIdentity` message as `correlationId`.
 */
@SerialVersionUID(1L)
final case class Identify(messageId: Any) extends AutoReceivedMessage with NotInfluenceReceiveTimeout

/**
 * Reply to [[akka.actor.Identify]]. Contains
 * `Some(ref)` with the `ActorRef` of the actor replying to the request or
 * `None` if no actor matched the request.
 * The `correlationId` is taken from the `messageId` in
 * the `Identify` message.
 */
@SerialVersionUID(1L)
final case class ActorIdentity(correlationId: Any, ref: Option[ActorRef]) {
  if (ref.isDefined && ref.get == null) {
    throw new IllegalArgumentException(
      "ActorIdentity created with ref = Some(null) is not allowed, " +
      "this could happen when serializing with Scala 2.12 and deserializing with Scala 2.11 which is not supported.")
  }

  /**
   * Java API: `ActorRef` of the actor replying to the request or
   * not defined if no actor matched the request.
   */
  def getActorRef: Optional[ActorRef] = {
    import scala.jdk.OptionConverters._
    ref.toJava
  }
}

/**
 * When Death Watch is used, the watcher will receive a Terminated(watched)
 * message when watched is terminated.
 * Terminated message can't be forwarded to another actor, since that actor
 * might not be watching the subject. Instead, if you need to forward Terminated
 * to another actor you should send the information in your own message.
 *
 * @param actor the watched actor that terminated
 * @param existenceConfirmed is false when the Terminated message was not sent
 *   directly from the watched actor, but derived from another source, such as
 *   when watching a non-local ActorRef, which might not have been resolved
 * @param addressTerminated the Terminated message was derived from
 *   that the remote node hosting the watched actor was detected as unreachable
 */
@SerialVersionUID(1L)
final case class Terminated private[akka] (@BeanProperty actor: ActorRef)(
    @BeanProperty val existenceConfirmed: Boolean,
    @BeanProperty val addressTerminated: Boolean)
    extends AutoReceivedMessage
    with PossiblyHarmful
    with DeadLetterSuppression
    with NoSerializationVerificationNeeded // local message, the remote one is DeathWatchNotification

/**
 * INTERNAL API
 *
 * Used for remote death watch. Failure detector publish this to the
 * [[akka.event.AddressTerminatedTopic]] when a remote node is detected to be unreachable and/or decided to
 * be removed.
 * The watcher ([[akka.actor.dungeon.DeathWatch]]) subscribes to the `AddressTerminatedTopic`
 * and translates this event to [[akka.actor.Terminated]], which is sent itself.
 */
@InternalApi
@SerialVersionUID(1L)
private[akka] final case class AddressTerminated(address: Address)
    extends AutoReceivedMessage
    with PossiblyHarmful
    with DeadLetterSuppression

abstract class ReceiveTimeout extends PossiblyHarmful

/**
 * When using ActorContext.setReceiveTimeout, the singleton instance of ReceiveTimeout will be sent
 * to the Actor when there hasn't been any message for that long.
 */
@SerialVersionUID(1L)
case object ReceiveTimeout extends ReceiveTimeout {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

/**
 * Marker trait to indicate that a message should not reset the receive timeout.
 */
trait NotInfluenceReceiveTimeout

/**
 * IllegalActorStateException is thrown when a core invariant in the Actor implementation has been violated.
 * For instance, if you try to create an Actor that doesn't extend Actor.
 *
 * Not for user instatiation
 */
@SerialVersionUID(1L)
final case class IllegalActorStateException private[akka] (message: String) extends AkkaException(message)

/**
 * ActorKilledException is thrown when an Actor receives the [[akka.actor.Kill]] message
 *
 * Not for user instatiation
 */
@SerialVersionUID(1L)
final case class ActorKilledException private[akka] (message: String) extends AkkaException(message) with NoStackTrace

/**
 * An InvalidActorNameException is thrown when you try to convert something, usually a String, to an Actor name
 * which doesn't validate.
 */
@SerialVersionUID(1L)
final case class InvalidActorNameException(message: String) extends AkkaException(message)

/**
 * An ActorInitializationException is thrown when the initialization logic for an Actor fails.
 *
 * There is an extractor which works for ActorInitializationException and its subtypes:
 *
 * {{{
 * ex match {
 *   case ActorInitializationException(actor, message, cause) => ...
 * }
 * }}}
 */
@SerialVersionUID(1L)
class ActorInitializationException protected (actor: ActorRef, message: String, cause: Throwable)
    extends AkkaException(ActorInitializationException.enrichedMessage(actor, message), cause) {
  def getActor: ActorRef = actor
}
object ActorInitializationException {
  private def enrichedMessage(actor: ActorRef, message: String) =
    if (actor == null) message else s"${actor.path}: $message"
  private[akka] def apply(actor: ActorRef, message: String, cause: Throwable = null): ActorInitializationException =
    new ActorInitializationException(actor, message, cause)
  private[akka] def apply(message: String): ActorInitializationException =
    new ActorInitializationException(null, message, null)
  def unapply(ex: ActorInitializationException): Option[(ActorRef, String, Throwable)] =
    Some((ex.getActor, ex.getMessage, ex.getCause))
}

/**
 * A PreRestartException is thrown when the preRestart() method failed; this
 * exception is not propagated to the supervisor, as it originates from the
 * already failed instance, hence it is only visible as log entry on the event
 * stream.
 *
 * @param actor is the actor whose preRestart() hook failed
 * @param cause is the exception thrown by that actor within preRestart()
 * @param originalCause is the exception which caused the restart in the first place
 * @param messageOption is the message which was optionally passed into preRestart()
 */
@SerialVersionUID(1L)
final case class PreRestartException private[akka] (
    actor: ActorRef,
    cause: Throwable,
    originalCause: Throwable,
    messageOption: Option[Any])
    extends ActorInitializationException(
      actor,
      "exception in preRestart(" +
      (if (originalCause == null) "null" else originalCause.getClass) + ", " +
      (messageOption match { case Some(m: AnyRef) => m.getClass; case _ => "None" }) +
      ")",
      cause)

/**
 * A PostRestartException is thrown when constructor or postRestart() method
 * fails during a restart attempt.
 *
 * @param actor is the actor whose constructor or postRestart() hook failed
 * @param cause is the exception thrown by that actor within preRestart()
 * @param originalCause is the exception which caused the restart in the first place
 */
@SerialVersionUID(1L)
final case class PostRestartException private[akka] (actor: ActorRef, cause: Throwable, originalCause: Throwable)
    extends ActorInitializationException(
      actor,
      "exception post restart (" + (if (originalCause == null) "null" else originalCause.getClass) + ")",
      cause)

/**
 * This is an extractor for retrieving the original cause (i.e. the first
 * failure) from a [[akka.actor.PostRestartException]]. In the face of multiple
 * “nested” restarts it will walk the origCause-links until it arrives at a
 * non-PostRestartException type.
 */
@SerialVersionUID(1L)
object OriginalRestartException {
  def unapply(ex: PostRestartException): Option[Throwable] = {
    @tailrec def rec(ex: PostRestartException): Option[Throwable] = ex match {
      case PostRestartException(_, _, e: PostRestartException) => rec(e)
      case PostRestartException(_, _, e)                       => Some(e)
    }
    rec(ex)
  }
}

/**
 * InvalidMessageException is thrown when an invalid message is sent to an Actor;
 * Currently only `null` is an invalid message.
 */
@SerialVersionUID(1L)
final case class InvalidMessageException private[akka] (message: String) extends AkkaException(message)

/**
 * A DeathPactException is thrown by an Actor that receives a Terminated(someActor) message
 * that it doesn't handle itself, effectively crashing the Actor and escalating to the supervisor.
 */
@SerialVersionUID(1L)
final case class DeathPactException private[akka] (dead: ActorRef)
    extends AkkaException("Monitored actor [" + dead + "] terminated")
    with NoStackTrace

/**
 * When an InterruptedException is thrown inside an Actor, it is wrapped as an ActorInterruptedException as to
 * avoid cascading interrupts to other threads than the originally interrupted one.
 */
@SerialVersionUID(1L)
class ActorInterruptedException private[akka] (cause: Throwable) extends AkkaException(cause.getMessage, cause)

/**
 * This message is published to the EventStream whenever an Actor receives a message it doesn't understand
 */
@SerialVersionUID(1L)
final case class UnhandledMessage(
    @BeanProperty message: Any,
    @BeanProperty sender: ActorRef,
    @BeanProperty recipient: ActorRef)
    extends NoSerializationVerificationNeeded
    with WrappedMessage
    with AllDeadLetters

/**
 * Superseeded by [[akka.pattern.StatusReply]], prefer that when possible.
 *
 * Classes for passing status back to the sender.
 * Used for internal ACKing protocol. But exposed as utility class for user-specific ACKing protocols as well.
 */
object Status {
  sealed trait Status extends Serializable

  /**
   * This class/message type is preferably used to indicate success of some operation performed.
   */
  @SerialVersionUID(1L)
  final case class Success(status: Any) extends Status

  /**
   * This class/message type is preferably used to indicate failure of some operation performed.
   * As an example, it is used to signal failure with AskSupport is used (ask/?).
   */
  @SerialVersionUID(1L)
  final case class Failure(cause: Throwable) extends Status
}

/**
 * Scala API: Mix in ActorLogging into your Actor to easily obtain a reference to a logger,
 * which is available under the name "log".
 *
 * {{{
 * class MyActor extends Actor with ActorLogging {
 *   def receive = {
 *     case "pigdog" => log.info("We've got yet another pigdog on our hands")
 *   }
 * }
 * }}}
 */
trait ActorLogging { this: Actor =>
  private var _log: LoggingAdapter = _

  def log: LoggingAdapter = {
    // only used in Actor, i.e. thread safe
    if (_log eq null)
      _log = akka.event.Logging(context.system, this)
    _log
  }

}

/**
 * Scala API: Mix in DiagnosticActorLogging into your Actor to easily obtain a reference to a logger with MDC support,
 * which is available under the name "log".
 * In the example bellow "the one who knocks" will be available under the key "iam" for using it in the logback pattern.
 *
 * {{{
 * class MyActor extends Actor with DiagnosticActorLogging {
 *
 *   override def mdc(currentMessage: Any): MDC = {
 *     Map("iam", "the one who knocks")
 *   }
 *
 *   def receive = {
 *     case "pigdog" => log.info("We've got yet another pigdog on our hands")
 *   }
 * }
 * }}}
 */
trait DiagnosticActorLogging extends Actor {
  import akka.event.Logging._
  val log = akka.event.Logging(this)
  def mdc(@nowarn("msg=never used") currentMessage: Any): MDC = emptyMDC

  override protected[akka] def aroundReceive(receive: Actor.Receive, msg: Any): Unit =
    try {
      log.mdc(mdc(msg))
      super.aroundReceive(receive, msg)
    } finally {
      log.clearMDC()
    }
}

object Actor {

  /**
   * Type alias representing a Receive-expression for Akka Actors.
   */
  //#receive
  type Receive = PartialFunction[Any, Unit]

  //#receive

  /**
   * emptyBehavior is a Receive-expression that matches no messages at all, ever.
   */
  @SerialVersionUID(1L)
  object emptyBehavior extends Receive {
    def isDefinedAt(x: Any) = false
    def apply(x: Any) = throw new UnsupportedOperationException("Empty behavior apply()")
  }

  /**
   * ignoringBehavior is a Receive-expression that consumes and ignores all messages.
   */
  @SerialVersionUID(1L)
  object ignoringBehavior extends Receive {
    def isDefinedAt(x: Any): Boolean = true
    def apply(x: Any): Unit = ()
  }

  /**
   * Default placeholder (null) used for "!" to indicate that there is no sender of the message,
   * that will be translated to the receiving system's deadLetters.
   */
  final val noSender: ActorRef = null

  /**
   * INTERNAL API
   */
  private final val NotHandled = new Object

  /**
   * INTERNAL API
   */
  private final val notHandledFun = (_: Any) => NotHandled
}

/**
 * Actor base trait that should be extended by or mixed to create an Actor with the semantics of the 'Actor Model':
 * <a href="https://en.wikipedia.org/wiki/Actor_model">https://en.wikipedia.org/wiki/Actor_model</a>
 *
 * An actor has a well-defined (non-cyclic) life-cycle.
 *  - ''RUNNING'' (created and started actor) - can receive messages
 *  - ''SHUTDOWN'' (when 'stop' is invoked) - can't do anything
 *
 * The Actor's own [[akka.actor.ActorRef]] is available as `self`, the current
 * message’s sender as `sender()` and the [[akka.actor.ActorContext]] as
 * `context`. The only abstract method is `receive` which shall return the
 * initial behavior of the actor as a partial function (behavior can be changed
 * using `context.become` and `context.unbecome`).
 *
 * This is the Scala API (hence the Scala code below), for the Java API see [[akka.actor.AbstractActor]].
 *
 * {{{
 * class ExampleActor extends Actor {
 *
 *   override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
 *     case _: ArithmeticException      => Resume
 *     case _: NullPointerException     => Restart
 *     case _: IllegalArgumentException => Stop
 *     case _: Exception                => Escalate
 *   }
 *
 *   def receive = {
 *                                      // directly calculated reply
 *     case Request(r)               => sender() ! calculate(r)
 *
 *                                      // just to demonstrate how to stop yourself
 *     case Shutdown                 => context.stop(self)
 *
 *                                      // error kernel with child replying directly to 'sender()'
 *     case Dangerous(r)             => context.actorOf(Props[ReplyToOriginWorker]).tell(PerformWork(r), sender())
 *
 *                                      // error kernel with reply going through us
 *     case OtherJob(r)              => context.actorOf(Props[ReplyToMeWorker]) ! JobRequest(r, sender())
 *     case JobReply(result, orig_s) => orig_s ! result
 *   }
 * }
 * }}}
 *
 * The last line demonstrates the essence of the error kernel design: spawn
 * one-off actors which terminate after doing their job, pass on `sender()` to
 * allow direct reply if that is what makes sense, or round-trip the sender
 * as shown with the fictitious JobRequest/JobReply message pair.
 *
 * If you don’t like writing `context` you can always `import context._` to get
 * direct access to `actorOf`, `stop` etc. This is not default in order to keep
 * the name-space clean.
 */
trait Actor {

  // to make type Receive known in subclasses without import
  type Receive = Actor.Receive

  /**
   * Scala API: Stores the context for this actor, including self, and sender.
   * It is implicit to support operations such as `forward`.
   *
   * WARNING: Only valid within the Actor itself, so do not close over it and
   * publish it to other threads!
   *
   * [[akka.actor.ActorContext]] is the Scala API. `getContext` returns a
   * [[akka.actor.AbstractActor.ActorContext]], which is the Java API of the actor
   * context.
   */
  implicit val context: ActorContext = {
    val contextStack = ActorCell.contextStack.get
    if (contextStack.isEmpty || (contextStack.head eq null))
      throw ActorInitializationException(
        s"You cannot create an instance of [${getClass.getName}] explicitly using the constructor (new). " +
        "You have to use one of the 'actorOf' factory methods to create a new actor. See the documentation.")
    val c = contextStack.head
    ActorCell.contextStack.set(null :: contextStack)
    c
  }

  /**
   * The 'self' field holds the ActorRef for this actor.
   * <p/>
   * Can be used to send messages to itself:
   * <pre>
   * self ! message
   * </pre>
   */
  implicit final val self: ActorRef = context.self //MUST BE A VAL, TRUST ME

  /**
   * The reference sender Actor of the last received message.
   * Is defined if the message was sent from another Actor,
   * else `deadLetters` in [[akka.actor.ActorSystem]].
   *
   * WARNING: Only valid within the Actor itself, so do not close over it and
   * publish it to other threads!
   */
  final def sender(): ActorRef = context.sender()

  /**
   * Scala API: This defines the initial actor behavior, it must return a partial function
   * with the actor logic.
   */
  //#receive
  def receive: Actor.Receive
  //#receive

  /**
   * INTERNAL API.
   *
   * Can be overridden to intercept calls to this actor's current behavior.
   *
   * @param receive current behavior.
   * @param msg current message.
   */
  @InternalApi
  protected[akka] def aroundReceive(receive: Actor.Receive, msg: Any): Unit = {
    // optimization: avoid allocation of lambda
    if (receive.applyOrElse(msg, Actor.notHandledFun).asInstanceOf[AnyRef] eq Actor.NotHandled) {
      unhandled(msg)
    }
  }

  /**
   * INTERNAL API.
   *
   * Can be overridden to intercept calls to `preStart`. Calls `preStart` by default.
   */
  @InternalApi
  protected[akka] def aroundPreStart(): Unit = preStart()

  /**
   * INTERNAL API.
   *
   * Can be overridden to intercept calls to `postStop`. Calls `postStop` by default.
   */
  @InternalApi
  protected[akka] def aroundPostStop(): Unit = postStop()

  /**
   * INTERNAL API.
   *
   * Can be overridden to intercept calls to `preRestart`. Calls `preRestart` by default.
   */
  @InternalApi
  protected[akka] def aroundPreRestart(reason: Throwable, message: Option[Any]): Unit = preRestart(reason, message)

  /**
   * INTERNAL API.
   *
   * Can be overridden to intercept calls to `postRestart`. Calls `postRestart` by default.
   */
  @InternalApi
  protected[akka] def aroundPostRestart(reason: Throwable): Unit = postRestart(reason)

  /**
   * User overridable definition the strategy to use for supervising
   * child actors.
   */
  def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy

  /**
   * User overridable callback.
   * <p/>
   * Is called when an Actor is started.
   * Actors are automatically started asynchronously when created.
   * Empty default implementation.
   */
  @throws(classOf[Exception]) // when changing this you MUST also change ActorDocTest
  //#lifecycle-hooks
  def preStart(): Unit = ()

  //#lifecycle-hooks

  /**
   * User overridable callback.
   * <p/>
   * Is called asynchronously after 'actor.stop()' is invoked.
   * Empty default implementation.
   */
  @throws(classOf[Exception]) // when changing this you MUST also change ActorDocTest
  //#lifecycle-hooks
  def postStop(): Unit = ()

  //#lifecycle-hooks

  /**
   * Scala API: User overridable callback: '''By default it disposes of all children and then calls `postStop()`.'''
   * @param reason the Throwable that caused the restart to happen
   * @param message optionally the current message the actor processed when failing, if applicable
   * <p/>
   * Is called on a crashed Actor right BEFORE it is restarted to allow clean
   * up of resources before Actor is terminated.
   */
  @throws(classOf[Exception]) // when changing this you MUST also change ActorDocTest
  //#lifecycle-hooks
  def preRestart(@nowarn("msg=never used") reason: Throwable, @nowarn("msg=never used") message: Option[Any]): Unit = {
    context.children.foreach { child =>
      context.unwatch(child)
      context.stop(child)
    }
    postStop()
  }

  //#lifecycle-hooks

  /**
   * User overridable callback: By default it calls `preStart()`.
   * @param reason the Throwable that caused the restart to happen
   * <p/>
   * Is called right AFTER restart on the newly created Actor to allow reinitialization after an Actor crash.
   */
  @throws(classOf[Exception]) // when changing this you MUST also change ActorDocTest
  //#lifecycle-hooks
  def postRestart(@nowarn("msg=never used") reason: Throwable): Unit = {
    preStart()
  }
  //#lifecycle-hooks

  /**
   * User overridable callback.
   * <p/>
   * Is called when a message isn't handled by the current behavior of the actor
   * by default it fails with either a [[akka.actor.DeathPactException]] (in
   * case of an unhandled [[akka.actor.Terminated]] message) or publishes an [[akka.actor.UnhandledMessage]]
   * to the actor's system's [[akka.event.EventStream]]
   */
  def unhandled(message: Any): Unit = {
    message match {
      case Terminated(dead) => throw DeathPactException(dead)
      case _                => context.system.eventStream.publish(UnhandledMessage(message, sender(), self))
    }
  }
}
