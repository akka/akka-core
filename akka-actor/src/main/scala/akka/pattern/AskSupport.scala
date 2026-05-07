/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import java.net.URLEncoder
import java.util.concurrent.TimeoutException

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.{ Future, Promise }
import scala.language.implicitConversions
import scala.util.{ Failure, Success }
import scala.util.control.NoStackTrace

import akka.actor._
import akka.annotation.{ InternalApi, InternalStableApi }
import akka.dispatch.sysmsg._
import akka.util.Timeout
import akka.util.ByteString

/**
 * This is what is used to complete a Future that is returned from an ask/? call,
 * when it times out. A typical reason for `AskTimeoutException` is that the recipient
 * actor didn't send a reply.
 */
class AskTimeoutException(message: String, cause: Throwable) extends TimeoutException(message) with NoStackTrace {
  def this(message: String) = this(message, null: Throwable)
  override def getCause(): Throwable = cause
}

/**
 * This object contains implementation details of the “ask” pattern.
 */
trait AskSupport {

  /**
   * Import this implicit conversion to gain `?` and `ask` methods on
   * [[akka.actor.ActorRef]], which will defer to the
   * `ask(actorRef, message)(timeout)` method defined here.
   *
   * {{{
   * import akka.pattern.ask
   *
   * val future = actor ? message             // => ask(actor, message)
   * val future = actor ask message           // => ask(actor, message)
   * val future = actor.ask(message)(timeout) // => ask(actor, message)(timeout)
   * }}}
   *
   * All of the above use an implicit [[akka.util.Timeout]].
   */
  implicit def ask(actorRef: ActorRef): AskableActorRef = new AskableActorRef(actorRef)

  /**
   * Sends a message asynchronously and returns a [[scala.concurrent.Future]]
   * holding the eventual reply message; this means that the target actor
   * needs to send the result to the `sender` reference provided.
   *
   * The Future will be completed with an [[akka.pattern.AskTimeoutException]] after the
   * given timeout has expired; this is independent from any timeout applied
   * while awaiting a result for this future (i.e. in
   * `Await.result(..., timeout)`). A typical reason for `AskTimeoutException` is that the
   * recipient actor didn't send a reply.
   *
   * <b>Warning:</b>
   * When using future callbacks, inside actors you need to carefully avoid closing over
   * the containing actor’s object, i.e. do not call methods or access mutable state
   * on the enclosing actor from within the callback. This would break the actor
   * encapsulation and may introduce synchronization bugs and race conditions because
   * the callback will be scheduled concurrently to the enclosing actor. Unfortunately
   * there is not yet a way to detect these illegal accesses at compile time.
   *
   * <b>Recommended usage:</b>
   *
   * {{{
   *   val f = ask(worker, request)(timeout)
   *   f.map { response =>
   *     EnrichedMessage(response)
   *   } pipeTo nextActor
   * }}}
   *
   */
  def ask(actorRef: ActorRef, message: Any)(implicit timeout: Timeout): Future[Any] =
    actorRef.internalAsk(message, timeout, ActorRef.noSender)
  def ask(actorRef: ActorRef, message: Any, sender: ActorRef)(implicit timeout: Timeout): Future[Any] =
    actorRef.internalAsk(message, timeout, sender)

  /**
   * Use for messages whose response is known to be a [[akka.pattern.StatusReply]]. When a [[akka.pattern.StatusReply.Success]] response
   * arrives the future is completed with the wrapped value, if a [[akka.pattern.StatusReply.Error]] arrives the future is instead
   * failed.
   */
  def askWithStatus(actorRef: ActorRef, message: Any)(implicit timeout: Timeout): Future[Any] =
    actorRef.internalAskWithStatus(message)(timeout, Actor.noSender)

  /**
   * Use for messages whose response is known to be a [[akka.pattern.StatusReply]]. When a [[akka.pattern.StatusReply.Success]] response
   * arrives the future is completed with the wrapped value, if a [[akka.pattern.StatusReply.Error]] arrives the future is instead
   * failed.
   */
  def askWithStatus(actorRef: ActorRef, message: Any, sender: ActorRef)(implicit timeout: Timeout): Future[Any] =
    actorRef.internalAskWithStatus(message)(timeout, sender)

  /**
   * Import this implicit conversion to gain `?` and `ask` methods on
   * [[akka.actor.ActorSelection]], which will defer to the
   * `ask(actorSelection, message)(timeout)` method defined here.
   *
   * {{{
   * import akka.pattern.ask
   *
   * val future = selection ? message             // => ask(selection, message)
   * val future = selection ask message           // => ask(selection, message)
   * val future = selection.ask(message)(timeout) // => ask(selection, message)(timeout)
   * }}}
   *
   * All of the above use an implicit [[akka.util.Timeout]].
   */
  implicit def ask(actorSelection: ActorSelection): AskableActorSelection = new AskableActorSelection(actorSelection)

  /**
   * Sends a message asynchronously and returns a [[scala.concurrent.Future]]
   * holding the eventual reply message; this means that the target actor
   * needs to send the result to the `sender` reference provided.
   *
   * The Future will be completed with an [[akka.pattern.AskTimeoutException]] after the
   * given timeout has expired; this is independent from any timeout applied
   * while awaiting a result for this future (i.e. in
   * `Await.result(..., timeout)`). A typical reason for `AskTimeoutException` is that the
   * recipient actor didn't send a reply.
   *
   * <b>Warning:</b>
   * When using future callbacks, inside actors you need to carefully avoid closing over
   * the containing actor’s object, i.e. do not call methods or access mutable state
   * on the enclosing actor from within the callback. This would break the actor
   * encapsulation and may introduce synchronization bugs and race conditions because
   * the callback will be scheduled concurrently to the enclosing actor. Unfortunately
   * there is not yet a way to detect these illegal accesses at compile time.
   *
   * <b>Recommended usage:</b>
   *
   * {{{
   *   val f = ask(worker, request)(timeout)
   *   f.map { response =>
   *     EnrichedMessage(response)
   *   } pipeTo nextActor
   * }}}
   *
   */
  def ask(actorSelection: ActorSelection, message: Any)(implicit timeout: Timeout): Future[Any] =
    actorSelection.internalAsk(message, timeout, ActorRef.noSender)
  def ask(actorSelection: ActorSelection, message: Any, sender: ActorRef)(implicit timeout: Timeout): Future[Any] =
    actorSelection.internalAsk(message, timeout, sender)
}

/**
 * This object contains implementation details of the “ask” pattern,
 * which can be combined with "replyTo" pattern.
 */
trait ExplicitAskSupport {

  /**
   * Import this implicit conversion to gain `?` and `ask` methods on
   * [[akka.actor.ActorRef]], which will defer to the
   * `ask(actorRef, askSender => message)(timeout)` method defined here.
   *
   * {{{
   * import akka.pattern.ask
   *
   * // same as `ask(actor, askSender => Request(askSender))`
   * val future = actor ? { askSender => Request(askSender) }
   *
   * // same as `ask(actor, Request(_))`
   * val future = actor ? (Request(_))
   *
   * // same as `ask(actor, Request(_))(timeout)`
   * val future = actor ? (Request(_))(timeout)
   * }}}
   *
   * All of the above use a required implicit [[akka.util.Timeout]] and optional implicit
   * sender [[akka.actor.ActorRef]].
   */
  implicit def ask(actorRef: ActorRef): ExplicitlyAskableActorRef = new ExplicitlyAskableActorRef(actorRef)

  /**
   * Sends a message asynchronously and returns a [[scala.concurrent.Future]]
   * holding the eventual reply message; this means that the target actor
   * needs to send the result to the `sender` reference provided.
   *
   * The Future will be completed with an [[akka.pattern.AskTimeoutException]] after the
   * given timeout has expired; this is independent from any timeout applied
   * while awaiting a result for this future (i.e. in
   * `Await.result(..., timeout)`). A typical reason for `AskTimeoutException` is that the
   * recipient actor didn't send a reply.
   *
   * <b>Warning:</b>
   * When using future callbacks, inside actors you need to carefully avoid closing over
   * the containing actor’s object, i.e. do not call methods or access mutable state
   * on the enclosing actor from within the callback. This would break the actor
   * encapsulation and may introduce synchronization bugs and race conditions because
   * the callback will be scheduled concurrently to the enclosing actor. Unfortunately
   * there is not yet a way to detect these illegal accesses at compile time.
   *
   * <b>Recommended usage:</b>
   *
   * {{{
   * val f = ask(worker, replyTo => Request(replyTo))(timeout)
   * f.map { response =>
   *   EnrichedMessage(response)
   * } pipeTo nextActor
   * }}}
   */
  def ask(actorRef: ActorRef, messageFactory: ActorRef => Any)(implicit timeout: Timeout): Future[Any] =
    actorRef.internalAsk(messageFactory, timeout, ActorRef.noSender)
  def ask(actorRef: ActorRef, messageFactory: ActorRef => Any, sender: ActorRef)(
      implicit timeout: Timeout): Future[Any] =
    actorRef.internalAsk(messageFactory, timeout, sender)

  /**
   * Import this implicit conversion to gain `?` and `ask` methods on
   * [[akka.actor.ActorSelection]], which will defer to the
   * `ask(actorSelection, message)(timeout)` method defined here.
   *
   * {{{
   * import akka.pattern.ask
   *
   * // same as `ask(selection, askSender => Request(askSender))`
   * val future = selection ? { askSender => Request(askSender) }
   *
   * // same as `ask(selection, Request(_))`
   * val future = selection ? (Request(_))
   *
   * // same as `ask(selection, Request(_))(timeout)`
   * val future = selection ? (Request(_))(timeout)
   * }}}
   *
   * All of the above use a required implicit [[akka.util.Timeout]] and optional implicit
   * sender [[akka.actor.ActorRef]].
   */
  implicit def ask(actorSelection: ActorSelection): ExplicitlyAskableActorSelection =
    new ExplicitlyAskableActorSelection(actorSelection)

  /**
   * Sends a message asynchronously and returns a [[scala.concurrent.Future]]
   * holding the eventual reply message; this means that the target actor
   * needs to send the result to the `sender` reference provided.
   *
   * The Future will be completed with an [[akka.pattern.AskTimeoutException]] after the
   * given timeout has expired; this is independent from any timeout applied
   * while awaiting a result for this future (i.e. in
   * `Await.result(..., timeout)`). A typical reason for `AskTimeoutException` is that the
   * recipient actor didn't send a reply.
   *
   * <b>Warning:</b>
   * When using future callbacks, inside actors you need to carefully avoid closing over
   * the containing actor’s object, i.e. do not call methods or access mutable state
   * on the enclosing actor from within the callback. This would break the actor
   * encapsulation and may introduce synchronization bugs and race conditions because
   * the callback will be scheduled concurrently to the enclosing actor. Unfortunately
   * there is not yet a way to detect these illegal accesses at compile time.
   *
   * <b>Recommended usage:</b>
   *
   * {{{
   * val f = ask(worker, replyTo => Request(replyTo))(timeout)
   * f.map { response =>
   *   EnrichedMessage(response)
   * } pipeTo nextActor
   * }}}
   *
   */
  def ask(actorSelection: ActorSelection, messageFactory: ActorRef => Any)(implicit timeout: Timeout): Future[Any] =
    actorSelection.internalAsk(messageFactory, timeout, ActorRef.noSender)
  def ask(actorSelection: ActorSelection, messageFactory: ActorRef => Any, sender: ActorRef)(
      implicit timeout: Timeout): Future[Any] =
    actorSelection.internalAsk(messageFactory, timeout, sender)
}

object AskableActorRef {

  private def messagePartOfException(message: Any, sender: ActorRef): String = {
    val msg = if (message == null) "unknown" else message
    val wasSentBy = if (sender == ActorRef.noSender) "" else s" was sent by [$sender]"
    s"Message of type [${msg.getClass.getName}]$wasSentBy."
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def negativeTimeoutException(
      recipient: Any,
      message: Any,
      sender: ActorRef): IllegalArgumentException = {
    new IllegalArgumentException(
      s"Timeout length must be positive, question not sent to [$recipient]. " +
      messagePartOfException(message, sender))
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def recipientTerminatedException(
      recipient: Any,
      message: Any,
      sender: ActorRef): AskTimeoutException = {
    new AskTimeoutException(
      s"Recipient [$recipient] had already been terminated. " +
      messagePartOfException(message, sender))
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def unsupportedRecipientType(
      recipient: Any,
      message: Any,
      sender: ActorRef): IllegalArgumentException = {
    new IllegalArgumentException(
      s"Unsupported recipient type, question not sent to [$recipient]. " +
      messagePartOfException(message, sender))
  }
}

/*
 * Implementation class of the “ask” pattern enrichment of ActorRef
 */
final class AskableActorRef(val actorRef: ActorRef) extends AnyVal {

  /**
   * INTERNAL API: for binary compatibility
   */
  protected def ask(message: Any, timeout: Timeout): Future[Any] =
    internalAsk(message, timeout, ActorRef.noSender)

  //todo add scaladoc
  def ask(message: Any)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Any] =
    internalAsk(message, timeout, sender)

  def askWithStatus(message: Any)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Any] =
    internalAskWithStatus(message)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[pattern] def internalAskWithStatus(
      message: Any)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Any] =
    StatusReply.flattenStatusFuture[Any](internalAsk(message, timeout, sender).mapTo[StatusReply[Any]])

  /**
   * INTERNAL API: for binary compatibility
   */
  protected def ?(message: Any)(implicit timeout: Timeout): Future[Any] =
    internalAsk(message, timeout, ActorRef.noSender)

  def ?(message: Any)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Any] =
    internalAsk(message, timeout, sender)

  /**
   * INTERNAL API: for binary compatibility
   */
  private[pattern] def internalAsk(message: Any, timeout: Timeout, sender: ActorRef) = actorRef match {
    case ref: InternalActorRef if ref.isTerminated =>
      actorRef ! message
      Future.failed[Any](AskableActorRef.recipientTerminatedException(actorRef, message, sender))
    case ref: InternalActorRef =>
      if (timeout.duration.length <= 0)
        Future.failed[Any](AskableActorRef.negativeTimeoutException(actorRef, message, sender))
      else {
        PromiseActorRef(ref.provider, timeout, targetName = actorRef, message.getClass.getName, ref.path.name, sender)
          .ask(actorRef, message, timeout)
      }
    case _ => Future.failed[Any](AskableActorRef.unsupportedRecipientType(actorRef, message, sender))
  }

}

/*
 * Implementation class of the “ask” with explicit sender pattern enrichment of ActorRef
 */
final class ExplicitlyAskableActorRef(val actorRef: ActorRef) extends AnyVal {

  def ask(message: ActorRef => Any)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Any] =
    internalAsk(message, timeout, sender)

  def ?(message: ActorRef => Any)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Any] =
    internalAsk(message, timeout, sender)

  /**
   * INTERNAL API: for binary compatibility
   */
  private[pattern] def internalAsk(messageFactory: ActorRef => Any, timeout: Timeout, sender: ActorRef): Future[Any] =
    actorRef match {
      case ref: InternalActorRef if ref.isTerminated =>
        val message = messageFactory(ref.provider.deadLetters)
        actorRef ! message
        Future.failed[Any](AskableActorRef.recipientTerminatedException(actorRef, message, sender))
      case ref: InternalActorRef =>
        if (timeout.duration.length <= 0) {
          val message = messageFactory(ref.provider.deadLetters)
          Future.failed[Any](AskableActorRef.negativeTimeoutException(actorRef, message, sender))
        } else {
          val a = PromiseActorRef(ref.provider, timeout, targetName = actorRef, "unknown", ref.path.name, sender)
          val message = messageFactory(a)
          a.messageClassName = message.getClass.getName
          a.ask(actorRef, message, timeout)
        }
      case _ if sender eq null =>
        Future.failed[Any](
          new IllegalArgumentException(
            "No recipient for the reply was provided, " +
            s"question not sent to [$actorRef]."))
      case _ =>
        val message =
          if (sender == null) null else messageFactory(sender.asInstanceOf[InternalActorRef].provider.deadLetters)
        Future.failed[Any](AskableActorRef.unsupportedRecipientType(actorRef, message, sender))
    }
}

/*
 * Implementation class of the “ask” pattern enrichment of ActorSelection
 */
final class AskableActorSelection(val actorSel: ActorSelection) extends AnyVal {

  /**
   * INTERNAL API: for binary compatibility
   */
  protected def ask(message: Any, timeout: Timeout): Future[Any] =
    internalAsk(message, timeout, ActorRef.noSender)

  def ask(message: Any)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Any] =
    internalAsk(message, timeout, sender)

  /**
   * INTERNAL API: for binary compatibility
   */
  protected def ?(message: Any)(implicit timeout: Timeout): Future[Any] =
    internalAsk(message, timeout, ActorRef.noSender)

  def ?(message: Any)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Any] =
    internalAsk(message, timeout, sender)

  /**
   * INTERNAL API: for binary compatibility
   */
  private[pattern] def internalAsk(message: Any, timeout: Timeout, sender: ActorRef): Future[Any] =
    actorSel.anchor match {
      case ref: InternalActorRef =>
        if (timeout.duration.length <= 0)
          Future.failed[Any](AskableActorRef.negativeTimeoutException(actorSel, message, sender))
        else {
          val refPrefix = URLEncoder.encode(actorSel.pathString.replace("/", "_"), ByteString.UTF_8)
          PromiseActorRef(ref.provider, timeout, targetName = actorSel, message.getClass.getName, refPrefix, sender)
            .ask(actorSel, message, timeout)
        }
      case _ => Future.failed[Any](AskableActorRef.unsupportedRecipientType(actorSel, message, sender))
    }
}

/*
 * Implementation class of the “ask” with explicit sender pattern enrichment of ActorSelection
 */
final class ExplicitlyAskableActorSelection(val actorSel: ActorSelection) extends AnyVal {

  def ask(message: ActorRef => Any)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Any] =
    internalAsk(message, timeout, sender)

  def ?(message: ActorRef => Any)(implicit timeout: Timeout, sender: ActorRef = Actor.noSender): Future[Any] =
    internalAsk(message, timeout, sender)

  /**
   * INTERNAL API: for binary compatibility
   */
  private[pattern] def internalAsk(messageFactory: ActorRef => Any, timeout: Timeout, sender: ActorRef): Future[Any] =
    actorSel.anchor match {
      case ref: InternalActorRef =>
        if (timeout.duration.length <= 0) {
          val message = messageFactory(ref.provider.deadLetters)
          Future.failed[Any](AskableActorRef.negativeTimeoutException(actorSel, message, sender))
        } else {
          val refPrefix = URLEncoder.encode(actorSel.pathString.replace("/", "_"), ByteString.UTF_8)
          val a = PromiseActorRef(ref.provider, timeout, targetName = actorSel, "unknown", refPrefix, sender)
          val message = messageFactory(a)
          a.messageClassName = message.getClass.getName
          a.ask(actorSel, message, timeout)
        }
      case _ if sender eq null =>
        Future.failed[Any](
          new IllegalArgumentException(
            "No recipient for the reply was provided, " +
            s"question not sent to [$actorSel]."))
      case _ =>
        val message =
          if (sender == null) null else messageFactory(sender.asInstanceOf[InternalActorRef].provider.deadLetters)
        Future.failed[Any](AskableActorRef.unsupportedRecipientType(actorSel, message, sender))
    }
}

/**
 * Akka private optimized representation of the temporary actor spawned to
 * receive the reply to an "ask" operation.
 *
 * INTERNAL API
 */
private[akka] final class PromiseActorRef(
    val provider: ActorRefProvider,
    val result: Promise[Any],
    _mcn: String,
    refPathPrefix: String)
    extends MinimalActorRef {
  import PromiseActorRef._

  // This is necessary for weaving the PromiseActorRef into the asked message, i.e. the replyTo pattern.
  @volatile var messageClassName = _mcn

  /**
   * As an optimization for the common (local) case we only register this PromiseActorRef
   * with the provider when the `path` member is actually queried, which happens during
   * serialization (but also during a simple call to `toString`, `equals` or `hashCode`!).
   *
   * Defined states:
   * null                  => started, path not yet created
   * Registering           => currently creating temp path and registering it
   * path: ActorPath       => path is available and was registered
   * StoppedWithPath(path) => stopped, path available
   * Stopped               => stopped, path not yet created
   */
  @volatile
  @nowarn("msg=never updated")
  private[this] var _stateDoNotCallMeDirectly: AnyRef = _

  @volatile
  @nowarn("msg=never updated")
  private[this] var _watchedByDoNotCallMeDirectly: Set[ActorRef] = ActorCell.emptyActorRefSet

  @nowarn private def _preventPrivateUnusedErasure = {
    _stateDoNotCallMeDirectly
    _watchedByDoNotCallMeDirectly
  }

  @inline
  private[this] def watchedBy: Set[ActorRef] =
    AbstractPromiseActorRef.watchedByHandle.getVolatile(this).asInstanceOf[Set[ActorRef]]

  @inline
  private[this] def updateWatchedBy(oldWatchedBy: Set[ActorRef], newWatchedBy: Set[ActorRef]): Boolean =
    AbstractPromiseActorRef.watchedByHandle.compareAndSet(this, oldWatchedBy, newWatchedBy)

  @tailrec // Returns false if the Promise is already completed
  private[this] final def addWatcher(watcher: ActorRef): Boolean = watchedBy match {
    case null  => false
    case other => updateWatchedBy(other, other + watcher) || addWatcher(watcher)
  }

  @tailrec
  private[this] final def remWatcher(watcher: ActorRef): Unit = watchedBy match {
    case null  => ()
    case other => if (!updateWatchedBy(other, other - watcher)) remWatcher(watcher)
  }

  @tailrec
  private[this] final def clearWatchers(): Set[ActorRef] = watchedBy match {
    case null  => ActorCell.emptyActorRefSet
    case other => if (!updateWatchedBy(other, null)) clearWatchers() else other
  }

  @inline
  private[this] def state: AnyRef =
    AbstractPromiseActorRef.stateHandle.getVolatile(this)

  @inline
  private[this] def updateState(oldState: AnyRef, newState: AnyRef): Boolean =
    AbstractPromiseActorRef.stateHandle.compareAndSet(this, oldState, newState)

  @inline
  private[this] def setState(newState: AnyRef): Unit =
    AbstractPromiseActorRef.stateHandle.setVolatile(this, newState)

  override def getParent: InternalActorRef = provider.tempContainer

  /**
   * Contract of this method:
   * Must always return the same ActorPath, which must have
   * been registered if we haven't been stopped yet.
   */
  @tailrec
  def path: ActorPath = state match {
    case null =>
      if (updateState(null, Registering)) {
        var p: ActorPath = null
        try {
          p = provider.tempPath(refPathPrefix)
          provider.registerTempActor(this, p)
          p
        } finally {
          setState(p)
        }
      } else path
    case p: ActorPath       => p
    case StoppedWithPath(p) => p
    case Stopped            =>
      // even if we are already stopped we still need to produce a proper path
      updateState(Stopped, StoppedWithPath(provider.tempPath(refPathPrefix)))
      path
    case Registering => path // spin until registration is completed
    case unexpected  => throw new IllegalStateException(s"Unexpected state: $unexpected")
  }

  override def !(message: Any)(implicit sender: ActorRef = Actor.noSender): Unit = state match {
    case Stopped | _: StoppedWithPath =>
      provider.deadLetters ! DeadLetter(message, if (sender eq Actor.noSender) provider.deadLetters else sender, this)
      onComplete(message, alreadyCompleted = true)
    case _ =>
      if (message == null) throw InvalidMessageException("Message is null")
      val promiseResult = message match {
        case Status.Success(r) => Success(r)
        case Status.Failure(f) => Failure(f)
        case other             => Success(other)
      }
      val alreadyCompleted = !result.tryComplete(promiseResult)
      if (alreadyCompleted)
        provider.deadLetters ! message
      onComplete(message, alreadyCompleted)
  }

  override def sendSystemMessage(message: SystemMessage): Unit = message match {
    case _: Terminate                      => stop()
    case DeathWatchNotification(a, ec, at) => this.!(Terminated(a)(existenceConfirmed = ec, addressTerminated = at))
    case Watch(watchee, watcher) =>
      if (watchee == this && watcher != this) {
        if (!addWatcher(watcher))
          // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
          watcher.sendSystemMessage(
            DeathWatchNotification(watchee, existenceConfirmed = true, addressTerminated = false))
      } else System.err.println("BUG: illegal Watch(%s,%s) for %s".format(watchee, watcher, this))
    case Unwatch(watchee, watcher) =>
      if (watchee == this && watcher != this) remWatcher(watcher)
      else System.err.println("BUG: illegal Unwatch(%s,%s) for %s".format(watchee, watcher, this))
    case _ =>
  }

  override private[akka] def isTerminated: Boolean = state match {
    case Stopped | _: StoppedWithPath => true
    case _                            => false
  }

  @tailrec
  override def stop(): Unit = {
    def ensureCompleted(): Unit = {
      result.tryComplete(ActorStopResult)
      val watchers = clearWatchers()
      if (watchers.nonEmpty) {
        watchers.foreach { watcher =>
          // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
          watcher
            .asInstanceOf[InternalActorRef]
            .sendSystemMessage(DeathWatchNotification(this, existenceConfirmed = true, addressTerminated = false))
        }
      }
    }
    state match {
      case null => // if path was never queried nobody can possibly be watching us, so we don't have to publish termination either
        if (updateState(null, Stopped)) ensureCompleted() else stop()
      case p: ActorPath =>
        if (updateState(p, StoppedWithPath(p))) {
          try ensureCompleted()
          finally provider.unregisterTempActor(p)
        } else stop()
      case Stopped | _: StoppedWithPath => // already stopped
      case Registering                  => stop() // spin until registration is completed before stopping
      case unexpected                   => throw new IllegalStateException(s"Unexpected state: $unexpected")
    }
  }

  @InternalStableApi
  private[akka] def ask(
      actorSel: ActorSelection,
      message: Any,
      @nowarn("msg=never used") timeout: Timeout): Future[Any] = {
    actorSel.tell(message, this)
    result.future
  }

  @InternalStableApi
  private[akka] def ask(actorRef: ActorRef, message: Any, @nowarn("msg=never used") timeout: Timeout): Future[Any] = {
    actorRef.tell(message, this)
    result.future
  }

  @InternalStableApi
  private[akka] def onComplete(message: Any, alreadyCompleted: Boolean): Unit = {}

  @InternalStableApi
  private[akka] def onTimeout(timeout: Timeout): Unit = {}
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object PromiseActorRef {
  private case object Registering
  private case object Stopped
  private final case class StoppedWithPath(path: ActorPath)

  private val ActorStopResult = Failure(ActorKilledException("Stopped"))
  private val defaultOnTimeout: String => Throwable = str => new AskTimeoutException(str)

  def apply(
      provider: ActorRefProvider,
      timeout: Timeout,
      targetName: Any,
      messageClassName: String,
      refPathPrefix: String,
      sender: ActorRef = Actor.noSender,
      onTimeout: String => Throwable = defaultOnTimeout): PromiseActorRef = {
    if (refPathPrefix.indexOf('/') > -1)
      throw new IllegalArgumentException(s"refPathPrefix must not contain slash, was: $refPathPrefix")
    val result = Promise[Any]()
    val scheduler = provider.guardian.underlying.system.scheduler
    val a = new PromiseActorRef(provider, result, messageClassName, refPathPrefix)
    implicit val ec = ExecutionContext.parasitic
    val f = scheduler.scheduleOnce(timeout.duration) {
      val timedOut = result.tryComplete {
        val wasSentBy = if (sender == ActorRef.noSender) "" else s" was sent by [$sender]"
        val messagePart = s"Message of type [${a.messageClassName}]$wasSentBy."
        Failure(
          onTimeout(
            s"Ask timed out on [$targetName] after [${timeout.duration.toMillis} ms]. " +
            messagePart +
            " A typical reason for `AskTimeoutException` is that the recipient actor didn't send a reply."))
      }
      if (timedOut) {
        a.onTimeout(timeout)
      }
    }
    result.future.onComplete { _ =>
      try a.stop()
      finally f.cancel()
    }
    a
  }

}
