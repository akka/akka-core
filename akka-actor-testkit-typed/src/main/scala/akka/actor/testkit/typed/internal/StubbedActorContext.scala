/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.internal

import java.util.concurrent.ThreadLocalRandom.{ current => rnd }

import scala.collection.immutable.TreeMap
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration

import org.slf4j.Logger
import org.slf4j.Marker
import org.slf4j.event.SubstituteLoggingEvent
import org.slf4j.helpers.{ MessageFormatter, SubstituteLoggerFactory }

import akka.{ actor => classic }
import akka.actor.{ ActorPath, ActorRefProvider, InvalidMessageException }
import akka.actor.testkit.typed.CapturedLogEvent
import akka.actor.typed._
import akka.actor.typed.internal._
import akka.annotation.InternalApi
import akka.util.Helpers

/**
 * INTERNAL API
 *
 * A local synchronous ActorRef that invokes the given function for every message send.
 * This reference cannot watch other references.
 */
@InternalApi
private[akka] final class FunctionRef[-T](override val path: ActorPath, send: (T, FunctionRef[T]) => Unit)
    extends ActorRef[T]
    with ActorRefImpl[T]
    with InternalRecipientRef[T] {

  override def tell(message: T): Unit = {
    if (message == null) throw InvalidMessageException("[null] is not an allowed message")
    send(message, this)
  }

  // impl ActorRefImpl
  override def sendSystem(signal: SystemMessage): Unit = ()
  // impl ActorRefImpl
  override def isLocal = true

  // impl InternalRecipientRef, ask not supported
  override def provider: ActorRefProvider =
    throw new UnsupportedOperationException(
      "ActorRefs created for synchronous testing cannot be used as targets for asking. Use asynchronous testing instead. " +
      "See https://doc.akka.io/libraries/akka-core/current/typed/testing.html#asynchronous-testing")

  // impl InternalRecipientRef
  def isTerminated: Boolean = false
}

/**
 * INTERNAL API
 *
 * A [[TypedActorContext]] for synchronous execution of a [[Behavior]] that
 * provides only stubs for the effects an Actor can perform and replaces
 * created child Actors by a synchronous Inbox (see `Inbox.sync`).
 */
@InternalApi private[akka] class StubbedActorContext[T](
    val system: ActorSystemStub,
    val path: ActorPath,
    currentBehaviorProvider: () => Behavior[T])
    extends ActorContextImpl[T] {

  def this(system: ActorSystemStub, name: String, currentBehaviorProvider: () => Behavior[T]) = {
    this(system, (system.path / name).withUid(rnd().nextInt()), currentBehaviorProvider)
  }

  def this(name: String, currentBehaviorProvider: () => Behavior[T]) = {
    this(new ActorSystemStub("StubbedActorContext"), name, currentBehaviorProvider)
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] val selfInbox = new TestInboxImpl[T](path)

  override val self = selfInbox.ref
  private var _children = TreeMap.empty[String, BehaviorTestKitImpl[_]]
  private val childName = Iterator.from(0).map(Helpers.base64(_))
  private val substituteLoggerFactory = new SubstituteLoggerFactory
  private val logger: Logger = substituteLoggerFactory.getLogger("StubbedLogger")
  private var unhandled: List[T] = Nil

  private[akka] def classicActorContext =
    throw new UnsupportedOperationException(
      "No classic ActorContext available with the stubbed actor context, to spawn materializers and run streams you will need a real actor")

  override def children: Iterable[ActorRef[Nothing]] = {
    checkCurrentActorThread()
    _children.values.map(_.context.self)
  }
  def childrenNames: Iterable[String] = _children.keys

  override def child(name: String): Option[ActorRef[Nothing]] = {
    checkCurrentActorThread()
    _children.get(name).map(_.context.self)
  }

  override def spawnAnonymous[U](behavior: Behavior[U], props: Props = Props.empty): ActorRef[U] = {
    checkCurrentActorThread()
    val btk = new BehaviorTestKitImpl[U](system, (path / childName.next()).withUid(rnd().nextInt()), behavior)
    _children += btk.context.self.path.name -> btk
    btk.context.self
  }
  override def spawn[U](behavior: Behavior[U], name: String, props: Props = Props.empty): ActorRef[U] = {
    checkCurrentActorThread()
    _children.get(name) match {
      case Some(_) => throw classic.InvalidActorNameException(s"actor name $name is already taken")
      case None =>
        val btk = new BehaviorTestKitImpl[U](system, (path / name).withUid(rnd().nextInt()), behavior)
        _children += name -> btk
        btk.context.self
    }
  }

  /**
   * Do not actually stop the child inbox, only simulate the liveness check.
   * Removal is asynchronous, explicit removeInbox is needed from outside afterwards.
   */
  override def stop[U](child: ActorRef[U]): Unit = {
    checkCurrentActorThread()
    if (child.path.parent != self.path)
      throw new IllegalArgumentException(
        "Only direct children of an actor can be stopped through the actor context, " +
        s"but [$child] is not a child of [$self]. Stopping other actors has to be expressed as " +
        "an explicit stop message that the actor accepts.")
    else {
      _children -= child.path.name
    }
  }
  override def watch[U](other: ActorRef[U]): Unit = {
    checkCurrentActorThread()
  }
  override def watchWith[U](other: ActorRef[U], message: T): Unit = {
    checkCurrentActorThread()
  }
  override def unwatch[U](other: ActorRef[U]): Unit = {
    checkCurrentActorThread()
  }
  override def setReceiveTimeout(d: FiniteDuration, message: T): Unit = {
    checkCurrentActorThread()
  }
  override def cancelReceiveTimeout(): Unit = {
    checkCurrentActorThread()
  }

  override def scheduleOnce[U](delay: FiniteDuration, target: ActorRef[U], message: U): classic.Cancellable =
    new classic.Cancellable {
      override def cancel() = false
      override def isCancelled = true
    }

  // TODO allow overriding of this
  override def executionContext: ExecutionContextExecutor = system.executionContext

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def internalSpawnMessageAdapter[U](f: U => T, name: String): ActorRef[U] = {

    val n = if (name != "") s"${childName.next()}-$name" else childName.next()
    val p = (path / n).withUid(rnd().nextInt())
    val i = new BehaviorTestKitImpl[U](system, p, BehaviorImpl.ignore)
    _children += p.name -> i

    new FunctionRef[U](p, (message, _) => {
      val m = f(message);
      if (m != null) {
        selfInbox.ref ! m; i.selfInbox().ref ! message
      }
    })
  }

  /**
   * Retrieve the inbox representing the given child actor. The passed ActorRef must be one that was returned
   * by one of the spawn methods earlier.
   */
  def childInbox[U](child: ActorRef[U]): TestInboxImpl[U] = {
    val btk = _children(child.path.name)
    if (btk.context.self != child) throw new IllegalArgumentException(s"$child is not a child of $this")
    btk.context.selfInbox.as[U]
  }

  /**
   * Retrieve the BehaviorTestKit for the given child actor. The passed ActorRef must be one that was returned
   * by one of the spawn methods earlier.
   */
  def childTestKit[U](child: ActorRef[U]): BehaviorTestKitImpl[U] = {
    val btk = _children(child.path.name)
    if (btk.context.self != child) throw new IllegalArgumentException(s"$child is not a child of $this")
    btk.as
  }

  /**
   * Retrieve the inbox representing the child actor with the given name.
   */
  def childInbox[U](name: String): Option[TestInboxImpl[U]] = _children.get(name).map(_.context.selfInbox.as[U])

  /**
   * Remove the given inbox from the list of children, for example after
   * having simulated its termination.
   */
  def removeChildInbox(child: ActorRef[Nothing]): Unit = _children -= child.path.name

  override def toString: String = s"Inbox($self)"

  override def log: Logger = {
    checkCurrentActorThread()
    logger
  }

  override def setLoggerName(name: String): Unit = {
    // nop as we don't track logger
    checkCurrentActorThread()
  }

  override def setLoggerName(clazz: Class[_]): Unit = {
    // nop as we don't track logger
    checkCurrentActorThread()
  }

  /**
   * The log entries logged through context.log.{debug, info, warn, error} are captured and can be inspected through
   * this method.
   */
  def logEntries: List[CapturedLogEvent] = {
    import scala.jdk.CollectionConverters._
    substituteLoggerFactory.getEventQueue
      .iterator()
      .asScala
      .map { evt =>
        CapturedLogEvent(
          level = evt.getLevel,
          message = MessageFormatter.arrayFormat(evt.getMessage, evt.getArgumentArray).getMessage,
          cause = Option(evt.getThrowable),
          marker = marker(evt))
      }
      .toList
  }

  private def marker(evt: SubstituteLoggingEvent): Option[Marker] = {
    val markers = evt.getMarkers
    if ((markers eq null) || markers.isEmpty)
      None
    else
      Option(markers.get(0))
  }

  /**
   * Clear the log entries.
   */
  def clearLog(): Unit =
    substituteLoggerFactory.getEventQueue.clear()

  override private[akka] def onUnhandled(msg: T): Unit =
    unhandled = msg :: unhandled

  /**
   * Messages that are marked as unhandled.
   */
  def unhandledMessages: List[T] = unhandled.reverse

  /**
   * Clear the list of captured unhandled messages.
   */
  def clearUnhandled(): Unit = unhandled = Nil

  override private[akka] def currentBehavior: Behavior[T] = currentBehaviorProvider()

}
