/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import scala.annotation.tailrec
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration

import akka.actor.Actor
import akka.actor.ActorLogMarker
import akka.actor.ActorRef
import akka.actor.AllDeadLetters
import akka.actor.DeadLetter
import akka.actor.DeadLetterActorRef
import akka.actor.DeadLetterSuppression
import akka.actor.Dropped
import akka.actor.UnhandledMessage
import akka.actor.WrappedMessage
import akka.event.Logging.Info
import akka.util.PrettyDuration._

class DeadLetterListener extends Actor {

  val eventStream: EventStream = context.system.eventStream
  protected val maxCount: Int = context.system.settings.LogDeadLetters
  private val isAlwaysLoggingDeadLetters = maxCount == Int.MaxValue
  protected var count: Int = 0

  override def preStart(): Unit = {
    eventStream.subscribe(self, classOf[DeadLetter])
    eventStream.subscribe(self, classOf[Dropped])
    eventStream.subscribe(self, classOf[UnhandledMessage])
  }

  // don't re-subscribe, skip call to preStart
  override def postRestart(reason: Throwable): Unit = ()

  // don't remove subscription, skip call to postStop, no children to stop
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = ()

  override def postStop(): Unit =
    eventStream.unsubscribe(self)

  private def incrementCount(): Unit = {
    // `count` is public API (for unknown reason) so for backwards compatibility reasons we
    // can't change it to Long
    if (count == Int.MaxValue) {
      Logging.getLogger(this).info("Resetting DeadLetterListener counter after reaching Int.MaxValue.")
      count = 1
    } else
      count += 1
  }

  def receive: Receive =
    if (isAlwaysLoggingDeadLetters) receiveWithAlwaysLogging
    else
      context.system.settings.LogDeadLettersSuspendDuration match {
        case suspendDuration: FiniteDuration => receiveWithSuspendLogging(suspendDuration)
        case _                               => receiveWithMaxCountLogging
      }

  private def receiveWithAlwaysLogging: Receive = {
    case d: AllDeadLetters =>
      if (!isWrappedSuppressed(d)) {
        incrementCount()
        logDeadLetter(d, doneMsg = "")
      }
  }

  private def receiveWithMaxCountLogging: Receive = {
    case d: AllDeadLetters =>
      if (!isWrappedSuppressed(d)) {
        incrementCount()
        if (count == maxCount) {
          logDeadLetter(d, ", no more dead letters will be logged")
          context.stop(self)
        } else {
          logDeadLetter(d, "")
        }
      }
  }

  private def receiveWithSuspendLogging(suspendDuration: FiniteDuration): Receive = {
    case d: AllDeadLetters =>
      if (!isWrappedSuppressed(d)) {
        incrementCount()
        if (count == maxCount) {
          val doneMsg = s", no more dead letters will be logged in next [${suspendDuration.pretty}]"
          logDeadLetter(d, doneMsg)
          context.become(receiveWhenSuspended(suspendDuration, Deadline.now + suspendDuration))
        } else
          logDeadLetter(d, "")
      }
  }

  private def receiveWhenSuspended(suspendDuration: FiniteDuration, suspendDeadline: Deadline): Receive = {
    case d: AllDeadLetters =>
      if (!isWrappedSuppressed(d)) {
        incrementCount()
        if (suspendDeadline.isOverdue()) {
          val doneMsg = s", of which ${count - maxCount - 1} were not logged. The counter will be reset now"
          logDeadLetter(d, doneMsg)
          count = 0
          context.become(receiveWithSuspendLogging(suspendDuration))
        }
      }
  }

  private def logDeadLetter(d: AllDeadLetters, doneMsg: String): Unit = {
    val origin = if (isReal(d.sender)) s" from ${d.sender}" else ""
    val unwrapped = WrappedMessage.unwrap(d.message)
    val messageStr = unwrapped.getClass.getName
    val wrappedIn = if (d.message.isInstanceOf[WrappedMessage]) s" wrapped in [${d.message.getClass.getName}]" else ""

    @tailrec def logMessage(msg: Any): String = msg match {
      case dl: DeadLetter =>
        logMessage(dl.message) // unwrap DeadLetter, e.g. DeadLetter(Dropped)
      case dropped: Dropped =>
        val destination = if (isReal(d.recipient)) s" to ${d.recipient}" else ""
        s"Message [$messageStr]$wrappedIn$origin$destination was dropped. ${dropped.reason}. " +
        s"[$count] dead letters encountered$doneMsg. "
      case _: UnhandledMessage =>
        val destination = if (isReal(d.recipient)) s" to ${d.recipient}" else ""
        s"Message [$messageStr]$wrappedIn$origin$destination was unhandled. " +
        s"[$count] dead letters encountered$doneMsg. "
      case _ =>
        s"Message [$messageStr]$wrappedIn$origin to ${d.recipient} was not delivered. " +
        s"[$count] dead letters encountered$doneMsg. " +
        s"If this is not an expected behavior then ${d.recipient} may have terminated unexpectedly. "
    }

    eventStream.publish(
      Info(
        d.recipient.path.toString,
        d.recipient.getClass,
        logMessage(d) +
        "This logging can be turned off or adjusted with configuration settings 'akka.log-dead-letters' " +
        "and 'akka.log-dead-letters-during-shutdown'.",
        Logging.emptyMDC,
        ActorLogMarker.deadLetter(messageStr)))
  }

  private def isReal(snd: ActorRef): Boolean = {
    (snd ne ActorRef.noSender) && (snd ne context.system.deadLetters) && !snd.isInstanceOf[DeadLetterActorRef]
  }

  private def isWrappedSuppressed(d: AllDeadLetters): Boolean = {
    d.message match {
      case w: WrappedMessage if w.message.isInstanceOf[DeadLetterSuppression] => true
      case _                                                                  => false
    }
  }

}
