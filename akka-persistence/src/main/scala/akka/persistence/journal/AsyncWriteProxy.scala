/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import scala.collection.immutable
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try

import akka.AkkaException
import akka.actor._
import akka.pattern.ask
import akka.persistence._
import akka.util._

/**
 * INTERNAL API.
 *
 * A journal that delegates actual storage to a target actor. For testing only.
 */
private[persistence] trait AsyncWriteProxy extends AsyncWriteJournal with Stash with ActorLogging {
  import AsyncWriteProxy._
  import AsyncWriteTarget._
  import context.dispatcher

  private var isInitialized = false
  private var isInitTimedOut = false
  protected var store: Option[ActorRef] = None
  private val storeNotInitialized =
    Future.failed(
      new TimeoutException(
        "Store not initialized. " +
        "Use `SharedLeveldbJournal.setStore(sharedStore, system)`"))

  override protected[akka] def aroundPreStart(): Unit = {
    context.system.scheduler.scheduleOnce(timeout.duration, self, InitTimeout)
    super.aroundPreStart()
  }

  override protected[akka] def aroundReceive(receive: Receive, msg: Any): Unit =
    if (isInitialized) {
      if (msg != InitTimeout) super.aroundReceive(receive, msg)
    } else
      msg match {
        case SetStore(ref) =>
          store = Some(ref)
          unstashAll()
          isInitialized = true
        case InitTimeout =>
          isInitTimedOut = true
          unstashAll() // will trigger appropriate failures
        case _ if isInitTimedOut => super.aroundReceive(receive, msg)
        case _                   => stash()
      }

  implicit def timeout: Timeout

  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] =
    store match {
      case Some(s) => (s ? WriteMessages(messages)).mapTo[immutable.Seq[Try[Unit]]]
      case None    => storeNotInitialized
    }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    store match {
      case Some(s) => (s ? DeleteMessagesTo(persistenceId, toSequenceNr)).mapTo[Unit]
      case None    => storeNotInitialized
    }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      replayCallback: PersistentRepr => Unit): Future[Unit] =
    store match {
      case Some(s) =>
        val replayCompletionPromise = Promise[Unit]()
        val mediator = context.actorOf(
          Props(new ReplayMediator(replayCallback, replayCompletionPromise, timeout.duration)).withDeploy(Deploy.local))
        s.tell(ReplayMessages(persistenceId, fromSequenceNr, toSequenceNr, max), mediator)
        replayCompletionPromise.future
      case None => storeNotInitialized
    }

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    store match {
      case Some(s) =>
        (s ? ReplayMessages(persistenceId, fromSequenceNr = 0L, toSequenceNr = 0L, max = 0L)).map {
          case ReplaySuccess(highest) => highest
          case _                      => throw new RuntimeException() // compiler exhaustiveness check pleaser
        }
      case None => storeNotInitialized
    }

}

/**
 * INTERNAL API.
 */
private[persistence] object AsyncWriteProxy {
  final case class SetStore(ref: ActorRef)
  case object InitTimeout
}

/**
 * INTERNAL API.
 */
private[persistence] object AsyncWriteTarget {
  @SerialVersionUID(1L)
  final case class WriteMessages(messages: immutable.Seq[AtomicWrite])

  @SerialVersionUID(1L)
  final case class DeleteMessagesTo(persistenceId: String, toSequenceNr: Long)

  @SerialVersionUID(1L)
  final case class ReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)

  @SerialVersionUID(1L)
  case class ReplaySuccess(highestSequenceNr: Long)

  @SerialVersionUID(1L)
  final case class ReplayFailure(cause: Throwable)

}

/**
 * Thrown if replay inactivity exceeds a specified timeout.
 */
@SerialVersionUID(1L)
class AsyncReplayTimeoutException(msg: String) extends AkkaException(msg)

private class ReplayMediator(
    replayCallback: PersistentRepr => Unit,
    replayCompletionPromise: Promise[Unit],
    replayTimeout: Duration)
    extends Actor {
  import AsyncWriteTarget._

  context.setReceiveTimeout(replayTimeout)

  def receive = {
    case p: PersistentRepr => replayCallback(p)
    case _: ReplaySuccess =>
      replayCompletionPromise.success(())
      context.stop(self)
    case ReplayFailure(cause) =>
      replayCompletionPromise.failure(cause)
      context.stop(self)
    case ReceiveTimeout =>
      replayCompletionPromise.failure(
        new AsyncReplayTimeoutException(s"replay timed out after ${replayTimeout.toSeconds} seconds inactivity"))
      context.stop(self)
  }
}
