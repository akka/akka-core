/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import scala.collection.immutable.Seq
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._
import scala.util.Try

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern.ask
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.persistence.journal.inmem.InmemJournal
import akka.testkit._
import akka.util.Timeout

object SteppingInmemJournal {

  /** allow the journal to do one operation */
  case object Token
  case object TokenConsumed

  /**
   * Allow the journal to do one operation, will block until that completes
   */
  def step(journal: ActorRef)(implicit system: ActorSystem): Unit = {
    implicit val timeout: Timeout = 3.seconds.dilated
    Await.result(journal ? SteppingInmemJournal.Token, timeout.duration)
  }

  def config(instanceId: String): Config =
    ConfigFactory.parseString(s"""
        |akka.persistence.journal.stepping-inmem.class=${classOf[SteppingInmemJournal].getName}
        |akka.persistence.journal.plugin = "akka.persistence.journal.stepping-inmem"
        |akka.persistence.journal.stepping-inmem.instance-id = "$instanceId"
      """.stripMargin)

  // keep it in a thread safe:d global so that tests can get their
  // hand on the actor ref and send Steps to it
  private[this] var _current: Map[String, ActorRef] = Map()

  // shhh don't tell anyone I sinn-croniz-ed
  /** get the actor ref to the journal for a given instance id, throws exception if not found */
  def getRef(instanceId: String): ActorRef = synchronized(_current(instanceId))

  private def putRef(instanceId: String, instance: ActorRef): Unit = synchronized {
    _current = _current + (instanceId -> instance)
  }
  private def remove(instanceId: String): Unit = synchronized(_current -= instanceId)
}

/**
 * An in memory journal that will not complete any persists or persistAsyncs until it gets tokens
 * to trigger those steps. Allows for tests that need to deterministically trigger the callbacks
 * intermixed with receiving messages.
 *
 * Configure your actor system using {{{SteppingInMemJournal.config}}} and then access
 * it using {{{SteppingInmemJournal.getRef(String)}}}, send it {{{SteppingInmemJournal.Token}}}s to
 * allow one journal operation to complete.
 */
final class SteppingInmemJournal extends InmemJournal {

  import SteppingInmemJournal._
  import context.dispatcher

  val instanceId = context.system.settings.config.getString("akka.persistence.journal.stepping-inmem.instance-id")

  var queuedOps: Seq[() => Future[Unit]] = Seq.empty
  var queuedTokenRecipients = List.empty[ActorRef]

  override def receivePluginInternal = super.receivePluginInternal.orElse {
    case Token if queuedOps.isEmpty => queuedTokenRecipients = queuedTokenRecipients :+ sender()
    case Token =>
      val op = queuedOps.head
      queuedOps = queuedOps.tail
      val tokenConsumer = sender()
      op().onComplete(_ => tokenConsumer ! TokenConsumed)
  }

  override def preStart(): Unit = {
    SteppingInmemJournal.putRef(instanceId, self)
    super.preStart()
  }

  override def postStop(): Unit = {
    super.postStop()
    SteppingInmemJournal.remove(instanceId)
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val futures = messages.map { message =>
      val promise = Promise[Try[Unit]]()
      val future = promise.future
      doOrEnqueue { () =>
        promise.completeWith(super.asyncWriteMessages(Seq(message)).map {
          case Nil      => AsyncWriteJournal.successUnit
          case nonEmpty => nonEmpty.head
        })
        future.map(_ => ())
      }
      future
    }

    Future.sequence(futures)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val promise = Promise[Unit]()
    val future = promise.future
    doOrEnqueue { () =>
      promise.completeWith(super.asyncDeleteMessagesTo(persistenceId, toSequenceNr))
      future
    }
    future
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val promise = Promise[Long]()
    val future = promise.future
    doOrEnqueue { () =>
      promise.completeWith(super.asyncReadHighestSequenceNr(persistenceId, fromSequenceNr))
      future.map(_ => ())
    }
    future
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: (PersistentRepr) => Unit): Future[Unit] = {
    val promise = Promise[Unit]()
    val future = promise.future
    doOrEnqueue { () =>
      promise.completeWith(
        super.asyncReplayMessages(persistenceId, fromSequenceNr, toSequenceNr, max)(recoveryCallback))
      future
    }

    future
  }

  private def doOrEnqueue(op: () => Future[Unit]): Unit = {
    if (queuedTokenRecipients.nonEmpty) {
      val completed = op()
      val tokenRecipient = queuedTokenRecipients.head
      val rest = queuedTokenRecipients.tail
      queuedTokenRecipients = rest
      completed.onComplete(_ => tokenRecipient ! TokenConsumed)
    } else {
      queuedOps = queuedOps :+ op
    }
  }
}
