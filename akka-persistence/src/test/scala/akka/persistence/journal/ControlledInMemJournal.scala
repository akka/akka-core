/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Try

import akka.persistence.AtomicWrite
import akka.persistence.journal.inmem.InmemJournal
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

object ControlledInmemJournal {

  /** the journal is attempting to persist `messages`. The result of this attempt will depend on how `promise` completes.
   *
   *  If the `promise` fails, the persist will fail, resulting in the journal actor responding with `WriteMessagesFailed`
   *  followed by `WriteMessageFailure` for each message (assuming the journal did not receive any `NonPersistentRepr`s).
   *
   *  If the `promise` succeeds and the completed sequence's length equals the length of `messages` (else this will be treated
   *  as a failure, as above), the persist will succeed, resulting in the journal actor responding with `WriteMessagesSuccessful`
   *  followed by `WriteMessageSuccess` for each message (if the journal received any `NonPersistentRepr`s, `LoopMessageSuccess`es
   *  may be interspersed) corresponding to a successful [[Try]]; messages corresponding to an unsuccessful [[Try]] will have a
   *  `WriteMessageRejected` response.
   *
   *  The resulting promise must eventually be completed, else other persist/persistAsync operations against the journal
   *  might not be observed to succeed or fail in the [[JournalProtocol]] (even if their respective promises were completed)
   */
  final case class WriteMessagesAttempt(messages: Seq[AtomicWrite], promise: Promise[Seq[Try[Unit]]])

  def config(instanceId: String) = ConfigFactory.parseString(s"""
    |akka.persistence.journal.controlled-in-mem.class = "${classOf[ControlledInmemJournal].getName}"
    |akka.persistence.journal.controlled-in-mem.instance-id = "$instanceId"
    |akka.persistence.journal.plugin = "akka.persistence.journal.controlled-in-mem"
    """.stripMargin)

  // yes this is synchronized on a global, but this is for testing
  /** Obtain a probe for the given instance ID which receives [[WriteMessagesAttempt]] messages, throws if not found */
  def getProbe(instanceId: String): TestProbe = synchronized(_current(instanceId))

  private def putProbe(instanceId: String, probe: TestProbe): Unit = synchronized {
    _current = _current.updated(instanceId, probe)
  }

  private def removeProbe(instanceId: String): Unit = synchronized {
    _current = _current.removed(instanceId)
  }

  private[this] var _current = Map.empty[String, TestProbe]
}

/** An in-memory journal that exposes a [[akka.testkit.TestProbe]] allowing a test suite to control the result
 *  of persist/persistAsync operations.  Other operations (reads, deletes) are the usual in-memory journal.
 *
 *  Configure the actor system using {{{ControlledInmemJournal.config(String)}}} and
 *  access the probe using {{{ControlledInmemJournal.getProbe(String)}}}.
 */
final class ControlledInmemJournal extends InmemJournal {
  import ControlledInmemJournal._

  val persistProbe: TestProbe = TestProbe()(context.system)
  val instanceId = context.system.settings.config.getString("akka.persistence.journal.controlled-in-mem.instance-id")

  override def preStart(): Unit = {
    putProbe(instanceId, persistProbe)
    persistProbe.watch(self)
    super.preStart()
  }

  override def postStop(): Unit = {
    super.postStop()
    removeProbe(instanceId)
  }

  override def receivePluginInternal: PartialFunction[Any, Unit] = super.receivePluginInternal.orElse {
    case WriteMessagesAttempt(messages, promise) =>
      if (sender() == context.self) {
        promise.completeWith(super.asyncWriteMessages(messages))
      }
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val promise = Promise[Seq[Try[Unit]]]()
    persistProbe.ref ! WriteMessagesAttempt(messages, promise)

    promise.future.flatMap { results =>
      if (results.length == messages.length) {
        val superPromise = Promise[Seq[Try[Unit]]]()
        // we are executing outside of the journal actor now, and the InmemJournal's journal
        // depends on the journal actor for synchronization
        context.self ! WriteMessagesAttempt(messages, superPromise)
        promise.future
      } else
        Future.failed(
          new AssertionError(
            s"Mismatch between ${messages.length} atomic writes and ${results.length} results in completed promise"))
    }(ExecutionContext.parasitic)
  }
}
