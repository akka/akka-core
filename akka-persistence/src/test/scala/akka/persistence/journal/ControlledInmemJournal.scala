/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.journal

import scala.concurrent.CancellationException
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Try

import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.pattern.ask
import akka.persistence.AtomicWrite
import akka.persistence.journal.inmem.InmemJournal
import akka.testkit.TestProbe
import akka.util.Timeout

object ControlledInmemJournal {

  /**
   * the journal is attempting to persist `messages`. The result of this attempt will depend on how `promise` completes.
   *
   *  If the `promise` fails, the persist will fail, resulting in the journal actor responding with `WriteMessagesFailed`
   *  followed by `WriteMessageFailure` for each message (assuming the journal did not receive any `NonPersistentRepr`s).
   *
   *  If the `promise` succeeds and the completed sequence's length equals the length of `messages` (else this will be treated
   *  as a failure, as above), the persist will succeed, resulting in the journal actor responding with `WriteMessagesSuccessful`
   *  followed by `WriteMessageSuccess` for each message (if the journal received any `NonPersistentRepr`s, `LoopMessageSuccess`es
   *  may be interspersed).  Nesting failures inside a successful promise is not presently supported.
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

  /** Obtain a probe for the given instance ID which receives [[WriteMessagesAttempt]] messages, throws if not found */
  def getProbe(instanceId: String): TestProbe = synchronized(_current(instanceId)._1)

  /**
   * If using the journal in multiple tests, call this after each test.  Any pending operations in the journal are
   * eagerly failed.
   *
   * Throws if the instance is not found
   *
   * @return a future of the number of operations which were eagerly failed
   */
  def drain(instanceId: String)(implicit timeout: Timeout): Future[Int] = synchronized {
    (_current(instanceId)._2 ? Flush).mapTo[Int]
  }

  private def putProbe(instanceId: String, probe: TestProbe, actor: ActorRef): Unit = synchronized {
    _current = _current.updated(instanceId, probe -> actor)
  }

  private def removeProbe(instanceId: String): Unit = synchronized {
    _current = _current.removed(instanceId)
  }

  private case object Flush
  private case class PromiseFinished(promise: Promise[Seq[Try[Unit]]])

  private[this] var _current = Map.empty[String, (TestProbe, ActorRef)]
}

/**
 *  An in-memory journal that exposes a [[akka.testkit.TestProbe]] allowing a test suite to control the result
 *  of persist/persistAsync operations.  Other operations (reads, deletes) are the usual in-memory journal.
 *
 *  Configure the actor system using {{{ControlledInmemJournal.config(String)}}} and
 *  access the probe using {{{ControlledInmemJournal.getProbe(String)}}}.
 */
final class ControlledInmemJournal extends InmemJournal {
  import ControlledInmemJournal._

  val persistProbe: TestProbe = TestProbe()(context.system)
  val instanceId = context.system.settings.config.getString("akka.persistence.journal.controlled-in-mem.instance-id")
  var promises = Set.empty[Promise[Seq[Try[Unit]]]]

  override def preStart(): Unit = {
    putProbe(instanceId, persistProbe, self)
    persistProbe.watch(self)
    super.preStart()
  }

  override def postStop(): Unit = {
    super.postStop()
    removeProbe(instanceId)
  }

  override def receivePluginInternal: Receive = super.receivePluginInternal.orElse {
    case WriteMessagesAttempt(messages, promise) =>
      if (sender() == self) {
        promise.completeWith(super.asyncWriteMessages(messages))
      }

    case Flush =>
      val failed = promises.foldLeft(0) { (sum, p) =>
        if (p.tryFailure(new CancellationException("test finished"))) {
          sum + 1
        } else sum
      }
      promises = Set.empty
      sender() ! failed

    case PromiseFinished(p) =>
      if (promises(p)) {
        // ensure that the promise completes before removal
        p.tryFailure(new CancellationException("promise finished"))
        promises = promises.excl(p)
      }
  }

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val promise = Promise[Seq[Try[Unit]]]()
    persistProbe.ref ! WriteMessagesAttempt(messages, promise)

    val result =
      promise.future.flatMap { results =>
        // if this journal supported journal rejections (maybe down the road TODO), those
        // would be represented as failures in the results
        val successfulResults = results.filter(_.isSuccess)

        if (successfulResults.length == messages.length) {
          val superPromise = Promise[Seq[Try[Unit]]]()
          // we are executing outside of the journal actor now, and the InmemJournal's journal
          // depends on the journal actor for synchronization
          self ! WriteMessagesAttempt(messages, superPromise)
          superPromise.future
        } else
          Future.failed(new AssertionError(
            s"Mismatch between ${messages.length} atomic writes and ${successfulResults.length} successful results in completed promise"))
      }(ExecutionContext.parasitic)

    val ret = Promise[Seq[Try[Unit]]]()
    promises = promises.incl(ret)
    ret.completeWith(result)
    ret.future.onComplete { _ =>
      self ! PromiseFinished(ret)
    }(ExecutionContext.parasitic)
    ret.future
  }
}
