/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.javadsl

import java.time.Duration
import java.util.{ List => JList }
import java.util.{ function => jf }

import scala.jdk.DurationConverters._

import akka.actor.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.testkit.{ EventStorage, ExpectedFailure, ExpectedRejection, JournalOperation }
import akka.persistence.testkit.scaladsl.{ PersistenceTestKit => ScalaTestKit }
import scala.jdk.CollectionConverters._

/**
 * Class for testing persisted events in persistent actors.
 */
@ApiMayChange
class PersistenceTestKit(scalaTestkit: ScalaTestKit) {

  def this(system: ActorSystem) = this(new ScalaTestKit(system))

  /**
   * Check that nothing has been saved in the storage.
   */
  def expectNothingPersisted(persistenceId: String): Unit = scalaTestkit.expectNothingPersisted(persistenceId)

  /**
   * Check for `max` time that nothing has been saved in the storage.
   */
  def expectNothingPersisted(persistenceId: String, max: Duration): Unit =
    scalaTestkit.expectNothingPersisted(persistenceId, max.toScala)

  /**
   * Check that `event` has been saved in the storage.
   */
  def expectNextPersisted[A](persistenceId: String, event: A): A =
    scalaTestkit.expectNextPersisted(persistenceId, event)

  /**
   * Check for `max` time that `event` has been saved in the storage.
   */
  def expectNextPersisted[A](persistenceId: String, event: A, max: Duration): A =
    scalaTestkit.expectNextPersisted(persistenceId, event, max.toScala)

  /**
   * Check that next persisted in storage for particular persistence id event has expected type.
   */
  def expectNextPersistedClass[A](persistenceId: String, cla: Class[A]): A =
    scalaTestkit.expectNextPersistedClass(persistenceId, cla)

  /**
   * Check for `max` time that next persisted in storage for particular persistence id event has expected type.
   */
  def expectNextPersistedClass[A](persistenceId: String, cla: Class[A], max: Duration): A =
    scalaTestkit.expectNextPersistedClass(persistenceId, cla, max.toScala)

  /**
   * Fail next `n` write operations with the `cause` exception for particular persistence id.
   */
  def failNextNPersisted(persistenceId: String, n: Int, cause: Throwable): Unit =
    scalaTestkit.failNextNPersisted(persistenceId, n, cause)

  /**
   * Fail next `n` write operations for particular persistence id.
   */
  def failNextNPersisted(persistenceId: String, n: Int): Unit = failNextNPersisted(persistenceId, n, ExpectedFailure)

  /**
   * Fail next `n` write operations with the `cause` exception for any persistence id.
   */
  def failNextNPersisted(n: Int, cause: Throwable): Unit = scalaTestkit.failNextNPersisted(n, cause)

  /**
   * Fail next `n` write operations with default exception for any persistence id.
   */
  def failNextNPersisted(n: Int): Unit = failNextNPersisted(n, ExpectedFailure)

  /**
   * Fail next write operation with `cause` exception for particular persistence id.
   */
  def failNextPersisted(persistenceId: String, cause: Throwable): Unit = failNextNPersisted(persistenceId, 1, cause)

  /**
   * Fail next write operation with default exception for particular persistence id.
   */
  def failNextPersisted(persistenceId: String): Unit = failNextNPersisted(persistenceId, 1)

  /**
   * Fail next write operation event with `cause` exception for any persistence id.
   */
  def failNextPersisted(cause: Throwable): Unit = failNextNPersisted(1, cause)

  /**
   * Fail next write operation with default exception for any persistence id.
   */
  def failNextPersisted(): Unit = failNextNPersisted(1)

  /**
   * Fail next read from storage (recovery) attempt with `cause` exception for any persistence id.
   */
  def failNextRead(cause: Throwable): Unit = failNextNReads(1, cause)

  /**
   * Fail next read from storage (recovery) attempt with default exception for any persistence id.
   */
  def failNextRead(): Unit = failNextNReads(1)

  /**
   * Fail next read from storage (recovery) attempt with `cause` exception for particular persistence id.
   */
  def failNextRead(persistenceId: String, cause: Throwable): Unit = failNextNReads(persistenceId, 1, cause)

  /**
   * Fail next read from storage (recovery) attempt with default exception for any persistence id.
   */
  def failNextRead(persistenceId: String): Unit = failNextNReads(persistenceId, 1)

  /**
   * Fail next n read from storage (recovery) attempts with `cause` exception for any persistence id.
   */
  def failNextNReads(n: Int, cause: Throwable): Unit = scalaTestkit.failNextNReads(n, cause)

  /**
   * Fail next n read from storage (recovery) attempts with default exception for any persistence id.
   */
  def failNextNReads(n: Int): Unit = failNextNReads(n, ExpectedFailure)

  /**
   * Fail next n read from storage (recovery) attempts with `cause` exception for particular persistence id.
   */
  def failNextNReads(persistenceId: String, n: Int, cause: Throwable): Unit =
    scalaTestkit.failNextNReads(persistenceId, n, cause)

  /**
   * Fail next n read from storage (recovery) attempts with default exception for particular persistence id.
   */
  def failNextNReads(persistenceId: String, n: Int): Unit = failNextNReads(persistenceId, n, ExpectedFailure)

  /**
   * Fail next delete from storage attempt with `cause` exception for any persistence id.
   */
  def failNextDelete(cause: Throwable): Unit = failNextNDeletes(1, cause)

  /**
   * Fail next delete from storage attempt with default exception for any persistence id.
   */
  def failNextDelete(): Unit = failNextNDeletes(1)

  /**
   * Fail next delete from storage attempt with `cause` exception for particular persistence id.
   */
  def failNextDelete(persistenceId: String, cause: Throwable): Unit = failNextNDeletes(persistenceId, 1, cause)

  /**
   * Fail next delete from storage attempt with default exception for particular persistence id.
   */
  def failNextDelete(persistenceId: String): Unit = failNextNDeletes(persistenceId, 1)

  /**
   * Fail next n delete from storage attempts with `cause` exception for any persistence id.
   */
  def failNextNDeletes(n: Int, cause: Throwable): Unit = scalaTestkit.failNextNDeletes(n, cause)

  /**
   * Fail next n delete from storage attempts with default exception for any persistence id.
   */
  def failNextNDeletes(n: Int): Unit = failNextNDeletes(n, ExpectedFailure)

  /**
   * Fail next n delete from storage attempts with `cause` exception for particular persistence id.
   */
  def failNextNDeletes(persistenceId: String, n: Int, cause: Throwable): Unit =
    scalaTestkit.failNextNDeletes(persistenceId, n, cause)

  /**
   * Fail next n delete from storage attempts with default exception for particular persistence id.
   */
  def failNextNDeletes(persistenceId: String, n: Int): Unit = failNextNDeletes(persistenceId, n, ExpectedFailure)

  /**
   * Receive next n events from the storage.
   */
  def receivePersisted[A](persistenceId: String, n: Int, cla: Class[A]): JList[A] =
    scalaTestkit.receivePersisted(persistenceId, n, cla).asJava

  /**
   * Receive for `max` time next n events from the storage.
   */
  def receivePersisted[A](persistenceId: String, n: Int, cla: Class[A], max: Duration): JList[A] =
    scalaTestkit.receivePersisted(persistenceId, n, cla, max.toScala).asJava

  /**
   * Reject next n save in storage operations for particular persistence id with `cause` exception.
   */
  def rejectNextNPersisted(persistenceId: String, n: Int, cause: Throwable): Unit =
    scalaTestkit.rejectNextNPersisted(persistenceId, n, cause)

  /**
   * Reject next n save in storage operations for particular persistence id with default exception.
   */
  def rejectNextNPersisted(persistenceId: String, n: Int): Unit =
    rejectNextNPersisted(persistenceId, n, ExpectedRejection)

  /**
   * Reject next n save in storage operations for any persistence id with default exception.
   */
  def rejectNextNPersisted(n: Int): Unit = rejectNextNPersisted(n, ExpectedRejection)

  /**
   * Reject next n save in storage operations for any persistence id with `cause` exception.
   */
  def rejectNextNPersisted(n: Int, cause: Throwable): Unit = scalaTestkit.rejectNextNPersisted(n, cause)

  /**
   * Reject next save in storage operation for particular persistence id with default exception.
   */
  def rejectNextPersisted(persistenceId: String): Unit = rejectNextNPersisted(persistenceId, 1)

  /**
   * Reject next save in storage operation for particular persistence id with `cause` exception.
   */
  def rejectNextPersisted(persistenceId: String, cause: Throwable): Unit = rejectNextNPersisted(persistenceId, 1, cause)

  /**
   * Reject next save in storage operation for any persistence id with `cause` exception.
   */
  def rejectNextPersisted(cause: Throwable): Unit = rejectNextNPersisted(1, cause)

  /**
   * Reject next save in storage operation for any persistence id with default exception.
   */
  def rejectNextPersisted(): Unit = rejectNextNPersisted(1)

  /**
   * Reject next read from storage operation for any persistence id with default exception.
   */
  def rejectNextRead(): Unit = rejectNextNReads(1)

  /**
   * Reject next read from storage operation for any persistence id with `cause` exception.
   */
  def rejectNextRead(cause: Throwable): Unit = rejectNextNReads(1, cause)

  /**
   * Reject next n read from storage operations for any persistence id with default exception.
   */
  def rejectNextNReads(n: Int): Unit = rejectNextNReads(n, ExpectedRejection)

  /**
   * Reject next n read from storage operations for any persistence id with `cause` exception.
   */
  def rejectNextNReads(n: Int, cause: Throwable): Unit = scalaTestkit.rejectNextNReads(n, cause)

  /**
   * Reject next read from storage operation for particular persistence id with default exception.
   */
  def rejectNextRead(persistenceId: String): Unit = rejectNextNReads(persistenceId, 1)

  /**
   * Reject next read from storage operation for particular persistence id with `cause` exception.
   */
  def rejectNextRead(persistenceId: String, cause: Throwable): Unit = rejectNextNReads(persistenceId, 1, cause)

  /**
   * Reject next n read from storage operations for particular persistence id with default exception.
   */
  def rejectNextNReads(persistenceId: String, n: Int): Unit = rejectNextNReads(persistenceId, n, ExpectedRejection)

  /**
   * Reject next n read from storage operations for particular persistence id with `cause` exception.
   */
  def rejectNextNReads(persistenceId: String, n: Int, cause: Throwable): Unit =
    scalaTestkit.rejectNextNReads(persistenceId, n, cause)

  /**
   * Reject next delete from storage operation for any persistence id with default exception.
   */
  def rejectNextDelete(): Unit = rejectNextNDeletes(1)

  /**
   * Reject next delete from storage operation for any persistence id with `cause` exception.
   */
  def rejectNextDelete(cause: Throwable): Unit = rejectNextNDeletes(1, cause)

  /**
   * Reject next n delete from storage operations for any persistence id with default exception.
   */
  def rejectNextNDeletes(n: Int): Unit = rejectNextNDeletes(n, ExpectedRejection)

  /**
   * Reject next n delete from storage operations for any persistence id with `cause` exception.
   */
  def rejectNextNDeletes(n: Int, cause: Throwable): Unit = scalaTestkit.rejectNextNDeletes(n, cause)

  /**
   * Reject next delete from storage operations for particular persistence id with default exception.
   */
  def rejectNextDelete(persistenceId: String): Unit = rejectNextNDeletes(persistenceId, 1)

  /**
   * Reject next delete from storage operations for particular persistence id with `cause` exception.
   */
  def rejectNextDelete(persistenceId: String, cause: Throwable): Unit = rejectNextNDeletes(persistenceId, 1, cause)

  /**
   * Reject next n delete from storage operations for particular persistence id with default exception.
   */
  def rejectNextNDeletes(persistenceId: String, n: Int): Unit = rejectNextNDeletes(persistenceId, n, ExpectedRejection)

  /**
   * Reject next n delete from storage operations for particular persistence id with `cause` exception.
   */
  def rejectNextNDeletes(persistenceId: String, n: Int, cause: Throwable): Unit =
    scalaTestkit.rejectNextNDeletes(persistenceId, n, cause)

  /**
   * Reject `n` following journal operations depending on the condition `cond`.
   * Rejection triggers, when `cond` returns true.
   * Reject operations with default `ExpectedRejection` exception.
   */
  def rejectNextNOpsCond(cond: jf.BiFunction[String, JournalOperation, Boolean], n: Int): Unit =
    rejectNextNOpsCond(cond, n, ExpectedRejection)

  /**
   * Reject `n` following journal operations depending on the condition `cond`.
   * Rejection triggers, when `cond` returns true.
   * Rejects operations with the `cause` exception.
   */
  def rejectNextNOpsCond(cond: jf.BiFunction[String, JournalOperation, Boolean], n: Int, cause: Throwable): Unit =
    scalaTestkit.rejectNextNOpsCond((l: String, r: JournalOperation) => cond.apply(l, r), n, cause)

  /**
   * Reject n following journal operations regardless of their type.
   * Rejects operations with default `ExpectedRejection` exception.
   */
  def rejectNextNOps(n: Int): Unit = rejectNextNOps(n, ExpectedRejection)

  /**
   * Reject `n` following journal operations regardless of their type.
   * Rejects operations with the `cause` exception.
   */
  def rejectNextNOps(n: Int, cause: Throwable): Unit = scalaTestkit.rejectNextNOps(n, cause)

  /**
   * Persist `events` into storage in order.
   */
  def persistForRecovery(persistenceId: String, events: JList[Any]): Unit =
    scalaTestkit.persistForRecovery(persistenceId, events.asScala.toVector)

  /**
   * Retrieve all events saved in storage by persistence id.
   */
  def persistedInStorage(persistenceId: String): JList[Any] = scalaTestkit.persistedInStorage(persistenceId).asJava

  /**
   * Clear all data from storage.
   *
   * NOTE! Also clears sequence numbers in storage!
   *
   * @see [[PersistenceTestKit.clearAllPreservingSeqNumbers()]]
   */
  def clearAll(): Unit = scalaTestkit.clearAll()

  /**
   * Clear all data from storage for particular persistence id.
   *
   * NOTE! Also clears sequence number in storage!
   *
   * @see [[PersistenceTestKit.clearByIdPreservingSeqNumbers()]]
   */
  def clearByPersistenceId(persistenceId: String): Unit = scalaTestkit.clearByPersistenceId(persistenceId)

  /**
   * Clear all data in storage preserving sequence numbers.
   *
   * @see [[PersistenceTestKit.clearAll()]]
   */
  def clearAllPreservingSeqNumbers(): Unit = scalaTestkit.clearAllPreservingSeqNumbers()

  /**
   * Clear all data in storage for particular persistence id preserving sequence numbers.
   *
   * @see [[PersistenceTestKit.clearByPersistenceId()]]
   */
  def clearByIdPreservingSeqNumbers(persistenceId: String): Unit =
    scalaTestkit.clearByIdPreservingSeqNumbers(persistenceId)

  /**
   * Fail `n` following journal operations depending on the condition `cond`.
   * Failure triggers, when `cond` returns true.
   * Fails operations with default `ExpectedFailure` exception.
   */
  def failNextNOpsCond(cond: jf.BiFunction[String, JournalOperation, Boolean], n: Int): Unit =
    failNextNOpsCond(cond, n, ExpectedFailure)

  /**
   * Fail `n` following journal operations depending on the condition `cond`.
   * Failure triggers, when `cond` returns true.
   * Fails operations with the `cause` exception.
   */
  def failNextNOpsCond(cond: jf.BiFunction[String, JournalOperation, Boolean], n: Int, cause: Throwable): Unit =
    scalaTestkit.failNextNOpsCond((l: String, r: JournalOperation) => cond.apply(l, r), n, cause)

  /**
   * Fail n following journal operations regardless of their type.
   * Fails operations with default `ExpectedFailure` exception.
   */
  def failNextNOps(n: Int): Unit =
    failNextNOps(n, ExpectedFailure)

  /**
   * Fail `n` following journal operations depending on the condition `cond`.
   * Failure triggers, when `cond` returns true.
   * Fails operations with the `cause` exception.
   */
  def failNextNOps(n: Int, cause: Throwable): Unit = scalaTestkit.failNextNOps(n, cause)

  /**
   * Set new processing policy for journal operations.
   * NOTE! Overrides previously invoked `failNext...` or `rejectNext...`
   */
  def withPolicy(policy: EventStorage.JournalPolicies.PolicyType): PersistenceTestKit = {
    scalaTestkit.withPolicy(policy)
    this
  }

  /**
   * Returns default policy if it was changed by [[PersistenceTestKit.withPolicy()]].
   */
  def resetPolicy(): Unit = scalaTestkit.resetPolicy()

}

object PersistenceTestKit {

  import akka.actor.typed.{ ActorSystem => TypedActorSystem }

  def create(system: ActorSystem): PersistenceTestKit = new PersistenceTestKit(system)

  def create(system: TypedActorSystem[_]): PersistenceTestKit = create(system.classicSystem)

}
