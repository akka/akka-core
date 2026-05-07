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
import akka.japi.Pair
import akka.persistence.testkit.{ ExpectedFailure, SnapshotMeta, SnapshotOperation, SnapshotStorage }
import akka.persistence.testkit.scaladsl.{ SnapshotTestKit => ScalaTestKit }
import scala.jdk.CollectionConverters._

/**
 * Class for testing persisted snapshots in persistent actors.
 */
@ApiMayChange
class SnapshotTestKit(scalaTestkit: ScalaTestKit) {

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
   * Check that `snapshot` has been saved in the storage.
   */
  def expectNextPersisted[A](persistenceId: String, snapshot: A): A =
    scalaTestkit.expectNextPersisted(persistenceId, snapshot)

  /**
   * Check for `max` time that `snapshot` has been saved in the storage.
   */
  def expectNextPersisted[A](persistenceId: String, snapshot: A, max: Duration): A =
    scalaTestkit.expectNextPersisted(persistenceId, snapshot, max.toScala)

  /**
   * Check that next persisted in storage for particular persistence id snapshot has expected type.
   */
  def expectNextPersistedClass[A](persistenceId: String, cla: Class[A]): A =
    scalaTestkit.expectNextPersistedClass[A](persistenceId, cla)

  /**
   * Check for `max` time that next persisted in storage for particular persistence id snapshot has expected type.
   */
  def expectNextPersistedClass[A](persistenceId: String, cla: Class[A], max: Duration): A =
    scalaTestkit.expectNextPersistedClass[A](persistenceId, cla, max.toScala)

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
   * Fail next write operations with `cause` exception for particular persistence id.
   */
  def failNextPersisted(persistenceId: String, cause: Throwable): Unit = failNextNPersisted(persistenceId, 1, cause)

  /**
   * Fail next write operations with default exception for particular persistence id.
   */
  def failNextPersisted(persistenceId: String): Unit = failNextNPersisted(persistenceId, 1)

  /**
   * Fail next write operations with `cause` exception for any persistence id.
   */
  def failNextPersisted(cause: Throwable): Unit = failNextNPersisted(1, cause)

  /**
   * Fail next write operations with default exception for any persistence id.
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
   * Receive next `n` snapshots that have been persisted in the storage.
   */
  def receivePersisted[A](persistenceId: String, n: Int, cla: Class[A]): JList[A] =
    scalaTestkit.receivePersisted[A](persistenceId, n, cla).asJava

  /**
   * Receive for `max` time next `n` snapshots that have been persisted in the storage.
   */
  def receivePersisted[A](persistenceId: String, n: Int, cla: Class[A], max: Duration): JList[A] =
    scalaTestkit.receivePersisted[A](persistenceId, n, cla, max.toScala).asJava

  /**
   * Persist `snapshots` with metadata into storage in order.
   */
  def persistForRecovery(persistenceId: String, snapshots: JList[Pair[SnapshotMeta, Any]]): Unit =
    scalaTestkit.persistForRecovery(persistenceId, snapshots.asScala.toVector.map(_.toScala))

  /**
   * Retrieve all snapshots and their metadata saved in storage by persistence id.
   */
  def persistedInStorage(persistenceId: String): JList[Pair[SnapshotMeta, Any]] =
    scalaTestkit.persistedInStorage(persistenceId).map(p => Pair(p._1, p._2)).asJava

  /**
   * Clear all data from storage.
   */
  def clearAll(): Unit = scalaTestkit.clearAll()

  /**
   * Clear all data from storage for particular persistence id.
   */
  def clearByPersistenceId(persistenceId: String): Unit = scalaTestkit.clearByPersistenceId(persistenceId)

  /**
   * Fail `n` following journal operations depending on the condition `cond`.
   * Failure triggers, when `cond` returns true.
   * Fails operations with default `ExpectedFailure` exception.
   */
  def failNextNOpsCond(cond: jf.BiFunction[String, SnapshotOperation, Boolean], n: Int): Unit =
    failNextNOpsCond(cond, n, ExpectedFailure)

  /**
   * Fail `n` following journal operations depending on the condition `cond`.
   * Failure triggers, when `cond` returns true.
   * Fails operations with the `cause` exception.
   */
  def failNextNOpsCond(cond: jf.BiFunction[String, SnapshotOperation, Boolean], n: Int, cause: Throwable): Unit =
    scalaTestkit.failNextNOpsCond((l: String, r: SnapshotOperation) => cond.apply(l, r), n, cause)

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
  def withPolicy(policy: SnapshotStorage.SnapshotPolicies.PolicyType): SnapshotTestKit = {
    scalaTestkit.withPolicy(policy)
    this
  }

  /**
   * Returns default policy if it was changed by [[SnapshotTestKit.withPolicy()]].
   */
  def resetPolicy(): Unit = scalaTestkit.resetPolicy()

}

object SnapshotTestKit {

  import akka.actor.typed.{ ActorSystem => TypedActorSystem }

  def create(system: ActorSystem): SnapshotTestKit = new SnapshotTestKit(system)

  def create(system: TypedActorSystem[_]): SnapshotTestKit = create(system.classicSystem)

}
