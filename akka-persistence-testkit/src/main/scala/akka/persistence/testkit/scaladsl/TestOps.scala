/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.scaladsl

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import akka.persistence.testkit.{ ExpectedFailure, ExpectedRejection }
import akka.persistence.testkit.ProcessingPolicy.DefaultPolicies
import akka.persistence.testkit.internal.TestKitStorage
import akka.testkit.TestKitBase
import akka.util
import akka.util.BoxedType

private[testkit] trait RejectSupport[U] {
  this: PolicyOpsTestKit[U] with HasStorage[U, _] =>

  /**
   * Reject `n` following journal operations depending on the condition `cond`.
   * Rejection triggers, when `cond` returns true.
   * Reject operations with default `ExpectedRejection` exception.
   */
  def rejectNextNOpsCond(cond: (String, U) => Boolean, n: Int): Unit =
    rejectNextNOpsCond(cond, n, ExpectedRejection)

  /**
   * Reject `n` following journal operations depending on the condition `cond`.
   * Rejection triggers, when `cond` returns true.
   * Rejects operations with the `cause` exception.
   */
  def rejectNextNOpsCond(cond: (String, U) => Boolean, n: Int, cause: Throwable): Unit = {
    val current = storage.currentPolicy
    val pol = new Policies.RejectNextNCond(n, cause, cond, withPolicy(current))
    withPolicy(pol)
  }

  /**
   * Reject n following journal operations regardless of their type.
   * Rejects operations with default `ExpectedRejection` exception.
   */
  def rejectNextNOps(n: Int): Unit =
    rejectNextNOps(n, ExpectedRejection)

  /**
   * Reject `n` following journal operations regardless of their type.
   * Rejects operations with the `cause` exception.
   */
  def rejectNextNOps(n: Int, cause: Throwable): Unit = {
    val current = storage.currentPolicy
    val pol = new Policies.RejectNextN(n, cause, withPolicy(current))
    withPolicy(pol)
  }

}

private[testkit] trait PolicyOpsTestKit[P] {
  this: HasStorage[P, _] =>

  private[testkit] val Policies: DefaultPolicies[P]

  /**
   * Fail `n` following journal operations depending on the condition `cond`.
   * Failure triggers, when `cond` returns true.
   * Fails operations with default `ExpectedFailure` exception.
   */
  def failNextNOpsCond(cond: (String, P) => Boolean, n: Int): Unit =
    failNextNOpsCond(cond, n, ExpectedFailure)

  /**
   * Fail `n` following journal operations depending on the condition `cond`.
   * Failure triggers, when `cond` returns true.
   * Fails operations with the `cause` exception.
   */
  def failNextNOpsCond(cond: (String, P) => Boolean, n: Int, cause: Throwable): Unit = {
    val current = storage.currentPolicy
    val pol = new Policies.FailNextNCond(n, cause, cond, withPolicy(current))
    withPolicy(pol)
  }

  /**
   * Fail n following journal operations regardless of their type.
   * Fails operations with default `ExpectedFailure` exception.
   */
  def failNextNOps(n: Int): Unit =
    failNextNOps(n, ExpectedFailure)

  /**
   * Fail `n` following journal operations regardless of their type.
   * Fails operations with the `cause` exception.
   */
  def failNextNOps(n: Int, cause: Throwable): Unit = {
    val current = storage.currentPolicy
    val pol = new Policies.FailNextN(n, cause, withPolicy(current))
    withPolicy(pol)
  }

  /**
   * Set new processing policy for journal operations.
   * NOTE! Overrides previously invoked `failNext...` or `rejectNext...`
   */
  def withPolicy(policy: Policies.PolicyType): this.type = {
    storage.setPolicy(policy)
    this
  }

  /**
   * Returns default policy if it was changed by [[PolicyOpsTestKit.this.withPolicy()]].
   */
  def resetPolicy(): Unit = storage.resetPolicy()

}

private[testkit] trait ExpectOps[U] {
  this: HasStorage[_, U] =>

  private[testkit] val probe: TestKitBase

  import probe._

  import akka.testkit._

  private[testkit] def pollInterval: FiniteDuration

  private[testkit] def maxTimeout: FiniteDuration

  private[testkit] def reprToAny(repr: U): Any

  /**
   * Check that next persisted in storage for particular persistence id event/snapshot was `event`.
   */
  def expectNextPersisted[A](persistenceId: String, event: A): A =
    expectNextPersisted(persistenceId, event, maxTimeout)

  def getItem(persistenceId: String, nextInd: Int): Option[Any] =
    storage.findOneByIndex(persistenceId, nextInd).map(reprToAny)

  /**
   * Check for `max` time that next persisted in storage for particular persistence id event/snapshot was `event`.
   */
  def expectNextPersisted[A](persistenceId: String, event: A, max: FiniteDuration): A = {
    val nextInd = nextIndex(persistenceId)
    val expected = Some(event)
    val res = awaitAssert({
      val actual = getItem(persistenceId, nextInd)
      assert(actual == expected, s"Failed to persist $event, got $actual instead")
      actual
    }, max = max.dilated, interval = pollInterval)

    setIndex(persistenceId, nextInd + 1)
    res.get.asInstanceOf[A]
  }

  /**
   * Check that next persisted in storage for particular persistence id event/snapshot has expected type.
   */
  def expectNextPersistedType[A](persistenceId: String)(implicit t: ClassTag[A]): A =
    expectNextPersistedType(persistenceId, maxTimeout)

  /**
   * Check for `max` time that next persisted in storage for particular persistence id event/snapshot has expected type.
   */
  def expectNextPersistedType[A](persistenceId: String, max: FiniteDuration)(implicit t: ClassTag[A]): A =
    expectNextPersistedClass(persistenceId, t.runtimeClass.asInstanceOf[Class[A]], max)

  /**
   * Check that next persisted in storage for particular persistence id event/snapshot has expected type.
   */
  def expectNextPersistedClass[A](persistenceId: String, cla: Class[A]): A =
    expectNextPersistedClass(persistenceId, cla, maxTimeout)

  /**
   * Check for `max` time that next persisted in storage for particular persistence id event/snapshot has expected type.
   */
  def expectNextPersistedClass[A](persistenceId: String, cla: Class[A], max: FiniteDuration): A = {
    val nextInd = nextIndex(persistenceId)
    val c = util.BoxedType(cla)
    val res = awaitAssert({
      val actual = storage.findOneByIndex(persistenceId, nextInd).map(reprToAny)
      assert(actual.isDefined, s"Expected: $cla but got no event")
      val a = actual.get
      assert(c.isInstance(a), s"Expected: $cla but got unexpected ${a.getClass}")
      a.asInstanceOf[A]
    }, max.dilated, interval = pollInterval)
    setIndex(persistenceId, nextInd + 1)
    res
  }

  /**
   * Check that nothing was persisted in storage for particular persistence id.
   */
  def expectNothingPersisted(persistenceId: String): Unit =
    expectNothingPersisted(persistenceId, maxTimeout)

  /**
   * Check for `max` time that nothing was persisted in storage for particular persistence id.
   */
  def expectNothingPersisted(persistenceId: String, max: FiniteDuration): Unit = {
    val nextInd = nextIndex(persistenceId)
    assertForDuration({
      val actual = storage.findOneByIndex(persistenceId, nextInd).map(reprToAny)
      val res = actual.isEmpty
      assert(res, s"Found persisted event $actual, but expected None instead")
    }, max = max.dilated, interval = pollInterval)
  }

  /**
   * Receive for `max` time next `n` events/snapshots that have been persisted in the storage.
   */
  def receivePersisted[A](persistenceId: String, n: Int, max: FiniteDuration)(
      implicit t: ClassTag[A]): immutable.Seq[A] =
    receivePersisted(persistenceId, n, t.runtimeClass.asInstanceOf[Class[A]], max)

  /**
   * Receive next `n` events/snapshots that have been persisted in the storage.
   */
  def receivePersisted[A](persistenceId: String, n: Int)(implicit t: ClassTag[A]): immutable.Seq[A] =
    receivePersisted(persistenceId, n, t.runtimeClass.asInstanceOf[Class[A]], maxTimeout)

  /**
   * Receive next `n` events/snapshots that have been persisted in the storage.
   */
  def receivePersisted[A](persistenceId: String, n: Int, cla: Class[A]): immutable.Seq[A] =
    receivePersisted(persistenceId, n, cla, maxTimeout)

  /**
   * Receive for `max` time next `n` events/snapshots that have been persisted in the storage.
   */
  def receivePersisted[A](persistenceId: String, n: Int, cla: Class[A], max: FiniteDuration): immutable.Seq[A] = {
    val nextInd = nextIndex(persistenceId)
    val bt = BoxedType(cla)
    val res =
      awaitAssert(
        {
          val actual = storage.findMany(persistenceId, nextInd, n)
          actual match {
            case Some(reprs) =>
              val ls = reprs.map(reprToAny)
              val filtered = ls.filter(e => !bt.isInstance(e))
              assert(ls.size == n, s"Could read only ${ls.size} events instead of expected $n")
              assert(filtered.isEmpty, s"Persisted events $filtered do not correspond to expected type")
            case None => assert(false, "No events were persisted")
          }
          actual.get.map(reprToAny)
        },
        max = max.dilated,
        interval = pollInterval)

    setIndex(persistenceId, nextInd + n)
    res.asInstanceOf[immutable.Seq[A]]
  }

}

private[testkit] trait ClearOps {
  this: HasStorage[_, _] =>

  /**
   * Clear all data from the storage.
   *
   * NOTE! Also clears sequence numbers in storage!
   *
   * @see [[ClearPreservingSeqNums.clearAllPreservingSeqNumbers()]]
   */
  def clearAll(): Unit = {
    storage.clearAll()
    clearIndexStorage()
  }

  /**
   * Clear all data from the storage for particular persistence id.
   *
   * NOTE! Also clears sequence number in the storage!
   *
   * @see [[ClearPreservingSeqNums.clearByIdPreservingSeqNumbers()]]
   */
  def clearByPersistenceId(persistenceId: String): Unit = {
    storage.removeKey(persistenceId)
    removeLastIndex(persistenceId)
  }

}

private[testkit] trait ClearPreservingSeqNums {
  this: HasStorage[_, _] =>

  /**
   * Clear all data in the storage preserving sequence numbers.
   *
   * @see [[ClearOps.clearAll()]]
   */
  def clearAllPreservingSeqNumbers(): Unit = {
    storage.clearAllPreservingSeqNumbers()
    clearIndexStorage()
  }

  /**
   * Clear all data in the storage for particular persistence id preserving sequence numbers.
   *
   * @see [[ClearOps.clearByPersistenceId()]]
   */
  def clearByIdPreservingSeqNumbers(persistenceId: String): Unit = {
    storage.removePreservingSeqNumber(persistenceId)
    removeLastIndex(persistenceId)
  }

}

/**
 * Abstract persistent storage for tests.
 * Has additional methods for keeping track of the indexes of last events persisted in the storage during test.
 */
private[testkit] trait HasStorage[P, R] {

  protected def storage: TestKitStorage[P, R]

  //todo needs to be thread safe (atomic read-increment-write) for parallel tests. Do we need parallel tests support?
  @volatile
  private var nextIndexByPersistenceId: immutable.Map[String, Int] = Map.empty

  private[testkit] def removeLastIndex(persistenceId: String): Unit =
    nextIndexByPersistenceId -= persistenceId

  private[testkit] def clearIndexStorage(): Unit =
    nextIndexByPersistenceId = Map.empty

  private[testkit] def nextIndex(persistenceId: String): Int =
    nextIndexByPersistenceId.getOrElse(persistenceId, 0)

  private[testkit] def setIndex(persistenceId: String, index: Int): Unit =
    nextIndexByPersistenceId += persistenceId -> index

  private[testkit] def addToIndex(persistenceId: String, add: Int): Unit = {
    val nextInd = nextIndexByPersistenceId.getOrElse(persistenceId, 0)
    nextIndexByPersistenceId += (persistenceId -> (nextInd + add))
  }

}
