/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.internal

import java.util.concurrent.atomic.AtomicReference

import scala.collection.immutable

import akka.annotation.InternalApi
import akka.persistence.testkit.ProcessingPolicy

/**
 * INTERNAL API
 */
@InternalApi
sealed trait InternalReprSupport[R] {

  type InternalRepr

  private[testkit] def toInternal(repr: R): InternalRepr

  private[testkit] def toRepr(internal: InternalRepr): R

}

/**
 * INTERNAL API
 */
@InternalApi
sealed trait InMemStorage[K, R] extends InternalReprSupport[R] {

  private val lock = new Object

  import scala.math._

  private var expectNextQueue: Map[K, Vector[InternalRepr]] = Map.empty
  private var eventsMap: Map[K, (Long, Vector[InternalRepr])] = Map.empty

  def reprToSeqNum(repr: R): Long

  def findMany(key: K, fromInclusive: Int, maxNum: Int): Option[Vector[R]] =
    read(key).flatMap(
      value =>
        if (value.size > fromInclusive)
          Some(value.drop(fromInclusive).take(maxNum))
        else None)

  def removeFirstInExpectNextQueue(key: K): Unit = lock.synchronized {
    expectNextQueue.get(key).foreach { item =>
      expectNextQueue = expectNextQueue.updated(key, item.tail)
    }
  }

  def firstInExpectNextQueue(key: K): Option[R] = lock.synchronized {
    expectNextQueue.get(key).flatMap { item =>
      item.headOption.map(toRepr)
    }
  }

  def findOneByIndex(key: K, index: Int): Option[R] = lock.synchronized {
    eventsMap
      .get(key)
      .flatMap {
        case (_, value) => if (value.size > index) Some(value(index)) else None
      }
      .map(toRepr)
  }

  def add(key: K, p: R): Unit = {
    lock.synchronized {
      if (!expectNextQueue.contains(key)) {
        expectNextQueue = expectNextQueue + (key -> Vector.empty)
      }
      expectNextQueue.get(key).foreach { items =>
        expectNextQueue = expectNextQueue - key
        val newItems = items :+ toInternal(p)
        expectNextQueue = expectNextQueue + (key -> newItems)
      }
      add(key, Vector(p))
    }
  }

  /**
   * Adds elements ordered by seqnum, sets new seqnum as max(old, max(newElemsSeqNums)))
   */
  def add(key: K, elems: immutable.Seq[R]): Unit =
    updateOrSetNew(key, v => v ++ elems)

  /**
   * Deletes elements preserving highest sequence number.
   */
  def delete(key: K, needsToBeDeleted: R => Boolean): Vector[R] =
    updateOrSetNew(key, v => v.filterNot(needsToBeDeleted))

  /**
   * Sets new elements returned by updater ordered by seqnum. Sets new seqnum as max(old, max(newElemsFromUpdaterSeqNums))
   */
  def updateOrSetNew(key: K, updater: Vector[R] => Vector[R]): Vector[R] = lock.synchronized {
    val (oldSn, oldElems) = eventsMap.getOrElse(key, (0L, Vector.empty))
    val newValue = {
      val upd = updater(oldElems.map(toRepr)).sortBy(reprToSeqNum)
      (max(getLastSeqNumber(upd), oldSn), upd.map(toInternal))
    }

    eventsMap = eventsMap.updated(key, newValue)
    newValue._2.map(toRepr)
  }

  def read(key: K): Option[Vector[R]] = lock.synchronized {
    eventsMap.get(key).map(_._2.map(toRepr))
  }

  def readAll(): Iterable[R] = lock.synchronized {
    eventsMap.values.flatMap { case (_, events) => events }.map(toRepr)
  }

  def clearAll(): Unit = lock.synchronized {
    eventsMap = Map.empty
  }

  /**
   * Removes key and the whole value including seqnum.
   */
  def removeKey(key: K): Vector[R] = lock.synchronized {
    val ret = eventsMap.get(key)
    eventsMap = eventsMap - key
    ret.map(_._2).getOrElse(Vector.empty).map(toRepr)
  }

  /**
   * Reads elems within the range of seqnums.
   */
  def read(key: K, fromInclusive: Long, toInclusive: Long, maxNumber: Long): immutable.Seq[R] = lock.synchronized {
    read(key)
      .getOrElse(Vector.empty)
      .dropWhile(reprToSeqNum(_) < fromInclusive)
      // we dont need to read highestSeqNumber because it will in any case stop at it if toInclusive > highestSeqNumber
      .takeWhile(reprToSeqNum(_) <= toInclusive)
      .take(if (maxNumber > Int.MaxValue) Int.MaxValue else maxNumber.toInt)
  }

  def removePreservingSeqNumber(key: K): Unit =
    updateOrSetNew(key, _ => Vector.empty)

  def getHighestSeqNumber(key: K): Long = lock.synchronized {
    eventsMap.get(key).map(_._1).getOrElse(0L)
  }

  def deleteToSeqNumber(key: K, toSeqNumberInclusive: Long): Unit =
    updateOrSetNew(key, value => {
      value.dropWhile(reprToSeqNum(_) <= toSeqNumberInclusive)
    })

  def clearAllPreservingSeqNumbers(): Unit = lock.synchronized {
    eventsMap.keys.foreach(removePreservingSeqNumber)
  }

  def keys(): immutable.Seq[K] = eventsMap.keys.toList

  private def getLastSeqNumber(elems: immutable.Seq[R]): Long =
    elems.lastOption.map(reprToSeqNum).getOrElse(0L)

}

/**
 * INTERNAL API
 */
@InternalApi
sealed trait PolicyOps[U] {

  type Policy = ProcessingPolicy[U]

  protected val DefaultPolicy: Policy

  private lazy val _processingPolicy: AtomicReference[Policy] =
    new AtomicReference(DefaultPolicy)

  def currentPolicy: Policy = _processingPolicy.get()

  def setPolicy(policy: Policy): Unit = _processingPolicy.set(policy)

  def resetPolicy(): Unit = setPolicy(DefaultPolicy)

}

/**
 * INTERNAL API
 */
@InternalApi
private[testkit] trait TestKitStorage[P, R] extends InMemStorage[String, R] with PolicyOps[P]
