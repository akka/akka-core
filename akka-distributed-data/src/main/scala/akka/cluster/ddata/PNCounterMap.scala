/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import java.math.BigInteger

import akka.annotation.InternalApi
import akka.cluster.Cluster
import akka.cluster.UniqueAddress
import akka.cluster.ddata.ORMap._

object PNCounterMap {

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case object PNCounterMapTag extends ZeroTag {
    override def zero: DeltaReplicatedData = PNCounterMap.empty
    override final val value: Int = 1
  }

  def empty[A]: PNCounterMap[A] = new PNCounterMap(new ORMap(ORSet.empty, Map.empty, zeroTag = PNCounterMapTag))
  def apply[A](): PNCounterMap[A] = empty

  /**
   * Java API
   */
  def create[A](): PNCounterMap[A] = empty

  /**
   * Extract the [[PNCounterMap#entries]].
   */
  def unapply[A](m: PNCounterMap[A]): Option[Map[A, BigInt]] = Some(m.entries)
}

/**
 * Map of named counters. Specialized [[ORMap]] with [[PNCounter]] values.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@SerialVersionUID(1L)
final class PNCounterMap[A] private[akka] (private[akka] val underlying: ORMap[A, PNCounter])
    extends DeltaReplicatedData
    with ReplicatedDataSerialization
    with RemovedNodePruning {

  type T = PNCounterMap[A]
  type D = ORMap.DeltaOp

  /** Scala API */
  def entries: Map[A, BigInt] = underlying.entries.map { case (k, c) => k -> c.value }

  /** Java API */
  def getEntries: java.util.Map[A, BigInteger] = {
    import scala.jdk.CollectionConverters._
    underlying.entries.map { case (k, c) => k -> c.value.bigInteger }.asJava
  }

  /**
   *  Scala API: The count for a key
   */
  def get(key: A): Option[BigInt] = underlying.get(key).map(_.value)

  /**
   * Java API: The count for a key, or `null` if it doesn't exist
   */
  def getValue(key: A): BigInteger = underlying.get(key).map(_.value.bigInteger).orNull

  def contains(key: A): Boolean = underlying.contains(key)

  def isEmpty: Boolean = underlying.isEmpty

  def size: Int = underlying.size

  /**
   * Increment the counter with the delta specified.
   * If the delta is negative then it will decrement instead of increment.
   */
  def incrementBy(key: A, delta: Long)(implicit node: SelfUniqueAddress): PNCounterMap[A] =
    increment(node.uniqueAddress, key, delta)

  /**
   * Increment the counter with the delta specified.
   * If the delta is negative then it will decrement instead of increment.
   */
  def increment(key: A, delta: Long = 1)(implicit node: Cluster): PNCounterMap[A] =
    increment(node.selfUniqueAddress, key, delta)

  /**
   * Increment the counter with the delta specified.
   * If the delta is negative then it will decrement instead of increment.
   */
  def increment(node: SelfUniqueAddress, key: A, delta: Long): PNCounterMap[A] =
    increment(node.uniqueAddress, key, delta)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def increment(node: UniqueAddress, key: A, delta: Long): PNCounterMap[A] =
    new PNCounterMap(underlying.updated(node, key, PNCounter())(_.increment(node, delta)))

  /**
   * Decrement the counter with the delta specified.
   * If the delta is negative then it will increment instead of decrement.
   * TODO add implicit after deprecated is EOL.
   */
  def decrementBy(key: A, delta: Long = 1)(implicit node: SelfUniqueAddress): PNCounterMap[A] =
    decrement(node, key, delta)

  /**
   * Decrement the counter with the delta specified.
   * If the delta is negative then it will increment instead of decrement.
   * TODO add implicit after deprecated is EOL.
   */
  def decrement(node: SelfUniqueAddress, key: A, delta: Long): PNCounterMap[A] =
    decrement(node.uniqueAddress, key, delta)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def decrement(node: UniqueAddress, key: A, delta: Long): PNCounterMap[A] = {
    new PNCounterMap(underlying.updated(node, key, PNCounter())(_.decrement(node, delta)))
  }

  /**
   * Removes an entry from the map.
   * Note that if there is a conflicting update on another node the entry will
   * not be removed after merge.
   */
  def remove(key: A)(implicit node: SelfUniqueAddress): PNCounterMap[A] =
    remove(node.uniqueAddress, key)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def remove(node: UniqueAddress, key: A): PNCounterMap[A] =
    new PNCounterMap(underlying.remove(node, key))

  override def merge(that: PNCounterMap[A]): PNCounterMap[A] =
    new PNCounterMap(underlying.merge(that.underlying))

  override def resetDelta: PNCounterMap[A] =
    new PNCounterMap(underlying.resetDelta)

  override def delta: Option[D] = underlying.delta

  override def mergeDelta(thatDelta: D): PNCounterMap[A] =
    new PNCounterMap(underlying.mergeDelta(thatDelta))

  override def modifiedByNodes: Set[UniqueAddress] =
    underlying.modifiedByNodes

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    underlying.needPruningFrom(removedNode)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): PNCounterMap[A] =
    new PNCounterMap(underlying.prune(removedNode, collapseInto))

  override def pruningCleanup(removedNode: UniqueAddress): PNCounterMap[A] =
    new PNCounterMap(underlying.pruningCleanup(removedNode))

  // this class cannot be a `case class` because we need different `unapply`

  override def toString: String = s"PNCounter$entries"

  override def equals(o: Any): Boolean = o match {
    case other: PNCounterMap[_] => underlying == other.underlying
    case _                      => false
  }

  override def hashCode: Int = underlying.hashCode
}

object PNCounterMapKey {
  def create[A](id: String): Key[PNCounterMap[A]] = PNCounterMapKey[A](id)
}

@SerialVersionUID(1L)
final case class PNCounterMapKey[A](_id: String) extends Key[PNCounterMap[A]](_id) with ReplicatedDataSerialization {
  override def withId(newId: Key.KeyId): PNCounterMapKey[A] =
    PNCounterMapKey(newId)
}
