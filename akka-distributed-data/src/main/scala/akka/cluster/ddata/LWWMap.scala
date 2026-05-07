/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.annotation.InternalApi
import akka.cluster.UniqueAddress
import akka.cluster.ddata.ORMap.ZeroTag

object LWWMap {

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case object LWWMapTag extends ZeroTag {
    override def zero: DeltaReplicatedData = LWWMap.empty
    override final val value: Int = 4
  }

  private val _empty: LWWMap[Any, Any] = new LWWMap(new ORMap(ORSet.empty, Map.empty, zeroTag = LWWMapTag))
  def empty[A, B]: LWWMap[A, B] = _empty.asInstanceOf[LWWMap[A, B]]
  def apply(): LWWMap[Any, Any] = _empty

  /**
   * Java API
   */
  def create[A, B](): LWWMap[A, B] = empty

  /**
   * Extract the [[LWWMap#entries]].
   */
  def unapply[A, B](m: LWWMap[A, B]): Option[Map[A, B]] = Some(m.entries)
}

/**
 * Specialized [[ORMap]] with [[LWWRegister]] values.
 *
 * `LWWRegister` relies on synchronized clocks and should only be used when the choice of
 * value is not important for concurrent updates occurring within the clock skew.
 *
 * Instead of using timestamps based on `System.currentTimeMillis()` time it is possible to
 * use a timestamp value based on something else, for example an increasing version number
 * from a database record that is used for optimistic concurrency control.
 *
 * The `defaultClock` is using max value of `System.currentTimeMillis()` and `currentTimestamp + 1`.
 * This means that the timestamp is increased for changes on the same node that occurs within
 * the same millisecond. It also means that it is safe to use the `LWWMap` without
 * synchronized clocks when there is only one active writer, e.g. a Cluster Singleton. Such a
 * single writer should then first read current value with `ReadMajority` (or more) before
 * changing and writing the value with `WriteMajority` (or more).
 *
 * For first-write-wins semantics you can use the [[LWWRegister#reverseClock]] instead of the
 * [[LWWRegister#defaultClock]]
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@SerialVersionUID(1L)
final class LWWMap[A, B] private[akka] (private[akka] val underlying: ORMap[A, LWWRegister[B]])
    extends DeltaReplicatedData
    with ReplicatedDataSerialization
    with RemovedNodePruning {
  import LWWRegister.{ defaultClock, Clock }

  type T = LWWMap[A, B]
  type D = ORMap.DeltaOp

  /**
   * Scala API: All entries of the map.
   */
  def entries: Map[A, B] = underlying.entries.map { case (k, r) => k -> r.value }

  /**
   * Java API: All entries of the map.
   */
  def getEntries(): java.util.Map[A, B] = {
    import scala.jdk.CollectionConverters._
    entries.asJava
  }

  def get(key: A): Option[B] = underlying.get(key).map(_.value)

  def contains(key: A): Boolean = underlying.contains(key)

  def isEmpty: Boolean = underlying.isEmpty

  def size: Int = underlying.size

  /**
   * Adds an entry to the map
   */
  def :+(entry: (A, B))(implicit node: SelfUniqueAddress): LWWMap[A, B] = {
    val (key, value) = entry
    put(node, key, value)
  }

  /**
   * Adds an entry to the map
   */
  def put(node: SelfUniqueAddress, key: A, value: B): LWWMap[A, B] =
    put(node.uniqueAddress, key, value, defaultClock[B])

  /**
   * Adds an entry to the map.
   *
   * You can provide your `clock` implementation instead of using timestamps based
   * on `System.currentTimeMillis()` time. The timestamp can for example be an
   * increasing version number from a database record that is used for optimistic
   * concurrency control.
   */
  def put(node: SelfUniqueAddress, key: A, value: B, clock: Clock[B]): LWWMap[A, B] =
    put(node.uniqueAddress, key, value, clock)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def put(node: UniqueAddress, key: A, value: B, clock: Clock[B]): LWWMap[A, B] = {
    val newRegister = underlying.get(key) match {
      case Some(r) => r.withValue(node, value, clock)
      case None    => LWWRegister(node, value, clock)
    }
    new LWWMap(underlying.put(node, key, newRegister))
  }

  /**
   * Removes an entry from the map.
   * Note that if there is a conflicting update on another node the entry will
   * not be removed after merge.
   */
  def remove(node: SelfUniqueAddress, key: A): LWWMap[A, B] =
    remove(node.uniqueAddress, key)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def remove(node: UniqueAddress, key: A): LWWMap[A, B] =
    new LWWMap(underlying.remove(node, key))

  override def resetDelta: LWWMap[A, B] =
    new LWWMap(underlying.resetDelta)

  override def delta: Option[D] = underlying.delta

  override def mergeDelta(thatDelta: D): LWWMap[A, B] =
    new LWWMap(underlying.mergeDelta(thatDelta))

  override def merge(that: LWWMap[A, B]): LWWMap[A, B] =
    new LWWMap(underlying.merge(that.underlying))

  override def modifiedByNodes: Set[UniqueAddress] =
    underlying.modifiedByNodes

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    underlying.needPruningFrom(removedNode)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): LWWMap[A, B] =
    new LWWMap(underlying.prune(removedNode, collapseInto))

  override def pruningCleanup(removedNode: UniqueAddress): LWWMap[A, B] =
    new LWWMap(underlying.pruningCleanup(removedNode))

  // this class cannot be a `case class` because we need different `unapply`

  override def toString: String = s"LWW$entries" //e.g. LWWMap(a -> 1, b -> 2)

  override def equals(o: Any): Boolean = o match {
    case other: LWWMap[_, _] => underlying == other.underlying
    case _                   => false
  }

  override def hashCode: Int = underlying.hashCode
}

object LWWMapKey {
  def create[A, B](id: String): Key[LWWMap[A, B]] = LWWMapKey(id)
}

@SerialVersionUID(1L)
final case class LWWMapKey[A, B](_id: String) extends Key[LWWMap[A, B]](_id) with ReplicatedDataSerialization {
  override def withId(newId: Key.KeyId): LWWMapKey[A, B] =
    LWWMapKey(newId)
}
