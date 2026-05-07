/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.annotation.InternalApi
import akka.cluster.UniqueAddress
import akka.cluster.ddata.ORMap._

object ORMultiMap {

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case object ORMultiMapTag extends ZeroTag {
    override def zero: DeltaReplicatedData = ORMultiMap.empty
    override final val value: Int = 2
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case object ORMultiMapWithValueDeltasTag extends ZeroTag {
    override def zero: DeltaReplicatedData = ORMultiMap.emptyWithValueDeltas
    override final val value: Int = 3
  }

  val _empty: ORMultiMap[Any, Any] =
    new ORMultiMap(new ORMap(ORSet.empty, Map.empty, zeroTag = ORMultiMapTag), withValueDeltas = false)
  val _emptyWithValueDeltas: ORMultiMap[Any, Any] =
    new ORMultiMap(new ORMap(ORSet.empty, Map.empty, zeroTag = ORMultiMapWithValueDeltasTag), withValueDeltas = true)

  /**
   * Provides an empty multimap.
   */
  def empty[A, B]: ORMultiMap[A, B] = _empty.asInstanceOf[ORMultiMap[A, B]]
  def emptyWithValueDeltas[A, B]: ORMultiMap[A, B] = _emptyWithValueDeltas.asInstanceOf[ORMultiMap[A, B]]
  def apply(): ORMultiMap[Any, Any] = _empty

  /**
   * Java API
   */
  def create[A, B](): ORMultiMap[A, B] = empty[A, B]

  /**
   * Extract the [[ORMultiMap#entries]].
   */
  def unapply[A, B](m: ORMultiMap[A, B]): Option[Map[A, Set[B]]] = Some(m.entries)

  /**
   * Extract the [[ORMultiMap#entries]] of an `ORMultiMap`.
   */
  def unapply[A, B <: ReplicatedData](value: Any): Option[Map[A, Set[B]]] = value match {
    case m: ORMultiMap[A, B] @unchecked => Some(m.entries)
    case _                              => None
  }
}

/**
 * An immutable multi-map implementation. This class wraps an
 * [[ORMap]] with an [[ORSet]] for the map's value.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 *
 * Note that on concurrent adds and removals for the same key (on the same set), removals can be lost.
 */
@SerialVersionUID(1L)
final class ORMultiMap[A, B] private[akka] (
    private[akka] val underlying: ORMap[A, ORSet[B]],
    private[akka] val withValueDeltas: Boolean)
    extends DeltaReplicatedData
    with ReplicatedDataSerialization
    with RemovedNodePruning {

  override type T = ORMultiMap[A, B]
  override type D = ORMap.DeltaOp

  override def merge(that: T): T =
    if (withValueDeltas == that.withValueDeltas) {
      if (withValueDeltas) {
        val newUnderlying = underlying.mergeRetainingDeletedValues(that.underlying)
        // Garbage collect the tombstones we no longer need, i.e. those that have Set() as a value.
        val newValues = newUnderlying.values.filterNot {
          case (key, value) => !newUnderlying.keys.contains(key) && value.isEmpty
        }
        new ORMultiMap[A, B](
          new ORMap(newUnderlying.keys, newValues, newUnderlying.zeroTag, newUnderlying.delta),
          withValueDeltas)
      } else
        new ORMultiMap(underlying.merge(that.underlying), withValueDeltas)
    } else throw new IllegalArgumentException("Trying to merge two ORMultiMaps of different map sub-type")

  /**
   * Scala API: All entries of a multimap where keys are strings and values are sets.
   */
  def entries: Map[A, Set[B]] =
    if (withValueDeltas)
      underlying.entries.collect { case (k, v) if underlying.keys.elements.contains(k) => k -> v.elements } else
      underlying.entries.map { case (k, v)                                             => k -> v.elements }

  /**
   * Java API: All entries of a multimap where keys are strings and values are sets.
   */
  def getEntries(): java.util.Map[A, java.util.Set[B]] = {
    import scala.jdk.CollectionConverters._
    val result = new java.util.HashMap[A, java.util.Set[B]]
    if (withValueDeltas)
      underlying.entries.foreach {
        case (k, v) => if (underlying.keys.elements.contains(k)) result.put(k, v.elements.asJava)
      } else
      underlying.entries.foreach { case (k, v) => result.put(k, v.elements.asJava) }
    result
  }

  /**
   * Get the set associated with the key if there is one.
   */
  def get(key: A): Option[Set[B]] =
    if (withValueDeltas && !underlying.keys.elements.contains(key))
      None
    else
      underlying.get(key).map(_.elements)

  /**
   * Scala API: Get the set associated with the key if there is one,
   * else return the given default.
   */
  def getOrElse(key: A, default: => Set[B]): Set[B] =
    get(key).getOrElse(default)

  def contains(key: A): Boolean = underlying.keys.elements.contains(key)

  def isEmpty: Boolean = underlying.keys.elements.isEmpty

  def size: Int = underlying.keys.elements.size

  /**
   * Convenience for put. Requires an implicit SelfUniqueAddress.
   * @see [[ORMultiMap#put(node:akka\.cluster\.ddata\.SelfUniqueAddress,key:A,value:Set*]]
   */
  def :+(entry: (A, Set[B]))(implicit node: SelfUniqueAddress): ORMultiMap[A, B] = {
    val (key, value) = entry
    put(node.uniqueAddress, key, value)
  }

  /**
   * Scala API: Associate an entire set with the key while retaining the history of the previous
   * replicated data set.
   */
  def put(node: SelfUniqueAddress, key: A, value: Set[B]): ORMultiMap[A, B] =
    put(node.uniqueAddress, key, value)

  /**
   * Java API: Associate an entire set with the key while retaining the history of the previous
   * replicated data set.
   */
  def put(node: SelfUniqueAddress, key: A, value: java.util.Set[B]): ORMultiMap[A, B] = {
    import scala.jdk.CollectionConverters._
    put(node.uniqueAddress, key, value.asScala.toSet)
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def put(node: UniqueAddress, key: A, value: Set[B]): ORMultiMap[A, B] = {
    val newUnderlying = underlying.updated(node, key, ORSet.empty[B], valueDeltas = withValueDeltas) { existing =>
      value.foldLeft(existing.clear()) { (s, element) =>
        s.add(node, element)
      }
    }
    new ORMultiMap(newUnderlying, withValueDeltas)
  }

  /**
   * Scala API
   * Remove an entire set associated with the key.
   */
  def remove(key: A)(implicit node: SelfUniqueAddress): ORMultiMap[A, B] = remove(node.uniqueAddress, key)

  /**
   * Java API
   * Remove an entire set associated with the key.
   */
  def remove(node: SelfUniqueAddress, key: A): ORMultiMap[A, B] = remove(node.uniqueAddress, key)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def remove(node: UniqueAddress, key: A): ORMultiMap[A, B] = {
    if (withValueDeltas) {
      val u = underlying.updated(node, key, ORSet.empty[B], valueDeltas = true) { existing =>
        existing.clear()
      }
      new ORMultiMap(u.removeKey(node, key), withValueDeltas)
    } else {
      new ORMultiMap(underlying.remove(node, key), withValueDeltas)
    }
  }

  /**
   * Add an element to a set associated with a key. If there is no existing set then one will be initialised.
   * TODO add implicit after deprecated is EOL.
   */
  def addBinding(node: SelfUniqueAddress, key: A, element: B): ORMultiMap[A, B] =
    addBinding(node.uniqueAddress, key, element)

  def addBindingBy(key: A, element: B)(implicit node: SelfUniqueAddress): ORMultiMap[A, B] =
    addBinding(node, key, element)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def addBinding(node: UniqueAddress, key: A, element: B): ORMultiMap[A, B] = {
    val newUnderlying =
      underlying.updated(node, key, ORSet.empty[B], valueDeltas = withValueDeltas)(_.add(node, element))
    new ORMultiMap(newUnderlying, withValueDeltas)
  }

  /**
   * Remove an element of a set associated with a key. If there are no more elements in the set then the
   * entire set will be removed.
   * TODO add implicit after deprecated is EOL.
   */
  def removeBinding(node: SelfUniqueAddress, key: A, element: B): ORMultiMap[A, B] =
    removeBinding(node.uniqueAddress, key, element)

  def removeBindingBy(key: A, element: B)(implicit node: SelfUniqueAddress): ORMultiMap[A, B] =
    removeBinding(node, key, element)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def removeBinding(node: UniqueAddress, key: A, element: B): ORMultiMap[A, B] = {
    val newUnderlying = {
      val u = underlying.updated(node, key, ORSet.empty[B], valueDeltas = withValueDeltas)(_.remove(node, element))
      u.get(key) match {
        case Some(s) if s.isEmpty =>
          if (withValueDeltas)
            u.removeKey(node, key)
          else
            u.remove(node, key)
        case _ => u
      }
    }
    new ORMultiMap(newUnderlying, withValueDeltas)
  }

  /**
   * Replace an element of a set associated with a key with a new one if it is different. This is useful when an element is removed
   * and another one is added within the same Update. The order of addition and removal is important in order
   * to retain history for replicated data.
   */
  def replaceBinding(node: SelfUniqueAddress, key: A, oldElement: B, newElement: B): ORMultiMap[A, B] =
    replaceBinding(node.uniqueAddress, key, oldElement, newElement)

  def replaceBindingBy(key: A, oldElement: B, newElement: B)(implicit node: SelfUniqueAddress): ORMultiMap[A, B] =
    replaceBinding(node, key, oldElement, newElement)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def replaceBinding(
      node: UniqueAddress,
      key: A,
      oldElement: B,
      newElement: B): ORMultiMap[A, B] =
    if (newElement != oldElement)
      addBinding(node, key, newElement).removeBinding(node, key, oldElement)
    else
      this

  override def resetDelta: ORMultiMap[A, B] =
    new ORMultiMap(underlying.resetDelta, withValueDeltas)

  override def delta: Option[D] = underlying.delta

  override def mergeDelta(thatDelta: D): ORMultiMap[A, B] =
    if (withValueDeltas) {
      val newUnderlying = underlying.mergeDeltaRetainingDeletedValues(thatDelta)
      // Garbage collect the tombstones we no longer need, i.e. those that have Set() as a value.
      val newValues = newUnderlying.values.filterNot {
        case (key, value) => !newUnderlying.keys.contains(key) && value.isEmpty
      }
      new ORMultiMap[A, B](
        new ORMap(newUnderlying.keys, newValues, newUnderlying.zeroTag, newUnderlying.delta),
        withValueDeltas)
    } else
      new ORMultiMap(underlying.mergeDelta(thatDelta), withValueDeltas)

  override def modifiedByNodes: Set[UniqueAddress] =
    underlying.modifiedByNodes

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    underlying.needPruningFrom(removedNode)

  override def pruningCleanup(removedNode: UniqueAddress): T =
    new ORMultiMap(underlying.pruningCleanup(removedNode), withValueDeltas)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): T =
    new ORMultiMap(underlying.prune(removedNode, collapseInto), withValueDeltas)

  // this class cannot be a `case class` because we need different `unapply`

  override def toString: String = s"ORMulti$entries"

  override def equals(o: Any): Boolean = o match {
    case other: ORMultiMap[_, _] => underlying == other.underlying
    case _                       => false
  }

  override def hashCode: Int = underlying.hashCode
}

object ORMultiMapKey {
  def create[A, B](id: String): Key[ORMultiMap[A, B]] = ORMultiMapKey(id)
}

@SerialVersionUID(1L)
final case class ORMultiMapKey[A, B](_id: String) extends Key[ORMultiMap[A, B]](_id) with ReplicatedDataSerialization {
  override def withId(newId: Key.KeyId): ORMultiMapKey[A, B] =
    ORMultiMapKey(newId)
}
