/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.collection.immutable

import akka.annotation.InternalApi
import akka.cluster.Cluster
import akka.cluster.UniqueAddress
import akka.cluster.ddata.ORMap.ZeroTag
import akka.util.HashCode

object ORMap {
  private val _empty: ORMap[Any, ReplicatedData] = new ORMap(ORSet.empty, Map.empty, VanillaORMapTag)
  def empty[A, B <: ReplicatedData]: ORMap[A, B] = _empty.asInstanceOf[ORMap[A, B]]
  def apply(): ORMap[Any, ReplicatedData] = _empty

  /**
   * Java API
   */
  def create[A, B <: ReplicatedData](): ORMap[A, B] = empty[A, B]

  /**
   * Extract the [[ORMap#entries]].
   */
  def unapply[A, B <: ReplicatedData](m: ORMap[A, B]): Option[Map[A, B]] = Some(m.entries)

  sealed trait DeltaOp extends ReplicatedDelta with RequiresCausalDeliveryOfDeltas with ReplicatedDataSerialization {
    type T = DeltaOp
    override def zero: DeltaReplicatedData
  }

  /**
   * INTERNAL API
   * Tags for ORMap.DeltaOp's, so that when the Replicator needs to re-create full value from delta,
   * the right map type will be used
   */
  @InternalApi private[akka] trait ZeroTag {
    def zero: DeltaReplicatedData
    def value: Int
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] case object VanillaORMapTag extends ZeroTag {
    override def zero: DeltaReplicatedData = ORMap.empty
    override final val value: Int = 0
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] sealed abstract class AtomicDeltaOp[A, B <: ReplicatedData]
      extends DeltaOp
      with ReplicatedDeltaSize {
    def underlying: ORSet.DeltaOp
    def zeroTag: ZeroTag
    override def zero: DeltaReplicatedData = zeroTag.zero
    override def merge(that: DeltaOp): DeltaOp = that match {
      case other: AtomicDeltaOp[_, _] => DeltaGroup(Vector(this, other))
      case DeltaGroup(ops)            => DeltaGroup(this +: ops)
    }
    override def deltaSize: Int = 1
  }

  // PutDeltaOp contains ORSet delta and full value
  /** INTERNAL API */
  @InternalApi private[akka] final case class PutDeltaOp[A, B <: ReplicatedData](
      underlying: ORSet.DeltaOp,
      value: (A, B),
      zeroTag: ZeroTag)
      extends AtomicDeltaOp[A, B] {
    override def merge(that: DeltaOp): DeltaOp = that match {
      case put: PutDeltaOp[_, _] if this.value._1 == put.value._1 =>
        new PutDeltaOp[A, B](this.underlying.merge(put.underlying), put.asInstanceOf[PutDeltaOp[A, B]].value, zeroTag)
      case update: UpdateDeltaOp[_, _]
          if update.values.size == 1 && update.asInstanceOf[UpdateDeltaOp[A, B]].values.contains(this.value._1) =>
        val (key, elem1) = this.value
        val newValue = elem1 match {
          case e1: DeltaReplicatedData =>
            val e2 = update.values.head._2.asInstanceOf[e1.D]
            (key, e1.mergeDelta(e2).asInstanceOf[B])
          case _ =>
            val elem2 = update.values.head._2.asInstanceOf[elem1.T]
            (key, elem1.merge(elem2).asInstanceOf[B])
        }
        new PutDeltaOp[A, B](this.underlying.merge(update.underlying), newValue, zeroTag)
      case other: AtomicDeltaOp[_, _] => DeltaGroup(Vector(this, other))
      case DeltaGroup(ops)            => DeltaGroup(this +: ops)
    }
  }

  // UpdateDeltaOp contains ORSet delta and either delta of value (in case where underlying type supports deltas) or full value
  /** INTERNAL API */
  @InternalApi private[akka] final case class UpdateDeltaOp[A, B <: ReplicatedData](
      underlying: ORSet.DeltaOp,
      values: Map[A, B],
      zeroTag: ZeroTag)
      extends AtomicDeltaOp[A, B] {
    override def merge(that: DeltaOp): DeltaOp = that match {
      case update: UpdateDeltaOp[_, _] =>
        new UpdateDeltaOp[A, B](
          this.underlying.merge(update.underlying),
          update.asInstanceOf[UpdateDeltaOp[A, B]].values.foldLeft(this.values) { (map, pair) =>
            val (key, value) = pair
            if (this.values.contains(key)) {
              val elem1 = this.values(key)
              val elem2 = value.asInstanceOf[elem1.T]
              map + (key -> elem1.merge(elem2).asInstanceOf[B])
            } else map + pair
          },
          zeroTag)
      case put: PutDeltaOp[_, _]
          if this.values.size == 1 && this.values.contains(put.asInstanceOf[PutDeltaOp[A, B]].value._1) =>
        new PutDeltaOp[A, B](this.underlying.merge(put.underlying), put.asInstanceOf[PutDeltaOp[A, B]].value, zeroTag)
      case other: AtomicDeltaOp[_, _] => DeltaGroup(Vector(this, other))
      case DeltaGroup(ops)            => DeltaGroup(this +: ops)
    }
  }

  // RemoveDeltaOp does not contain any value at all - the propagated 'value' map would be empty
  /** INTERNAL API */
  @InternalApi private[akka] final case class RemoveDeltaOp[A, B <: ReplicatedData](
      underlying: ORSet.DeltaOp,
      zeroTag: ZeroTag)
      extends AtomicDeltaOp[A, B]

  // RemoveKeyDeltaOp contains a single value - to provide the recipient with the removed key for value map
  /** INTERNAL API */
  @InternalApi private[akka] final case class RemoveKeyDeltaOp[A, B <: ReplicatedData](
      underlying: ORSet.DeltaOp,
      removedKey: A,
      zeroTag: ZeroTag)
      extends AtomicDeltaOp[A, B]

  // DeltaGroup is effectively a causally ordered list of individual deltas
  /** INTERNAL API */
  @InternalApi private[akka] final case class DeltaGroup[A, B <: ReplicatedData](ops: immutable.IndexedSeq[DeltaOp])
      extends DeltaOp
      with ReplicatedDeltaSize {
    override def merge(that: DeltaOp): DeltaOp = that match {
      case that: AtomicDeltaOp[_, _] =>
        ops.last match {
          case thisPut: PutDeltaOp[_, _] =>
            val merged = thisPut.merge(that)
            merged match {
              case op: AtomicDeltaOp[_, _] => DeltaGroup(ops.dropRight(1) :+ op)
              case DeltaGroup(thatOps)     => DeltaGroup(ops.dropRight(1) ++ thatOps)
            }
          case thisUpdate: UpdateDeltaOp[_, _] =>
            val merged = thisUpdate.merge(that)
            merged match {
              case op: AtomicDeltaOp[_, _] => DeltaGroup(ops.dropRight(1) :+ op)
              case DeltaGroup(thatOps)     => DeltaGroup(ops.dropRight(1) ++ thatOps)
            }
          case _ => DeltaGroup(ops :+ that)
        }
      case DeltaGroup(thatOps) => DeltaGroup(ops ++ thatOps)
    }

    override def zero: DeltaReplicatedData =
      ops.headOption.fold(ORMap.empty[A, B].asInstanceOf[DeltaReplicatedData])(_.zero)

    override def deltaSize: Int = ops.size
  }
}

/**
 * Implements a 'Observed Remove Map' CRDT, also called a 'OR-Map'.
 *
 * It has similar semantics as an [[ORSet]], but in case of concurrent updates
 * the values are merged, and must therefore be [[ReplicatedData]] types themselves.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@SerialVersionUID(1L)
final class ORMap[A, B <: ReplicatedData] private[akka] (
    private[akka] val keys: ORSet[A],
    private[akka] val values: Map[A, B],
    private[akka] val zeroTag: ZeroTag,
    override val delta: Option[ORMap.DeltaOp] = None)
    extends DeltaReplicatedData
    with ReplicatedDataSerialization
    with RemovedNodePruning {

  import ORMap.{ PutDeltaOp, RemoveDeltaOp, RemoveKeyDeltaOp, UpdateDeltaOp }

  type T = ORMap[A, B]
  type D = ORMap.DeltaOp

  /**
   * Scala API: All entries of the map.
   */
  def entries: Map[A, B] = values

  /**
   * Java API: All entries of the map.
   */
  def getEntries(): java.util.Map[A, B] = {
    import scala.jdk.CollectionConverters._
    entries.asJava
  }

  def get(key: A): Option[B] = values.get(key)

  /**
   * Scala API: Get the value associated with the key if there is one,
   * else return the given default.
   */
  def getOrElse(key: A, default: => B): B = values.getOrElse(key, default)

  def contains(key: A): Boolean = values.contains(key)

  def isEmpty: Boolean = values.isEmpty

  def size: Int = values.size

  /**
   * Adds an entry to the map
   * @see [[ORMap#put(node:akka\.cluster\.ddata\.SelfUniqueAddress*]]
   */
  def :+(entry: (A, B))(implicit node: SelfUniqueAddress): ORMap[A, B] = {
    val (key, value) = entry
    put(node.uniqueAddress, key, value)
  }

  /**
   * Adds an entry to the map.
   * Note that the new `value` will be merged with existing values
   * on other nodes and the outcome depends on what `ReplicatedData`
   * type that is used.
   *
   * Consider using [[ORMap#updated(node:akka\.cluster\.ddata\.SelfUniqueAddress*]]
   * instead of `put` if you want modify existing entry.
   *
   * `IllegalArgumentException` is thrown if you try to replace an existing `ORSet`
   * value, because important history can be lost when replacing the `ORSet` and
   * undesired effects of merging will occur. Use [[ORMultiMap]] or
   * [[ORMap#updated(node:akka\.cluster\.ddata\.SelfUniqueAddress*]] instead.
   */
  def put(node: SelfUniqueAddress, key: A, value: B): ORMap[A, B] = put(node.uniqueAddress, key, value)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def put(node: UniqueAddress, key: A, value: B): ORMap[A, B] =
    if (value.isInstanceOf[ORSet[_]] && values.contains(key))
      throw new IllegalArgumentException(
        "`ORMap.put` must not be used to replace an existing `ORSet` " +
        "value, because important history can be lost when replacing the `ORSet` and " +
        "undesired effects of merging will occur. Use `ORMultiMap` or `ORMap.updated` instead.")
    else {
      val newKeys = keys.resetDelta.add(node, key)
      val putDeltaOp = PutDeltaOp(newKeys.delta.get, key -> value, zeroTag)
      // put forcibly damages history, so we consciously propagate full value that will overwrite previous value
      new ORMap(newKeys, values.updated(key, value), zeroTag, Some(newDelta(putDeltaOp)))
    }

  /**
   * Scala API: Replace a value by applying the `modify` function on the existing value.
   *
   * If there is no current value for the `key` the `initial` value will be
   * passed to the `modify` function.
   */
  def updated(node: SelfUniqueAddress, key: A, initial: B)(modify: B => B): ORMap[A, B] =
    updated(node.uniqueAddress, key, initial)(modify)

  /**
   * Java API: Replace a value by applying the `modify` function on the existing value.
   *
   * If there is no current value for the `key` the `initial` value will be
   * passed to the `modify` function.
   */
  @Deprecated
  @deprecated("use update for the Java API as updated is ambiguous with the Scala API", "2.5.20")
  def updated(node: Cluster, key: A, initial: B, modify: java.util.function.Function[B, B]): ORMap[A, B] =
    updated(node.selfUniqueAddress, key, initial)(value => modify.apply(value))

  /**
   * Java API: Replace a value by applying the `modify` function on the existing value.
   *
   * If there is no current value for the `key` the `initial` value will be
   * passed to the `modify` function.
   */
  def update(node: SelfUniqueAddress, key: A, initial: B, modify: java.util.function.Function[B, B]): ORMap[A, B] =
    updated(node.uniqueAddress, key, initial)(value => modify.apply(value))

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def updated(node: UniqueAddress, key: A, initial: B, valueDeltas: Boolean = false)(
      modify: B => B): ORMap[A, B] = {
    val (oldValue, hasOldValue) = values.get(key) match {
      case Some(old) => (old, true)
      case _         => (initial, false)
    }
    // Optimization: for some types - like GSet, GCounter, PNCounter and ORSet  - that are delta based
    // we can emit (and later merge) their deltas instead of full updates.
    // However to avoid necessity of tombstones, the derived map type needs to support this
    // with clearing the value (e.g. removing all elements if value is a set)
    // before removing the key - like e.g. ORMultiMap.emptyWithValueDeltas does
    val newKeys = keys.resetDelta.add(node, key)
    oldValue match {
      case _: DeltaReplicatedData if valueDeltas =>
        val newValue = modify(oldValue.asInstanceOf[DeltaReplicatedData].resetDelta.asInstanceOf[B])
        val newValueDelta = newValue.asInstanceOf[DeltaReplicatedData].delta
        val deltaOp = newValueDelta match {
          case Some(d) if hasOldValue => UpdateDeltaOp(newKeys.delta.get, Map(key -> d), zeroTag)
          case _                      => PutDeltaOp(newKeys.delta.get, key -> newValue, zeroTag)
        }
        new ORMap(newKeys, values.updated(key, newValue), zeroTag, Some(newDelta(deltaOp)))
      case _ =>
        val newValue = modify(oldValue)
        val deltaOp = PutDeltaOp(newKeys.delta.get, key -> newValue, zeroTag)
        new ORMap(newKeys, values.updated(key, newValue), zeroTag, Some(newDelta(deltaOp)))
    }
  }

  /**
   * Scala API
   * Removes an entry from the map.
   * Note that if there is a conflicting update on another node the entry will
   * not be removed after merge.
   */
  def remove(key: A)(implicit node: SelfUniqueAddress): ORMap[A, B] = remove(node.uniqueAddress, key)

  /**
   * Java API
   * Removes an entry from the map.
   * Note that if there is a conflicting update on another node the entry will
   * not be removed after merge.
   */
  def remove(node: SelfUniqueAddress, key: A): ORMap[A, B] = remove(node.uniqueAddress, key)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def remove(node: UniqueAddress, key: A): ORMap[A, B] = {
    // for removals the delta values map emitted will be empty
    val newKeys = keys.resetDelta.remove(node, key)
    val removeDeltaOp = RemoveDeltaOp(newKeys.delta.get, zeroTag)
    new ORMap(newKeys, values - key, zeroTag, Some(newDelta(removeDeltaOp)))
  }

  /**
   * INTERNAL API
   * This function is only to be used by derived maps that avoid remove anomalies
   * by keeping the vvector (in form of key -> value pair) for deleted keys
   */
  @InternalApi private[akka] def removeKey(node: UniqueAddress, key: A): ORMap[A, B] = {
    val newKeys = keys.resetDelta.remove(node, key)
    val removeKeyDeltaOp = RemoveKeyDeltaOp(newKeys.delta.get, key, zeroTag)
    new ORMap(newKeys, values, zeroTag, Some(newDelta(removeKeyDeltaOp)))
  }

  private def dryMerge(that: ORMap[A, B], mergedKeys: ORSet[A], valueKeysIterator: Iterator[A]): ORMap[A, B] = {
    var mergedValues = Map.empty[A, B]
    valueKeysIterator.foreach { key =>
      (this.values.get(key), that.values.get(key)) match {
        case (Some(thisValue), Some(thatValue)) =>
          if (thisValue.getClass != thatValue.getClass) {
            val errMsg = s"Wrong type for merging [$key] in [${getClass.getName}], existing type " +
              s"[${thisValue.getClass.getName}], got [${thatValue.getClass.getName}]"
            throw new IllegalArgumentException(errMsg)
          }
          // TODO can we get rid of these (safe) casts?
          val mergedValue = thisValue.merge(thatValue.asInstanceOf[thisValue.T]).asInstanceOf[B]
          mergedValues = mergedValues.updated(key, mergedValue)
        case (Some(thisValue), None) =>
          if (mergedKeys.contains(key))
            mergedValues = mergedValues.updated(key, thisValue)
        // else thisValue is a tombstone, but we don't want to carry it forward, as the other side does not have the element at all
        case (None, Some(thatValue)) =>
          if (mergedKeys.contains(key))
            mergedValues = mergedValues.updated(key, thatValue)
        // else thatValue is a tombstone, but we don't want to carry it forward, as the other side does not have the element at all
        case (None, None) => throw new IllegalStateException(s"missing value for $key")
      }
    }
    new ORMap(mergedKeys, mergedValues, zeroTag = zeroTag)
  }

  override def merge(that: ORMap[A, B]): ORMap[A, B] = {
    val mergedKeys = keys.merge(that.keys)
    dryMerge(that, mergedKeys, mergedKeys.elementsMap.keysIterator)
  }

  /**
   * INTERNAL API
   * This function is only to be used by derived maps that avoid remove anomalies
   * by keeping the vvector (in form of key -> value pair) for deleted keys
   */
  @InternalApi private[akka] def mergeRetainingDeletedValues(that: ORMap[A, B]): ORMap[A, B] = {
    val mergedKeys = keys.merge(that.keys)
    dryMerge(that, mergedKeys, (this.values.keySet ++ that.values.keySet).iterator)
  }

  override def resetDelta: ORMap[A, B] =
    if (delta.isEmpty) this
    else new ORMap[A, B](keys.resetDelta, values, zeroTag = zeroTag)

  private def dryMergeDelta(thatDelta: ORMap.DeltaOp, withValueDeltas: Boolean = false): ORMap[A, B] = {
    def mergeValue(lvalue: ReplicatedData, rvalue: ReplicatedData): B =
      (lvalue, rvalue) match {
        case (v: DeltaReplicatedData, replicatedDelta: ReplicatedDelta) =>
          v.mergeDelta(replicatedDelta.asInstanceOf[v.D]).asInstanceOf[B]
        case _ =>
          lvalue.merge(rvalue.asInstanceOf[lvalue.T]).asInstanceOf[B]
      }

    var mergedKeys: ORSet[A] = this.keys
    var (mergedValues, tombstonedVals): (Map[A, B], Map[A, B]) = this.values.partition {
      case (k, _) => this.keys.contains(k)
    }

    val processDelta: PartialFunction[ORMap.DeltaOp, Unit] = {
      case putOp: PutDeltaOp[_, _] =>
        val keyDelta = putOp.underlying
        mergedKeys = mergedKeys.mergeDelta(keyDelta)
        mergedValues = mergedValues + putOp
            .asInstanceOf[PutDeltaOp[A, B]]
            .value // put is destructive and propagates only full values of B!
      case removeOp: RemoveDeltaOp[_, _] =>
        val removedKey = removeOp.underlying match {
          // if op is RemoveDeltaOp then it must have exactly one element in the elements
          case op: ORSet.RemoveDeltaOp[_] => op.underlying.elements.head.asInstanceOf[A]
          case _                          => throw new IllegalArgumentException("ORMap.RemoveDeltaOp must contain ORSet.RemoveDeltaOp inside")
        }
        mergedValues = mergedValues - removedKey
        mergedKeys = mergedKeys.mergeDelta(removeOp.underlying)
      // please note that if RemoveDeltaOp is not preceded by update clearing the value
      // anomalies may result
      case removeKeyOp: RemoveKeyDeltaOp[_, _] =>
        // removeKeyOp tombstones values for later use
        if (mergedValues.contains(removeKeyOp.asInstanceOf[RemoveKeyDeltaOp[A, B]].removedKey)) {
          tombstonedVals = tombstonedVals + (removeKeyOp
              .asInstanceOf[RemoveKeyDeltaOp[A, B]]
              .removedKey -> mergedValues(removeKeyOp.asInstanceOf[RemoveKeyDeltaOp[A, B]].removedKey))
        }
        mergedValues = mergedValues - removeKeyOp.asInstanceOf[RemoveKeyDeltaOp[A, B]].removedKey
        mergedKeys = mergedKeys.mergeDelta(removeKeyOp.underlying)
      case updateOp: UpdateDeltaOp[_, _] =>
        mergedKeys = mergedKeys.mergeDelta(updateOp.underlying)
        updateOp.asInstanceOf[UpdateDeltaOp[A, B]].values.foreach {
          case (key, value) =>
            if (mergedKeys.contains(key)) {
              if (mergedValues.contains(key)) {
                mergedValues = mergedValues + (key -> mergeValue(mergedValues(key), value))
              } else if (tombstonedVals.contains(key)) {
                mergedValues = mergedValues + (key -> mergeValue(tombstonedVals(key), value))
              } else {
                value match {
                  case _: ReplicatedDelta =>
                    mergedValues = mergedValues + (key -> mergeValue(value.asInstanceOf[ReplicatedDelta].zero, value))
                  case _ =>
                    mergedValues = mergedValues + (key -> value.asInstanceOf[B])
                }
              }
            }
        }
    }

    val processNestedDelta: PartialFunction[ORMap.DeltaOp, Unit] = {
      case ORMap.DeltaGroup(ops) =>
        ops.foreach {
          processDelta.orElse {
            case ORMap.DeltaGroup(_) =>
              throw new IllegalStateException("Cannot nest DeltaGroups")
          }
        }
    }

    processDelta.orElse(processNestedDelta)(thatDelta)

    if (withValueDeltas)
      new ORMap[A, B](mergedKeys, tombstonedVals ++ mergedValues, zeroTag = zeroTag)
    else
      new ORMap[A, B](mergedKeys, mergedValues, zeroTag = zeroTag)
  }

  override def mergeDelta(thatDelta: ORMap.DeltaOp): ORMap[A, B] = {
    val thisWithDeltas = dryMergeDelta(thatDelta)
    this.merge(thisWithDeltas)
  }

  /**
   * INTERNAL API
   * This function is only to be used by derived maps that avoid remove anomalies
   * by keeping the vvector (in form of key -> value pair) for deleted keys
   */
  @InternalApi private[akka] def mergeDeltaRetainingDeletedValues(thatDelta: ORMap.DeltaOp): ORMap[A, B] = {
    val thisWithDeltas = dryMergeDelta(thatDelta, withValueDeltas = true)
    this.mergeRetainingDeletedValues(thisWithDeltas)
  }

  private def newDelta(deltaOp: ORMap.DeltaOp) = delta match {
    case Some(d) =>
      d.merge(deltaOp)
    case None =>
      deltaOp
  }

  override def modifiedByNodes: Set[UniqueAddress] = {
    keys.modifiedByNodes.union(values.foldLeft(Set.empty[UniqueAddress]) {
      case (acc, (_, data: RemovedNodePruning)) => acc.union(data.modifiedByNodes)
      case (acc, _)                             => acc
    })
  }

  override def needPruningFrom(removedNode: UniqueAddress): Boolean = {
    keys.needPruningFrom(removedNode) || values.exists {
      case (_, data: RemovedNodePruning) => data.needPruningFrom(removedNode)
      case _                             => false
    }
  }

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): ORMap[A, B] = {
    val prunedKeys = keys.prune(removedNode, collapseInto)
    val prunedValues = values.foldLeft(values) {
      case (acc, (key, data: RemovedNodePruning)) if data.needPruningFrom(removedNode) =>
        acc.updated(key, data.prune(removedNode, collapseInto).asInstanceOf[B])
      case (acc, _) => acc
    }
    new ORMap(prunedKeys, prunedValues, zeroTag = zeroTag)
  }

  override def pruningCleanup(removedNode: UniqueAddress): ORMap[A, B] = {
    val pruningCleanupedKeys = keys.pruningCleanup(removedNode)
    val pruningCleanupedValues = values.foldLeft(values) {
      case (acc, (key, data: RemovedNodePruning)) if data.needPruningFrom(removedNode) =>
        acc.updated(key, data.pruningCleanup(removedNode).asInstanceOf[B])
      case (acc, _) => acc
    }
    new ORMap(pruningCleanupedKeys, pruningCleanupedValues, zeroTag = zeroTag)
  }

  // this class cannot be a `case class` because we need different `unapply`

  override def toString: String = s"OR$entries"

  override def equals(o: Any): Boolean = o match {
    case other: ORMap[_, _] => keys == other.keys && values == other.values
    case _                  => false
  }

  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, keys)
    result = HashCode.hash(result, values)
    result
  }

}

object ORMapKey {
  def create[A, B <: ReplicatedData](id: String): Key[ORMap[A, B]] = ORMapKey(id)
}

@SerialVersionUID(1L)
final case class ORMapKey[A, B <: ReplicatedData](_id: String)
    extends Key[ORMap[A, B]](_id)
    with ReplicatedDataSerialization {
  override def withId(newId: Key.KeyId): ORMapKey[A, B] =
    ORMapKey(newId)
}
