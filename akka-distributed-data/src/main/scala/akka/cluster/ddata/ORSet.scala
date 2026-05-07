/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.immutable

import akka.annotation.InternalApi
import akka.cluster.UniqueAddress
import akka.util.HashCode

object ORSet {
  private val _empty: ORSet[Any] = new ORSet(Map.empty, VersionVector.empty)
  def empty[A]: ORSet[A] = _empty.asInstanceOf[ORSet[A]]
  def apply(): ORSet[Any] = _empty

  /**
   * Java API
   */
  def create[A](): ORSet[A] = empty[A]

  /**
   * Extract the [[ORSet#elements]].
   */
  def unapply[A](s: ORSet[A]): Option[Set[A]] = Some(s.elements)

  /**
   * Extract the [[ORSet#elements]] of an `ORSet`.
   */
  def unapply(a: ReplicatedData): Option[Set[Any]] = a match {
    case s: ORSet[Any] @unchecked => Some(s.elements)
    case _                        => None
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] type Dot = VersionVector

  sealed trait DeltaOp extends ReplicatedDelta with RequiresCausalDeliveryOfDeltas with ReplicatedDataSerialization {
    type T = DeltaOp
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] sealed abstract class AtomicDeltaOp[A] extends DeltaOp with ReplicatedDeltaSize {
    def underlying: ORSet[A]
    override def zero: ORSet[A] = ORSet.empty
    override def deltaSize: Int = 1
  }

  /** INTERNAL API */
  @InternalApi private[akka] final case class AddDeltaOp[A](underlying: ORSet[A]) extends AtomicDeltaOp[A] {

    override def merge(that: DeltaOp): DeltaOp = that match {
      case AddDeltaOp(u) =>
        // Note that we only merge deltas originating from the same node
        AddDeltaOp(
          new ORSet(concatElementsMap(u.elementsMap.asInstanceOf[Map[A, Dot]]), underlying.vvector.merge(u.vvector)))
      case _: AtomicDeltaOp[_] => DeltaGroup(Vector(this, that))
      case DeltaGroup(ops)     => DeltaGroup(this +: ops)
    }

    private def concatElementsMap(thatMap: Map[A, Dot]): Map[A, Dot] = {
      if (thatMap.size == 1) {
        val head = thatMap.head
        underlying.elementsMap.updated(head._1, head._2)
      } else
        underlying.elementsMap ++ thatMap
    }
  }

  /** INTERNAL API */
  @InternalApi private[akka] final case class RemoveDeltaOp[A](underlying: ORSet[A]) extends AtomicDeltaOp[A] {
    if (underlying.size != 1)
      throw new IllegalArgumentException(s"RemoveDeltaOp should contain one removed element, but was $underlying")

    override def merge(that: DeltaOp): DeltaOp = that match {
      case _: AtomicDeltaOp[_] => DeltaGroup(Vector(this, that)) // keep it simple for removals
      case DeltaGroup(ops)     => DeltaGroup(this +: ops)
    }
  }

  /** INTERNAL API: Used for `clear` but could be used for other cases also */
  @InternalApi private[akka] final case class FullStateDeltaOp[A](underlying: ORSet[A]) extends AtomicDeltaOp[A] {
    override def merge(that: DeltaOp): DeltaOp = that match {
      case _: AtomicDeltaOp[_] => DeltaGroup(Vector(this, that))
      case DeltaGroup(ops)     => DeltaGroup(this +: ops)
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] final case class DeltaGroup[A](ops: immutable.IndexedSeq[DeltaOp])
      extends DeltaOp
      with ReplicatedDeltaSize {
    override def merge(that: DeltaOp): DeltaOp = that match {
      case thatAdd: AddDeltaOp[_] =>
        // merge AddDeltaOp into last AddDeltaOp in the group, if possible
        ops.last match {
          case thisAdd: AddDeltaOp[_] => DeltaGroup(ops.dropRight(1) :+ thisAdd.merge(thatAdd))
          case _                      => DeltaGroup(ops :+ thatAdd)
        }
      case DeltaGroup(thatOps) => DeltaGroup(ops ++ thatOps)
      case _                   => DeltaGroup(ops :+ that)
    }

    override def zero: ORSet[A] = ORSet.empty

    override def deltaSize: Int = ops.size
  }

  /**
   * INTERNAL API
   * Subtract the `vvector` from the `dot`.
   * What this means is that any (node, version) pair in
   * `dot` that is &lt;= an entry in `vvector` is removed from `dot`.
   * Example [{a, 3}, {b, 2}, {d, 14}, {g, 22}] -
   *         [{a, 4}, {b, 1}, {c, 1}, {d, 14}, {e, 5}, {f, 2}] =
   *         [{b, 2}, {g, 22}]
   */
  @InternalApi private[akka] def subtractDots(dot: Dot, vvector: VersionVector): Dot = {

    @tailrec def dropDots(
        remaining: List[(UniqueAddress, Long)],
        acc: List[(UniqueAddress, Long)]): List[(UniqueAddress, Long)] =
      remaining match {
        case Nil => acc
        case (d @ (node, v1)) :: rest =>
          val v2 = vvector.versionAt(node)
          if (v2 >= v1)
            // dot is dominated by version vector, drop it
            dropDots(rest, acc)
          else
            dropDots(rest, d :: acc)
      }

    if (dot.isEmpty)
      VersionVector.empty
    else {
      dot match {
        case OneVersionVector(node, v1) =>
          // if dot is dominated by version vector, drop it
          if (vvector.versionAt(node) >= v1) VersionVector.empty
          else dot

        case ManyVersionVector(vs) =>
          val remaining = vs.toList
          val newDots = dropDots(remaining, Nil)
          VersionVector(newDots)
      }
    }
  }

  /**
   * INTERNAL API
   * @see [[ORSet#merge]]
   */
  @InternalApi private[akka] def mergeCommonKeys[A](
      commonKeys: Set[A],
      lhs: ORSet[A],
      rhs: ORSet[A]): Map[A, ORSet.Dot] =
    mergeCommonKeys(commonKeys.iterator, lhs, rhs)

  private def mergeCommonKeys[A](commonKeys: Iterator[A], lhs: ORSet[A], rhs: ORSet[A]): Map[A, ORSet.Dot] = {
    commonKeys.foldLeft(Map.empty[A, ORSet.Dot]) {
      case (acc, k) =>
        val lhsDots = lhs.elementsMap(k)
        val rhsDots = rhs.elementsMap(k)
        (lhsDots, rhsDots) match {
          case (OneVersionVector(n1, v1), OneVersionVector(n2, v2)) =>
            if (n1 == n2 && v1 == v2)
              // one single common dot
              acc.updated(k, lhsDots)
            else {
              // no common, lhsUniqueDots == lhsDots, rhsUniqueDots == rhsDots
              val lhsKeep = ORSet.subtractDots(lhsDots, rhs.vvector)
              val rhsKeep = ORSet.subtractDots(rhsDots, lhs.vvector)
              val merged = lhsKeep.merge(rhsKeep)
              // Perfectly possible that an item in both sets should be dropped
              if (merged.isEmpty) acc
              else acc.updated(k, merged)
            }
          case (ManyVersionVector(lhsVs), ManyVersionVector(rhsVs)) =>
            val commonDots = lhsVs.filter {
              case (thisDotNode, v) => rhsVs.get(thisDotNode).contains(v)
            }
            val commonDotsKeys = commonDots.keys
            val lhsUniqueDots = lhsVs -- commonDotsKeys
            val rhsUniqueDots = rhsVs -- commonDotsKeys
            val lhsKeep = ORSet.subtractDots(VersionVector(lhsUniqueDots), rhs.vvector)
            val rhsKeep = ORSet.subtractDots(VersionVector(rhsUniqueDots), lhs.vvector)
            val merged = lhsKeep.merge(rhsKeep).merge(VersionVector(commonDots))
            // Perfectly possible that an item in both sets should be dropped
            if (merged.isEmpty) acc
            else acc.updated(k, merged)
          case (ManyVersionVector(lhsVs), OneVersionVector(n2, v2)) =>
            val commonDots = lhsVs.filter {
              case (n1, v1) => v1 == v2 && n1 == n2
            }
            val commonDotsKeys = commonDots.keys
            val lhsUniqueDots = lhsVs -- commonDotsKeys
            val rhsUnique = if (commonDotsKeys.isEmpty) rhsDots else VersionVector.empty
            val lhsKeep = ORSet.subtractDots(VersionVector(lhsUniqueDots), rhs.vvector)
            val rhsKeep = ORSet.subtractDots(rhsUnique, lhs.vvector)
            val merged = lhsKeep.merge(rhsKeep).merge(VersionVector(commonDots))
            // Perfectly possible that an item in both sets should be dropped
            if (merged.isEmpty) acc
            else acc.updated(k, merged)
          case (OneVersionVector(n1, v1), ManyVersionVector(rhsVs)) =>
            val commonDots = rhsVs.filter {
              case (n2, v2) => v1 == v2 && n1 == n2
            }
            val commonDotsKeys = commonDots.keys
            val lhsUnique = if (commonDotsKeys.isEmpty) lhsDots else VersionVector.empty
            val rhsUniqueDots = rhsVs -- commonDotsKeys
            val lhsKeep = ORSet.subtractDots(lhsUnique, rhs.vvector)
            val rhsKeep = ORSet.subtractDots(VersionVector(rhsUniqueDots), lhs.vvector)
            val merged = lhsKeep.merge(rhsKeep).merge(VersionVector(commonDots))
            // Perfectly possible that an item in both sets should be dropped
            if (merged.isEmpty) acc
            else acc.updated(k, merged)
        }
    }
  }

  /**
   * INTERNAL API
   * @see [[ORSet#merge]]
   */
  @InternalApi private[akka] def mergeDisjointKeys[A](
      keys: Set[A],
      elementsMap: Map[A, ORSet.Dot],
      vvector: VersionVector,
      accumulator: Map[A, ORSet.Dot]): Map[A, ORSet.Dot] =
    mergeDisjointKeys(keys.iterator, elementsMap, vvector, accumulator)

  private def mergeDisjointKeys[A](
      keys: Iterator[A],
      elementsMap: Map[A, ORSet.Dot],
      vvector: VersionVector,
      accumulator: Map[A, ORSet.Dot]): Map[A, ORSet.Dot] = {
    keys.foldLeft(accumulator) {
      case (acc, k) =>
        val dots = elementsMap(k)
        if (vvector > dots || vvector == dots)
          acc
        else {
          // Optimise the set of stored dots to include only those unseen
          val newDots = subtractDots(dots, vvector)
          acc.updated(k, newDots)
        }
    }
  }
}

/**
 * Implements a 'Observed Remove Set' CRDT, also called a 'OR-Set'.
 * Elements can be added and removed any number of times. Concurrent add wins
 * over remove.
 *
 * It is not implemented as in the paper
 * <a href="https://hal.inria.fr/file/index/docid/555588/filename/techreport.pdf">A comprehensive study of Convergent and Commutative Replicated Data Types</a>.
 * This is more space efficient and doesn't accumulate garbage for removed elements.
 * It is described in the paper
 * <a href="https://hal.inria.fr/file/index/docid/738680/filename/RR-8083.pdf">An optimized conflict-free replicated set</a>
 * The implementation is inspired by the Riak DT <a href="https://github.com/basho/riak_dt/blob/develop/src/riak_dt_orswot.erl">
 * riak_dt_orswot</a>.
 *
 * The ORSet has a version vector that is incremented when an element is added to
 * the set. The `node -&gt; count` pair for that increment is stored against the
 * element as its "birth dot". Every time the element is re-added to the set,
 * its "birth dot" is updated to that of the `node -&gt; count` version vector entry
 * resulting from the add. When an element is removed, we simply drop it, no tombstones.
 *
 * When an element exists in replica A and not replica B, is it because A added
 * it and B has not yet seen that, or that B removed it and A has not yet seen that?
 * In this implementation we compare the `dot` of the present element to the version vector
 * in the Set it is absent from. If the element dot is not "seen" by the Set version vector,
 * that means the other set has yet to see this add, and the item is in the merged
 * Set. If the Set version vector dominates the dot, that means the other Set has removed this
 * element already, and the item is not in the merged Set.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@SerialVersionUID(1L)
final class ORSet[A] private[akka] (
    private[akka] val elementsMap: Map[A, ORSet.Dot],
    private[akka] val vvector: VersionVector,
    override val delta: Option[ORSet.DeltaOp] = None)
    extends DeltaReplicatedData
    with ReplicatedDataSerialization
    with RemovedNodePruning
    with FastMerge {

  type T = ORSet[A]
  type D = ORSet.DeltaOp

  /**
   * Scala API
   */
  def elements: Set[A] = elementsMap.keySet

  /**
   * Java API
   */
  def getElements(): java.util.Set[A] = {
    import scala.jdk.CollectionConverters._
    elements.asJava
  }

  def contains(a: A): Boolean = elementsMap.contains(a)

  def isEmpty: Boolean = elementsMap.isEmpty

  def size: Int = elementsMap.size

  /** Adds an element to the set. */
  def :+(element: A)(implicit node: SelfUniqueAddress): ORSet[A] = add(node, element)

  /** Adds an element to the set. */
  def add(node: SelfUniqueAddress, element: A): ORSet[A] = add(node.uniqueAddress, element)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def add(node: UniqueAddress, element: A): ORSet[A] = {
    val newVvector = vvector + node
    val newDot = VersionVector(node, newVvector.versionAt(node))
    val newDelta = delta match {
      case None =>
        ORSet.AddDeltaOp(new ORSet(Map(element -> newDot), newDot))
      case Some(existing: ORSet.AddDeltaOp[_]) =>
        existing.merge(ORSet.AddDeltaOp(new ORSet(Map(element -> newDot), newDot)))
      case Some(d) =>
        d.merge(ORSet.AddDeltaOp(new ORSet(Map(element -> newDot), newDot)))
    }
    assignAncestor(new ORSet(elementsMap.updated(element, newDot), newVvector, Some(newDelta)))
  }

  /**
   * Scala API
   * Removes an element from the set.
   */
  def remove(element: A)(implicit node: SelfUniqueAddress): ORSet[A] = remove(node.uniqueAddress, element)

  /**
   * Java API
   * Removes an element from the set.
   */
  def remove(node: SelfUniqueAddress, element: A): ORSet[A] = remove(node.uniqueAddress, element)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def remove(node: UniqueAddress, element: A): ORSet[A] = {
    val deltaDot = VersionVector(node, vvector.versionAt(node))
    val rmOp = ORSet.RemoveDeltaOp(new ORSet(Map(element -> deltaDot), vvector))
    val newDelta = delta match {
      case None    => rmOp
      case Some(d) => d.merge(rmOp)
    }
    assignAncestor(copy(elementsMap = elementsMap - element, delta = Some(newDelta)))
  }

  /**
   * Removes all elements from the set, but keeps the history.
   * This has the same result as using
   * [[ORSet#remove(node:akka\.cluster\.ddata\.SelfUniqueAddress*]]
   * for each element, but it is more efficient.
   */
  def clear(@nowarn("msg=never used") node: SelfUniqueAddress): ORSet[A] = clear()

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def clear(): ORSet[A] = {
    val newFullState = new ORSet[A](elementsMap = Map.empty, vvector)
    val clearOp = ORSet.FullStateDeltaOp(newFullState)
    val newDelta = delta match {
      case None    => clearOp
      case Some(d) => d.merge(clearOp)
    }
    assignAncestor(newFullState.copy(delta = Some(newDelta)))
  }

  /**
   * When element is in this Set but not in that Set:
   * Compare the "birth dot" of the present element to the version vector in the Set it is absent from.
   * If the element dot is not "seen" by other Set version vector, that means the other set has yet to
   * see this add, and the element is to be in the merged Set.
   * If the other Set version vector dominates the dot, that means the other Set has removed
   * the element already, and the element is not to be in the merged Set.
   *
   * When element in both this Set and in that Set:
   * Some dots may still need to be shed. If this Set has dots that the other Set does not have,
   * and the other Set version vector dominates those dots, then we need to drop those dots.
   * Keep only common dots, and dots that are not dominated by the other sides version vector
   */
  override def merge(that: ORSet[A]): ORSet[A] = {
    if ((this eq that) || that.isAncestorOf(this)) this.clearAncestor()
    else if (this.isAncestorOf(that)) that.clearAncestor()
    else dryMerge(that, addDeltaOp = false)
  }

  // share merge impl between full state merge and AddDeltaOp merge
  private def dryMerge(that: ORSet[A], addDeltaOp: Boolean): ORSet[A] = {
    val commonKeys =
      if (this.elementsMap.size < that.elementsMap.size)
        this.elementsMap.keysIterator.filter(that.elementsMap.contains)
      else
        that.elementsMap.keysIterator.filter(this.elementsMap.contains)
    val entries00 = ORSet.mergeCommonKeys(commonKeys, this, that)
    val entries0 =
      if (addDeltaOp)
        entries00 ++ this.elementsMap.filter { case (elem, _) => !that.elementsMap.contains(elem) } else {
        val thisUniqueKeys = this.elementsMap.keysIterator.filterNot(that.elementsMap.contains)
        ORSet.mergeDisjointKeys(thisUniqueKeys, this.elementsMap, that.vvector, entries00)
      }
    val thatUniqueKeys = that.elementsMap.keysIterator.filterNot(this.elementsMap.contains)
    val entries = ORSet.mergeDisjointKeys(thatUniqueKeys, that.elementsMap, this.vvector, entries0)
    val mergedVvector = this.vvector.merge(that.vvector)

    clearAncestor()
    new ORSet(entries, mergedVvector)
  }

  override def mergeDelta(thatDelta: ORSet.DeltaOp): ORSet[A] = {
    thatDelta match {
      case d: ORSet.AddDeltaOp[_]    => dryMerge(d.asInstanceOf[ORSet.AddDeltaOp[A]].underlying, addDeltaOp = true)
      case d: ORSet.RemoveDeltaOp[_] => mergeRemoveDelta(d.asInstanceOf[ORSet.RemoveDeltaOp[A]])
      case d: ORSet.FullStateDeltaOp[_] =>
        dryMerge(d.asInstanceOf[ORSet.FullStateDeltaOp[A]].underlying, addDeltaOp = false)
      case ORSet.DeltaGroup(ops) =>
        ops.foldLeft(this) {
          case (acc, op: ORSet.AddDeltaOp[_]) =>
            acc.dryMerge(op.asInstanceOf[ORSet.AddDeltaOp[A]].underlying, addDeltaOp = true)
          case (acc, op: ORSet.RemoveDeltaOp[_]) => acc.mergeRemoveDelta(op.asInstanceOf[ORSet.RemoveDeltaOp[A]])
          case (acc, op: ORSet.FullStateDeltaOp[_]) =>
            acc.dryMerge(op.asInstanceOf[ORSet.FullStateDeltaOp[A]].underlying, addDeltaOp = false)
          case (_, _: ORSet.DeltaGroup[_]) =>
            throw new IllegalArgumentException("ORSet.DeltaGroup should not be nested")
        }
    }
  }

  private def mergeRemoveDelta(thatDelta: ORSet.RemoveDeltaOp[A]): ORSet[A] = {
    val that = thatDelta.underlying
    val (elem, thatDot) = that.elementsMap.head
    def deleteDots = that.vvector.versionsIterator
    def deleteDotsNodes = deleteDots.map { case (dotNode, _) => dotNode }
    val newElementsMap = {
      val thisDotOption = this.elementsMap.get(elem)
      val deleteDotsAreGreater = deleteDots.forall {
        case (dotNode, dotV) =>
          thisDotOption match {
            case Some(thisDot) => thisDot.versionAt(dotNode) <= dotV
            case None          => false
          }
      }
      if (deleteDotsAreGreater) {
        thisDotOption match {
          case Some(thisDot) =>
            if (thisDot.versionsIterator.forall { case (thisDotNode, _) => deleteDotsNodes.contains(thisDotNode) })
              elementsMap - elem
            else elementsMap
          case None =>
            elementsMap
        }
      } else
        elementsMap
    }
    clearAncestor()

    val newVvector = vvector.merge(thatDot)
    new ORSet(newElementsMap, newVvector)
  }

  override def resetDelta: ORSet[A] =
    if (delta.isEmpty) this
    else assignAncestor(new ORSet(elementsMap, vvector))

  override def modifiedByNodes: Set[UniqueAddress] =
    vvector.modifiedByNodes

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    vvector.needPruningFrom(removedNode)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): ORSet[A] = {
    val pruned = elementsMap.foldLeft(Map.empty[A, ORSet.Dot]) {
      case (acc, (elem, dot)) =>
        if (dot.needPruningFrom(removedNode)) acc.updated(elem, dot.prune(removedNode, collapseInto))
        else acc
    }
    if (pruned.isEmpty)
      copy(vvector = vvector.prune(removedNode, collapseInto))
    else {
      // re-add elements that were pruned, to bump dots to right vvector
      val newSet = new ORSet(elementsMap = elementsMap ++ pruned, vvector = vvector.prune(removedNode, collapseInto))
      pruned.keys.foldLeft(newSet) {
        case (s, elem) => s.add(collapseInto, elem)
      }
    }
  }

  override def pruningCleanup(removedNode: UniqueAddress): ORSet[A] = {
    val updated = elementsMap.foldLeft(elementsMap) {
      case (acc, (elem, dot)) =>
        if (dot.needPruningFrom(removedNode)) acc.updated(elem, dot.pruningCleanup(removedNode))
        else acc
    }
    new ORSet(updated, vvector.pruningCleanup(removedNode))
  }

  private def copy(
      elementsMap: Map[A, ORSet.Dot] = this.elementsMap,
      vvector: VersionVector = this.vvector,
      delta: Option[ORSet.DeltaOp] = this.delta): ORSet[A] =
    new ORSet(elementsMap, vvector, delta)

  // this class cannot be a `case class` because we need different `unapply`

  override def toString: String = s"OR$elements"

  override def equals(o: Any): Boolean = o match {
    case other: ORSet[_] => vvector == other.vvector && elementsMap == other.elementsMap
    case _               => false
  }

  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, elementsMap)
    result = HashCode.hash(result, vvector)
    result
  }
}

object ORSetKey {
  def create[A](id: String): Key[ORSet[A]] = ORSetKey(id)
}

@SerialVersionUID(1L)
final case class ORSetKey[A](_id: String) extends Key[ORSet[A]](_id) with ReplicatedDataSerialization {
  override def withId(newId: Key.KeyId): ORSetKey[A] =
    ORSetKey(newId)
}
