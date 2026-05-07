/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.crdt

import scala.annotation.tailrec
import scala.collection.immutable

import akka.annotation.InternalApi
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.crdt.ORSet.DeltaOp
import akka.persistence.typed.internal.{ ManyVersionVector, OneVersionVector, VersionVector }
import akka.util.HashCode

object ORSet {
  def empty[A](originReplica: ReplicaId): ORSet[A] = new ORSet(originReplica.id, Map.empty, VersionVector.empty)
  def apply[A](originReplica: ReplicaId): ORSet[A] = empty(originReplica)

  /**
   * Java API
   */
  def create[A](originReplica: ReplicaId): ORSet[A] = empty(originReplica)

  /**
   * Extract the [[ORSet#elements]].
   */
  def unapply[A](s: ORSet[A]): Option[Set[A]] = Some(s.elements)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] type Dot = VersionVector

  sealed trait DeltaOp {
    def merge(that: DeltaOp): DeltaOp
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] sealed abstract class AtomicDeltaOp[A] extends DeltaOp {
    def underlying: ORSet[A]
  }

  object AddDeltaOp {
    def apply[A](underlying: ORSet[A]): AddDeltaOp[A] =
      new AddDeltaOp(underlying)

    def unapply[A](arg: AddDeltaOp[A]): Option[ORSet[A]] =
      Some(arg.underlying)
  }

  final class AddDeltaOp[A](val underlying: ORSet[A]) extends AtomicDeltaOp[A] {

    override def merge(that: DeltaOp): DeltaOp = that match {
      case AddDeltaOp(u) =>
        // Note that we only merge deltas originating from the same DC
        AddDeltaOp(
          new ORSet(
            underlying.originReplica,
            concatElementsMap(u.elementsMap.asInstanceOf[Map[A, Dot]]),
            underlying.vvector.merge(u.vvector)))
      case _: AtomicDeltaOp[A @unchecked] => DeltaGroup(Vector(this, that))
      case g: DeltaGroup[A @unchecked]    => DeltaGroup(this +: g.ops)
    }

    private def concatElementsMap(thatMap: Map[A, Dot]): Map[A, Dot] = {
      if (thatMap.size == 1) {
        val head = thatMap.head
        underlying.elementsMap.updated(head._1, head._2)
      } else
        underlying.elementsMap ++ thatMap
    }

    override def toString: String = s"AddDeltaOp($underlying)"

    override def equals(obj: Any): Boolean = obj match {
      case other: AddDeltaOp[_] => underlying == other.underlying
      case _                    => false
    }

    override def hashCode(): Int = underlying.hashCode
  }

  object RemoveDeltaOp {
    def apply[A](underlying: ORSet[A]): RemoveDeltaOp[A] =
      new RemoveDeltaOp(underlying)

    def unapply[A](arg: RemoveDeltaOp[A]): Option[ORSet[A]] =
      Some(arg.underlying)
  }

  final class RemoveDeltaOp[A](val underlying: ORSet[A]) extends AtomicDeltaOp[A] {
    if (underlying.size != 1)
      throw new IllegalArgumentException(s"RemoveDeltaOp should contain one removed element, but was $underlying")

    override def merge(that: DeltaOp): DeltaOp = that match {
      case _: AtomicDeltaOp[A @unchecked] => DeltaGroup(Vector(this, that)) // keep it simple for removals
      case g: DeltaGroup[A @unchecked]    => DeltaGroup(this +: g.ops)
    }

    override def toString: String = s"RemoveDeltaOp($underlying)"

    override def equals(obj: Any): Boolean = obj match {
      case other: RemoveDeltaOp[_] => underlying == other.underlying
      case _                       => false
    }

    override def hashCode(): Int = underlying.hashCode
  }

  object FullStateDeltaOp {
    def apply[A](underlying: ORSet[A]): FullStateDeltaOp[A] =
      new FullStateDeltaOp(underlying)

    def unapply[A](arg: FullStateDeltaOp[A]): Option[ORSet[A]] =
      Some(arg.underlying)
  }

  /** Used for `clear` but could be used for other cases also */
  final class FullStateDeltaOp[A](val underlying: ORSet[A]) extends AtomicDeltaOp[A] {
    override def merge(that: DeltaOp): DeltaOp = that match {
      case _: AtomicDeltaOp[A @unchecked] => DeltaGroup(Vector(this, that))
      case g: DeltaGroup[A @unchecked]    => DeltaGroup(this +: g.ops)
    }

    override def toString: String = s"FullStateDeltaOp($underlying)"

    override def equals(obj: Any): Boolean = obj match {
      case other: FullStateDeltaOp[_] => underlying == other.underlying
      case _                          => false
    }

    override def hashCode(): Int = underlying.hashCode
  }

  object DeltaGroup {
    def apply[A](ops: immutable.IndexedSeq[DeltaOp]): DeltaGroup[A] =
      new DeltaGroup(ops)

    def unapply[A](arg: DeltaGroup[A]): Option[immutable.IndexedSeq[DeltaOp]] =
      Some(arg.ops)
  }

  final class DeltaGroup[A](val ops: immutable.IndexedSeq[DeltaOp]) extends DeltaOp {
    override def merge(that: DeltaOp): DeltaOp = that match {
      case thatAdd: AddDeltaOp[A @unchecked] =>
        // merge AddDeltaOp into last AddDeltaOp in the group, if possible
        ops.last match {
          case thisAdd: AddDeltaOp[A @unchecked] => DeltaGroup(ops.dropRight(1) :+ thisAdd.merge(thatAdd))
          case _                                 => DeltaGroup(ops :+ thatAdd)
        }
      case g: DeltaGroup[A @unchecked] => DeltaGroup(ops ++ g.ops)
      case _                           => DeltaGroup(ops :+ that)
    }

    override def toString: String = s"DeltaGroup($ops)"

    override def equals(obj: Any): Boolean = obj match {
      case other: DeltaGroup[_] => ops == other.ops
      case _                    => false
    }

    override def hashCode(): Int = ops.hashCode

  }

  /**
   * INTERNAL API
   * Subtract the `vvector` from the `dot`.
   * What this means is that any (dc, version) pair in
   * `dot` that is &lt;= an entry in `vvector` is removed from `dot`.
   * Example [{a, 3}, {b, 2}, {d, 14}, {g, 22}] -
   *         [{a, 4}, {b, 1}, {c, 1}, {d, 14}, {e, 5}, {f, 2}] =
   *         [{b, 2}, {g, 22}]
   */
  @InternalApi private[akka] def subtractDots(dot: Dot, vvector: VersionVector): Dot = {

    @tailrec def dropDots(remaining: List[(String, Long)], acc: List[(String, Long)]): List[(String, Long)] =
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
              case (thisDotNode, v) => rhsVs.get(thisDotNode).exists(_ == v)
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
 * Implements a 'Observed Remove Set' operation based CRDT, also called a 'OR-Set'.
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
 * the set. The `DC -&gt; count` pair for that increment is stored against the
 * element as its "birth dot". Every time the element is re-added to the set,
 * its "birth dot" is updated to that of the `DC -&gt; count` version vector entry
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
final class ORSet[A] private[akka] (
    val originReplica: String,
    private[akka] val elementsMap: Map[A, ORSet.Dot],
    private[akka] val vvector: VersionVector)
    extends OpCrdt[DeltaOp] {

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

  /**
   * Adds an element to the set
   */
  def +(element: A): ORSet.DeltaOp = add(element)

  /**
   * Adds an element to the set
   */
  def add(element: A): ORSet.DeltaOp = {
    val newVvector = vvector + originReplica
    val newDot = VersionVector(originReplica, newVvector.versionAt(originReplica))
    ORSet.AddDeltaOp(new ORSet(originReplica, Map(element -> newDot), newDot))
  }

  /**
   * Java API: Add several elements to the set.
   * `elems` must not be empty.
   */
  def addAll(elems: java.util.Set[A]): ORSet.DeltaOp = {
    import scala.jdk.CollectionConverters._
    addAll(elems.asScala.toSet)
  }

  /**
   * Scala API: Add several elements to the set.
   * `elems` must not be empty.
   */
  def addAll(elems: Set[A]): ORSet.DeltaOp = {
    if (elems.size == 0) throw new IllegalArgumentException("addAll elems must not be empty")
    else if (elems.size == 1) add(elems.head)
    else {
      val (first, rest) = elems.splitAt(1)
      val firstOp = add(first.head)
      val (mergedOps, _) = rest.foldLeft((firstOp, applyOperation(firstOp))) {
        case ((op, state), elem) =>
          val nextOp = state.add(elem)
          val mergedOp = op.merge(nextOp)
          (mergedOp, state.applyOperation(nextOp))
      }
      mergedOps
    }
  }

  /**
   * Removes an element from the set.
   */
  def -(element: A): ORSet.DeltaOp = remove(element)

  /**
   * Removes an element from the set.
   */
  def remove(element: A): ORSet.DeltaOp = {
    val deltaDot = VersionVector(originReplica, vvector.versionAt(originReplica))
    ORSet.RemoveDeltaOp(new ORSet(originReplica, Map(element -> deltaDot), vvector))
  }

  /**
   * Java API: Remove several elements from the set.
   * `elems` must not be empty.
   */
  def removeAll(elems: java.util.Set[A]): ORSet.DeltaOp = {
    import scala.jdk.CollectionConverters._
    removeAll(elems.asScala.toSet)
  }

  /**
   * Scala API: Remove several elements from the set.
   * `elems` must not be empty.
   */
  def removeAll(elems: Set[A]): ORSet.DeltaOp = {
    if (elems.size == 0) throw new IllegalArgumentException("removeAll elems must not be empty")
    else if (elems.size == 1) remove(elems.head)
    else {
      val (first, rest) = elems.splitAt(1)
      val firstOp = remove(first.head)
      val (mergedOps, _) = rest.foldLeft((firstOp, applyOperation(firstOp))) {
        case ((op, state), elem) =>
          val nextOp = state.remove(elem)
          val mergedOp = op.merge(nextOp)
          (mergedOp, state.applyOperation(nextOp))
      }
      mergedOps
    }
  }

  /**
   * Removes all elements from the set, but keeps the history.
   * This has the same result as using [[#remove]] for each
   * element, but it is more efficient.
   */
  def clear(): ORSet.DeltaOp = {
    val newFullState = new ORSet[A](originReplica, elementsMap = Map.empty, vvector)
    ORSet.FullStateDeltaOp(newFullState)
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
  private def merge(that: ORSet[A], addDeltaOp: Boolean): ORSet[A] = {
    if (this eq that) this
    else {
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

      new ORSet(originReplica, entries, mergedVvector)
    }
  }

  override def applyOperation(thatDelta: ORSet.DeltaOp): ORSet[A] = {
    thatDelta match {
      case d: ORSet.AddDeltaOp[A @unchecked]       => merge(d.underlying, addDeltaOp = true)
      case d: ORSet.RemoveDeltaOp[A @unchecked]    => mergeRemoveDelta(d)
      case d: ORSet.FullStateDeltaOp[A @unchecked] => merge(d.underlying, addDeltaOp = false)
      case g: ORSet.DeltaGroup[A @unchecked] =>
        g.ops.foldLeft(this) {
          case (acc, op: ORSet.AddDeltaOp[A @unchecked])       => acc.merge(op.underlying, addDeltaOp = true)
          case (acc, op: ORSet.RemoveDeltaOp[A @unchecked])    => acc.mergeRemoveDelta(op)
          case (acc, op: ORSet.FullStateDeltaOp[A @unchecked]) => acc.merge(op.underlying, addDeltaOp = false)
          case (_, _: ORSet.DeltaGroup[A @unchecked]) =>
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

    val newVvector = vvector.merge(thatDot)
    new ORSet(originReplica, newElementsMap, newVvector)
  }

  // this class cannot be a `case class` because we need different `unapply`

  override def toString: String = s"OR$elements"

  override def equals(o: Any): Boolean = o match {
    case other: ORSet[_] =>
      originReplica == other.originReplica && vvector == other.vvector && elementsMap == other.elementsMap
    case _ => false
  }

  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, originReplica)
    result = HashCode.hash(result, elementsMap)
    result = HashCode.hash(result, vvector)
    result
  }
}
