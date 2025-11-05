/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

import akka.annotation.InternalApi
import akka.cluster.UniqueAddress

/**
 * VersionVector module with helper classes and methods.
 */
object VersionVector {

  private val emptyVersions: TreeMap[UniqueAddress, Long] = TreeMap.empty
  val empty: VersionVector = ManyVersionVector(emptyVersions)

  def apply(): VersionVector = empty

  def apply(versions: TreeMap[UniqueAddress, Long]): VersionVector =
    if (versions.isEmpty) empty
    else if (versions.size == 1) apply(versions.head._1, versions.head._2)
    else ManyVersionVector(versions)

  def apply(node: UniqueAddress, version: Long): VersionVector = OneVersionVector(node, version)

  /** INTERNAL API */
  @InternalApi private[akka] def apply(versions: List[(UniqueAddress, Long)]): VersionVector =
    if (versions.isEmpty) empty
    else if (versions.tail.isEmpty) apply(versions.head._1, versions.head._2)
    else apply(emptyVersions ++ versions)

  /**
   * Java API
   */
  def create(): VersionVector = empty

  sealed trait Ordering
  case object After extends Ordering
  case object Before extends Ordering
  case object Same extends Ordering
  case object Concurrent extends Ordering

  /**
   * Marker to ensure that we do a full order comparison instead of bailing out early.
   */
  private case object FullOrder extends Ordering

  /**
   * Java API: The `VersionVector.After` instance
   */
  def AfterInstance = After

  /**
   * Java API: The `VersionVector.Before` instance
   */
  def BeforeInstance = Before

  /**
   * Java API: The `VersionVector.Same` instance
   */
  def SameInstance = Same

  /**
   * Java API: The `VersionVector.Concurrent` instance
   */
  def ConcurrentInstance = Concurrent

  /** INTERNAL API */
  @InternalApi private[akka] object Timestamp {
    final val Zero = 0L
    final val EndMarker = Long.MinValue
    val counter = new AtomicLong(1L)
  }

  /**
   * Marker to signal that we have reached the end of a version vector.
   */
  private val cmpEndMarker = (null, Timestamp.EndMarker)

}

/**
 * Representation of a Vector-based clock (counting clock), inspired by Lamport logical clocks.
 * {{{
 * Reference:
 *    1) Leslie Lamport (1978). "Time, clocks, and the ordering of events in a distributed system". Communications of the ACM 21 (7): 558-565.
 *    2) Friedemann Mattern (1988). "Virtual Time and Global States of Distributed Systems". Workshop on Parallel and Distributed Algorithms: pp. 215-226
 * }}}
 *
 * Based on code from `akka.cluster.VectorClock`.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@SerialVersionUID(1L)
sealed abstract class VersionVector extends ReplicatedData with ReplicatedDataSerialization with RemovedNodePruning {

  type T = VersionVector

  import VersionVector._

  /**
   * Increment the version for the node passed as argument. Returns a new VersionVector.
   */
  def :+(node: SelfUniqueAddress): VersionVector = increment(node)

  /**
   * INTERNAL API
   * Increment the version for the node passed as argument. Returns a new VersionVector.
   */
  @InternalApi private[akka] def +(node: UniqueAddress): VersionVector = increment(node)

  /**
   * Increment the version for the node passed as argument. Returns a new VersionVector.
   */
  def increment(node: SelfUniqueAddress): VersionVector = increment(node.uniqueAddress)

  def isEmpty: Boolean

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def size: Int

  /**
   * INTERNAL API
   * Increment the version for the node passed as argument. Returns a new VersionVector.
   */
  @InternalApi private[akka] def increment(node: UniqueAddress): VersionVector

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def versionAt(node: UniqueAddress): Long

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def contains(node: UniqueAddress): Boolean

  /**
   * Returns true if <code>this</code> and <code>that</code> are concurrent else false.
   */
  def <>(that: VersionVector): Boolean = compareOnlyTo(that, Concurrent) eq Concurrent

  /**
   * Returns true if <code>this</code> is before <code>that</code> else false.
   */
  def <(that: VersionVector): Boolean = compareOnlyTo(that, Before) eq Before

  /**
   * Returns true if <code>this</code> is after <code>that</code> else false.
   */
  def >(that: VersionVector): Boolean = compareOnlyTo(that, After) eq After

  /**
   * Returns true if this VersionVector has the same history as the 'that' VersionVector else false.
   */
  def ==(that: VersionVector): Boolean = compareOnlyTo(that, Same) eq Same

  /**
   * Version vector comparison according to the semantics described by compareTo, with the ability to bail
   * out early if the we can't reach the Ordering that we are looking for.
   *
   * The ordering always starts with Same and can then go to Same, Before or After
   * If we're on After we can only go to After or Concurrent
   * If we're on Before we can only go to Before or Concurrent
   * If we go to Concurrent we exit the loop immediately
   *
   * If you send in the ordering FullOrder, you will get a full comparison.
   */
  private final def compareOnlyTo(that: VersionVector, order: Ordering): Ordering = {
    def nextOrElse[A](iter: Iterator[A], default: A): A = if (iter.hasNext) iter.next() else default

    def compare(
        i1: Iterator[(UniqueAddress, Long)],
        i2: Iterator[(UniqueAddress, Long)],
        requestedOrder: Ordering): Ordering = {
      @tailrec
      def compareNext(nt1: (UniqueAddress, Long), nt2: (UniqueAddress, Long), currentOrder: Ordering): Ordering =
        if ((requestedOrder ne FullOrder) && (currentOrder ne Same) && (currentOrder ne requestedOrder)) currentOrder
        else if ((nt1 eq cmpEndMarker) && (nt2 eq cmpEndMarker)) currentOrder
        // i1 is empty but i2 is not, so i1 can only be Before
        else if (nt1 eq cmpEndMarker) {
          if (currentOrder eq After) Concurrent else Before
        }
        // i2 is empty but i1 is not, so i1 can only be After
        else if (nt2 eq cmpEndMarker) {
          if (currentOrder eq Before) Concurrent else After
        } else {
          // compare the nodes
          val nc = nt1._1.compareTo(nt2._1)
          if (nc == 0) {
            // both nodes exist compare the timestamps
            // same timestamp so just continue with the next nodes
            if (nt1._2 == nt2._2) compareNext(nextOrElse(i1, cmpEndMarker), nextOrElse(i2, cmpEndMarker), currentOrder)
            else if (nt1._2 < nt2._2) {
              // t1 is less than t2, so i1 can only be Before
              if (currentOrder eq After) Concurrent
              else compareNext(nextOrElse(i1, cmpEndMarker), nextOrElse(i2, cmpEndMarker), Before)
            } else {
              // t2 is less than t1, so i1 can only be After
              if (currentOrder eq Before) Concurrent
              else compareNext(nextOrElse(i1, cmpEndMarker), nextOrElse(i2, cmpEndMarker), After)
            }
          } else if (nc < 0) {
            // this node only exists in i1 so i1 can only be After
            if (currentOrder eq Before) Concurrent
            else compareNext(nextOrElse(i1, cmpEndMarker), nt2, After)
          } else {
            // this node only exists in i2 so i1 can only be Before
            if (currentOrder eq After) Concurrent
            else compareNext(nt1, nextOrElse(i2, cmpEndMarker), Before)
          }
        }

      compareNext(nextOrElse(i1, cmpEndMarker), nextOrElse(i2, cmpEndMarker), Same)
    }

    if (this eq that) Same
    else compare(this.versionsIterator, that.versionsIterator, if (order eq Concurrent) FullOrder else order)
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def versionsIterator: Iterator[(UniqueAddress, Long)]

  /**
   * Compare two version vectors. The outcome will be one of the following:
   * <p/>
   * {{{
   *   1. Version 1 is SAME (==)       as Version 2 iff for all i c1(i) == c2(i)
   *   2. Version 1 is BEFORE (<)      Version 2 iff for all i c1(i) <= c2(i) and there exist a j such that c1(j) < c2(j)
   *   3. Version 1 is AFTER (>)       Version 2 iff for all i c1(i) >= c2(i) and there exist a j such that c1(j) > c2(j).
   *   4. Version 1 is CONCURRENT (<>) to Version 2 otherwise.
   * }}}
   */
  def compareTo(that: VersionVector): Ordering = {
    compareOnlyTo(that, FullOrder)
  }

  /**
   * Merges this VersionVector with another VersionVector. E.g. merges its versioned history.
   */
  def merge(that: VersionVector): VersionVector

  override def needPruningFrom(removedNode: UniqueAddress): Boolean

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): VersionVector

  override def pruningCleanup(removedNode: UniqueAddress): VersionVector

  /** INTERNAL API */
  @InternalApi private[akka] def estimatedSize: Int

}

final case class OneVersionVector private[akka] (node: UniqueAddress, version: Long) extends VersionVector {
  import VersionVector.Timestamp

  override def isEmpty: Boolean = false

  /** INTERNAL API */
  @InternalApi private[akka] override def size: Int = 1

  /** INTERNAL API */
  @InternalApi private[akka] override def increment(n: UniqueAddress): VersionVector = {
    val v = Timestamp.counter.getAndIncrement()
    if (n == node) copy(version = v)
    else ManyVersionVector(TreeMap(node -> version, n -> v))
  }

  /** INTERNAL API */
  @InternalApi private[akka] override def versionAt(n: UniqueAddress): Long =
    if (n == node) version
    else Timestamp.Zero

  /** INTERNAL API */
  @InternalApi private[akka] override def contains(n: UniqueAddress): Boolean =
    n == node

  /** INTERNAL API */
  @InternalApi private[akka] override def versionsIterator: Iterator[(UniqueAddress, Long)] =
    Iterator.single((node, version))

  override def merge(that: VersionVector): VersionVector = {
    that match {
      case OneVersionVector(n2, v2) =>
        if (node == n2) if (version >= v2) this else OneVersionVector(n2, v2)
        else ManyVersionVector(TreeMap(node -> version, n2 -> v2))
      case ManyVersionVector(vs2) =>
        val v2 = vs2.getOrElse(node, Timestamp.Zero)
        val mergedVersions =
          if (v2 >= version) vs2
          else vs2.updated(node, version)
        VersionVector(mergedVersions)
    }
  }

  override def modifiedByNodes: Set[UniqueAddress] =
    Set(node)

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    node == removedNode

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): VersionVector =
    (if (node == removedNode) VersionVector.empty else this) + collapseInto

  override def pruningCleanup(removedNode: UniqueAddress): VersionVector =
    if (node == removedNode) VersionVector.empty else this

  override def toString: String =
    s"VersionVector($node -> $version)"

  /** INTERNAL API */
  @InternalApi override private[akka] def estimatedSize: Int =
    EstimatedSize.UniqueAddress + EstimatedSize.LongValue

}

final case class ManyVersionVector(versions: TreeMap[UniqueAddress, Long]) extends VersionVector {
  import VersionVector.Timestamp

  override def isEmpty: Boolean = versions.isEmpty

  /** INTERNAL API */
  @InternalApi private[akka] override def size: Int = versions.size

  /** INTERNAL API */
  @InternalApi private[akka] override def increment(node: UniqueAddress): VersionVector = {
    val v = Timestamp.counter.getAndIncrement()
    VersionVector(versions.updated(node, v))
  }

  /** INTERNAL API */
  @InternalApi private[akka] override def versionAt(node: UniqueAddress): Long = versions.get(node) match {
    case Some(v) => v
    case None    => Timestamp.Zero
  }

  /** INTERNAL API */
  @InternalApi private[akka] override def contains(node: UniqueAddress): Boolean =
    versions.contains(node)

  /** INTERNAL API */
  @InternalApi private[akka] override def versionsIterator: Iterator[(UniqueAddress, Long)] =
    versions.iterator

  override def merge(that: VersionVector): VersionVector = {
    if (that.isEmpty) this
    else if (this.isEmpty) that
    else
      that match {
        case ManyVersionVector(vs2) =>
          var mergedVersions = vs2
          for ((node, time) <- versions) {
            val mergedVersionsCurrentTime = mergedVersions.getOrElse(node, Timestamp.Zero)
            if (time > mergedVersionsCurrentTime)
              mergedVersions = mergedVersions.updated(node, time)
          }
          VersionVector(mergedVersions)
        case OneVersionVector(n2, v2) =>
          val v1 = versions.getOrElse(n2, Timestamp.Zero)
          val mergedVersions =
            if (v1 >= v2) versions
            else versions.updated(n2, v2)
          VersionVector(mergedVersions)
      }
  }

  override def modifiedByNodes: Set[UniqueAddress] =
    versions.keySet

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    versions.contains(removedNode)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): VersionVector =
    VersionVector(versions = versions - removedNode) + collapseInto

  override def pruningCleanup(removedNode: UniqueAddress): VersionVector =
    if (versions.contains(removedNode)) VersionVector(versions = versions - removedNode)
    else this

  override def toString: String =
    versions.map { case ((n, v)) => n.toString + " -> " + v }.mkString("VersionVector(", ", ", ")")

  /** INTERNAL API */
  @InternalApi override private[akka] def estimatedSize: Int =
    versions.size * (EstimatedSize.UniqueAddress + EstimatedSize.LongValue)
}
