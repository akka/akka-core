/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal
import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

import akka.annotation.InternalApi

/**
 * INTERNAL API
 *
 * VersionVector module with helper classes and methods.
 */
@InternalApi
private[akka] object VersionVector {

  private val emptyVersions: TreeMap[String, Long] = TreeMap.empty
  val empty: VersionVector = ManyVersionVector(emptyVersions)

  def apply(): VersionVector = empty

  def apply(versions: TreeMap[String, Long]): VersionVector =
    if (versions.isEmpty) empty
    else if (versions.size == 1) apply(versions.head._1, versions.head._2)
    else ManyVersionVector(versions)

  def apply(key: String, version: Long): VersionVector = OneVersionVector(key, version)

  /** INTERNAL API */
  @InternalApi private[akka] def apply(versions: List[(String, Long)]): VersionVector =
    if (versions.isEmpty) empty
    else if (versions.tail.isEmpty) apply(versions.head._1, versions.head._2)
    else apply(emptyVersions ++ versions)

  sealed trait Ordering
  case object After extends Ordering
  case object Before extends Ordering
  case object Same extends Ordering
  case object Concurrent extends Ordering

  /**
   * Marker to ensure that we do a full order comparison instead of bailing out early.
   */
  private case object FullOrder extends Ordering

  /** INTERNAL API */
  @InternalApi private[akka] object Timestamp {
    final val Zero = 0L
    final val EndMarker = Long.MinValue
  }

  /**
   * Marker to signal that we have reached the end of a version vector.
   */
  private val cmpEndMarker = (null, Timestamp.EndMarker)

}

/**
 * INTERNAL API
 *
 * Representation of a Vector-based clock (counting clock), inspired by Lamport logical clocks.
 * {{{
 * Reference:
 *    1) Leslie Lamport (1978). "Time, clocks, and the ordering of events in a distributed system". Communications of the ACM 21 (7): 558-565.
 *    2) Friedemann Mattern (1988). "Virtual Time and Global States of Distributed Systems". Workshop on Parallel and Distributed Algorithms: pp. 215-226
 * }}}
 *
 * Based on `akka.cluster.ddata.VersionVector`.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@InternalApi
private[akka] sealed abstract class VersionVector {

  type T = VersionVector

  import VersionVector._

  /**
   * Increment the version for the key passed as argument. Returns a new VersionVector.
   */
  def +(key: String): VersionVector = increment(key)

  /**
   * Increment the version for the key passed as argument. Returns a new VersionVector.
   */
  def increment(key: String): VersionVector

  def updated(key: String, version: Long): VersionVector

  def isEmpty: Boolean

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def size: Int

  def versionAt(key: String): Long

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def contains(key: String): Boolean

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

    def compare(i1: Iterator[(String, Long)], i2: Iterator[(String, Long)], requestedOrder: Ordering): Ordering = {
      @tailrec
      def compareNext(nt1: (String, Long), nt2: (String, Long), currentOrder: Ordering): Ordering =
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
          // compare the entries
          val nc = nt1._1.compareTo(nt2._1)
          if (nc == 0) {
            // both entries exist compare the timestamps
            // same timestamp so just continue with the next entry
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
            // this entry only exists in i1 so i1 can only be After
            if (currentOrder eq Before) Concurrent
            else compareNext(nextOrElse(i1, cmpEndMarker), nt2, After)
          } else {
            // this entry only exists in i2 so i1 can only be Before
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
  @InternalApi private[akka] def versionsIterator: Iterator[(String, Long)]

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

  def merge(that: VersionVector): VersionVector

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class OneVersionVector private[akka] (key: String, version: Long)
    extends VersionVector {
  import VersionVector.Timestamp

  override def isEmpty: Boolean = false

  /** INTERNAL API */
  @InternalApi private[akka] override def size: Int = 1

  override def increment(k: String): VersionVector = {
    val v = version + 1
    if (k == key) copy(version = v)
    else ManyVersionVector(TreeMap(key -> version, k -> v))
  }

  override def updated(k: String, v: Long): VersionVector = {
    if (k == key) copy(version = v)
    else ManyVersionVector(TreeMap(key -> version, k -> v))
  }

  override def versionAt(k: String): Long =
    if (k == key) version
    else Timestamp.Zero

  /** INTERNAL API */
  @InternalApi private[akka] override def contains(k: String): Boolean =
    k == key

  /** INTERNAL API */
  @InternalApi private[akka] override def versionsIterator: Iterator[(String, Long)] =
    Iterator.single((key, version))

  override def merge(that: VersionVector): VersionVector = {
    that match {
      case OneVersionVector(n2, v2) =>
        if (key == n2) if (version >= v2) this else OneVersionVector(n2, v2)
        else ManyVersionVector(TreeMap(key -> version, n2 -> v2))
      case ManyVersionVector(vs2) =>
        val v2 = vs2.getOrElse(key, Timestamp.Zero)
        val mergedVersions =
          if (v2 >= version) vs2
          else vs2.updated(key, version)
        VersionVector(mergedVersions)
    }
  }
  override def toString: String =
    s"VersionVector($key -> $version)"

}

// TODO we could add more specialized/optimized implementations for 2 and 3 entries, because
// that will be the typical number of data centers

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class ManyVersionVector(versions: TreeMap[String, Long]) extends VersionVector {
  import VersionVector.Timestamp

  override def isEmpty: Boolean = versions.isEmpty

  /** INTERNAL API */
  @InternalApi private[akka] override def size: Int = versions.size

  override def increment(key: String): VersionVector = {
    val v = versionAt(key) + 1
    VersionVector(versions.updated(key, v))
  }

  override def updated(key: String, v: Long): VersionVector =
    VersionVector(versions.updated(key, v))

  override def versionAt(key: String): Long = versions.get(key) match {
    case Some(v) => v
    case None    => Timestamp.Zero
  }

  /** INTERNAL API */
  @InternalApi private[akka] override def contains(key: String): Boolean =
    versions.contains(key)

  /** INTERNAL API */
  @InternalApi private[akka] override def versionsIterator: Iterator[(String, Long)] =
    versions.iterator

  override def merge(that: VersionVector): VersionVector = {
    if (that.isEmpty) this
    else if (this.isEmpty) that
    else
      that match {
        case ManyVersionVector(vs2) =>
          var mergedVersions = vs2
          for ((key, time) <- versions) {
            val mergedVersionsCurrentTime = mergedVersions.getOrElse(key, Timestamp.Zero)
            if (time > mergedVersionsCurrentTime)
              mergedVersions = mergedVersions.updated(key, time)
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

  override def toString: String =
    versions.map { case (k, v) => k + " -> " + v }.mkString("VersionVector(", ", ", ")")
}
