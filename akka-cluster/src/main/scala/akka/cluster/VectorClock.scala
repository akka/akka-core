/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.security.MessageDigest

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

/**
 * VectorClock module with helper classes and methods.
 *
 * Based on code from the 'vlock' VectorClock library by Coda Hale.
 */
private[cluster] object VectorClock {

  /**
   * Hash representation of a versioned node name.
   */
  type Node = String

  object Node {

    def apply(name: String): Node = hash(name)

    def fromHash(hash: String): Node = hash

    private def hash(name: String): String = {
      val digester = MessageDigest.getInstance("MD5")
      digester.update(name.getBytes("UTF-8"))
      digester.digest.map { h =>
        "%02x".format(0xFF & h)
      }.mkString
    }
  }

  object Timestamp {
    final val Zero = 0L
    final val EndMarker = Long.MinValue
  }

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
   * Marker to signal that we have reached the end of a vector clock.
   */
  private val cmpEndMarker = (VectorClock.Node("endmarker"), Timestamp.EndMarker)

}

/**
 * Representation of a Vector-based clock (counting clock), inspired by Lamport logical clocks.
 * {{{
 * Reference:
 *    1) Leslie Lamport (1978). "Time, clocks, and the ordering of events in a distributed system". Communications of the ACM 21 (7): 558-565.
 *    2) Friedemann Mattern (1988). "Virtual Time and Global States of Distributed Systems". Workshop on Parallel and Distributed Algorithms: pp. 215-226
 * }}}
 *
 * Based on code from the 'vlock' VectorClock library by Coda Hale.
 */
@SerialVersionUID(1L)
final case class VectorClock(versions: TreeMap[VectorClock.Node, Long] = TreeMap.empty[VectorClock.Node, Long]) {

  import VectorClock._

  /**
   * Increment the version for the node passed as argument. Returns a new VectorClock.
   */
  def :+(node: Node): VectorClock = {
    val currentTimestamp = versions.getOrElse(node, Timestamp.Zero)
    copy(versions = versions.updated(node, currentTimestamp + 1))
  }

  /**
   * Returns true if <code>this</code> and <code>that</code> are concurrent else false.
   */
  def <>(that: VectorClock): Boolean = compareOnlyTo(that, Concurrent) eq Concurrent

  /**
   * Returns true if <code>this</code> is before <code>that</code> else false.
   */
  def <(that: VectorClock): Boolean = compareOnlyTo(that, Before) eq Before

  /**
   * Returns true if <code>this</code> is after <code>that</code> else false.
   */
  def >(that: VectorClock): Boolean = compareOnlyTo(that, After) eq After

  /**
   * Returns true if this VectorClock has the same history as the 'that' VectorClock else false.
   */
  def ==(that: VectorClock): Boolean = compareOnlyTo(that, Same) eq Same

  /**
   * Vector clock comparison according to the semantics described by compareTo, with the ability to bail
   * out early if the we can't reach the Ordering that we are looking for.
   *
   * The ordering always starts with Same and can then go to Same, Before or After
   * If we're on After we can only go to After or Concurrent
   * If we're on Before we can only go to Before or Concurrent
   * If we go to Concurrent we exit the loop immediately
   *
   * If you send in the ordering FullOrder, you will get a full comparison.
   */
  private def compareOnlyTo(that: VectorClock, order: Ordering): Ordering = {
    def nextOrElse[T](iter: Iterator[T], default: T): T = if (iter.hasNext) iter.next() else default

    def compare(i1: Iterator[(Node, Long)], i2: Iterator[(Node, Long)], requestedOrder: Ordering): Ordering = {
      @tailrec
      def compareNext(nt1: (Node, Long), nt2: (Node, Long), currentOrder: Ordering): Ordering =
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

    if ((this eq that) || (this.versions eq that.versions)) Same
    else compare(this.versions.iterator, that.versions.iterator, if (order eq Concurrent) FullOrder else order)
  }

  /**
   * Compare two vector clocks. The outcome will be one of the following:
   * <p/>
   * {{{
   *   1. Clock 1 is SAME (==)       as Clock 2 iff for all i c1(i) == c2(i)
   *   2. Clock 1 is BEFORE (<)      Clock 2 iff for all i c1(i) <= c2(i) and there exist a j such that c1(j) < c2(j)
   *   3. Clock 1 is AFTER (>)       Clock 2 iff for all i c1(i) >= c2(i) and there exist a j such that c1(j) > c2(j).
   *   4. Clock 1 is CONCURRENT (<>) to Clock 2 otherwise.
   * }}}
   */
  def compareTo(that: VectorClock): Ordering = {
    compareOnlyTo(that, FullOrder)
  }

  /**
   * Merges this VectorClock with another VectorClock. E.g. merges its versioned history.
   */
  def merge(that: VectorClock): VectorClock = {
    var mergedVersions = that.versions
    for ((node, time) <- versions) {
      val mergedVersionsCurrentTime = mergedVersions.getOrElse(node, Timestamp.Zero)
      if (time > mergedVersionsCurrentTime)
        mergedVersions = mergedVersions.updated(node, time)
    }
    VectorClock(mergedVersions)
  }

  def prune(removedNode: Node): VectorClock =
    if (versions.contains(removedNode))
      copy(versions = versions - removedNode)
    else
      this

  override def toString = versions.map { case (n, t) => n + " -> " + t }.mkString("VectorClock(", ", ", ")")
}
