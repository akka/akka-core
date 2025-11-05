/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * INTERNAL API
 */
private[akka] case class CacheStatistics(entries: Int, maxProbeDistance: Int, averageProbeDistance: Double)

/**
 * INTERNAL API
 *
 * This class is based on a Robin-Hood hashmap
 * (https://www.sebastiansylvan.com/post/robin-hood-hashing-should-be-your-default-hash-table-implementation/)
 * with backshift (https://codecapsule.com/2013/11/17/robin-hood-hashing-backward-shift-deletion/).
 *
 * The main modification compared to an RH hashmap is that it never grows the map (no rehashes) instead it is allowed
 * to kick out entires that are considered old. The implementation tries to keep the map close to full, only evicting
 * old entries when needed.
 */
private[akka] abstract class LruBoundedCache[K <: AnyRef: ClassTag, V <: AnyRef: ClassTag](
    capacity: Int,
    evictAgeThreshold: Int) {
  require(capacity > 0, "Capacity must be larger than zero")
  require((capacity & (capacity - 1)) == 0, "Capacity must be power of two")
  require(evictAgeThreshold <= capacity, "Age threshold must be less than capacity.")

  private[this] val Mask = capacity - 1

  // Practically guarantee an overflow
  private[this] var epoch = Int.MaxValue - 1

  private[this] val keys = Array.ofDim[K](capacity)
  private[this] val values = Array.ofDim[V](capacity)
  private[this] val hashes = new Array[Int](capacity)
  private[this] val epochs = Array.fill[Int](capacity)(epoch - evictAgeThreshold) // Guarantee existing "values" are stale

  final def get(k: K): Option[V] = {
    val h = hash(k)

    @tailrec def find(position: Int, probeDistance: Int): Option[V] = {
      val otherProbeDistance = probeDistanceOf(position)
      if (values(position) eq null) {
        None
      } else if (probeDistance > otherProbeDistance) {
        None
      } else if (hashes(position) == h && k == keys(position)) {
        Some(values(position))
      } else {
        find((position + 1) & Mask, probeDistance + 1)
      }
    }

    find(position = h & Mask, probeDistance = 0)
  }

  final def stats: CacheStatistics = {
    var i = 0
    var sum = 0
    var count = 0
    var max = 0
    while (i < hashes.length) {
      if (values(i) ne null) {
        val dist = probeDistanceOf(i)
        sum += dist
        count += 1
        max = math.max(dist, max)
      }
      i += 1
    }
    CacheStatistics(count, max, sum.toDouble / count)
  }

  final def getOrCompute(k: K): V =
    if (!isKeyCacheable(k)) {
      compute(k)
    } else {
      val h = hash(k)
      epoch += 1

      @tailrec def findOrCalculate(position: Int, probeDistance: Int): V = {
        if (values(position) eq null) {
          val value = compute(k)
          if (isCacheable(value)) {
            keys(position) = k
            values(position) = value
            hashes(position) = h
            epochs(position) = epoch
          }
          value
        } else {
          val otherProbeDistance = probeDistanceOf(position)
          // If probe distance of the element we try to get is larger than the current slot's, then the element cannot be in
          // the table since because of the Robin-Hood property we would have swapped it with the current element.
          if (probeDistance > otherProbeDistance) {
            val value = compute(k)
            if (isCacheable(value)) move(position, k, h, value, epoch, probeDistance)
            value
          } else if (hashes(position) == h && k == keys(position)) {
            // Update usage
            epochs(position) = epoch
            values(position)
          } else {
            // This is not our slot yet
            findOrCalculate((position + 1) & Mask, probeDistance + 1)
          }
        }
      }

      findOrCalculate(position = h & Mask, probeDistance = 0)
    }

  @tailrec private def removeAt(position: Int): Unit = {
    val next = (position + 1) & Mask
    if ((values(next) eq null) || probeDistanceOf(next) == 0) {
      // Next is not movable, just empty this slot
      values(position) = null.asInstanceOf[V]
    } else {
      // Shift the next slot here
      keys(position) = keys(next)
      values(position) = values(next)
      hashes(position) = hashes(next)
      epochs(position) = epochs(next)
      // remove the shifted slot
      removeAt(next)
    }
  }

  // Wraparound distance of the element that is in this slot. (X + capacity) & Mask ensures that there are no
  // negative numbers on wraparound
  private def probeDistanceOf(slot: Int): Int = probeDistanceOf(idealSlot = hashes(slot) & Mask, actualSlot = slot)

  // Protected for exposing it to unit tests
  protected def probeDistanceOf(idealSlot: Int, actualSlot: Int) = ((actualSlot - idealSlot) + capacity) & Mask

  @tailrec private def move(position: Int, k: K, h: Int, value: V, elemEpoch: Int, probeDistance: Int): Unit = {
    if (values(position) eq null) {
      // Found an empty place, done.
      keys(position) = k
      values(position) = value
      hashes(position) = h
      epochs(position) = elemEpoch // Do NOT update the epoch of the elem. It was not touched, just moved
    } else {
      val otherEpoch = epochs(position)
      // Check if the current entry is too old
      if (epoch - otherEpoch >= evictAgeThreshold) {
        // Remove old entry to make space
        removeAt(position)
        // Try to insert our element in hand to its ideal slot
        move(h & Mask, k, h, value, elemEpoch, 0)
      } else {
        val otherProbeDistance = probeDistanceOf(position)
        val otherEpoch = epochs(position)

        // Check whose probe distance is larger. The one with the larger one wins the slot.
        if (probeDistance > otherProbeDistance) {
          // Due to the Robin-Hood property, we now take away this slot from the "richer" and take it for ourselves
          val otherKey = keys(position)
          val otherValue = values(position)
          val otherHash = hashes(position)

          keys(position) = k
          values(position) = value
          hashes(position) = h
          epochs(position) = elemEpoch

          // Move out the old one
          move((position + 1) & Mask, otherKey, otherHash, otherValue, otherEpoch, otherProbeDistance + 1)
        } else {
          // We are the "richer" so we need to find another slot
          move((position + 1) & Mask, k, h, value, elemEpoch, probeDistance + 1)
        }

      }
    }

  }

  protected def compute(k: K): V

  protected def hash(k: K): Int

  protected def isKeyCacheable(k: K): Boolean
  protected def isCacheable(v: V): Boolean

  override def toString =
    s"LruBoundedCache(" +
    s" values = ${values.mkString("[", ",", "]")}," +
    s" hashes = ${hashes.map(_ & Mask).mkString("[", ",", "]")}," +
    s" epochs = ${epochs.mkString("[", ",", "]")}," +
    s" distances = ${hashes.indices.map(probeDistanceOf).mkString("[", ",", "]")}," +
    s" $epoch)"
}
