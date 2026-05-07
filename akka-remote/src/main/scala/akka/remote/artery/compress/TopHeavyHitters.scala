/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

import java.util.Objects

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * INTERNAL API
 *
 * Mutable, open-addressing with linear-probing (though naive one which in theory could get pathological) heavily optimised "top N heavy hitters" data-structure.
 *
 * Keeps a number of specific heavy hitters around in memory.
 *
 * See also Section 5.2 of https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
 * for a discussion about the assumptions made and guarantees about the Heavy Hitters made in this model.
 * We assume the Cash Register model in which there are only additions, which simplifies HH detection significantly.
 *
 * This class is a hybrid data structure containing a hashmap and a heap pointing to slots in the hashmap. The capacity
 * of the hashmap is twice that of the heap to reduce clumping of entries on collisions.
 */
private[remote] final class TopHeavyHitters[T >: Null](val max: Int)(implicit classTag: ClassTag[T]) { self =>

  private val adjustedMax = if (max == 0) 1 else max // need at least one
  require(
    (adjustedMax & (adjustedMax - 1)) == 0,
    "Maximum numbers of heavy hitters should be in form of 2^k for any natural k")

  val capacity = adjustedMax * 2
  val mask = capacity - 1

  import TopHeavyHitters._

  // Contains the hash value for each entry in the hashmap. Used for quicker lookups (equality check can be avoided
  // if hashes don't match)
  private[this] val hashes: Array[Int] = new Array(capacity)
  // Actual stored elements in the hashmap
  private[this] val items: Array[T] = Array.ofDim[T](capacity)
  // Index of stored element in the associated heap
  private[this] val heapIndex: Array[Int] = Array.fill(capacity)(-1)
  // Weights associated with an entry in the hashmap. Used to maintain the heap property and give easy access to low
  // weight entries
  private[this] val weights: Array[Long] = new Array(capacity)

  // Heap structure containing indices to slots in the hashmap
  private[this] val heap: Array[Int] = Array.fill(adjustedMax)(-1)

  /*
   * Invariants (apart from heap and hashmap invariants):
   * - Heap is ordered by weights in an increasing order, so lowest weight is at the top of the heap (heap(0))
   * - Each heap slot contains the index of the hashmap slot where the actual element is, or contains -1
   * - Empty heap slots contain a negative index
   * - Each hashmap slot should contain the index of the heap slot where the element currently is
   * - Initially the heap is filled with "pseudo" elements (-1 entries with weight 0). This ensures that when the
   *   heap is not full a "lowestHitterWeight" of zero is reported. This also ensures that we can always safely
   *   remove the "lowest hitter" without checking the capacity, as when the heap is not full it will contain a
   *   "pseudo" element as lowest hitter which can be safely removed without affecting real entries.
   */

  /*
   * Insertion of a new element works like this:
   *  1. Check if its weight is larger than the lowest hitter we know. If the heap is not full, then due to the
   *    presence of "pseudo" elements (-1 entries in heap with weight 0) this will always succeed.
   *  2. Do a quick scan for a matching hashcode with findHashIdx. This only searches for the first candidate (one
   *    with equal hash).
   *    a. If this returns -1, we know this is a new entry (there is no other entry with matching hashcode).
   *    b. Else, this returns a nonnegative number. Now we do a more thorough scan from here, actually checking for
   *       object equality by calling findItemIdx (discarding false positives due to hash collisions). Since this
   *       is a new entry, the result will be -1 (the candidate was a colliding false positive).
   *  3. Call insertKnownHeavy
   *  4. This removes the lowest hitter first from the hashmap.
   *    a. If the heap is not full, this will be just a "pseudo" element (-1 entry with weight 0) so no "real" entries
   *       are evicted.
   *    b. If the heap is full, then a real entry is evicted. This is correct as the new entry has higher weight than
   *       the evicted one due to the check in step 1.
   *  5. The new entry is inserted into the hashmap and its entry index is recorded
   *  6. Overwrite the top of the heap with inserted element's index (again, correct, this was the evicted real or
   *     "pseudo" element).
   *  7. Restore heap property by pushing down the new element from the top if necessary.
   */

  /*
   * Update of an existing element works like this:
   * 1. Check if its weight is larger than the lowest hitter we know. This will succeed as weights can only be
   *    incremented.
   * 2. Do a quick scan for a matching hashcode with findHashIdx. This only searches for the first candidate (one
   *    with equal hash). This does not check for actual entry equality yet, but it can quickly skip surely non-matching
   *    entries. Since this is an existing element, we will find an index that is a candidate for actual equality.
   * 3. Now do a more thorough scan from this candidate index checking for hash *and* object equality (to skip
   *    potentially colliding entries). We will now have the real index of the existing entry if the previous scan
   *    found a false positive.
   * 4. Call updateExistingHeavyHitter
   * 5. Update the recorded weight for this entry in the hashmap (at the index we previously found)
   * 6. Fix the Heap property (since now weight can be larger than one of its heap children nodes). Please note that
   *    we just swap heap entries around here, so no entry will be evicted.
   */

  /**
   * Iterates over the current heavy hitters, order is not of significance.
   * Not thread safe, accesses internal heap directly (to avoid copying of data). Access must be synchronised externally.
   */
  def iterator: Iterator[T] =
    new Iterator[T] {
      var i = 0

      @tailrec override final def hasNext: Boolean = {
        // note that this is using max and not adjustedMax so will be empty if disabled (max=0)
        (i < self.max) && ((value != null) || { next(); hasNext })
      }

      override final def next(): T = {
        val v = value
        i += 1
        v
      }

      @inline private final def index: Int = heap(i)
      @inline private final def value: T = {
        val idx = index
        if (idx < 0) null else items(idx)
      }
    }

  def toDebugString =
    s"""TopHeavyHitters(
        |  max: $max,
        |  lowestHitterIdx: $lowestHitterIndex (weight: $lowestHitterWeight)
        |
        |  hashes:      ${hashes.toList.mkString("[", ", ", "]")}
        |  weights:     ${weights.toList.mkString("[", ", ", "]")}
        |  items:       ${items.toList.mkString("[", ", ", "]")}
        |  heapIndex:   ${heapIndex.toList.mkString("[", ", ", "]")}
        |  heap:   ${heap.toList.mkString("[", ", ", "]")}
        |)""".stripMargin

  /**
   * Attempt adding item to heavy hitters set, if it does not fit in the top yet,
   * it will be dropped and the method will return `false`.
   *
   * WARNING: It is only allowed to *increase* the weight of existing elements, decreasing is disallowed.
   *
   * @return `true` if the added item has become a heavy hitter.
   */
  // TODO possibly can be optimised further? (there is a benchmark)
  def update(item: T, count: Long): Boolean =
    isHeavy(count) && { // O(1) terminate execution ASAP if known to not be a heavy hitter anyway
      val hashCode = new HashCodeVal(item.hashCode()) // avoid re-calculating hashCode
      val startIndex = hashCode.get & mask

      // We first try to find the slot where an element with an equal hash value is. This is a possible candidate
      // for an actual matching entry (unless it is an entry with a colliding hash value).
      // worst case O(n), common O(1 + alpha), can't really bin search here since indexes are kept in synch with other arrays hmm...
      val candidateIndex = findHashIdx(startIndex, hashCode)

      if (candidateIndex == -1) {
        // No matching hash value entry is found, so we are sure we don't have this entry yet.
        insertKnownNewHeavy(hashCode, item, count) // O(log n + alpha)
        true
      } else {
        // We now found, relatively cheaply, the first index where our searched entry *might* be (hashes are equal).
        // This is not guaranteed to be the one we are searching for, yet (hash values may collide).
        // From this position we can invoke the more costly search which checks actual object equalities.
        // With this two step search we avoid equality checks completely for many non-colliding entries.
        val actualIdx = findItemIdx(candidateIndex, hashCode, item)

        // usually O(1), worst case O(n) if we need to scan due to hash conflicts
        if (actualIdx == -1) {
          // So we don't have this entry so far (only a colliding one, it was a false positive from findHashIdx).
          insertKnownNewHeavy(hashCode, item, count) // O(1 + log n), we simply replace the current lowest heavy hitter
          true
        } else {
          // The entry exists, let's update it.
          updateExistingHeavyHitter(actualIdx, count)
          // not a "new" heavy hitter, since we only replaced it (so it was signaled as new once before)
          false
        }
      }

    }

  /**
   * Checks the lowest weight entry in this structure and returns true if the given count is larger than that. In
   * other words this checks if a new entry can be added as it is larger than the known least weight.
   */
  private def isHeavy(count: Long): Boolean =
    count > lowestHitterWeight

  /**
   * Finds the index of an element in the hashtable (or returns -1 if it is not found). It is usually a good idea
   * to find the first eligible index with findHashIdx before invoking this method since that finds the first
   * eligible candidate (one with an equal hash value) faster than this method (since this checks actual object
   * equality).
   */
  private def findItemIdx(searchFromIndex: Int, hashCode: HashCodeVal, o: T): Int = {
    @tailrec def loop(index: Int, start: Int, hashCodeVal: HashCodeVal, o: T): Int = {
      // Scanned the whole table, returned to the start => element not in table
      if (index == start) -1
      else if (hashCodeVal.get == hashes(index)) { // First check on hashcode to avoid costly equality
        val item: T = items(index)
        if (Objects.equals(item, o)) {
          // Found the item, return its index
          index
        } else {
          // Item did not match, continue probing.
          // TODO: This will probe the *whole* table all the time for non-entries, there is no option for early exit.
          // TODO: Maybe record probe lengths so we can bail out early.
          loop((index + 1) & mask, start, hashCodeVal, o)
        }
      } else {
        // hashcode did not match the one in slot, move to next index (linear probing)
        loop((index + 1) & mask, start, hashCodeVal, o)
      }
    }

    if (searchFromIndex == -1) -1
    else if (Objects.equals(items(searchFromIndex), o)) searchFromIndex
    else loop((searchFromIndex + 1) & mask, searchFromIndex, hashCode, o)
  }

  /**
   * Replace existing heavy hitter – give it a new `count` value. This will also restore the heap property, so this
   * might make a previously lowest hitter no longer be one.
   */
  private def updateExistingHeavyHitter(foundHashIndex: Int, count: Long): Unit = {
    if (weights(foundHashIndex) > count)
      throw new IllegalArgumentException(
        s"Weights can be only incremented or kept the same, not decremented. " +
        s"Previous weight was [${weights(foundHashIndex)}], attempted to modify it to [$count].")
    weights(foundHashIndex) = count // we don't need to change `hashCode`, `heapIndex` or `item`, those remain the same
    // Position in the heap might have changed as count was incremented
    fixHeap(heapIndex(foundHashIndex))
  }

  /**
   * Search in the hashmap the first slot that contains an equal hashcode to the one we search for. This is an
   * optimization: before we start to compare elements, we find the first eligible candidate (whose hashmap matches
   * the one we search for). From this index usually findItemIndex is called which does further probing, but checking
   * actual element equality.
   */
  private def findHashIdx(searchFromIndex: Int, hashCode: HashCodeVal): Int = {
    var i: Int = 0
    while (i < hashes.length) {
      val index = (i + searchFromIndex) & mask
      if (hashes(index) == hashCode.get) {
        return index
      }
      i += 1
    }
    -1
  }

  /**
   * Call this if the weight of an entry at heap node `index` was incremented. The heap property says that
   * "The key stored in each node is less than or equal to the keys in the node's children". Since we incremented
   * (or kept the same) weight for this index, we only need to restore the heap "downwards", parents are not affected.
   */
  @tailrec
  private def fixHeap(index: Int): Unit = {
    val leftIndex = index * 2 + 1
    val rightIndex = index * 2 + 2
    val currentWeight: Long = weights(heap(index))
    if (rightIndex < adjustedMax) {
      val leftValueIndex: Int = heap(leftIndex)
      val rightValueIndex: Int = heap(rightIndex)
      if (leftValueIndex < 0) {
        swapHeapNode(index, leftIndex)
        fixHeap(leftIndex)
      } else if (rightValueIndex < 0) {
        swapHeapNode(index, rightIndex)
        fixHeap(rightIndex)
      } else {
        val rightWeight: Long = weights(rightValueIndex)
        val leftWeight: Long = weights(leftValueIndex)
        if (leftWeight < rightWeight) {
          if (currentWeight > leftWeight) {
            swapHeapNode(index, leftIndex)
            fixHeap(leftIndex)
          }
        } else {
          if (currentWeight > rightWeight) {
            swapHeapNode(index, rightIndex)
            fixHeap(rightIndex)
          }
        }
      }
    } else if (leftIndex < adjustedMax) {
      val leftValueIndex: Int = heap(leftIndex)
      if (leftValueIndex < 0) {
        swapHeapNode(index, leftIndex)
        fixHeap(leftIndex)
      } else {
        val leftWeights: Long = weights(leftValueIndex)
        if (currentWeight > leftWeights) {
          swapHeapNode(index, leftIndex)
          fixHeap(leftIndex)
        }
      }
    }
  }

  /**
   * Swaps two elements in `heap` array and maintain correct index in `heapIndex`.
   *
   * @param a index of first element
   * @param b index of second element
   */
  private def swapHeapNode(a: Int, b: Int): Unit = {
    if (heap(a) >= 0) {
      heapIndex(heap(a)) = b
    }
    if (heap(b) >= 0) {
      heapIndex(heap(b)) = a
    }
    val temp = heap(a)
    heap(a) = heap(b)
    heap(b) = temp
  }

  /**
   * Removes the current lowest hitter (can be a "pseudo" entry) from the hashmap and inserts the new entry. Then
   * it replaces the top of the heap (the removed entry) with the new entry and restores heap property.
   */
  private def insertKnownNewHeavy(hashCode: HashCodeVal, item: T, count: Long): Unit = {
    // Before we insert into the hashmap, remove the lowest hitter. It is guaranteed that we have a `count`
    // larger than `lowestHitterWeight` here, so we can safely remove the lowest hitter. Note, that this might be a
    // "pseudo" element (-1 entry) in the heap if we are not at full capacity.
    removeHash(lowestHitterIndex)
    val hashTableIndex = insert(hashCode, item, count)
    // Insert to the top of the heap.
    heap(0) = hashTableIndex
    heapIndex(hashTableIndex) = 0
    fixHeap(0)
  }

  /**
   * Remove value from hash-table based on position.
   */
  private def removeHash(index: Int): Unit = {
    if (index >= 0) {
      items(index) = null
      heapIndex(index) = -1
      hashes(index) = 0
      weights(index) = 0
    }
  }

  /**
   * Insert value in hash-table.
   *
   * Using open addressing for resolving collisions.
   * Initial index is reminder in division hashCode and table size.
   *
   * @param hashCode hashCode of item
   * @param item     value which should be added to hash-table
   * @param count    count associated to value
   * @return Index in hash-table where was inserted
   */
  private def insert(hashCode: HashCodeVal, item: T, count: Long): Int = {
    var index: Int = hashCode.get & mask
    while (items(index) != null) {
      index = (index + 1) & mask
    }
    hashes(index) = hashCode.get
    items(index) = item
    weights(index) = count
    index
  }

  /**
   * Weight of lowest heavy hitter, if a new inserted item has a weight greater than this, it is a heavy hitter.
   * This gets the index of the lowest heavy hitter from the top of the heap (lowestHitterIndex) if there is any
   * and looks up its weight.
   * If there is no entry in the table (lowestHitterIndex returns a negative index) this returns a zero weight.
   */
  def lowestHitterWeight: Long = {
    val index: Int = lowestHitterIndex
    if (index >= 0) {
      weights(index)
    } else {
      0
    }

  }

  /**
   * Returns the index to the hashmap slot that contains the element with the lowest recorded hit count. If the table
   * is empty, this returns a negative index.
   * Implemented by looking at the top of the heap and extracting the index which is O(1).
   */
  private def lowestHitterIndex: Int = {
    heap(0)
  }

  override def toString =
    s"${getClass.getSimpleName}(max:$max)"
}

/**
 * INTERNAL API
 */
private[remote] object TopHeavyHitters {

  /** Value class to avoid mixing up count and hashCode in APIs. */
  private[compress] final class HashCodeVal(val get: Int) extends AnyVal

}
