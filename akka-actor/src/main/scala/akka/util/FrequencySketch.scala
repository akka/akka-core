/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.util.hashing.MurmurHash3

import akka.annotation.InternalApi

/**
 * INTERNAL API
 *
 * A frequency sketch for estimating the popularity of items. For implementing the TinyLFU cache admission policy.
 * The frequency sketch includes the TinyLFU reset operation, which periodically halves all counters.
 */
@InternalApi
private[akka] object FrequencySketch {

  /**
   * Create a new FrequencySketch based on the cache capacity (which will be increased to the nearest power of two).
   *
   * @param capacity the cache capacity (maximum items that will be cached)
   * @param widthMultiplier a multiplier for the width of the sketch
   * @param resetMultiplier the multiplier on the capacity for the reset size
   * @param depth the depth of count-min sketch (number of hash functions)
   * @param counterBits the size of the counters in bits: 2, 4, 8, 16, 32, or 64 bits
   * @param hasher the hash function for the element type
   * @return a configured FrequencySketch
   */
  def apply[A](
      capacity: Int,
      widthMultiplier: Int = 4,
      resetMultiplier: Double = 10,
      depth: Int = 4,
      counterBits: Int = 4)(implicit hasher: Hasher[A]): FrequencySketch[A] = {
    val width = widthMultiplier * Bits.ceilingPowerOfTwo(capacity)
    val resetSize = (resetMultiplier * capacity).toInt
    new FrequencySketch(depth, width, counterBits, resetSize, hasher)
  }

  sealed trait Hasher[A] {
    def hash(value: A): Int
  }

  object Hasher {
    final val DefaultSeed = 135283237

    implicit val StringHasher: StringHasher = new StringHasher(DefaultSeed)

    final class StringHasher(seed: Int) extends Hasher[String] {
      override def hash(value: String): Int = MurmurHash3.stringHash(value, seed)
    }
  }

  object Bits {
    def isPowerOfTwo(i: Int): Boolean = (i & (i - 1)) == 0

    def powerOfTwoExponent(i: Int): Int = 32 - Integer.numberOfLeadingZeros(i - 1)

    def ceilingPowerOfTwo(i: Int): Int = 1 << -Integer.numberOfLeadingZeros(i - 1)
  }
}

/**
 * INTERNAL API
 *
 * A frequency sketch for estimating the popularity of items. For implementing the TinyLFU cache admission policy.

 * This is a generalised frequency sketch with configurable depth (number of hash functions) and counter size.
 *
 * The matrix of counters is a two-dimensional array of longs, which each hold multiple counters depending on the
 * counter size (the number of bits for each counter). Powers of two are used to enable bit manipulation operations.
 *
 * The frequency sketch includes the TinyLFU reset operation, which periodically halves all counters, to allow
 * smaller counters to be used while retaining reasonable accuracy of relative frequencies.
 *
 * To get pairwise independent hash functions for the given depth, this implementation combines two hash functions
 * using the "Building a Better Bloom Filter" approach, where gi(x) = h1(x) + i * h2(x) mod p.
 *
 * References:
 *
 * "TinyLFU: A Highly Efficient Cache Admission Policy"
 * Gil Einziger, Roy Friedman, Ben Manes
 *
 * "An Improved Data Stream Summary: The Count-Min Sketch and its Applications"
 * Graham Cormode, S. Muthukrishnan
 *
 * "Less Hashing, Same Performance: Building a Better Bloom Filter"
 * Adam Kirsch, Michael Mitzenmacher
 *
 * @param depth depth of the count-min sketch (number of hash functions)
 * @param width width of the count-min sketch (number of counters)
 * @param counterBits the size of the counters in bits: 2, 4, 8, 16, 32, or 64 bits
 * @param resetSize the size (number of counter increments) to apply the reset operation
 * @param hasher the hash function for the element type
 */
@InternalApi
private[akka] final class FrequencySketch[A](
    depth: Int,
    width: Int,
    counterBits: Int,
    resetSize: Int,
    hasher: FrequencySketch.Hasher[A]) {

  require(FrequencySketch.Bits.isPowerOfTwo(width), "width must be a power of two")
  require(Set(2, 4, 8, 16, 32, 64)(counterBits), "counterBits must be 2, 4, 8, 16, 32, or 64 bits")

  private final val SlotBits = 64

  private[this] val counterWidth = counterBits
  private[this] val slots = SlotBits / counterWidth
  private[this] val rowWidth = math.max(1, width / slots)
  private[this] val columnMask = width - 1
  private[this] val slotShift = FrequencySketch.Bits.powerOfTwoExponent(slots)
  private[this] val slotMask = slots - 1
  private[this] val counterShift = FrequencySketch.Bits.powerOfTwoExponent(counterWidth)
  private[this] val counterMask = if (counterBits == 64) Long.MaxValue else (1L << counterWidth) - 1

  private[this] val oddMask = (1 to slots).foldLeft(1L)((mask, count) => mask | (1L << (count * counterWidth)))

  private[this] val resetMask = {
    val counterResetMask = counterMask >> 1
    (1 to slots).foldLeft(counterResetMask)((mask, count) => mask | (counterResetMask << (count * counterWidth)))
  }

  private[this] val matrix = Array.fill[Array[Long]](depth)(Array.ofDim[Long](rowWidth))
  private[this] val rowSizes = Array.ofDim[Int](depth)
  private[this] var updatedSize = 0

  /**
   * Get the current size of the sketch (the number of incremented counters).
   */
  def size: Int = updatedSize

  /**
   * Get the estimated frequency for a value. Limited by the maximum size of the counters.
   * Note that frequencies are also periodically halved as an aging mechanism.
   */
  def frequency(value: A): Int = {
    val hash1 = hasher.hash(value)
    val hash2 = rehash(hash1)
    var minCount = Int.MaxValue
    var row = 0
    while (row < depth) {
      val hash = hash1 + row * hash2
      minCount = Math.min(minCount, getCounter(row, hash))
      row += 1
    }
    minCount
  }

  /**
   * Increment the estimated frequency of a value. Limited by the maximum size of the counters.
   * Note that frequencies are also periodically halved as an aging mechanism.
   */
  def increment(value: A): Unit = {
    val hash1 = hasher.hash(value)
    val hash2 = rehash(hash1)
    var updated = false
    var row = 0
    while (row < depth) {
      val hash = hash1 + row * hash2
      updated |= incrementCounter(row, hash)
      row += 1
    }
    if (updated) {
      updatedSize += 1
      if (updatedSize == resetSize) reset()
    }
  }

  private def rehash(hash: Int): Int =
    MurmurHash3.finalizeHash(MurmurHash3.mixLast(hash, hash), 2)

  private def getCounter(row: Int, hash: Int): Int = {
    val column = (hash & columnMask) >>> slotShift
    val slot = (hash & slotMask) << counterShift
    ((matrix(row)(column) >>> slot) & counterMask).toInt
  }

  private def incrementCounter(row: Int, hash: Int): Boolean = {
    val column = (hash & columnMask) >>> slotShift
    val slot = (hash & slotMask) << counterShift
    val mask = counterMask << slot
    if ((matrix(row)(column) & mask) != mask) {
      matrix(row)(column) += (1L << slot)
      rowSizes(row) += 1
      true
    } else false
  }

  /**
   * The TinyLFU reset operation (periodically halving all counters).
   * Adjusts for truncation from integer division (bit shift for efficiency)
   * by adjusting for the number of odd counters per row (each off by 0.5).
   */
  private def reset(): Unit = {
    var row = 0
    while (row < depth) {
      var column = 0
      var odd = 0
      while (column < rowWidth) {
        odd += java.lang.Long.bitCount(matrix(row)(column) & oddMask)
        matrix(row)(column) = (matrix(row)(column) >>> 1) & resetMask
        column += 1
      }
      rowSizes(row) = (rowSizes(row) - odd) >>> 1
      row += 1
    }
    updatedSize = rowSizes.max
  }

  def toDebugString: String = FrequencySketchUtil.debugString(matrix, rowWidth, slots, counterWidth, counterMask)
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object FastFrequencySketch {

  /**
   * Create a new FastFrequencySketch based on the cache capacity (which will be increased to the nearest power of two).
   *
   * @param capacity the cache capacity (maximum items that will be cached)
   * @param widthMultiplier a multiplier for the width of the sketch
   * @param resetMultiplier the multiplier on the capacity for the reset size
   * @return a configured FastFrequencySketch
   */
  def apply[A](capacity: Int, widthMultiplier: Int = 4, resetMultiplier: Double = 10): FastFrequencySketch[A] = {
    val width = widthMultiplier * FrequencySketch.Bits.ceilingPowerOfTwo(capacity)
    val resetSize = (resetMultiplier * capacity).toInt
    new FastFrequencySketch(width, resetSize)
  }
}

/**
 * INTERNAL API
 *
 * A faster implementation of the frequency sketch (around twice as fast).

 * This frequency sketch uses a fixed depth (number of hash functions) of 4 and a counter size of 4 bits (0-15),
 * so that constants can be used for improved efficiency. It also uses its own rehashing of item hash codes.
 *
 * The implementation is inspired by the approach used in the Caffeine caching library:
 * https://github.com/ben-manes/caffeine
 *
 * @param width width of the count-min sketch (number of counters)
 * @param resetSize the size (number of counter increments) to apply the reset operation
 */
@InternalApi
private[akka] final class FastFrequencySketch[A](width: Int, resetSize: Int) {
  require(FrequencySketch.Bits.isPowerOfTwo(width), "width must be a power of two")

  private final val Depth = 4
  private final val SlotShift = 4
  private final val SlotMask = 0xF
  private final val CounterShift = 2
  private final val CounterMask = 0xFL
  private final val OddMask = 0x1111111111111111L
  private final val ResetMask = 0x7777777777777777L

  // seeds are large primes between 2^63 and 2^64
  private final val Seed0 = 0xC3A5C85C97CB3127L
  private final val Seed1 = 0xB492B66FBE98F273L
  private final val Seed2 = 0x9AE16A3B2F90404FL
  private final val Seed3 = 0xCBF29CE484222325L

  private[this] val rowWidth = math.max(1, width >>> SlotShift)
  private[this] val indexMask = width - 1

  private[this] val matrix = Array.fill[Array[Long]](Depth)(Array.ofDim[Long](rowWidth))
  private[this] val rowSizes = Array.ofDim[Int](Depth)
  private[this] var updatedSize = 0

  def size: Int = updatedSize

  def frequency(value: A): Int = {
    val hash = rehash(value.hashCode)
    var minCount = getCounter(row = 0, index(hash, Seed0))
    minCount = Math.min(minCount, getCounter(row = 1, index(hash, Seed1)))
    minCount = Math.min(minCount, getCounter(row = 2, index(hash, Seed2)))
    minCount = Math.min(minCount, getCounter(row = 3, index(hash, Seed3)))
    minCount
  }

  def increment(value: A): Unit = {
    val hash = rehash(value.hashCode)
    var updated = incrementCounter(row = 0, index(hash, Seed0))
    updated |= incrementCounter(row = 1, index(hash, Seed1))
    updated |= incrementCounter(row = 2, index(hash, Seed2))
    updated |= incrementCounter(row = 3, index(hash, Seed3))
    if (updated) {
      updatedSize += 1
      if (updatedSize == resetSize) reset()
    }
  }

  // A low-bias hash function found by Hash Function Prospector
  // https://github.com/skeeto/hash-prospector
  private def rehash(hash: Int): Int = {
    var x = hash
    x = ((x >>> 15) ^ x) * 0xd168aaad
    x = ((x >>> 15) ^ x) * 0xaf723597
    (x >>> 15) ^ x
  }

  private def index(hash: Int, seed: Long): Int = {
    val x = (hash + seed) * seed
    (x + (x >>> 32)).toInt & indexMask
  }

  private def getCounter(row: Int, index: Int): Int = {
    val column = index >>> SlotShift
    val slot = (index & SlotMask) << CounterShift
    ((matrix(row)(column) >>> slot) & CounterMask).toInt
  }

  private def incrementCounter(row: Int, index: Int): Boolean = {
    val column = index >>> SlotShift
    val slot = (index & SlotMask) << CounterShift
    val mask = CounterMask << slot
    if ((matrix(row)(column) & mask) != mask) {
      matrix(row)(column) += (1L << slot)
      rowSizes(row) += 1
      true
    } else false
  }

  private def reset(): Unit = {
    var row = 0
    while (row < 4) {
      var column = 0
      var odd = 0
      while (column < rowWidth) {
        odd += java.lang.Long.bitCount(matrix(row)(column) & OddMask)
        matrix(row)(column) = (matrix(row)(column) >>> 1) & ResetMask
        column += 1
      }
      rowSizes(row) = (rowSizes(row) - odd) >>> 1
      row += 1
    }
    updatedSize = rowSizes.max
  }

  def toDebugString: String =
    FrequencySketchUtil.debugString(matrix, rowWidth, slots = 16, counterWidth = 4, CounterMask)
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object FrequencySketchUtil {

  /**
   * Create a pretty table with all the frequency sketch counters for debugging (smaller) sketches.
   */
  def debugString(
      matrix: Array[Array[Long]],
      rowWidth: Int,
      slots: Int,
      counterWidth: Int,
      counterMax: Long): String = {
    def digits(n: Long): Int = math.floor(math.log10(n.toDouble)).toInt + 1
    val indexDigits = digits(rowWidth)
    val counterDigits = math.max(2, digits(counterMax))
    def divider(start: String, line: String, separator1: String, separator: String, end: String): String =
      start + (line * (indexDigits + 2)) + separator1 + (line * (counterDigits + 2)) +
      ((separator + (line * (counterDigits + 2))) * (slots - 1)) + end + "\n"
    val builder = new StringBuilder
    builder ++= divider("╔", "═", "╦", "╤", "╗")
    builder ++= "║" + (" " * (indexDigits + 2))
    for (slot <- 0 until slots) {
      builder ++= (if (slot == 0) "║" else "│")
      builder ++= s" %${counterDigits}d ".format(slot)
    }
    builder ++= "║\n"
    for (row <- matrix.indices) {
      for (column <- matrix(0).indices) {
        builder ++= (if (column == 0) divider("╠", "═", "╬", "╪", "╣")
                     else divider("╟", "─", "╫", "┼", "╢"))
        builder ++= s"║ %${indexDigits}d ".format(column)
        var shift = 0
        while (shift < 64) {
          val count = (matrix(row)(column) >>> shift) & counterMax
          builder ++= (if (shift == 0) "║" else "│")
          builder ++= s" %${counterDigits}d ".format(count)
          shift += counterWidth
        }
        builder ++= "║\n"
      }
    }
    builder ++= divider("╚", "═", "╩", "╧", "╝")
    builder.result()
  }
}
