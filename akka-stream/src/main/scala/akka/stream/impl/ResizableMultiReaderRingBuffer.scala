/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.annotation.tailrec
import scala.util.control.NoStackTrace

import ResizableMultiReaderRingBuffer.{ Cursor, Cursors, NothingToReadException }

import akka.annotation.InternalApi

/**
 * INTERNAL API
 * A mutable RingBuffer that can grow in size and supports multiple readers.
 * Contrary to many other ring buffer implementations this one does not automatically overwrite the oldest
 * elements, rather, if full, the buffer tries to grow and rejects further writes if max capacity is reached.
 */
@InternalApi private[akka] class ResizableMultiReaderRingBuffer[T](
    initialSize: Int, // constructor param, not field
    maxSize: Int, // constructor param, not field
    val cursors: Cursors) {
  require(
    Integer.lowestOneBit(maxSize) == maxSize && 0 < maxSize && maxSize <= Int.MaxValue / 2,
    "maxSize must be a power of 2 that is > 0 and < Int.MaxValue/2")
  require(
    Integer.lowestOneBit(initialSize) == initialSize && 0 < initialSize && initialSize <= maxSize,
    "initialSize must be a power of 2 that is > 0 and <= maxSize")

  private[this] val maxSizeBit = Integer.numberOfTrailingZeros(maxSize)
  private[this] var array = new Array[Any](initialSize)

  /*
   * two counters counting the number of elements ever written and read; wrap-around is
   * handled by always looking at differences or masked values
   */
  private[this] var writeIx = 0
  private[this] var readIx = 0 // the "oldest" of all read cursor indices, i.e. the one that is most behind

  // current array.length log2, we don't keep it as an extra field because `Integer.numberOfTrailingZeros`
  // is a JVM intrinsic compiling down to a `BSF` instruction on x86, which is very fast on modern CPUs
  private def lenBit: Int = Integer.numberOfTrailingZeros(array.length)

  // bit mask for converting a cursor into an array index
  private def mask: Int = Int.MaxValue >> (31 - lenBit)

  /**
   * The number of elements currently in the buffer.
   */
  def size: Int = writeIx - readIx

  def isEmpty: Boolean = size == 0

  def nonEmpty: Boolean = !isEmpty

  /**
   * The number of elements the buffer can still take without having to be resized.
   */
  def immediatelyAvailable: Int = array.length - size

  /**
   * The maximum number of elements the buffer can still take.
   */
  def maxAvailable: Int = (1 << maxSizeBit) - size

  /**
   * Returns the number of elements that the buffer currently contains for the given cursor.
   */
  def count(cursor: Cursor): Int = writeIx - cursor.cursor

  /**
   * Initializes the given Cursor to the oldest buffer entry that is still available.
   */
  def initCursor(cursor: Cursor): Unit = cursor.cursor = readIx

  /**
   * Tries to write the given value into the buffer thereby potentially growing the backing array.
   * Returns `true` if the write was successful and false if the buffer is full and cannot grow anymore.
   */
  def write(value: T): Boolean =
    if (size < array.length) { // if we have space left we can simply write and be done
      array(writeIx & mask) = value
      writeIx += 1
      true
    } else if (lenBit < maxSizeBit) { // if we are full but can grow we do so
      // the growing logic is quite simple: we assemble all current buffer entries in the new array
      // in their natural order (removing potential wrap around) and rebase all indices to zero
      val r = readIx & mask
      val newArray = new Array[Any](array.length << 1)
      System.arraycopy(array, r, newArray, 0, array.length - r)
      System.arraycopy(array, 0, newArray, array.length - r, r)
      @tailrec def rebaseCursors(remaining: List[Cursor]): Unit = remaining match {
        case head :: tail =>
          head.cursor -= readIx
          rebaseCursors(tail)
        case _ => // done
      }
      rebaseCursors(cursors.cursors)
      array = newArray
      val w = size
      array(w & mask) = value
      writeIx = w + 1
      readIx = 0
      true
    } else false

  /**
   * Tries to read from the buffer using the given Cursor.
   * If there are no more data to be read (i.e. the cursor is already
   * at writeIx) the method throws ResizableMultiReaderRingBuffer.NothingToReadException!
   */
  def read(cursor: Cursor): T = {
    val c = cursor.cursor
    if (c - writeIx < 0) {
      cursor.cursor += 1
      val ret = array(c & mask).asInstanceOf[T]
      if (c == readIx) updateReadIx()
      ret
    } else throw NothingToReadException
  }

  def onCursorRemoved(cursor: Cursor): Unit =
    if (cursor.cursor == readIx) // if this cursor is the last one it must be at readIx
      updateReadIx()

  private def updateReadIx(): Unit = {
    @tailrec def minCursor(remaining: List[Cursor], result: Int): Int =
      remaining match {
        case head :: tail => minCursor(tail, math.min(head.cursor - writeIx, result))
        case _            => result
      }
    val newReadIx = writeIx + minCursor(cursors.cursors, 0)
    while (readIx != newReadIx) {
      array(readIx & mask) = null
      readIx += 1
    }
  }

  protected def underlyingArray: Array[Any] = array

  override def toString: String =
    s"ResizableMultiReaderRingBuffer(size=$size, writeIx=$writeIx, readIx=$readIx, cursors=${cursors.cursors.size})"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ResizableMultiReaderRingBuffer {
  object NothingToReadException extends RuntimeException with NoStackTrace

  trait Cursors {
    def cursors: List[Cursor]
  }
  trait Cursor {
    def cursor: Int
    def cursor_=(ix: Int): Unit
  }
}
