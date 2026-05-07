/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.nio.{ ByteBuffer, ByteOrder }

import scala.annotation.tailrec
import scala.collection.BufferedIterator
import scala.collection.LinearSeq
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import akka.util.Collections.EmptyImmutableSeq

import scala.annotation.nowarn

object ByteIterator {
  object ByteArrayIterator {

    protected[akka] def apply(array: Array[Byte]): ByteArrayIterator =
      new ByteArrayIterator(array, 0, array.length)

    protected[akka] def apply(array: Array[Byte], from: Int, until: Int): ByteArrayIterator =
      new ByteArrayIterator(array, from, until)

    val empty: ByteArrayIterator = apply(Array.emptyByteArray)
  }

  class ByteArrayIterator private (private var array: Array[Byte], private var from: Int, private var until: Int)
      extends ByteIterator {
    iterator =>

    @inline final def len: Int = until - from

    @inline final def hasNext: Boolean = from < until

    @inline final def head: Byte = array(from)

    final def next(): Byte = {
      if (!hasNext) EmptyImmutableSeq.iterator.next()
      else {
        val i = from; from = from + 1; array(i)
      }
    }

    def clear(): Unit = { this.array = Array.emptyByteArray; from = 0; until = from }

    final override def size: Int = { val l = len; clear(); l }

    final override def ++(that: IterableOnce[Byte]): ByteIterator = that match {
      case that: ByteIterator =>
        if (that.isEmpty) this
        else if (this.isEmpty) that
        else
          that match {
            case that: ByteArrayIterator =>
              if ((this.array eq that.array) && (this.until == that.from)) {
                this.until = that.until
                that.clear()
                this
              } else {
                val result = MultiByteArrayIterator(List(this, that))
                this.clear()
                result
              }
            case that: MultiByteArrayIterator => this ++: that
          }
      case _ => super.++(that)
    }

    final override def clone: ByteArrayIterator = new ByteArrayIterator(array, from, until)

    final override def take(n: Int): this.type = {
      if (n < len) until = { if (n > 0) (from + n) else from }
      this
    }

    final override def drop(n: Int): this.type = {
      if (n > 0) from = { if (n < len) (from + n) else until }
      this
    }

    final override def takeWhile(p: Byte => Boolean): this.type = {
      val prev = from
      dropWhile(p)
      until = from; from = prev
      this
    }

    final override def dropWhile(p: Byte => Boolean): this.type = {
      var stop = false
      while (!stop && hasNext) {
        if (p(array(from))) {
          from = from + 1
        } else {
          stop = true
        }
      }
      this
    }

    @nowarn("msg=deprecated")
    override def copyToArray[B >: Byte](xs: Array[B], start: Int): Int =
      this.copyToArray(xs, start, xs.length)

    @nowarn("msg=deprecated")
    override def copyToArray[B >: Byte](xs: Array[B]): Int =
      this.copyToArray(xs, 0, xs.length)

    final override def copyToArray[B >: Byte](xs: Array[B], start: Int, len: Int): Int = {
      val n = 0 max ((xs.length - start) min this.len min len)
      Array.copy(this.array, from, xs, start, n)
      this.drop(n)
      n
    }

    final override def toByteString: ByteString = {
      val result =
        if ((from == 0) && (until == array.length)) ByteString.ByteString1C(array)
        else ByteString.ByteString1(array, from, len)
      clear()
      result
    }

    @throws[NoSuchElementException]
    def getBytes(xs: Array[Byte], offset: Int, n: Int): this.type = {
      if (n <= this.len) {
        Array.copy(this.array, this.from, xs, offset, n)
        this.drop(n)
      } else throw new NoSuchElementException("next on empty iterator")
    }

    private def wrappedByteBuffer: ByteBuffer = ByteBuffer.wrap(array, from, len).asReadOnlyBuffer

    def getShorts(xs: Array[Short], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type = {
      wrappedByteBuffer.order(byteOrder).asShortBuffer.get(xs, offset, n); drop(2 * n)
    }

    def getInts(xs: Array[Int], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type = {
      wrappedByteBuffer.order(byteOrder).asIntBuffer.get(xs, offset, n); drop(4 * n)
    }

    def getLongs(xs: Array[Long], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type = {
      wrappedByteBuffer.order(byteOrder).asLongBuffer.get(xs, offset, n); drop(8 * n)
    }

    def getFloats(xs: Array[Float], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type = {
      wrappedByteBuffer.order(byteOrder).asFloatBuffer.get(xs, offset, n); drop(4 * n)
    }

    def getDoubles(xs: Array[Double], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type = {
      wrappedByteBuffer.order(byteOrder).asDoubleBuffer.get(xs, offset, n); drop(8 * n)
    }

    def copyToBuffer(buffer: ByteBuffer): Int = {
      val copyLength = math.min(buffer.remaining, len)
      if (copyLength > 0) {
        buffer.put(array, from, copyLength)
        drop(copyLength)
      }
      copyLength
    }

    def asInputStream: java.io.InputStream = new java.io.InputStream {
      override def available: Int = iterator.len

      def read: Int = if (hasNext) (next().toInt & 0xff) else -1

      override def read(b: Array[Byte], off: Int, len: Int): Int = {
        if ((off < 0) || (len < 0) || (off + len > b.length)) throw new IndexOutOfBoundsException
        if (len == 0) 0
        else if (!isEmpty) {
          val nRead = math.min(available(), len)
          copyToArray(b, off, nRead)
          nRead
        } else -1
      }

      override def skip(n: Long): Long = {
        val nSkip = math.min(iterator.len, n.toInt)
        iterator.drop(nSkip)
        nSkip
      }
    }
  }

  object MultiByteArrayIterator {
    protected val clearedList: List[ByteArrayIterator] = List(ByteArrayIterator.empty)

    val empty: MultiByteArrayIterator = new MultiByteArrayIterator(Nil)

    protected[akka] def apply(iterators: LinearSeq[ByteArrayIterator]): MultiByteArrayIterator =
      new MultiByteArrayIterator(iterators)
  }

  class MultiByteArrayIterator private (private var iterators: LinearSeq[ByteArrayIterator]) extends ByteIterator {
    // After normalization:
    // * iterators.isEmpty == false
    // * (!iterator.head.isEmpty || iterators.tail.isEmpty) == true
    private def normalize(): this.type = {
      @tailrec def norm(xs: LinearSeq[ByteArrayIterator]): LinearSeq[ByteArrayIterator] = {
        if (xs.isEmpty) MultiByteArrayIterator.clearedList
        else if (xs.head.isEmpty) norm(xs.tail)
        else xs
      }
      iterators = norm(iterators)
      this
    }
    normalize()

    @inline private def current: ByteArrayIterator = iterators.head
    @inline private def dropCurrent(): Unit = { iterators = iterators.tail }
    @inline def clear(): Unit = { iterators = MultiByteArrayIterator.empty.iterators }

    @inline final def hasNext: Boolean = current.hasNext

    @inline final def head: Byte = current.head

    final def next(): Byte = {
      val result = current.next()
      normalize()
      result
    }

    final override def len: Int = iterators.foldLeft(0) { _ + _.len }

    final override def size: Int = {
      val result = len
      clear()
      result
    }

    private[akka] def ++:(that: ByteArrayIterator): this.type = {
      iterators = that +: iterators
      this
    }

    final override def ++(that: IterableOnce[Byte]): ByteIterator = that match {
      case that: ByteIterator =>
        if (that.isEmpty) this
        else if (this.isEmpty) that
        else {
          that match {
            case that: ByteArrayIterator =>
              iterators = this.iterators :+ that
              that.clear()
              this
            case that: MultiByteArrayIterator =>
              iterators = this.iterators ++ that.iterators
              that.clear()
              this
          }
        }
      case _ => super.++(that)
    }

    final override def clone: MultiByteArrayIterator = {
      val clonedIterators: List[ByteArrayIterator] = iterators.iterator.map(_.clone).to(List)
      new MultiByteArrayIterator(clonedIterators)
    }

    /** For performance sensitive code, call take() directly on ByteString (it's optimised there) */
    final override def take(n: Int): this.type = {
      var rest = n
      val builder = new ListBuffer[ByteArrayIterator]
      while ((rest > 0) && !iterators.isEmpty) {
        current.take(rest)
        if (current.hasNext) {
          rest -= current.len
          builder += current
        }
        iterators = iterators.tail
      }
      iterators = builder.result()
      normalize()
    }

    /** For performance sensitive code, call drop() directly on ByteString (it's optimised there) */
    final override def drop(n: Int): this.type =
      if ((n > 0) && !isEmpty) {
        val nCurrent = math.min(n, current.len)
        current.drop(n)
        val rest = n - nCurrent
        assert(current.isEmpty || (rest == 0))
        normalize()
        drop(rest)
      } else this

    final override def takeWhile(p: Byte => Boolean): this.type = {
      var stop = false
      val builder = new ListBuffer[ByteArrayIterator]
      while (!stop && !iterators.isEmpty) {
        val lastLen = current.len
        current.takeWhile(p)
        if (current.hasNext) builder += current
        if (current.len < lastLen) stop = true
        dropCurrent()
      }
      iterators = builder.result()
      normalize()
    }

    @tailrec final override def dropWhile(p: Byte => Boolean): this.type =
      if (!isEmpty) {
        current.dropWhile(p)
        val dropMore = current.isEmpty
        normalize()
        if (dropMore) dropWhile(p) else this
      } else this

    final override def copyToArray[B >: Byte](xs: Array[B], start: Int, len: Int): Int = {
      var pos = start
      var rest = len
      while ((rest > 0) && !iterators.isEmpty && pos < xs.length) {
        val n = 0 max ((xs.length - pos) min current.len min rest)
        current.copyToArray(xs, pos, n)
        pos += n
        rest -= n
        if (current.isEmpty) {
          dropCurrent()
        }
      }
      normalize()
      len - rest
    }

    override def foreach[@specialized U](f: Byte => U): Unit = {
      iterators.foreach { _.foreach(f) }
      clear()
    }

    final override def toByteString: ByteString = {
      if (iterators.tail.isEmpty) iterators.head.toByteString
      else {
        val result = iterators.foldLeft(ByteString.empty) { _ ++ _.toByteString }
        clear()
        result
      }
    }

    @tailrec protected final def getToArray[A](xs: Array[A], offset: Int, n: Int, elemSize: Int)(getSingle: => A)(
        getMult: (Array[A], Int, Int) => Unit): this.type =
      if (n <= 0) this
      else {
        if (isEmpty) EmptyImmutableSeq.iterator.next()
        val nDone = if (current.len >= elemSize) {
          val nCurrent = math.min(n, current.len / elemSize)
          getMult(xs, offset, nCurrent)
          nCurrent
        } else {
          xs(offset) = getSingle
          1
        }
        normalize()
        getToArray(xs, offset + nDone, n - nDone, elemSize)(getSingle)(getMult)
      }

    def getBytes(xs: Array[Byte], offset: Int, n: Int): this.type =
      getToArray(xs, offset, n, 1) { getByte } { current.getBytes(_, _, _) }

    def getShorts(xs: Array[Short], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type =
      getToArray(xs, offset, n, 2) { getShort(byteOrder) } { current.getShorts(_, _, _)(byteOrder) }

    def getInts(xs: Array[Int], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type =
      getToArray(xs, offset, n, 4) { getInt(byteOrder) } { current.getInts(_, _, _)(byteOrder) }

    def getLongs(xs: Array[Long], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type =
      getToArray(xs, offset, n, 8) { getLong(byteOrder) } { current.getLongs(_, _, _)(byteOrder) }

    def getFloats(xs: Array[Float], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type =
      getToArray(xs, offset, n, 8) { getFloat(byteOrder) } { current.getFloats(_, _, _)(byteOrder) }

    def getDoubles(xs: Array[Double], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type =
      getToArray(xs, offset, n, 8) { getDouble(byteOrder) } { current.getDoubles(_, _, _)(byteOrder) }

    /** For performance sensitive code, call copyToBuffer() directly on ByteString (it's optimised there) */
    override def copyToBuffer(buffer: ByteBuffer): Int = {
      // the fold here is better than indexing into the LinearSeq
      val n = iterators.foldLeft(0) { _ + _.copyToBuffer(buffer) }
      normalize()
      n
    }

    def asInputStream: java.io.InputStream = new java.io.InputStream {
      override def available: Int = current.len

      def read: Int = if (hasNext) (next().toInt & 0xff) else -1

      override def read(b: Array[Byte], off: Int, len: Int): Int = {
        val nRead = current.asInputStream.read(b, off, len)
        normalize()
        nRead
      }

      override def skip(n: Long): Long = {
        @tailrec def skipImpl(n: Long, skipped: Long): Long =
          if (n > 0) {
            if (!isEmpty) {
              val m = current.asInputStream.skip(n)
              normalize()
              val newN = n - m
              val newSkipped = skipped + m
              if (newN > 0) skipImpl(newN, newSkipped)
              else newSkipped
            } else 0
          } else 0

        skipImpl(n, 0)
      }
    }
  }
}

/**
 * An iterator over a ByteString.
 */
abstract class ByteIterator extends BufferedIterator[Byte] {
  def len: Int

  def head: Byte

  def next(): Byte

  protected def clear(): Unit

  def ++(that: IterableOnce[Byte]): ByteIterator = {
    val it = that.iterator
    if (it.isEmpty) this
    else ByteIterator.ByteArrayIterator(it.toArray)
  }

  // *must* be overridden by derived classes. This construction is necessary
  // to specialize the return type, as the method is already implemented in
  // the parent class.
  override def clone: ByteIterator =
    throw new UnsupportedOperationException("Method clone is not implemented in ByteIterator")

  override def duplicate: (ByteIterator, ByteIterator) = (this, clone)

  // *must* be overridden by derived classes. This construction is necessary
  // to specialize the return type, as the method is already implemented in
  // the parent class.
  override def take(n: Int): this.type =
    throw new UnsupportedOperationException("Method take is not implemented in ByteIterator")

  // *must* be overridden by derived classes. This construction is necessary
  // to specialize the return type, as the method is already implemented in
  // the parent class.
  override def drop(n: Int): this.type =
    throw new UnsupportedOperationException("Method drop is not implemented in ByteIterator")

  override def slice(from: Int, until: Int): this.type = {
    if (from > 0) drop(from).take(until - from)
    else take(until)
  }

  // *must* be overridden by derived classes. This construction is necessary
  // to specialize the return type, as the method is already implemented in
  // the parent class.
  override def takeWhile(p: Byte => Boolean): this.type =
    throw new UnsupportedOperationException("Method takeWhile is not implemented in ByteIterator")

  // *must* be overridden by derived classes. This construction is necessary
  // to specialize the return type, as the method is already implemented in
  // the parent class.
  override def dropWhile(p: Byte => Boolean): this.type =
    throw new UnsupportedOperationException("Method dropWhile is not implemented in ByteIterator")

  override def span(p: Byte => Boolean): (ByteIterator, ByteIterator) = {
    val that = clone
    this.takeWhile(p)
    that.drop(this.len)
    (this, that)
  }

  override def indexWhere(p: Byte => Boolean, from: Int = 0): Int = {
    var index = 0
    while (index < from && hasNext) {
      next()
      index += 1
    }
    var found = false
    while (!found && hasNext) if (p(next())) {
      found = true
    } else {
      index += 1
    }
    if (found) index else -1
  }

  def indexOf(elem: Byte): Int = indexOf(elem, 0)
  def indexOf(elem: Byte, from: Int): Int = indexWhere(_ == elem, from)

  override def indexOf[B >: Byte](elem: B): Int = indexOf(elem, 0)
  override def indexOf[B >: Byte](elem: B, from: Int): Int = indexWhere(_ == elem, from)

  def toByteString: ByteString

  override def toSeq: ByteString = toByteString

  override def foreach[@specialized U](f: Byte => U): Unit =
    while (hasNext) f(next())

  override def foldLeft[@specialized B](z: B)(op: (B, Byte) => B): B = {
    var acc = z
    foreach { byte =>
      acc = op(acc, byte)
    }
    acc
  }

  override def toArray[B >: Byte](implicit arg0: ClassTag[B]): Array[B] = {
    val target = new Array[B](len)
    copyToArray(target)
    target
  }

  /**
   * Get a single Byte from this iterator. Identical to next().
   */
  def getByte: Byte = next()

  /**
   * Get a single Short from this iterator.
   */
  def getShort(implicit byteOrder: ByteOrder): Short = {
    if (byteOrder == ByteOrder.BIG_ENDIAN)
      ((next() & 0xff) << 8 | (next() & 0xff) << 0).toShort
    else if (byteOrder == ByteOrder.LITTLE_ENDIAN)
      ((next() & 0xff) << 0 | (next() & 0xff) << 8).toShort
    else throw new IllegalArgumentException("Unknown byte order " + byteOrder)
  }

  /**
   * Get a single Int from this iterator.
   */
  def getInt(implicit byteOrder: ByteOrder): Int = {
    if (byteOrder == ByteOrder.BIG_ENDIAN)
      ((next() & 0xff) << 24
      | (next() & 0xff) << 16
      | (next() & 0xff) << 8
      | (next() & 0xff) << 0)
    else if (byteOrder == ByteOrder.LITTLE_ENDIAN)
      ((next() & 0xff) << 0
      | (next() & 0xff) << 8
      | (next() & 0xff) << 16
      | (next() & 0xff) << 24)
    else throw new IllegalArgumentException("Unknown byte order " + byteOrder)
  }

  /**
   * Get a single Long from this iterator.
   */
  def getLong(implicit byteOrder: ByteOrder): Long = {
    if (byteOrder == ByteOrder.BIG_ENDIAN)
      ((next().toLong & 0xff) << 56
      | (next().toLong & 0xff) << 48
      | (next().toLong & 0xff) << 40
      | (next().toLong & 0xff) << 32
      | (next().toLong & 0xff) << 24
      | (next().toLong & 0xff) << 16
      | (next().toLong & 0xff) << 8
      | (next().toLong & 0xff) << 0)
    else if (byteOrder == ByteOrder.LITTLE_ENDIAN)
      ((next().toLong & 0xff) << 0
      | (next().toLong & 0xff) << 8
      | (next().toLong & 0xff) << 16
      | (next().toLong & 0xff) << 24
      | (next().toLong & 0xff) << 32
      | (next().toLong & 0xff) << 40
      | (next().toLong & 0xff) << 48
      | (next().toLong & 0xff) << 56)
    else throw new IllegalArgumentException("Unknown byte order " + byteOrder)
  }

  /**
   * Get a Long from this iterator where only the least significant `n`
   * bytes were encoded.
   */
  def getLongPart(n: Int)(implicit byteOrder: ByteOrder): Long = {
    if (byteOrder == ByteOrder.BIG_ENDIAN) {
      var x = 0L
      (1 to n).foreach(_ => x = (x << 8) | (next() & 0xff))
      x
    } else if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
      var x = 0L
      (0 until n).foreach(i => x |= (next() & 0xff) << 8 * i)
      x
    } else throw new IllegalArgumentException("Unknown byte order " + byteOrder)
  }

  def getFloat(implicit byteOrder: ByteOrder): Float =
    java.lang.Float.intBitsToFloat(getInt(byteOrder))

  def getDouble(implicit byteOrder: ByteOrder): Double =
    java.lang.Double.longBitsToDouble(getLong(byteOrder))

  /**
   * Get a specific number of Bytes from this iterator. In contrast to
   * copyToArray, this method will fail if this.len &lt; xs.length.
   */
  def getBytes(xs: Array[Byte]): this.type = getBytes(xs, 0, xs.length)

  /**
   * Get a specific number of Bytes from this iterator. In contrast to
   * copyToArray, this method will fail if length &lt; n or if (xs.length - offset) &lt; n.
   */
  def getBytes(xs: Array[Byte], offset: Int, n: Int): this.type

  /**
   * Get a specific number of Bytes from this iterator. In contrast to
   * copyToArray, this method will fail if this.len &lt; n.
   */
  def getBytes(n: Int): Array[Byte] = {
    val bytes = new Array[Byte](n)
    getBytes(bytes, 0, n)
    bytes
  }

  /**
   * Get a ByteString with specific number of Bytes from this iterator. In contrast to
   * copyToArray, this method will fail if this.len &lt; n.
   */
  def getByteString(n: Int): ByteString = {
    val bs = clone.take(n).toByteString
    drop(n)
    bs
  }

  /**
   * Get a number of Shorts from this iterator.
   */
  def getShorts(xs: Array[Short])(implicit byteOrder: ByteOrder): this.type =
    getShorts(xs, 0, xs.length)(byteOrder)

  /**
   * Get a number of Shorts from this iterator.
   */
  def getShorts(xs: Array[Short], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type

  /**
   * Get a number of Ints from this iterator.
   */
  def getInts(xs: Array[Int])(implicit byteOrder: ByteOrder): this.type =
    getInts(xs, 0, xs.length)(byteOrder)

  /**
   * Get a number of Ints from this iterator.
   */
  def getInts(xs: Array[Int], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type

  /**
   * Get a number of Longs from this iterator.
   */
  def getLongs(xs: Array[Long])(implicit byteOrder: ByteOrder): this.type =
    getLongs(xs, 0, xs.length)(byteOrder)

  /**
   * Get a number of Longs from this iterator.
   */
  def getLongs(xs: Array[Long], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type

  /**
   * Get a number of Floats from this iterator.
   */
  def getFloats(xs: Array[Float])(implicit byteOrder: ByteOrder): this.type =
    getFloats(xs, 0, xs.length)(byteOrder)

  /**
   * Get a number of Floats from this iterator.
   */
  def getFloats(xs: Array[Float], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type

  /**
   * Get a number of Doubles from this iterator.
   */
  def getDoubles(xs: Array[Double])(implicit byteOrder: ByteOrder): this.type =
    getDoubles(xs, 0, xs.length)(byteOrder)

  /**
   * Get a number of Doubles from this iterator.
   */
  def getDoubles(xs: Array[Double], offset: Int, n: Int)(implicit byteOrder: ByteOrder): this.type

  /**
   * Copy as many bytes as possible to a ByteBuffer, starting from it's
   * current position. This method will not overflow the buffer.
   *
   * @param buffer a ByteBuffer to copy bytes to
   * @return the number of bytes actually copied
   */
  /** For performance sensitive code, call take() directly on ByteString (it's optimised there) */
  def copyToBuffer(buffer: ByteBuffer): Int

  /**
   * Directly wraps this ByteIterator in an InputStream without copying.
   * Read and skip operations on the stream will advance the iterator
   * accordingly.
   */
  def asInputStream: java.io.InputStream
}
