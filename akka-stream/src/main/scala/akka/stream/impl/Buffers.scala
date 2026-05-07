/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.{ util => ju }

import scala.collection.mutable

import akka.annotation.{ InternalApi, InternalStableApi }
import akka.stream._

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait Buffer[T] {
  def capacity: Int
  def used: Int
  def isFull: Boolean
  def isEmpty: Boolean
  def nonEmpty: Boolean

  def enqueue(elem: T): Unit
  def dequeue(): T

  def peek(): T
  def clear(): Unit
  def dropHead(): Unit
  def dropTail(): Unit
}

private[akka] object Buffer {
  val FixedQueueSize = 128
  val FixedQueueMask = 127

  def apply[T](size: Int, effectiveAttributes: Attributes): Buffer[T] =
    apply(size, effectiveAttributes.mandatoryAttribute[ActorAttributes.MaxFixedBufferSize].size)

  @InternalStableApi def apply[T](size: Int, max: Int): Buffer[T] =
    if (size < FixedQueueSize || size < max) FixedSizeBuffer(size)
    else new BoundedBuffer(size)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object FixedSizeBuffer {

  /**
   * INTERNAL API
   *
   * Returns a fixed size buffer backed by an array. The buffer implementation DOES NOT check against overflow or
   * underflow, it is the responsibility of the user to track or check the capacity of the buffer before enqueueing
   * dequeueing or dropping.
   *
   * Returns a specialized instance for power-of-two sized buffers.
   */
  @InternalStableApi private[akka] def apply[T](size: Int): FixedSizeBuffer[T] =
    if (size < 1) throw new IllegalArgumentException("size must be positive")
    else if (((size - 1) & size) == 0) new PowerOfTwoFixedSizeBuffer(size)
    else new ModuloFixedSizeBuffer(size)

  sealed abstract class FixedSizeBuffer[T](val capacity: Int) extends Buffer[T] {
    override def toString =
      s"Buffer($capacity, $readIdx, $writeIdx)(${(readIdx until writeIdx).map(get).mkString(", ")})"
    private val buffer = new Array[AnyRef](capacity)

    protected var readIdx = 0L
    protected var writeIdx = 0L
    def used: Int = (writeIdx - readIdx).toInt

    def isFull: Boolean = used == capacity
    def nonFull: Boolean = used < capacity
    def remainingCapacity: Int = capacity - used

    def isEmpty: Boolean = used == 0
    def nonEmpty: Boolean = used != 0

    def enqueue(elem: T): Unit = {
      put(writeIdx, elem, maintenance = false)
      writeIdx += 1
    }

    // for the maintenance parameter see dropHead
    protected def toOffset(idx: Long, maintenance: Boolean): Int

    private def put(idx: Long, elem: T, maintenance: Boolean): Unit =
      buffer(toOffset(idx, maintenance)) = elem.asInstanceOf[AnyRef]
    private def get(idx: Long): T = buffer(toOffset(idx, maintenance = false)).asInstanceOf[T]

    def peek(): T = get(readIdx)

    def dequeue(): T = {
      val result = get(readIdx)
      dropHead()
      result
    }

    def clear(): Unit = {
      java.util.Arrays.fill(buffer, null)
      readIdx = 0
      writeIdx = 0
    }

    def dropHead(): Unit = {
      /*
       * this is the only place where readIdx is advanced, so give ModuloFixedSizeBuffer
       * a chance to prevent its fatal wrap-around
       */
      put(readIdx, null.asInstanceOf[T], maintenance = true)
      readIdx += 1
    }

    def dropTail(): Unit = {
      writeIdx -= 1
      put(writeIdx, null.asInstanceOf[T], maintenance = false)
    }
  }

  private[akka] final class ModuloFixedSizeBuffer[T](_size: Int) extends FixedSizeBuffer[T](_size) {
    override protected def toOffset(idx: Long, maintenance: Boolean): Int = {
      if (maintenance && readIdx > Int.MaxValue) {
        /*
         * In order to be able to run perpetually we must ensure that the counters
         * don’t overrun into negative territory, so set them back by as many multiples
         * of the capacity as possible when both are above Int.MaxValue.
         */
        val shift = Int.MaxValue - (Int.MaxValue % capacity)
        readIdx -= shift
        writeIdx -= shift
      }
      (idx % capacity).toInt
    }
  }

  private[akka] final class PowerOfTwoFixedSizeBuffer[T](_size: Int) extends FixedSizeBuffer[T](_size) {
    private val Mask = capacity - 1
    override protected def toOffset(idx: Long, maintenance: Boolean): Int = idx.toInt & Mask
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class BoundedBuffer[T](val capacity: Int) extends Buffer[T] {

  import BoundedBuffer._

  def used: Int = q.used

  def isFull: Boolean = q.isFull

  def isEmpty: Boolean = q.isEmpty

  def nonEmpty: Boolean = q.nonEmpty

  def enqueue(elem: T): Unit = q.enqueue(elem)

  def dequeue(): T = q.dequeue()

  def peek(): T = q.peek()

  def clear(): Unit = q.clear()

  def dropHead(): Unit = q.dropHead()

  def dropTail(): Unit = q.dropTail()

  private var q: Buffer[T] = new FixedQueue[T](capacity, newBuffer => q = newBuffer)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object BoundedBuffer {
  private final class FixedQueue[T](override val capacity: Int, switchBuffer: Buffer[T] => Unit) extends Buffer[T] {
    import Buffer._

    private val queue = new Array[AnyRef](FixedQueueSize)
    private var head = 0
    private var tail = 0

    override def used = tail - head
    override def isFull = used == capacity
    override def isEmpty = tail == head
    override def nonEmpty = tail != head

    override def enqueue(elem: T): Unit =
      if (tail - head == FixedQueueSize) {
        val queue = new DynamicQueue[T](capacity)
        while (nonEmpty) {
          queue.enqueue(dequeue())
        }
        switchBuffer(queue)
        queue.enqueue(elem)
      } else {
        queue(tail & FixedQueueMask) = elem.asInstanceOf[AnyRef]
        tail += 1
      }
    override def dequeue(): T = {
      val pos = head & FixedQueueMask
      val ret = queue(pos).asInstanceOf[T]
      queue(pos) = null
      head += 1
      ret
    }

    override def peek(): T =
      if (tail == head) null.asInstanceOf[T]
      else queue(head & FixedQueueMask).asInstanceOf[T]
    override def clear(): Unit =
      while (nonEmpty) {
        dequeue()
      }
    override def dropHead(): Unit = dequeue()
    override def dropTail(): Unit = {
      tail -= 1
      queue(tail & FixedQueueMask) = null
    }
  }

  /**
   * INTERNAL API
   *
   * A buffer backed by a linked list, so as not to take up space when empty.
   */
  @InternalApi
  private[impl] final class DynamicQueue[T](override val capacity: Int) extends ju.LinkedList[T] with Buffer[T] {
    override def used = size
    override def isFull = size == capacity
    override def nonEmpty = !isEmpty()

    override def enqueue(elem: T): Unit = add(elem)
    override def dequeue(): T = remove()

    override def dropHead(): Unit = remove()
    override def dropTail(): Unit = removeLast()
  }
}

/**
 * INTERNAL API
 * Represents a buffer which has two parts: head and tail.  Elements are dequeued from the head and enqueued into
 * the head unless the head is full, in which case they are enqueued into the tail.  If there are elements in the
 * tail, dequeueing will, in addition to dequeueing from the head, dequeue from the tail and enqueue that element
 * into the head.
 *
 * It is also possible to arrange for a callback to execute whenever an element is enqueued into the head.
 *
 * This enables two potentially useful functionalities:
 * * One can use a fixed-size buffer for the head and a dynamic queue as the tail while treating it as one buffer
 * * One can use the head buffer as a collection of task slots to execute in some order
 */
@InternalApi
private[impl] final class ChainedBuffer[T](headBuffer: Buffer[T], tailBuffer: Buffer[T], onEnqueueToHead: T => Unit)
    extends Buffer[T] {
  def capacity: Int = headBuffer.capacity + tailBuffer.capacity
  def used: Int = headBuffer.used + tailBuffer.used
  def isFull: Boolean = tailBuffer.isFull
  def isEmpty: Boolean = headBuffer.isEmpty
  def nonEmpty: Boolean = headBuffer.nonEmpty

  def enqueue(elem: T): Unit =
    if (headBuffer.isFull) tailBuffer.enqueue(elem)
    else enqueueToHead(elem)

  def dequeue(): T = {
    val ret = headBuffer.dequeue()

    if (tailBuffer.nonEmpty) {
      val toHead = tailBuffer.dequeue()
      enqueueToHead(toHead)
    }

    ret
  }

  def peek(): T = headBuffer.peek()

  def clear(): Unit = {
    headBuffer.clear()
    tailBuffer.clear()
  }

  def dropHead(): Unit = dequeue()
  def dropTail(): Unit =
    if (tailBuffer.nonEmpty) tailBuffer.dropTail()
    else headBuffer.dropTail()

  private def enqueueToHead(elem: T): Unit = {
    headBuffer.enqueue(elem)
    onEnqueueToHead(elem)
  }
}

/**
 * INTERNAL API
 * Represents a buffer which also partitions its elements into sub-buffers by key.  The sub-buffers may be individually
 * dequeued without dequeueing from the overall buffer.  If an element A with partition key K is enqueued before
 * element B with the same partition key K, then the following invariants hold after B has been enqueued:
 *
 * * if element A has not been dequeued, element B has not been dequeued (i.e. normal queue ordering holds)
 * * if element B has been dequeued, element A has also been deqeued
 * * if element A has not been dequeued from its sub-buffer, element B has not been dequeued from that sub-buffer
 * * if element A has been dequeued, it was dequeued from the sub-buffer
 */
@InternalApi
private[impl] final class PartitionedBuffer[K, V](size: Int) {

  private val linearBuffer = Buffer[(K, V)](size, size)

  private val partitionBuffers: mutable.Map[K, Buffer[V]] = mutable.Map.empty

  def capacity: Int = linearBuffer.capacity
  def used: Int = linearBuffer.used
  def isFull: Boolean = linearBuffer.isFull
  def isEmpty: Boolean = linearBuffer.isEmpty
  def nonEmpty: Boolean = linearBuffer.nonEmpty

  def containsPartition(key: K): Boolean = partitionBuffers.contains(key)

  def enqueue(key: K, elem: V): Unit = {
    linearBuffer.enqueue(key -> elem)

    partitionBuffers.get(key) match {
      case Some(pbuf) => pbuf.enqueue(elem)
      case None =>
        throw new IllegalStateException(
          s"Enqueuing to key $key that does not have a buffer, make sure to addPartition for each key before enqueuing")
    }
  }

  def usedInPartition(key: K): Int =
    partitionBuffers.get(key) match {
      case Some(buffer) => buffer.used
      case None         => 0
    }

  def addPartition(key: K, buffer: Buffer[V]): Unit = {
    partitionBuffers += (key -> buffer)
  }

  def dequeue(): V = {
    val (key, value) = linearBuffer.dequeue()

    partitionBuffers.get(key) match {
      case Some(pbuf) =>
        // value could have been removed through dropOnlyPartitionHead
        if (pbuf.peek() == value) {
          pbuf.dropHead()
          if (pbuf.isEmpty) {
            partitionBuffers.remove(key)
          }
        }
      case None =>
    }

    value
  }

  def dropOnlyPartitionHead(key: K): Boolean =
    partitionBuffers.get(key) match {
      case Some(pbuf) =>
        pbuf.dropHead()
        if (pbuf.isEmpty) {
          partitionBuffers.remove(key)
        }
        true
      case None => false
    }

  def peek(): V = linearBuffer.peek()._2

  def peekPartition(key: K): Option[V] = {
    val pbuf = partitionBuffers.get(key)
    pbuf.map(_.peek())
  }

  def partitionHead(key: K): Option[V] = {
    val pbuf = partitionBuffers.get(key)
    pbuf.map(_.peek())
  }

  def clear(): Unit = {
    linearBuffer.clear()
    // ensure that all sub-buffers are cleared
    partitionBuffers.foreach {
      case (_, buf) => buf.clear()
    }
    partitionBuffers.clear()
  }

  def dropHead(): Unit =
    if (nonEmpty) {
      val (key, head) = linearBuffer.dequeue()

      partitionBuffers.get(key).foreach { pbuf =>
        if (pbuf.peek() == head) {
          pbuf.dropHead()
          if (pbuf.isEmpty) {
            partitionBuffers.remove(key)
          }
        }
      }
    }

  def dropTail(): Unit =
    // not entirely accurate, but this would require either a peekTail or a dequeue/enqueue cycle
    throw new UnsupportedOperationException("cannot drop tail of a partitioned buffer")

}
