/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.{ AbstractQueue, Comparator, Iterator, PriorityQueue }
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong

/**
 * PriorityQueueStabilizer wraps a priority queue so that it respects FIFO for elements of equal priority.
 */
trait PriorityQueueStabilizer[E <: AnyRef] extends AbstractQueue[E] {
  val backingQueue: AbstractQueue[PriorityQueueStabilizer.WrappedElement[E]]
  val seqNum = new AtomicLong(0)

  override def peek(): E = {
    val wrappedElement = backingQueue.peek()
    if (wrappedElement eq null) null.asInstanceOf[E] else wrappedElement.element
  }

  override def size(): Int = backingQueue.size()

  override def offer(e: E): Boolean = {
    if (e eq null) throw new NullPointerException
    val wrappedElement = new PriorityQueueStabilizer.WrappedElement(e, seqNum.incrementAndGet)
    backingQueue.offer(wrappedElement)
  }

  override def iterator(): Iterator[E] = new Iterator[E] {
    private[this] val backingIterator = backingQueue.iterator()
    def hasNext: Boolean = backingIterator.hasNext
    def next(): E = backingIterator.next().element
    override def remove() = backingIterator.remove()
  }

  override def poll(): E = {
    val wrappedElement = backingQueue.poll()
    if (wrappedElement eq null) null.asInstanceOf[E] else wrappedElement.element
  }
}

object PriorityQueueStabilizer {
  class WrappedElement[E](val element: E, val seqNum: Long)
  class WrappedElementComparator[E](val cmp: Comparator[E]) extends Comparator[WrappedElement[E]] {
    def compare(e1: WrappedElement[E], e2: WrappedElement[E]): Int = {
      val baseComparison = cmp.compare(e1.element, e2.element)
      if (baseComparison != 0) baseComparison
      else {
        val diff = e1.seqNum - e2.seqNum
        java.lang.Long.signum(diff)
      }
    }
  }
}

/**
 * StablePriorityQueue is a priority queue that preserves order for elements of equal priority.
 * @param capacity - the initial capacity of this Queue, needs to be &gt; 0.
 * @param cmp - Comparator for comparing Queue elements
 */
class StablePriorityQueue[E <: AnyRef](capacity: Int, cmp: Comparator[E]) extends PriorityQueueStabilizer[E] {
  val backingQueue = new PriorityQueue[PriorityQueueStabilizer.WrappedElement[E]](
    capacity,
    new PriorityQueueStabilizer.WrappedElementComparator[E](cmp))
}

/**
 * StablePriorityBlockingQueue is a blocking priority queue that preserves order for elements of equal priority.
 * @param capacity - the initial capacity of this Queue, needs to be &gt; 0.
 * @param cmp - Comparator for comparing Queue elements
 */
class StablePriorityBlockingQueue[E <: AnyRef](capacity: Int, cmp: Comparator[E]) extends PriorityQueueStabilizer[E] {
  val backingQueue = new PriorityBlockingQueue[PriorityQueueStabilizer.WrappedElement[E]](
    capacity,
    new PriorityQueueStabilizer.WrappedElementComparator[E](cmp))
}
