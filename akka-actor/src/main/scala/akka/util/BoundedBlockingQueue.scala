/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.util.{ AbstractQueue, Collection, Iterator, Queue }
import java.util.concurrent.{ BlockingQueue, TimeUnit }
import java.util.concurrent.locks.{ Condition, ReentrantLock }

import annotation.tailrec

/**
 * BoundedBlockingQueue wraps any Queue and turns the result into a BlockingQueue with a limited capacity.
 * @param maxCapacity - the maximum capacity of this Queue, needs to be &gt; 0
 * @param backing - the backing Queue
 */
class BoundedBlockingQueue[E <: AnyRef](val maxCapacity: Int, private val backing: Queue[E])
    extends AbstractQueue[E]
    with BlockingQueue[E] {

  backing match {
    case null => throw new IllegalArgumentException("Backing Queue may not be null")
    case b: BlockingQueue[_] =>
      require(maxCapacity > 0)
      require(b.size() == 0)
      require(b.remainingCapacity >= maxCapacity)
    case b: Queue[_] =>
      require(b.size() == 0)
      require(maxCapacity > 0)
  }

  protected val lock = createLock()
  protected val notEmpty = createNotEmptyCondition()
  protected val notFull = createNotFullCondition()

  protected def createLock(): ReentrantLock = new ReentrantLock(false)
  protected def createNotEmptyCondition(): Condition = lock.newCondition()
  protected def createNotFullCondition(): Condition = lock.newCondition()

  def put(e: E): Unit = { //Blocks until not full
    if (e eq null) throw new NullPointerException
    lock.lockInterruptibly()

    try {
      @tailrec def putElement(): Unit = {
        if (backing.size() < maxCapacity) {
          require(backing.offer(e))
          notEmpty.signal()
        } else {
          notFull.await()
          putElement()
        }
      }
      putElement()
    } finally lock.unlock()
  }

  def take(): E = { //Blocks until not empty
    lock.lockInterruptibly()
    try {
      @tailrec def takeElement(): E = {
        if (!backing.isEmpty()) {
          val e = backing.poll()
          require(e ne null)
          notFull.signal()
          e
        } else {
          notEmpty.await()
          takeElement()
        }
      }
      takeElement()
    } finally lock.unlock()
  }

  def offer(e: E): Boolean = { //Tries to do it immediately, if fail return false
    if (e eq null) throw new NullPointerException
    lock.lock()
    try {
      if (backing.size() == maxCapacity) false
      else {
        require(backing.offer(e)) //Should never fail
        notEmpty.signal()
        true
      }
    } finally lock.unlock()
  }

  def offer(e: E, timeout: Long, unit: TimeUnit): Boolean = { //Tries to do it within the timeout, return false if fail
    if (e eq null) throw new NullPointerException
    lock.lockInterruptibly()
    try {
      @tailrec def offerElement(remainingNanos: Long): Boolean = {
        if (backing.size() < maxCapacity) {
          require(backing.offer(e)) //Should never fail
          notEmpty.signal()
          true
        } else if (remainingNanos <= 0) false
        else offerElement(notFull.awaitNanos(remainingNanos))
      }
      offerElement(unit.toNanos(timeout))
    } finally lock.unlock()
  }

  def poll(timeout: Long, unit: TimeUnit): E = { //Tries to do it within the timeout, returns null if fail
    lock.lockInterruptibly()
    try {
      @tailrec def pollElement(remainingNanos: Long): E = {
        backing.poll() match {
          case null if remainingNanos <= 0 => null.asInstanceOf[E]
          case null                        => pollElement(notEmpty.awaitNanos(remainingNanos))
          case e => {
            notFull.signal()
            e
          }
        }
      }
      pollElement(unit.toNanos(timeout))
    } finally lock.unlock()
  }

  def poll(): E = { //Tries to remove the head of the queue immediately, if fail, return null
    lock.lock()
    try {
      backing.poll() match {
        case null => null.asInstanceOf[E]
        case e =>
          notFull.signal()
          e
      }
    } finally lock.unlock()
  }

  override def remove(e: AnyRef): Boolean = { //Tries to do it immediately, if fail, return false
    if (e eq null) throw new NullPointerException
    lock.lock()
    try {
      if (backing.remove(e)) {
        notFull.signal()
        true
      } else false
    } finally lock.unlock()
  }

  override def contains(e: AnyRef): Boolean = {
    if (e eq null) throw new NullPointerException
    lock.lock()
    try backing.contains(e)
    finally lock.unlock()
  }

  override def clear(): Unit = {
    lock.lock()
    try {
      backing.clear()
      notFull.signalAll()
    } finally lock.unlock()
  }

  def remainingCapacity(): Int = {
    lock.lock()
    try {
      maxCapacity - backing.size()
    } finally lock.unlock()
  }

  def size(): Int = {
    lock.lock()
    try backing.size()
    finally lock.unlock()
  }

  def peek(): E = {
    lock.lock()
    try backing.peek()
    finally lock.unlock()
  }

  def drainTo(c: Collection[_ >: E]): Int = drainTo(c, Int.MaxValue)

  def drainTo(c: Collection[_ >: E], maxElements: Int): Int = {
    if (c eq null) throw new NullPointerException
    if (c eq this) throw new IllegalArgumentException
    if (c eq backing) throw new IllegalArgumentException
    if (maxElements <= 0) 0
    else {
      lock.lock()
      try {
        @tailrec def drainOne(n: Int = 0): Int = {
          if (n < maxElements) {
            backing.poll() match {
              case null => n
              case e    => c.add(e); drainOne(n + 1)
            }
          } else n
        }
        val n = drainOne()
        if (n > 0) notFull.signalAll()
        n
      } finally lock.unlock()
    }
  }

  override def containsAll(c: Collection[_]): Boolean = {
    lock.lock()
    try backing.containsAll(c)
    finally lock.unlock()
  }

  override def removeAll(c: Collection[_]): Boolean = {
    lock.lock()
    try {
      if (backing.removeAll(c)) {
        val sz = backing.size()
        if (sz < maxCapacity) notFull.signal()
        if (sz > 0) notEmpty.signal()
        true
      } else false
    } finally lock.unlock()
  }

  override def retainAll(c: Collection[_]): Boolean = {
    lock.lock()
    try {
      if (backing.retainAll(c)) {
        val sz = backing.size()
        if (sz < maxCapacity) notFull.signal()
        if (sz > 0) notEmpty.signal()
        true
      } else false
    } finally lock.unlock()
  }

  def iterator(): Iterator[E] = {
    lock.lock
    try {
      val elements = backing.toArray
      new Iterator[E] {
        var at = 0
        var last = -1

        def hasNext(): Boolean = at < elements.length

        def next(): E = {
          if (at >= elements.length) throw new NoSuchElementException
          last = at
          at += 1
          elements(last).asInstanceOf[E]
        }

        override def remove(): Unit = {
          if (last < 0) throw new IllegalStateException
          val target = elements(last)
          last = -1 //To avoid 2 subsequent removes without a next in between
          lock.lock()
          try {
            @tailrec def removeTarget(i: Iterator[E] = backing.iterator()): Unit =
              if (i.hasNext) {
                if (i.next eq target) {
                  i.remove()
                  notFull.signal()
                } else removeTarget(i)
              }

            removeTarget()
          } finally lock.unlock()
        }
      }
    } finally lock.unlock()
  }

  override def toArray(): Array[AnyRef] = {
    lock.lock()
    try backing.toArray
    finally lock.unlock()
  }

  override def isEmpty(): Boolean = {
    lock.lock()
    try backing.isEmpty()
    finally lock.unlock()
  }

  override def toArray[X](a: Array[X with AnyRef]) = {
    lock.lock()
    try backing.toArray[X](a)
    finally lock.unlock()
  }
}
