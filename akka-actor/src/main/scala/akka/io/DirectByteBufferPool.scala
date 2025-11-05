/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.nio.ByteBuffer

import scala.util.control.NonFatal

trait BufferPool {
  def acquire(): ByteBuffer
  def release(buf: ByteBuffer): Unit
}

/**
 * INTERNAL API
 *
 * A buffer pool which keeps a free list of direct buffers of a specified default
 * size in a simple fixed size stack.
 *
 * If the stack is full the buffer is de-referenced and available to be
 * freed by normal garbage collection.
 *
 * Using a direct ByteBuffer when dealing with NIO operations has been proven
 * to be faster than wrapping on-heap Arrays. There is ultimately no performance
 * benefit to wrapping in-heap JVM data when writing with NIO.
 */
private[akka] class DirectByteBufferPool(defaultBufferSize: Int, maxPoolEntries: Int) extends BufferPool {
  private[this] val pool: Array[ByteBuffer] = new Array[ByteBuffer](maxPoolEntries)
  private[this] var buffersInPool: Int = 0

  def acquire(): ByteBuffer =
    takeBufferFromPool()

  def release(buf: ByteBuffer): Unit =
    offerBufferToPool(buf)

  private def allocate(size: Int): ByteBuffer =
    ByteBuffer.allocateDirect(size)

  private final def takeBufferFromPool(): ByteBuffer = {
    val buffer = pool.synchronized {
      if (buffersInPool > 0) {
        buffersInPool -= 1
        pool(buffersInPool)
      } else null
    }

    // allocate new and clear outside the lock
    if (buffer == null)
      allocate(defaultBufferSize)
    else {
      buffer.clear()
      buffer
    }
  }

  private final def offerBufferToPool(buf: ByteBuffer): Unit = {
    val clean =
      pool.synchronized {
        if (buffersInPool < maxPoolEntries) {
          pool(buffersInPool) = buf
          buffersInPool += 1
          false
        } else {
          // try to clean it outside the lock, or let the buffer be gc'd
          true
        }
      }
    if (clean)
      tryCleanDirectByteBuffer(buf)
  }

  private final def tryCleanDirectByteBuffer(toBeDestroyed: ByteBuffer): Unit =
    DirectByteBufferPool.tryCleanDirectByteBuffer(toBeDestroyed)
}

/** INTERNAL API */
private[akka] object DirectByteBufferPool {
  private val CleanDirectBuffer: ByteBuffer => Unit =
    try {
      val cleanerMethod = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner")
      cleanerMethod.setAccessible(true)

      val cleanMethod = Class.forName("sun.misc.Cleaner").getMethod("clean")
      cleanMethod.setAccessible(true)

      { (bb: ByteBuffer) =>
        try if (bb.isDirect) {
          val cleaner = cleanerMethod.invoke(bb)
          cleanMethod.invoke(cleaner)
        } catch { case NonFatal(_) => /* ok, best effort attempt to cleanup failed */ }
      }
    } catch { case NonFatal(_) => _ => () /* reflection failed, use no-op fallback */ }

  /**
   * DirectByteBuffers are garbage collected by using a phantom reference and a
   * reference queue. Every once a while, the JVM checks the reference queue and
   * cleans the DirectByteBuffers. However, as this doesn't happen
   * immediately after discarding all references to a DirectByteBuffer, it's
   * easy to OutOfMemoryError yourself using DirectByteBuffers. This function
   * explicitly calls the Cleaner method of a DirectByteBuffer.
   *
   * Utilizes reflection to avoid dependency to `sun.misc.Cleaner`.
   */
  def tryCleanDirectByteBuffer(byteBuffer: ByteBuffer): Unit = CleanDirectBuffer(byteBuffer)
}
