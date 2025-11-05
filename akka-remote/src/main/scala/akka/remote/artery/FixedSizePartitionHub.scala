/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import org.agrona.concurrent.OneToOneConcurrentArrayQueue

import akka.annotation.InternalApi
import akka.stream.scaladsl.PartitionHub

/**
 * INTERNAL API
 */
@InternalApi private[akka] class FixedSizePartitionHub[T](partitioner: T => Int, lanes: Int, bufferSize: Int)
    extends PartitionHub[T](
      // during tear down or restart it's possible that some streams have been removed
      // and then we must drop elements (return -1)
      () => (info, elem) => if (info.size < lanes) -1 else info.consumerIdByIdx(partitioner(elem)),
      lanes,
      bufferSize - 1) {
  // -1 because of the Completed token

  override def createQueue(): PartitionHub.Internal.PartitionQueue =
    new FixedSizePartitionQueue(lanes, bufferSize)

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class FixedSizePartitionQueue(lanes: Int, capacity: Int)
    extends PartitionHub.Internal.PartitionQueue {

  private val queues = {
    val arr = new Array[OneToOneConcurrentArrayQueue[AnyRef]](lanes)
    var i = 0
    while (i < arr.length) {
      arr(i) = new OneToOneConcurrentArrayQueue(capacity)
      i += 1
    }
    arr
  }

  override def init(id: Long): Unit = ()

  override def totalSize: Int = {
    var sum = 0
    var i = 0
    while (i < lanes) {
      sum += queues(i).size
      i += 1
    }
    sum
  }

  override def size(id: Long): Int =
    queues(id.toInt).size

  override def isEmpty(id: Long): Boolean =
    queues(id.toInt).isEmpty

  override def nonEmpty(id: Long): Boolean =
    !isEmpty(id)

  override def offer(id: Long, elem: Any): Unit = {
    if (!queues(id.toInt).offer(elem.asInstanceOf[AnyRef]))
      throw new IllegalStateException(s"queue is full, id [$id]")
  }

  override def poll(id: Long): AnyRef =
    queues(id.toInt).poll()

  override def remove(id: Long): Unit =
    queues(id.toInt).clear()

}
