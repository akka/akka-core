/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.stream.testkit.StreamSpec

class PartitionedBufferSpec extends StreamSpec {

  "PartitionedBuffer" must {
    "have capacity 4" in {
      val partitionBuffer = new PartitionedBuffer(4)
      partitionBuffer.capacity shouldBe 4
    }

    "partition enqueued elements" in {
      val partitionBuffer = new PartitionedBuffer[Int, Int](6)

      // odd/even
      partitionBuffer.addPartition(0, Buffer(2, 8))
      partitionBuffer.addPartition(1, Buffer(2, 8))

      (0 to 5).foreach(n => partitionBuffer.enqueue(n % 2, n))

      // last element enqueued for each partition
      partitionBuffer.peekPartition(0) should contain(4)
      partitionBuffer.peekPartition(1) should contain(5)

      // capacity is constant but buffer is full
      partitionBuffer.capacity should ===(6)
      partitionBuffer.isFull should ===(true)
    }

    "support dropHead'ing from partition sub-buffers" in {
      val partitionBuffer = new PartitionedBuffer[Int, Int](6)

      // odd/even
      partitionBuffer.addPartition(0, Buffer(2, 8))
      partitionBuffer.addPartition(1, Buffer(2, 8))

      (0 to 3).foreach(n => partitionBuffer.enqueue(n % 2, n))

      // this drops it from the per partition buffer, but keeps it
      // in the linear queue - use case is the MapAsyncPartitioned where
      // we want to move on with executing elems for the partition, but
      // keep the completed result for emitting once out is ready
      partitionBuffer.dropOnlyPartitionHead(0) should ===(true) // dropping 0
      partitionBuffer.dequeue() should ===(0) // elem still in linear buffer

      partitionBuffer.peekPartition(0) should contain(2)

      partitionBuffer.dropOnlyPartitionHead(1) should ===(true) // dropping 1
      partitionBuffer.dequeue() should ===(1) // elem still in linear buffer

      partitionBuffer.peekPartition(1) should contain(3)

      partitionBuffer.dropOnlyPartitionHead(0) should ===(true) // dropping 2
      partitionBuffer.peekPartition(0) shouldBe empty // nothing left in partition 0
    }

    "dequeue from partition sub-buffers when dequeueing" in {
      val partitionBuffer = new PartitionedBuffer[Int, Int](6)

      // odd/even
      partitionBuffer.addPartition(0, Buffer(4, 8))
      partitionBuffer.addPartition(1, Buffer(4, 8))

      (0 to 5).foreach(n => partitionBuffer.enqueue(n % 2, n))

      (0 to 4).foreach(n => partitionBuffer.dequeue() should ===(n))

      partitionBuffer.peek() should ===(5)

      partitionBuffer.peekPartition(0) shouldBe empty // nothing left in partition 0, we dequeued all
      partitionBuffer.peekPartition(1) should contain(5)
    }

    "clear sub-buffers when clearing" in {
      val partitionBuffer = new PartitionedBuffer[Int, Int](6)

      // odd/even
      partitionBuffer.addPartition(0, Buffer(2, 8))
      partitionBuffer.addPartition(1, Buffer(2, 8))

      partitionBuffer.enqueue(0, 0)
      partitionBuffer.enqueue(1, 1)
      partitionBuffer.clear()

      partitionBuffer.isEmpty shouldBe true
      partitionBuffer.peekPartition(0) shouldBe empty
      partitionBuffer.peekPartition(1) shouldBe empty
    }

    "report usage of partitions" in {
      val partitionBuffer = new PartitionedBuffer[Int, Int](6)

      // odd/even
      partitionBuffer.addPartition(0, Buffer(2, 8))
      partitionBuffer.addPartition(1, Buffer(2, 8))

      def sumOfPartitionUsed() =
        (0 to 1).map { partition =>
          partitionBuffer.usedInPartition(partition)
        }.sum

      partitionBuffer.usedInPartition(0) shouldBe 0
      partitionBuffer.usedInPartition(1) shouldBe 0
      sumOfPartitionUsed() shouldBe partitionBuffer.used // invariant

      partitionBuffer.enqueue(0, 0)

      partitionBuffer.usedInPartition(0) shouldBe 1
      sumOfPartitionUsed() shouldBe partitionBuffer.used // invariant

      partitionBuffer.enqueue(1, 1)

      partitionBuffer.usedInPartition(1) shouldBe 1
      sumOfPartitionUsed() shouldBe partitionBuffer.used // invariant

      partitionBuffer.dequeue()

      partitionBuffer.usedInPartition(0) shouldBe 0
      sumOfPartitionUsed() shouldBe partitionBuffer.used // invariant

      partitionBuffer.dequeue()

      partitionBuffer.usedInPartition(1) shouldBe 0
      sumOfPartitionUsed() shouldBe partitionBuffer.used // invariant
    }
  }
}
