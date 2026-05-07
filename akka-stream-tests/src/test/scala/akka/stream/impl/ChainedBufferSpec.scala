/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.annotation.tailrec

import org.scalatest.compatible.Assertion

import akka.stream.testkit.StreamSpec

class ChainedBufferSpec extends StreamSpec {
  val doNothing: Any => Unit = _ => ()

  Seq(1, 2, 8).foreach { headCapacity =>
    Seq(1, 2, 8).foreach { tailCapacity =>
      def buffers[T](callback: T => Unit = doNothing): (Buffer[T], Buffer[T], ChainedBuffer[T]) = {
        val headBuffer = FixedSizeBuffer[T](headCapacity)
        val tailBuffer = FixedSizeBuffer[T](tailCapacity)

        (headBuffer, tailBuffer, new ChainedBuffer(headBuffer, tailBuffer, callback))
      }

      s"ChainedBuffer of head-capacity $headCapacity and tail-capacity $tailCapacity" must {
        s"have capacity of $headCapacity + $tailCapacity" in {
          val (_, _, chainedBuffer) = buffers[Int]()

          chainedBuffer.capacity shouldBe (headCapacity + tailCapacity)
        }

        "report empty until first enqueue" in {
          val (_, _, chainedBuffer) = buffers[Int]()

          chainedBuffer.isEmpty shouldBe true

          chainedBuffer.enqueue(1)

          chainedBuffer.isEmpty shouldBe false
        }

        "not report full until capacity elements have been enqueued" in {
          val (_, _, chainedBuffer) = buffers[Int]()

          (0 until chainedBuffer.capacity).foreach { n =>
            assert(!chainedBuffer.isFull, s"buffer was full after $n enqueues")
            chainedBuffer.enqueue(n)
          }

          chainedBuffer.isFull shouldBe true
        }

        "not report empty unless as many drops as enqueues have been performed" in {
          val (_, _, chainedBuffer) = buffers[Int]()
          var enqueues = 0
          var drops = 0

          @tailrec
          def iter: Assertion = {
            assert(enqueues >= drops, "BAD TEST: should not drop more than enqueue!")

            if (enqueues == drops) {
              assert(chainedBuffer.isEmpty, s"buffer was not empty after $enqueues enqueues and $drops drops")
            } else {
              assert(!chainedBuffer.isEmpty, s"buffer was empty after $enqueues enqueues and $drops drops")
            }

            if (enqueues > 16 && drops > 16) succeed
            else {
              if (chainedBuffer.isEmpty || (!chainedBuffer.isFull && scala.util.Random.nextBoolean())) {
                chainedBuffer.enqueue(enqueues)
                enqueues += 1
              } else {
                if (scala.util.Random.nextBoolean()) chainedBuffer.dropHead()
                else chainedBuffer.dropTail()

                drops += 1
              }

              iter
            }
          }

          iter
        }

        "drop head" in {
          var hitWith = -1
          val (head, tail, chainedBuffer) = buffers[Int] {
            hitWith = _
          }
          (0 until chainedBuffer.capacity).foreach(chainedBuffer.enqueue)
          hitWith = -1
          chainedBuffer.isFull shouldBe true

          chainedBuffer.dropHead()

          hitWith shouldBe headCapacity
          chainedBuffer.isFull shouldBe false
          assert(head.isFull, "head buffer should still be full")
          assert(!tail.isFull, "tail buffer should not be full")
          (1 until chainedBuffer.capacity).foreach { i =>
            chainedBuffer.dequeue() shouldBe i
          }
          chainedBuffer.isEmpty shouldBe true
        }

        "drop tail" in {
          var hitWith = -1
          val (head, tail, chainedBuffer) = buffers[Int] {
            hitWith = _
          }
          (0 until chainedBuffer.capacity).foreach(chainedBuffer.enqueue)
          hitWith = -1
          chainedBuffer.isFull shouldBe true

          chainedBuffer.dropTail()

          hitWith shouldBe -1
          chainedBuffer.isFull shouldBe false
          assert(head.isFull, "head buffer should still be full")
          assert(!tail.isFull, "tail buffer should not be full")
          (0 until chainedBuffer.capacity - 1).foreach { i =>
            chainedBuffer.dequeue() shouldBe i
          }
          chainedBuffer.isEmpty shouldBe true
        }

        "dequeue" in {
          var sum = 0
          val (head, _, chainedBuffer) = buffers[Int] {
            sum += _
          }
          sum shouldBe 0
          val numEnqueues = 1 + scala.util.Random.nextInt(chainedBuffer.capacity)
          (1 to numEnqueues).foreach(chainedBuffer.enqueue)

          // sum of 1 .. numEnqueues
          val allSum = (numEnqueues * (numEnqueues + 1) / 2)

          if (numEnqueues < headCapacity) {
            head.isFull shouldBe false
            sum shouldBe allSum
          } else {
            head.isFull shouldBe true
            sum shouldBe (headCapacity * (headCapacity + 1) / 2)
          }

          var preSum = sum
          (1 to numEnqueues).foreach { i =>
            chainedBuffer.dequeue() shouldBe i
            if (i > (numEnqueues - headCapacity)) {
              sum shouldBe allSum
            } else {
              sum shouldBe preSum + (i + headCapacity)
              preSum = sum
            }
          }
        }
      }
    }
  }
}
