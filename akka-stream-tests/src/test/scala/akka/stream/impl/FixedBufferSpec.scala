/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import scala.annotation.nowarn

import akka.stream.ActorAttributes
import akka.stream.ActorAttributes.MaxFixedBufferSize
import akka.stream.ActorMaterializerSettings
import akka.stream.testkit.StreamSpec

@nowarn("msg=deprecated")
class FixedBufferSpec extends StreamSpec {

  for (size <- List(1, 3, 4)) {

    s"FixedSizeBuffer of size $size" must {

      "start as empty" in {
        val buf = FixedSizeBuffer(size)
        buf.isEmpty should be(true)
        buf.isFull should be(false)
      }

      "become nonempty after enqueueing" in {
        val buf = FixedSizeBuffer[String](size)
        buf.enqueue("test")
        buf.isEmpty should be(false)
        buf.isFull should be(size == 1)
      }

      "become full after size elements are enqueued" in {
        val buf = FixedSizeBuffer[String](size)
        for (_ <- 1 to size) buf.enqueue("test")
        buf.isEmpty should be(false)
        buf.isFull should be(true)
      }

      "become empty after enqueueing and tail drop" in {
        val buf = FixedSizeBuffer[String](size)
        buf.enqueue("test")
        buf.dropTail()
        buf.isEmpty should be(true)
        buf.isFull should be(false)
      }

      "become empty after enqueueing and head drop" in {
        val buf = FixedSizeBuffer[String](size)
        buf.enqueue("test")
        buf.dropHead()
        buf.isEmpty should be(true)
        buf.isFull should be(false)
      }

      "drop head properly" in {
        val buf = FixedSizeBuffer[Int](size)
        for (elem <- 1 to size) buf.enqueue(elem)
        buf.dropHead()
        for (elem <- 2 to size) buf.dequeue() should be(elem)
      }

      "drop tail properly" in {
        val buf = FixedSizeBuffer[Int](size)
        for (elem <- 1 to size) buf.enqueue(elem)
        buf.dropTail()
        for (elem <- 1 to size - 1) buf.dequeue() should be(elem)
      }

      "become non-full after tail dropped from full buffer" in {
        val buf = FixedSizeBuffer[String](size)
        for (_ <- 1 to size) buf.enqueue("test")
        buf.dropTail()
        buf.isEmpty should be(size == 1)
        buf.isFull should be(false)
      }

      "become non-full after head dropped from full buffer" in {
        val buf = FixedSizeBuffer[String](size)
        for (_ <- 1 to size) buf.enqueue("test")
        buf.dropHead()
        buf.isEmpty should be(size == 1)
        buf.isFull should be(false)
      }

      "peek shows head of queue" in {
        val buf = FixedSizeBuffer[Int](size)
        for (n <- 1 to size) {
          buf.enqueue(n)
          buf.peek() should ===(1)
        }
      }

      "work properly with full-range filling/draining cycles" in {
        val buf = FixedSizeBuffer[Int](size)

        for (_ <- 1 to 10) {
          buf.isEmpty should be(true)
          buf.isFull should be(false)
          for (elem <- 1 to size) buf.enqueue(elem)
          buf.isEmpty should be(false)
          buf.isFull should be(true)
          for (elem <- 1 to size) buf.dequeue() should be(elem)
        }
      }

      "work when indexes wrap around at Int.MaxValue" in {
        import language.reflectiveCalls
        val buf = FixedSizeBuffer[Int](size)

        try {
          val cheat = buf.asInstanceOf[{ def readIdx_=(l: Long): Unit; def writeIdx_=(l: Long): Unit }]
          cheat.readIdx_=(Int.MaxValue)
          cheat.writeIdx_=(Int.MaxValue)

          for (_ <- 1 to 10) {
            buf.isEmpty should be(true)
            buf.isFull should be(false)
            for (elem <- 1 to size) buf.enqueue(elem)
            buf.isEmpty should be(false)
            buf.isFull should be(true)
            for (elem <- 1 to size) buf.dequeue() should be(elem)
          }
        } catch {
          case _: NoSuchMethodException =>
          // our 'cheat' doesn't work on Scala 3
        }
      }

    }
  }

  "Buffer factory" must {
    val default = ActorMaterializerSettings(system).toAttributes

    "default to one billion for maxFixedBufferSize" in {
      default.mandatoryAttribute[MaxFixedBufferSize].size should ===(1000000000)
    }

    "produce BoundedBuffers when capacity > max-fixed-buffer-size" in {
      Buffer(Int.MaxValue, default) shouldBe a[BoundedBuffer[_]]
    }

    "produce FixedSizeBuffers when capacity < max-fixed-buffer-size" in {
      Buffer(1000, default) shouldBe a[FixedSizeBuffer.ModuloFixedSizeBuffer[_]]
      Buffer(1024, default) shouldBe a[FixedSizeBuffer.PowerOfTwoFixedSizeBuffer[_]]
    }

    "produce FixedSizeBuffers when max-fixed-buffer-size < BoundedBufferSize" in {
      val settings = default and ActorAttributes.maxFixedBufferSize(9)
      Buffer(5, settings) shouldBe a[FixedSizeBuffer.ModuloFixedSizeBuffer[_]]
      Buffer(10, settings) shouldBe a[FixedSizeBuffer.ModuloFixedSizeBuffer[_]]
      Buffer(16, settings) shouldBe a[FixedSizeBuffer.PowerOfTwoFixedSizeBuffer[_]]
    }

  }

}
