/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import java.io.IOException
import java.lang.management.ManagementFactory
import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.duration.Duration.Zero
import scala.util.Random

import akka.stream._
import akka.stream.Attributes.inputBuffer
import akka.stream.impl.io.OutputStreamSourceStage
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.StreamConverters
import akka.stream.testkit._
import akka.stream.testkit.Utils._
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString

class OutputStreamSourceSpec extends StreamSpec(UnboundedMailboxConfig) {

  import system.dispatcher

  val timeout = 3.seconds
  val bytesArray = Array.fill[Byte](3)(Random.nextInt(1024).asInstanceOf[Byte])
  val byteString = ByteString(bytesArray)

  def expectTimeout[T](f: Future[T], timeout: Duration) =
    the[Exception] thrownBy Await.result(f, timeout) shouldBe a[TimeoutException]

  def expectSuccess[T](f: Future[T], value: T) =
    Await.result(f, remainingOrDefault) should be(value)

  def assertNoBlockedThreads(): Unit = {
    def threadsBlocked =
      ManagementFactory.getThreadMXBean
        .dumpAllThreads(true, true)
        .toSeq
        .filter(
          t =>
            t.getThreadName.startsWith("OutputStreamSourceSpec") &&
            t.getLockName != null &&
            t.getLockName.startsWith("java.util.concurrent.locks.AbstractQueuedSynchronizer") &&
            t.getStackTrace.exists(s => s.getClassName.startsWith(classOf[OutputStreamSourceStage].getName)))

    awaitAssert(threadsBlocked should ===(Seq()), 5.seconds, interval = 500.millis)
  }

  "OutputStreamSource" must {
    "read bytes from OutputStream" in {
      val (outputStream, probe) = StreamConverters.asOutputStream().toMat(TestSink[ByteString]())(Keep.both).run()
      val s = probe.expectSubscription()

      outputStream.write(bytesArray)
      s.request(1)
      probe.expectNext(byteString)
      outputStream.close()
      probe.expectComplete()
    }

    // https://github.com/akka/akka-core/issues/25983
    "not truncate the stream on close" in {
      for (_ <- 1 to 10) {
        val (outputStream, result) =
          StreamConverters
            .asOutputStream()
            .toMat(Sink.fold[ByteString, ByteString](ByteString.empty)(_ ++ _))(Keep.both)
            .run()
        outputStream.write(bytesArray)
        outputStream.close()
        result.futureValue should be(ByteString(bytesArray))
      }
    }

    "not block flushes when buffer is empty" in {
      val (outputStream, probe) = StreamConverters.asOutputStream().toMat(TestSink[ByteString]())(Keep.both).run()
      val s = probe.expectSubscription()

      outputStream.write(bytesArray)

      val f = Future(outputStream.flush())
      s.request(1)
      expectSuccess(f, ())
      probe.expectNext(byteString)

      val f2 = Future(outputStream.flush())
      expectSuccess(f2, ())

      outputStream.close()
      probe.expectComplete()
    }

    "block writes when buffer is full" in {
      val (outputStream, probe) = StreamConverters
        .asOutputStream()
        .toMat(TestSink[ByteString]())(Keep.both)
        .withAttributes(Attributes.inputBuffer(16, 16))
        .run()
      val s = probe.expectSubscription()

      (1 to 16).foreach { _ =>
        outputStream.write(bytesArray)
      }

      //blocked call
      val f = Future(outputStream.write(bytesArray))

      expectTimeout(f, timeout)
      probe.expectNoMessage(Zero)

      s.request(17)
      expectSuccess(f, ())
      probe.expectNextN(List.fill(17)(byteString).toSeq)

      outputStream.close()
      probe.expectComplete()
    }

    "throw error when write after stream is closed" in {
      val (outputStream, probe) = StreamConverters.asOutputStream().toMat(TestSink[ByteString]())(Keep.both).run()

      probe.expectSubscription()
      outputStream.close()
      probe.expectComplete()
      the[Exception] thrownBy outputStream.write(bytesArray) shouldBe a[IOException]
    }

    "throw IOException when writing to the stream after the subscriber has cancelled the reactive stream" in {
      val (outputStream, sink) = StreamConverters.asOutputStream().toMat(TestSink[ByteString]())(Keep.both).run()

      val s = sink.expectSubscription()

      outputStream.write(bytesArray)
      s.request(1)

      sink.expectNext(byteString)
      s.cancel()

      awaitAssert {
        the[Exception] thrownBy outputStream.write(bytesArray) shouldBe a[IOException]
      }
    }

    "fail to materialize with zero sized input buffer" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        StreamConverters.asOutputStream(timeout).withAttributes(inputBuffer(0, 0)).runWith(Sink.head)
        /*
             With Sink.head we test the code path in which the source
             itself throws an exception when being materialized. If
             Sink.ignore is used, the same exception is thrown by
             Materializer.
       */
      }
    }

    "not leave blocked threads" in {
      // make sure previous tests didn't leak
      assertNoBlockedThreads()

      val (_, probe) =
        StreamConverters.asOutputStream(timeout).toMat(TestSink[ByteString]())(Keep.both).run()

      val sub = probe.expectSubscription()

      // triggers a blocking read on the queue
      // and then cancel the stage before we got anything
      sub.request(1)
      sub.cancel()

      assertNoBlockedThreads()
    }

    "not leave blocked threads when materializer shutdown" in {
      val materializer2 = Materializer(system)
      val (_, probe) =
        StreamConverters.asOutputStream(timeout).toMat(TestSink[ByteString]())(Keep.both).run()(materializer2)

      val sub = probe.expectSubscription()

      // triggers a blocking read on the queue
      // and then shutdown the materializer before we got anything
      sub.request(1)
      materializer2.shutdown()

      assertNoBlockedThreads()
    }

    "correctly complete the stage after close" in {
      // actually this was a race, so it only happened in at least one of 20 runs

      val bufSize = 4
      val (outputStream, probe) = StreamConverters
        .asOutputStream(timeout)
        .addAttributes(Attributes.inputBuffer(bufSize, bufSize))
        .toMat(TestSink[ByteString]())(Keep.both)
        .run()

      // fill the buffer up
      (1 to (bufSize - 1)).foreach(outputStream.write)
      Future {
        outputStream.close()
      }
      // here is the race, has the elements reached the stage buffer yet?
      Thread.sleep(500)
      probe.request(bufSize - 1)
      probe.expectNextN(bufSize - 1)
      probe.expectComplete()
    }

  }
}
