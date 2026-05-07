/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import java.io.{ ByteArrayInputStream, InputStream }
import java.util.concurrent.CountDownLatch

import scala.annotation.nowarn
import scala.util.Success

import akka.Done
import akka.stream.{ AbruptStageTerminationException, ActorMaterializer, ActorMaterializerSettings, IOResult }
import akka.stream.scaladsl.{ Keep, Sink, StreamConverters }
import akka.stream.testkit._
import akka.stream.testkit.Utils._
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString

@nowarn
class InputStreamSourceSpec extends StreamSpec(UnboundedMailboxConfig) {

  val settings = ActorMaterializerSettings(system).withDispatcher("akka.actor.default-dispatcher")
  implicit val materializer: ActorMaterializer = ActorMaterializer(settings)

  private def inputStreamFor(bytes: Array[Byte]): InputStream =
    new ByteArrayInputStream(bytes)

  "InputStream Source" must {

    "read bytes from InputStream" in {
      val f =
        StreamConverters.fromInputStream(() => inputStreamFor(Array('a', 'b', 'c').map(_.toByte))).runWith(Sink.head)

      f.futureValue should ===(ByteString("abc"))
    }

    "record number of bytes read" in {
      StreamConverters
        .fromInputStream(() => inputStreamFor(Array(1, 2, 3)))
        .toMat(Sink.ignore)(Keep.left)
        .run()
        .futureValue shouldEqual IOResult(3, Success(Done))
    }

    "return fail if close fails" in {
      val fail = new RuntimeException("oh dear")
      StreamConverters
        .fromInputStream(() =>
          new InputStream {
            override def read(): Int = -1
            override def close(): Unit = throw fail
          })
        .toMat(Sink.ignore)(Keep.left)
        .run()
        .failed
        .futureValue
        .getCause shouldEqual fail
    }

    "return failure if creation fails" in {
      val fail = new RuntimeException("oh dear indeed")
      StreamConverters
        .fromInputStream(() => {
          throw fail
        })
        .toMat(Sink.ignore)(Keep.left)
        .run()
        .failed
        .futureValue
        .getCause shouldEqual fail
    }

    "handle failure on read" in {
      val fail = new RuntimeException("oh dear indeed")
      StreamConverters
        .fromInputStream(() => () => throw fail)
        .toMat(Sink.ignore)(Keep.left)
        .run()
        .failed
        .futureValue
        .getCause shouldEqual fail
    }

    "include number of bytes when downstream doesn't read all of it" in {
      val f = StreamConverters
        .fromInputStream(() => inputStreamFor(Array.fill(100)(1)), 1)
        .take(1) // stream is not completely read
        .toMat(Sink.ignore)(Keep.left)
        .run()
        .futureValue

      f.status shouldEqual Success(Done)
      f.count shouldBe >=(1L)
    }

    "handle actor materializer shutdown" in {
      val mat = ActorMaterializer()
      val source = StreamConverters.fromInputStream(() => inputStreamFor(Array(1, 2, 3)))
      val pubSink = Sink.asPublisher[ByteString](false)
      val (f, neverPub) = source.toMat(pubSink)(Keep.both).run()(mat)
      val c = TestSubscriber.manualProbe[ByteString]()
      neverPub.subscribe(c)
      c.expectSubscription()
      mat.shutdown()
      f.failed.futureValue shouldBe an[AbruptStageTerminationException]
    }

    "emit as soon as read" in {
      val latch = new CountDownLatch(1)
      val probe = StreamConverters
        .fromInputStream(
          () =>
            new InputStream {
              @volatile var emitted = false
              override def read(): Int = {
                if (!emitted) {
                  emitted = true
                  'M'.toInt
                } else {
                  latch.await()
                  -1
                }
              }
            },
          chunkSize = 1)
        .runWith(TestSink())

      probe.request(4)
      probe.expectNext(ByteString("M"))
      latch.countDown()
      probe.expectComplete()
    }
  }

}
