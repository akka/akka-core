/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery
package tcp

import scala.util.Random

import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.util.ByteString

class TcpFramingSpec extends AkkaSpec("""
    akka.stream.materializer.debug.fuzzing-mode = on
  """) with ImplicitSender {
  import TcpFraming.encodeFrameHeader

  private val maxFrameLength = 100
  private val framingFlow = Flow[ByteString].via(new TcpFraming(maxFrameLength))

  private val payload5 = ByteString((1 to 5).map(_.toByte).toArray)

  private def frameBytes(numberOfFrames: Int): ByteString =
    (1 to numberOfFrames).foldLeft(ByteString.empty)((acc, _) => acc ++ encodeFrameHeader(payload5.size) ++ payload5)

  private val rndSeed = System.currentTimeMillis()
  private val rnd = new Random(rndSeed)

  private def rechunk(bytes: ByteString): Iterator[ByteString] = {
    var remaining = bytes
    new Iterator[ByteString] {
      override def hasNext: Boolean = remaining.nonEmpty

      override def next(): ByteString = {
        val chunkSize = rnd.nextInt(remaining.size) + 1 // no 0 length frames
        val chunk = remaining.take(chunkSize)
        remaining = remaining.drop(chunkSize)
        chunk
      }
    }
  }

  "TcpFraming stage" must {

    "grab streamId from connection header" in {
      val bytes = TcpFraming.encodeConnectionHeader(2) ++ frameBytes(1)
      val frames = Source(List(bytes)).via(framingFlow).runWith(Sink.seq).futureValue
      frames.head.streamId should ===(2)
    }

    "grab streamId from connection header in single chunk" in {
      val frames =
        Source(List(TcpFraming.encodeConnectionHeader(1), frameBytes(1))).via(framingFlow).runWith(Sink.seq).futureValue
      frames.head.streamId should ===(1)
    }

    "reject invalid magic" in {
      val bytes = frameBytes(2)
      val fail = Source(List(bytes)).via(framingFlow).runWith(Sink.seq).failed.futureValue
      fail shouldBe a[FramingException]
    }

    "include streamId in each frame" in {
      val bytes = TcpFraming.encodeConnectionHeader(3) ++ frameBytes(3)
      val frames = Source(List(bytes)).via(framingFlow).runWith(Sink.seq).futureValue
      frames(0).streamId should ===(3)
      frames(1).streamId should ===(3)
      frames(2).streamId should ===(3)
    }

    "parse frames from random chunks" in {
      val numberOfFrames = 100
      val bytes = TcpFraming.encodeConnectionHeader(3) ++ frameBytes(numberOfFrames)
      withClue(s"Random chunks seed: $rndSeed") {
        val frames = Source.fromIterator(() => rechunk(bytes)).via(framingFlow).runWith(Sink.seq).futureValue
        frames.size should ===(numberOfFrames)
        frames.foreach { frame =>
          frame.byteBuffer.limit() should ===(payload5.size)
          val payload = new Array[Byte](frame.byteBuffer.limit())
          frame.byteBuffer.get(payload)
          ByteString(payload) should ===(payload5)
          frame.streamId should ===(3)
        }
      }
    }

    "report truncated frames" in {
      val bytes = TcpFraming.encodeConnectionHeader(3) ++ frameBytes(3).drop(1)
      Source(List(bytes)).via(framingFlow).runWith(Sink.seq).failed.futureValue shouldBe a[FramingException]
    }

    "work with empty stream" in {
      val frames = Source.empty.via(framingFlow).runWith(Sink.seq).futureValue
      frames.size should ===(0)
    }

    "reject too long frame" in {
      val payload = ByteString((1 to maxFrameLength + 1).map(_.toByte).toArray)
      val bytes = TcpFraming.encodeConnectionHeader(3) ++ encodeFrameHeader(payload.size) ++ payload
      val failed = Source(List(bytes)).via(framingFlow).runWith(Sink.seq).failed.futureValue
      failed shouldBe a[FramingException]
      failed.getMessage should startWith("Too long frame")
    }

  }

}
