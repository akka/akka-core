/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.nio.ByteOrder
import java.util.concurrent.ThreadLocalRandom

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import akka.stream._
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.stage._
import akka.stream.stage.GraphStage
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.util.ByteString
import akka.util.ByteStringBuilder

class FramingSpec extends StreamSpec {

  class Rechunker extends GraphStage[FlowShape[ByteString, ByteString]] {

    val out: Outlet[ByteString] = Outlet("Rechunker.out")
    val in: Inlet[ByteString] = Inlet("Rechunker.in")

    override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {

        private var rechunkBuffer = ByteString.empty

        private def rechunk() = {
          if (!isClosed(in) && ThreadLocalRandom.current().nextBoolean()) pull(in)
          else {
            val nextChunkSize =
              if (rechunkBuffer.isEmpty) 0
              else ThreadLocalRandom.current().nextInt(0, rechunkBuffer.size + 1)
            val newChunk = rechunkBuffer.take(nextChunkSize).compact
            rechunkBuffer = rechunkBuffer.drop(nextChunkSize).compact
            if (isClosed(in) && rechunkBuffer.isEmpty) {
              push(out, newChunk)
              completeStage()
            } else push(out, newChunk)
          }
        }

        override def onPush(): Unit = {
          rechunkBuffer ++= grab(in)
          rechunk()
        }

        override def onPull(): Unit = {
          rechunk()
        }

        override def onUpstreamFinish(): Unit = {
          if (rechunkBuffer.isEmpty) completeStage()
          else if (isAvailable(out))
            onPull()
        }

        setHandlers(in, out, this)
      }
  }

  val rechunk = Flow[ByteString].via(new Rechunker).named("rechunker")

  override def expectedTestDuration = 2.minutes

  "Delimiter bytes based framing" must {

    val delimiterBytes = List("\n", "\r\n", "FOO").map(ByteString(_))
    val baseTestSequences = List("", "foo", "hello world").map(ByteString(_))

    // Helper to simplify testing
    def simpleLines(delimiter: String, maximumBytes: Int, allowTruncation: Boolean = true) =
      Framing.delimiter(ByteString(delimiter), maximumBytes, allowTruncation).map(_.utf8String).named("lineFraming")

    def completeTestSequences(delimiter: ByteString): immutable.Iterable[ByteString] =
      for (prefix <- delimiter.indices; s <- baseTestSequences)
        yield delimiter.take(prefix) ++ s

    "work with various delimiters and test sequences" in {
      import system.dispatcher
      val resultFutures = for {
        delimiter <- delimiterBytes
        _ <- 1 to 5
      } yield {
        val testSequence = completeTestSequences(delimiter)
        val f =
          Source(testSequence).map(_ ++ delimiter).via(rechunk).via(Framing.delimiter(delimiter, 256)).runWith(Sink.seq)

        f.map(result => (result, testSequence, delimiter))
      }

      val futureResults = Future.sequence(resultFutures)
      futureResults.futureValue.foreach {
        case (result, expected, delimiter) =>
          withClue(s"delimiter: $delimiter") {
            result should ===(expected)
          }
      }

    }

    "Respect maximum line settings" in {
      // The buffer will contain more than 1 bytes, but the individual frames are less
      Source
        .single(ByteString("a\nb\nc\nd\n"))
        .via(simpleLines("\n", 1))
        .limit(100)
        .runWith(Sink.seq)
        .futureValue should ===(List("a", "b", "c", "d"))

      Source
        .single(ByteString("ab\n"))
        .via(simpleLines("\n", 1))
        .limit(100)
        .runWith(Sink.seq)
        .failed
        .futureValue shouldBe a[FramingException]

      Source
        .single(ByteString("aaa"))
        .via(simpleLines("\n", 2))
        .limit(100)
        .runWith(Sink.seq)
        .failed
        .futureValue shouldBe a[FramingException]

    }

    "work with empty streams" in {
      Source.empty.via(simpleLines("\n", 256)).runFold(Vector.empty[String])(_ :+ _).futureValue should ===(
        Vector.empty)
    }

    "report truncated frames" in {
      Source
        .single(ByteString("I have no end"))
        .via(simpleLines("\n", 256, allowTruncation = false))
        .grouped(1000)
        .runWith(Sink.head)
        .failed
        .futureValue shouldBe a[FramingException]
    }

    "allow truncated frames if configured so" in {
      Source
        .single(ByteString("I have no end"))
        .via(simpleLines("\n", 256, allowTruncation = true))
        .grouped(1000)
        .runWith(Sink.head)
        .futureValue should ===(List("I have no end"))
    }

  }

  "Length field based framing" must {

    val referenceChunk = ByteString(scala.util.Random.nextString(0x100001))

    val byteOrders = List(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)
    val frameLengths = List(0, 1, 2, 3, 0xFF, 0x100, 0x101, 0xFFF, 0x1000, 0x1001, 0xFFFF, 0x10000, 0x10001)
    val fieldLengths = List(1, 2, 3, 4)
    val fieldOffsets = List(0, 1, 2, 3, 15, 16, 31, 32, 44, 107)

    def encode(payload: ByteString, fieldOffset: Int, fieldLength: Int, byteOrder: ByteOrder): ByteString = {
      encodeComplexFrame(payload, fieldLength, byteOrder, ByteString(new Array[Byte](fieldOffset)), ByteString.empty)
    }

    def encodeComplexFrame(
        payload: ByteString,
        fieldLength: Int,
        byteOrder: ByteOrder,
        offset: ByteString,
        tail: ByteString): ByteString = {
      val header = {
        val h = (new ByteStringBuilder).putInt(payload.size)(byteOrder).result()
        byteOrder match {
          case ByteOrder.LITTLE_ENDIAN => h.take(fieldLength)
          case ByteOrder.BIG_ENDIAN    => h.drop(4 - fieldLength)
          case unexpected              => throw new RuntimeException(s"Unexpected: $unexpected")
        }
      }
      offset ++ header ++ payload ++ tail
    }

    "work with various byte orders, frame lengths and offsets" in {
      import system.dispatcher
      val resultFutures = for {
        byteOrder <- byteOrders
        lengthFieldOffset <- fieldOffsets
        fieldLength <- fieldLengths
      } yield {

        val encodedFrames = frameLengths.filter(_ < (1L << (fieldLength * 8))).map { length =>
          val payload = referenceChunk.take(length)
          encode(payload, lengthFieldOffset, fieldLength, byteOrder)
        }

        Source(encodedFrames)
          .via(rechunk)
          .via(Framing.lengthField(fieldLength, lengthFieldOffset, Int.MaxValue, byteOrder))
          .grouped(10000)
          .runWith(Sink.head)
          .map(result => (result, encodedFrames, (byteOrder, lengthFieldOffset, fieldLength)))

      }

      val futureResults = Future.sequence(resultFutures)
      futureResults.futureValue.foreach {
        case (result, expected, (byteOrder, fieldOffset, fieldLength)) =>
          withClue(s"byteOrder: $byteOrder, fieldOffset: $fieldOffset, fieldLength: $fieldLength") {
            result should ===(expected)
          }
      }

    }

    "work with various byte orders, frame lengths and offsets using computeFrameSize" in {
      import system.dispatcher
      val resultFutures = for {
        byteOrder <- byteOrders
        fieldOffset <- fieldOffsets
        fieldLength <- fieldLengths
      } yield {

        def computeFrameSize(offset: Array[Byte], length: Int): Int = {
          val sizeWithoutTail = offset.length + fieldLength + length
          if (offset.length > 0) offset(0) + sizeWithoutTail else sizeWithoutTail
        }

        def offset(): Array[Byte] = {
          val arr = new Array[Byte](fieldOffset)
          if (arr.length > 0) arr(0) = Random.nextInt(128).toByte
          arr
        }

        val encodedFrames = frameLengths.filter(_ < (1L << (fieldLength * 8))).map { length =>
          val payload = referenceChunk.take(length)
          val offsetBytes = offset()
          val tailBytes = if (offsetBytes.length > 0) new Array[Byte](offsetBytes(0)) else Array.empty[Byte]
          encodeComplexFrame(payload, fieldLength, byteOrder, ByteString(offsetBytes), ByteString(tailBytes))
        }

        Source(encodedFrames)
          .via(rechunk)
          .via(Framing.lengthField(fieldLength, fieldOffset, Int.MaxValue, byteOrder, computeFrameSize))
          .grouped(10000)
          .runWith(Sink.head)
          .map(result => (result, encodedFrames, (byteOrder, fieldOffset, fieldLength)))
      }

      val futureResults = Future.sequence(resultFutures)
      futureResults.futureValue.foreach {
        case (result, encodedFrames, (byteOrder, fieldOffset, fieldLength)) =>
          withClue(s"byteOrder: $byteOrder, fieldOffset: $fieldOffset, fieldLength: $fieldLength") {
            result should ===(encodedFrames)
          }
      }

    }

    "work with empty streams" in {
      Source.empty
        .via(Framing.lengthField(4, 0, Int.MaxValue, ByteOrder.BIG_ENDIAN))
        .runFold(Vector.empty[ByteString])(_ :+ _)
        .futureValue should ===(Vector.empty)
    }

    "work with grouped frames" in {
      val groupSize = 5
      val single = encode(referenceChunk.take(100), 0, 1, ByteOrder.BIG_ENDIAN)
      val groupedFrames = (1 to groupSize).map(_ => single).fold(ByteString.empty)((result, bs) => result ++ bs)

      val publisher = TestPublisher.probe[ByteString]()
      val substriber = TestSubscriber.manualProbe[ByteString]()

      Source
        .fromPublisher(publisher)
        .via(Framing.lengthField(1, 0, Int.MaxValue, ByteOrder.BIG_ENDIAN))
        .to(Sink.fromSubscriber(substriber))
        .run()

      val subscription = substriber.expectSubscription()

      publisher.sendNext(groupedFrames)
      publisher.sendComplete()
      for (_ <- 1 to groupSize) {
        subscription.request(1)
        substriber.expectNext(single)
      }
      substriber.expectComplete()
      subscription.cancel()
    }

    "report oversized frames" in {
      Source
        .single(encode(referenceChunk.take(100), 0, 1, ByteOrder.BIG_ENDIAN))
        .via(Framing.lengthField(1, 0, 99, ByteOrder.BIG_ENDIAN))
        .runFold(Vector.empty[ByteString])(_ :+ _)
        .failed
        .futureValue shouldBe a[FramingException]

      Source
        .single(encode(referenceChunk.take(100), 49, 1, ByteOrder.BIG_ENDIAN))
        .via(Framing.lengthField(1, 0, 100, ByteOrder.BIG_ENDIAN))
        .runFold(Vector.empty[ByteString])(_ :+ _)
        .failed
        .futureValue shouldBe a[FramingException]
    }

    "report truncated frames" in {
      import system.dispatcher
      val resultFutures: List[Future[(Throwable, (ByteOrder, Int, Int, Int))]] = for {
        //_ <- 1 to 10
        byteOrder <- byteOrders
        fieldOffset <- fieldOffsets
        fieldLength <- fieldLengths
        frameLength <- frameLengths if frameLength < (1 << (fieldLength * 8)) && (frameLength != 0)
      } yield {

        val fullFrame = encode(referenceChunk.take(frameLength), fieldOffset, fieldLength, byteOrder)
        val partialFrame = fullFrame.dropRight(1)

        Source(List(fullFrame, partialFrame))
          .via(rechunk)
          .via(Framing.lengthField(fieldLength, fieldOffset, Int.MaxValue, byteOrder))
          .grouped(10000)
          .runWith(Sink.head)
          .failed
          .map(ex => (ex, (byteOrder, fieldOffset, fieldLength, frameLength)))
      }

      val futureResults = Future.sequence(resultFutures)
      futureResults.futureValue.foreach {
        case (ex, (byteOrder, fieldOffset, fieldLength, frameLength)) =>
          withClue(
            s"byteOrder: $byteOrder, fieldOffset: $fieldOffset, fieldLength: $fieldLength, frameLength: $frameLength") {
            ex shouldBe a[FramingException]
          }
      }

    }

    "support simple framing adapter" in {
      val rechunkBidi = BidiFlow.fromFlowsMat(rechunk, rechunk)(Keep.left)
      val codecFlow =
        Framing
          .simpleFramingProtocol(1024)
          .atop(rechunkBidi)
          .atop(Framing.simpleFramingProtocol(1024).reversed)
          .join(Flow[ByteString]) // Loopback

      val testMessages = List.fill(100)(referenceChunk.take(Random.nextInt(1024)))
      Source(testMessages).via(codecFlow).limit(1000).runWith(Sink.seq).futureValue should ===(testMessages)
    }

    "fail the stage on negative length field values (#22367)" in {
      implicit val bo = java.nio.ByteOrder.LITTLE_ENDIAN

      // A 4-byte message containing only an Int specifying the length of the payload
      // The issue shows itself if length in message is less than or equal
      // to -4 (if expected length field is length 4)
      val bs = ByteString.newBuilder.putInt(-4).result()

      val res =
        Source.single(bs).via(Flow[ByteString].via(Framing.lengthField(4, 0, 1000))).runWith(Sink.seq)

      val ex = res.failed.futureValue
      ex shouldBe a[FramingException]
      ex.getMessage should ===("Decoded frame header reported negative size -4")
    }

    "ignore length field value when provided computeFrameSize (#27884)" in {
      implicit val bo = java.nio.ByteOrder.LITTLE_ENDIAN

      def computeFrameSize(arr: Array[Byte], l: Int): Int = 8

      val bs = ByteString.newBuilder.putInt(0xFF010203).putInt(0x04050607).result()

      val res =
        Source
          .single(bs)
          .via(Flow[ByteString].via(Framing.lengthField(4, 0, 1000, bo, computeFrameSize)))
          .runWith(Sink.seq)

      res.futureValue should equal(Seq(bs))
    }

    "fail the stage on computeFrameSize values less than minimum chunk size" in {
      implicit val bo = java.nio.ByteOrder.LITTLE_ENDIAN

      def computeFrameSize(arr: Array[Byte], l: Int): Int = 3

      // A 4-byte message containing only an Int specifying the length of the payload
      val bs = ByteString.newBuilder.putInt(4).result()

      val res =
        Source
          .single(bs)
          .via(Flow[ByteString].via(Framing.lengthField(4, 0, 1000, bo, computeFrameSize)))
          .runWith(Sink.seq)

      val ex = res.failed.futureValue
      ex shouldBe a[FramingException]
      ex.getMessage should ===("Computed frame size 3 is less than minimum chunk size 4")
    }

    "let zero length field values pass through (#22367)" in {
      implicit val bo = java.nio.ByteOrder.LITTLE_ENDIAN

      // Interleave empty frames with a frame with data
      val encodedPayload = encode(ByteString(42), 0, 4, bo)
      val emptyFrame = encode(ByteString(), 0, 4, bo)
      val bs = Vector(emptyFrame, encodedPayload, emptyFrame)

      val res =
        Source(bs).via(Flow[ByteString].via(Framing.lengthField(4, 0, 1000))).runWith(Sink.seq)

      res.futureValue should equal(Seq(emptyFrame, encodedPayload, emptyFrame))

    }
  }

}
