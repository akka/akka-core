/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.nio.ByteOrder

import scala.annotation.tailrec
import scala.reflect.ClassTag

import akka.NotUsed
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.stage._
import akka.util.{ ByteIterator, ByteString, OptionVal }

object Framing {

  /**
   * Creates a Flow that handles decoding a stream of unstructured byte chunks into a stream of frames where the
   * incoming chunk stream uses a specific byte-sequence to mark frame boundaries.
   *
   * The decoded frames will not include the separator sequence.
   *
   * If there are buffered bytes (an incomplete frame) when the input stream finishes and ''allowTruncation'' is set to
   * false then this Flow will fail the stream reporting a truncated frame.
   *
   * @param delimiter          The byte sequence to be treated as the end of the frame.
   * @param allowTruncation    If `false`, then when the last frame being decoded contains no valid delimiter this Flow
   *                           fails the stream instead of returning a truncated frame.
   * @param maximumFrameLength The maximum length of allowed frames while decoding. If the maximum length is
   *                           exceeded this Flow will fail the stream.
   */
  def delimiter(
      delimiter: ByteString,
      maximumFrameLength: Int,
      allowTruncation: Boolean = false): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .via(new DelimiterFramingStage(delimiter, maximumFrameLength, allowTruncation))
      .named("delimiterFraming")

  /**
   * Creates a Flow that decodes an incoming stream of unstructured byte chunks into a stream of frames, assuming that
   * incoming frames have a field that encodes their length.
   *
   * If the input stream finishes before the last frame has been fully decoded, this Flow will fail the stream reporting
   * a truncated frame.
   *
   * @param fieldLength        The length of the "size" field in bytes
   * @param fieldOffset        The offset of the field from the beginning of the frame in bytes
   * @param maximumFrameLength The maximum length of allowed frames while decoding. If the maximum length is exceeded
   *                           this Flow will fail the stream. This length *includes* the header (i.e the offset and
   *                           the length of the size field)
   * @param byteOrder          The ''ByteOrder'' to be used when decoding the field
   */
  def lengthField(
      fieldLength: Int,
      fieldOffset: Int = 0,
      maximumFrameLength: Int,
      byteOrder: ByteOrder = ByteOrder.LITTLE_ENDIAN): Flow[ByteString, ByteString, NotUsed] = {
    require(fieldLength >= 1 && fieldLength <= 4, "Length field length must be 1, 2, 3 or 4.")
    Flow[ByteString]
      .via(new LengthFieldFramingStage(fieldLength, fieldOffset, maximumFrameLength, byteOrder))
      .named("lengthFieldFraming")
  }

  /**
   * Creates a Flow that decodes an incoming stream of unstructured byte chunks into a stream of frames, assuming that
   * incoming frames have a field that encodes their length.
   *
   * If the input stream finishes before the last frame has been fully decoded, this Flow will fail the stream reporting
   * a truncated frame.
   *
   * @param fieldLength        The length of the "size" field in bytes
   * @param fieldOffset        The offset of the field from the beginning of the frame in bytes
   * @param maximumFrameLength The maximum length of allowed frames while decoding. If the maximum length is exceeded
   *                           this Flow will fail the stream. This length *includes* the header (i.e the offset and
   *                           the length of the size field)
   * @param byteOrder          The ''ByteOrder'' to be used when decoding the field
   * @param computeFrameSize   This function can be supplied if frame size is varied or needs to be computed in a special fashion.
   *                           For example, frame can have a shape like this: `[offset bytes][body size bytes][body bytes][footer bytes]`.
   *                           Then computeFrameSize can be used to compute the frame size: `(offset bytes, computed size) => (actual frame size)`.
   *                           ''Actual frame size'' must be equal or bigger than sum of `fieldOffset` and `fieldLength`, the operator fails otherwise.
   *                           Must not mutate the given byte array.
   *
   */
  def lengthField(
      fieldLength: Int,
      fieldOffset: Int,
      maximumFrameLength: Int,
      byteOrder: ByteOrder,
      computeFrameSize: (Array[Byte], Int) => Int): Flow[ByteString, ByteString, NotUsed] = {
    require(fieldLength >= 1 && fieldLength <= 4, "Length field length must be 1, 2, 3 or 4.")
    Flow[ByteString]
      .via(new LengthFieldFramingStage(fieldLength, fieldOffset, maximumFrameLength, byteOrder, Some(computeFrameSize)))
      .named("lengthFieldFraming")
  }

  /**
   * Returns a BidiFlow that implements a simple framing protocol. This is a convenience wrapper over [[Framing#lengthField]]
   * and simply attaches a length field header of four bytes (using big endian encoding) to outgoing messages, and decodes
   * such messages in the inbound direction. The decoded messages do not contain the header.
   * {{{
   *       +--------------------------------+
   *       | Framing BidiFlow               |
   *       |                                |
   *       |  +--------------------------+  |
   * in2 ~~>  |        Decoding          | ~~> out2
   *       |  +--------------------------+  |
   *       |                                |
   *       |  +--------------------------+  |
   * out1 <~~ |Encoding(Add length field)| <~~ in1
   *       |  +--------------------------+  |
   *       +--------------------------------+
   * }}}
   * This BidiFlow is useful if a simple message framing protocol is needed (for example when TCP is used to send
   * individual messages) but no compatibility with existing protocols is necessary.
   *
   * The encoded frames have the layout
   * {{{
   *   [4 bytes length field, Big Endian][User Payload]
   * }}}
   * The length field encodes the length of the user payload excluding the header itself.
   *
   * @param maximumMessageLength Maximum length of allowed messages. If sent or received messages exceed the configured
   *                             limit this BidiFlow will fail the stream. The header attached by this BidiFlow are not
   *                             included in this limit.
   */
  def simpleFramingProtocol(
      maximumMessageLength: Int): BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] = {
    BidiFlow.fromFlowsMat(
      simpleFramingProtocolEncoder(maximumMessageLength),
      simpleFramingProtocolDecoder(maximumMessageLength))(Keep.left)
  }

  /**
   * Protocol decoder that is used by [[Framing#simpleFramingProtocol]]
   */
  def simpleFramingProtocolDecoder(maximumMessageLength: Int): Flow[ByteString, ByteString, NotUsed] =
    lengthField(4, 0, maximumMessageLength + 4, ByteOrder.BIG_ENDIAN).map(_.drop(4))

  /**
   * Protocol encoder that is used by [[Framing#simpleFramingProtocol]]
   */
  def simpleFramingProtocolEncoder(maximumMessageLength: Int): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].via(new SimpleFramingProtocolEncoder(maximumMessageLength))

  class FramingException(msg: String) extends RuntimeException(msg)

  private final val bigEndianDecoder: (ByteIterator, Int) => Int = (bs, length) => {
    var count = length
    var decoded = 0
    while (count > 0) {
      decoded <<= 8
      decoded |= bs.next().toInt & 0xFF
      count -= 1
    }
    decoded
  }

  private final val littleEndianDecoder: (ByteIterator, Int) => Int = (bs, length) => {
    val highestOctet = (length - 1) << 3
    val Mask = ((1L << (length << 3)) - 1).toInt
    var count = length
    var decoded = 0
    while (count > 0) {
      decoded >>>= 8
      decoded += (bs.next().toInt & 0xFF) << highestOctet
      count -= 1
    }
    decoded & Mask
  }

  private class SimpleFramingProtocolEncoder(maximumMessageLength: Long) extends SimpleLinearGraphStage[ByteString] {
    override def createLogic(inheritedAttributes: Attributes) =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        setHandlers(in, out, this)

        override def onPush(): Unit = {
          val message = grab(in)
          val msgSize = message.size

          if (msgSize > maximumMessageLength)
            failStage(
              new FramingException(
                s"Maximum allowed message size is $maximumMessageLength but tried to send $msgSize bytes"))
          else {
            val header =
              ByteString((msgSize >> 24) & 0xFF, (msgSize >> 16) & 0xFF, (msgSize >> 8) & 0xFF, msgSize & 0xFF)
            push(out, header ++ message)
          }
        }

        override def onPull(): Unit = pull(in)
      }
  }

  private class DelimiterFramingStage(
      val separatorBytes: ByteString,
      val maximumLineBytes: Int,
      val allowTruncation: Boolean)
      extends GraphStage[FlowShape[ByteString, ByteString]] {

    val in = Inlet[ByteString]("DelimiterFramingStage.in")
    val out = Outlet[ByteString]("DelimiterFramingStage.out")
    override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

    override def initialAttributes: Attributes = DefaultAttributes.delimiterFraming
    override def toString: String = "DelimiterFraming"

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        private val firstSeparatorByte = separatorBytes.head
        private var buffer = ByteString.empty
        private var nextPossibleMatch = 0

        // We use an efficient unsafe array implementation and must be use with caution.
        // It contains all indices computed during search phase.
        // The capacity is fixed at 256 to preserve fairness and prevent uneccessary allocation during parsing phase.
        // This array provide a way to check remaining capacity and must be use to prevent out of bounds exception.
        // In this use case, we compute all possibles indices up to 256 and then parse everything.
        private val indices = new LightArray[(Int, Int)](256)

        override def onPush(): Unit = {
          buffer ++= grab(in)
          searchIndices()
        }

        override def onPull(): Unit = searchIndices()

        override def onUpstreamFinish(): Unit = {
          if (buffer.isEmpty) {
            completeStage()
          } else if (isAvailable(out)) {
            searchIndices()
          } // else swallow the termination and wait for pull
        }

        private def tryPull(): Unit = {
          if (isClosed(in)) {
            if (allowTruncation) {
              push(out, buffer)
              completeStage()
            } else
              failStage(new FramingException("Stream finished but there was a truncated final frame in the buffer"))
          } else pull(in)
        }

        @tailrec
        private def searchIndices(): Unit = {
          // Next possible position for the delimiter
          val possibleMatchPos = buffer.indexOf(firstSeparatorByte, from = nextPossibleMatch)

          // Retrive previous position
          val previous = indices.lastOption match {
            case OptionVal.Some((_, i)) => i + separatorBytes.size
            case _                      => 0
          }

          if (possibleMatchPos - previous > maximumLineBytes) {
            failStage(
              new FramingException(
                s"Read ${possibleMatchPos - previous} bytes " +
                s"which is more than $maximumLineBytes without seeing a line terminator"))
          } else if (possibleMatchPos == -1) {
            if (buffer.length - previous > maximumLineBytes)
              failStage(
                new FramingException(
                  s"Read ${buffer.length - previous} bytes " +
                  s"which is more than $maximumLineBytes without seeing a line terminator"))
            else {
              // No matching character, we need to accumulate more bytes into the buffer
              nextPossibleMatch = buffer.length
              doParse()
            }
          } else if (possibleMatchPos + separatorBytes.length > buffer.length) {
            // We have found a possible match (we found the first character of the terminator
            // sequence) but we don't have yet enough bytes. We remember the position to
            // retry from next time.
            nextPossibleMatch = possibleMatchPos
            doParse()
          } else if (buffer.slice(possibleMatchPos, possibleMatchPos + separatorBytes.length) == separatorBytes) {
            // Found a match, mark start and end position and iterate if possible
            indices += (previous -> possibleMatchPos)
            nextPossibleMatch = possibleMatchPos + separatorBytes.length
            if (nextPossibleMatch == buffer.length || indices.isFull) {
              doParse()
            } else {
              searchIndices()
            }
          } else {
            // possibleMatchPos was not actually a match
            nextPossibleMatch += 1
            searchIndices()
          }
        }

        private def doParse(): Unit =
          if (indices.isEmpty) tryPull()
          else if (indices.length == 1) {
            // Emit result and compact buffer
            val indice = indices(0)
            push(out, buffer.slice(indice._1, indice._2).compact)
            reset()
            if (isClosed(in) && buffer.isEmpty) completeStage()
          } else {
            // Emit results and compact buffer
            emitMultiple(out, new FrameIterator(), () => {
              reset()
              if (isClosed(in) && buffer.isEmpty) completeStage()
            })
          }

        private def reset(): Unit = {
          val previous = indices.lastOption match {
            case OptionVal.Some((_, i)) => i + separatorBytes.size
            case _                      => 0
          }

          buffer = buffer.drop(previous).compact
          indices.setLength(0)
          nextPossibleMatch = 0
        }

        // Iterator able to iterate over precompute frame based on start and end position
        private class FrameIterator(private var index: Int = 0) extends Iterator[ByteString] {
          def hasNext: Boolean = index != indices.length

          def next(): ByteString = {
            val indice = indices(index)
            index += 1
            buffer.slice(indice._1, indice._2).compact
          }
        }

        // Basic array implementation that allow unsafe resize.
        private class LightArray[T: ClassTag](private val capacity: Int, private var index: Int = 0) {

          private val underlying = Array.ofDim[T](capacity)

          def apply(i: Int) = underlying(i)

          def +=(el: T): Unit = {
            underlying(index) = el
            index += 1
          }

          def isEmpty: Boolean = length == 0

          def isFull: Boolean = capacity == length

          def setLength(length: Int): Unit = index = length

          def length: Int = index

          def lastOption: OptionVal[T] =
            if (index > 0) OptionVal.Some(underlying(index - 1))
            else OptionVal.none
        }
        setHandlers(in, out, this)
      }
  }

  private final class LengthFieldFramingStage(
      val lengthFieldLength: Int,
      val lengthFieldOffset: Int,
      val maximumFrameLength: Int,
      val byteOrder: ByteOrder,
      computeFrameSize: Option[(Array[Byte], Int) => Int])
      extends GraphStage[FlowShape[ByteString, ByteString]] {

    //for the sake of binary compatibility
    def this(lengthFieldLength: Int, lengthFieldOffset: Int, maximumFrameLength: Int, byteOrder: ByteOrder) =
      this(lengthFieldLength, lengthFieldOffset, maximumFrameLength, byteOrder, None)

    private val minimumChunkSize = lengthFieldOffset + lengthFieldLength
    private val intDecoder = byteOrder match {
      case ByteOrder.BIG_ENDIAN    => bigEndianDecoder
      case ByteOrder.LITTLE_ENDIAN => littleEndianDecoder
      case _                       => throw new RuntimeException() // won't happen, compiler exhaustiveness check pleaser
    }

    val in = Inlet[ByteString]("LengthFieldFramingStage.in")
    val out = Outlet[ByteString]("LengthFieldFramingStage.out")
    override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        private var buffer = ByteString.empty
        private var frameSize = Int.MaxValue

        /**
         * push, and reset frameSize and buffer
         *
         */
        private def pushFrame() = {
          val emit = buffer.take(frameSize).compact
          buffer = buffer.drop(frameSize)
          frameSize = Int.MaxValue
          push(out, emit)
          if (buffer.isEmpty && isClosed(in)) {
            completeStage()
          }
        }

        /**
         * try to push downstream, if failed then try to pull upstream
         *
         */
        private def tryPushFrame() = {
          val buffSize = buffer.size
          if (buffSize >= frameSize) {
            pushFrame()
          } else if (buffSize >= minimumChunkSize) {
            val parsedLength = intDecoder(buffer.iterator.drop(lengthFieldOffset), lengthFieldLength)
            frameSize = computeFrameSize match {
              case Some(f) => f(buffer.take(lengthFieldOffset).toArrayUnsafe(), parsedLength)
              case None    => parsedLength + minimumChunkSize
            }
            if (frameSize > maximumFrameLength) {
              failStage(new FramingException(
                s"Maximum allowed frame size is $maximumFrameLength but decoded frame header reported size $frameSize"))
            } else if (computeFrameSize.isEmpty && parsedLength < 0) {
              failStage(new FramingException(s"Decoded frame header reported negative size $parsedLength"))
            } else if (frameSize < minimumChunkSize) {
              failStage(
                new FramingException(
                  s"Computed frame size $frameSize is less than minimum chunk size $minimumChunkSize"))
            } else if (buffSize >= frameSize) {
              pushFrame()
            } else tryPull()
          } else tryPull()
        }

        private def tryPull() = {
          if (isClosed(in)) {
            failStage(new FramingException("Stream finished but there was a truncated final frame in the buffer"))
          } else pull(in)
        }

        override def onPush(): Unit = {
          buffer ++= grab(in)
          tryPushFrame()
        }

        override def onPull() = tryPushFrame()

        override def onUpstreamFinish(): Unit = {
          if (buffer.isEmpty) {
            completeStage()
          } else if (isAvailable(out)) {
            tryPushFrame()
          } // else swallow the termination and wait for pull
        }

        setHandlers(in, out, this)
      }
  }

}
