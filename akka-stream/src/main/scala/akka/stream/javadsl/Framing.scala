/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import java.nio.ByteOrder

import akka.NotUsed
import akka.stream.scaladsl
import akka.util.ByteString

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
   * Default truncation behavior is: when the last frame being decoded contains no valid delimiter this Flow
   * fails the stream instead of returning a truncated frame.
   *
   * @param delimiter The byte sequence to be treated as the end of the frame.
   * @param maximumFrameLength The maximum length of allowed frames while decoding. If the maximum length is
   *                           exceeded this Flow will fail the stream.
   */
  def delimiter(delimiter: ByteString, maximumFrameLength: Int): Flow[ByteString, ByteString, NotUsed] = {
    scaladsl.Framing.delimiter(delimiter, maximumFrameLength).asJava
  }

  /**
   * Creates a Flow that handles decoding a stream of unstructured byte chunks into a stream of frames where the
   * incoming chunk stream uses a specific byte-sequence to mark frame boundaries.
   *
   * The decoded frames will not include the separator sequence.
   *
   * If there are buffered bytes (an incomplete frame) when the input stream finishes and ''allowTruncation'' is set to
   * false then this Flow will fail the stream reporting a truncated frame.
   *
   * @param delimiter The byte sequence to be treated as the end of the frame.
   * @param allowTruncation If set to `DISALLOW`, then when the last frame being decoded contains no valid delimiter this Flow
   *                        fails the stream instead of returning a truncated frame.
   * @param maximumFrameLength The maximum length of allowed frames while decoding. If the maximum length is
   *                           exceeded this Flow will fail the stream.
   */
  def delimiter(
      delimiter: ByteString,
      maximumFrameLength: Int,
      allowTruncation: FramingTruncation): Flow[ByteString, ByteString, NotUsed] = {
    val truncationAllowed = allowTruncation == FramingTruncation.ALLOW
    scaladsl.Framing.delimiter(delimiter, maximumFrameLength, truncationAllowed).asJava
  }

  /**
   * Creates a Flow that decodes an incoming stream of unstructured byte chunks into a stream of frames, assuming that
   * incoming frames have a field that encodes their length.
   *
   * If the input stream finishes before the last frame has been fully decoded, this Flow will fail the stream reporting
   * a truncated frame.
   *
   * The byte order used for when decoding the field defaults to little-endian.
   *
   * @param fieldLength The length of the "size" field in bytes
   * @param fieldOffset The offset of the field from the beginning of the frame in bytes
   * @param maximumFrameLength The maximum length of allowed frames while decoding. If the maximum length is exceeded
   *                           this Flow will fail the stream. This length *includes* the header (i.e the offset and
   *                           the length of the size field)
   */
  def lengthField(fieldLength: Int, fieldOffset: Int, maximumFrameLength: Int): Flow[ByteString, ByteString, NotUsed] =
    scaladsl.Framing.lengthField(fieldLength, fieldOffset, maximumFrameLength).asJava

  /**
   * Creates a Flow that decodes an incoming stream of unstructured byte chunks into a stream of frames, assuming that
   * incoming frames have a field that encodes their length.
   *
   * If the input stream finishes before the last frame has been fully decoded, this Flow will fail the stream reporting
   * a truncated frame.
   *
   * @param fieldLength The length of the "size" field in bytes
   * @param fieldOffset The offset of the field from the beginning of the frame in bytes
   * @param maximumFrameLength The maximum length of allowed frames while decoding. If the maximum length is exceeded
   *                           this Flow will fail the stream. This length *includes* the header (i.e the offset and
   *                           the length of the size field)
   * @param byteOrder The ''ByteOrder'' to be used when decoding the field
   */
  def lengthField(
      fieldLength: Int,
      fieldOffset: Int,
      maximumFrameLength: Int,
      byteOrder: ByteOrder): Flow[ByteString, ByteString, NotUsed] =
    scaladsl.Framing.lengthField(fieldLength, fieldOffset, maximumFrameLength, byteOrder).asJava

  /**
   * Creates a Flow that decodes an incoming stream of unstructured byte chunks into a stream of frames, assuming that
   * incoming frames have a field that encodes their length.
   *
   * If the input stream finishes before the last frame has been fully decoded, this Flow will fail the stream reporting
   * a truncated frame.
   *
   * @param fieldLength The length of the "size" field in bytes
   * @param fieldOffset The offset of the field from the beginning of the frame in bytes
   * @param maximumFrameLength The maximum length of allowed frames while decoding. If the maximum length is exceeded
   *                           this Flow will fail the stream. This length *includes* the header (i.e the offset and
   *                           the length of the size field)
   * @param byteOrder The ''ByteOrder'' to be used when decoding the field
   * @param computeFrameSize This function can be supplied if frame size is varied or needs to be computed in a special fashion.
   *                         For example, frame can have a shape like this: `[offset bytes][body size bytes][body bytes][footer bytes]`.
   *                         Then computeFrameSize can be used to compute the frame size: `(offset bytes, computed size) => (actual frame size)`.
   *                         ''Actual frame size'' must be equal or bigger than sum of `fieldOffset` and `fieldLength`, the operator fails otherwise.
   *
   */
  def lengthField(
      fieldLength: Int,
      fieldOffset: Int,
      maximumFrameLength: Int,
      byteOrder: ByteOrder,
      computeFrameSize: akka.japi.function.Function2[Array[Byte], Integer, Integer])
      : Flow[ByteString, ByteString, NotUsed] =
    scaladsl.Framing
      .lengthField(
        fieldLength,
        fieldOffset,
        maximumFrameLength,
        byteOrder,
        (a: Array[Byte], s: Int) => computeFrameSize.apply(a, s))
      .asJava

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
      maximumMessageLength: Int): BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] =
    scaladsl.Framing.simpleFramingProtocol(maximumMessageLength).asJava

}
