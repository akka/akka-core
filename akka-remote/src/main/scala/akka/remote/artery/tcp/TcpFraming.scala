/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery
package tcp

import java.nio.ByteBuffer
import java.nio.ByteOrder

import akka.annotation.InternalApi
import akka.remote.artery.ArteryTransport.LargeStreamId
import akka.stream.Attributes
import akka.stream.impl.io.ByteStringParser
import akka.stream.impl.io.ByteStringParser.ByteReader
import akka.stream.impl.io.ByteStringParser.ParseResult
import akka.stream.impl.io.ByteStringParser.ParseStep
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.stage.GraphStageLogic
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TcpFraming {
  val Undefined = Int.MinValue

  /**
   * The first 4 bytes of a new connection must be these `0x64 0x75 0x75 0x64` (AKKA).
   * The purpose of the "magic" is to detect and reject weird (accidental) accesses.
   */
  val Magic = ByteString('A'.toByte, 'K'.toByte, 'K'.toByte, 'A'.toByte)

  /**
   * When establishing the connection this header is sent first.
   * It contains a "magic" and the stream identifier for selecting control, ordinary, large
   * inbound streams.
   *
   * The purpose of the "magic" is to detect and reject weird (accidental) accesses.
   * The magic 4 bytes are `0x64 0x75 0x75 0x64` (AKKA).
   *
   * The streamId` is encoded as 1 byte.
   */
  def encodeConnectionHeader(streamId: Int): ByteString =
    Magic ++ ByteString.fromArrayUnsafe(Array(streamId.toByte))

  /**
   * Each frame starts with the frame header that contains the length
   * of the frame. The `frameLength` is encoded as 4 bytes (little endian).
   */
  def encodeFrameHeader(frameLength: Int): ByteString =
    ByteString.fromArrayUnsafe(
      Array[Byte](
        (frameLength & 0xff).toByte,
        ((frameLength & 0xff00) >> 8).toByte,
        ((frameLength & 0xff0000) >> 16).toByte,
        ((frameLength & 0xff000000) >> 24).toByte))
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TcpFraming(maxFrameLength: Int, maxLargeFrameLength: Int = -1)
    extends ByteStringParser[EnvelopeBuffer] {

  // large frames are only expected on the dedicated large message stream, other streams
  // must be bounded by the (smaller) ordinary maxFrameLength; -1 means "not configured",
  // i.e. same bound as ordinary frames
  private def maxFrameLengthFor(streamId: Int): Int =
    if (streamId == LargeStreamId && maxLargeFrameLength >= 0) maxLargeFrameLength else maxFrameLength

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new ParsingLogic {

    abstract class Step extends ParseStep[EnvelopeBuffer]
    startWith(ReadMagic)

    case object ReadMagic extends Step {
      override def parse(reader: ByteReader): ParseResult[EnvelopeBuffer] = {
        val magic = reader.take(TcpFraming.Magic.length)
        if (magic == TcpFraming.Magic)
          ParseResult(None, ReadStreamId)
        else
          throw new FramingException(
            "Stream didn't start with expected magic bytes, " +
            s"got [${(magic ++ reader.remainingData).take(10).map("%02x".format(_)).mkString(" ")}] " +
            "Connection is rejected. Probably invalid accidental access.")
      }
    }
    case object ReadStreamId extends Step {
      override def parse(reader: ByteReader): ParseResult[EnvelopeBuffer] =
        ParseResult(None, ReadFrame(reader.readByte()))
    }
    case class ReadFrame(streamId: Int) extends Step {
      override def onTruncation(): Unit =
        failStage(new FramingException("Stream finished but there was a truncated final frame in the buffer"))

      override def parse(reader: ByteReader): ParseResult[EnvelopeBuffer] = {
        val frameLength = reader.readIntLE()
        val currentMaxFrameLength = maxFrameLengthFor(streamId)
        if (frameLength < 0 || frameLength > currentMaxFrameLength)
          throw new FramingException(
            s"Invalid frame length [$frameLength], must be between 0 and [$currentMaxFrameLength]")
        val buffer = createBuffer(reader.take(frameLength))
        ParseResult(Some(buffer), this)
      }

      private def createBuffer(bs: ByteString): EnvelopeBuffer = {
        val buffer = ByteBuffer.wrap(bs.toArray)
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        RemotingFlightRecorder.tcpInboundReceived(buffer.limit)
        val res = new EnvelopeBuffer(buffer)
        res.setStreamId(streamId)
        res
      }
    }
  }
}
