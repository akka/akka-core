/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io.compression

import java.util.zip.{ CRC32, Inflater, ZipException }

import akka.annotation.InternalApi
import akka.stream.Attributes
import akka.stream.impl.io.ByteStringParser
import akka.stream.impl.io.ByteStringParser.{ ParseResult, ParseStep }
import akka.util.ByteString

/** INTERNAL API */
@InternalApi private[akka] class GzipDecompressor(maxBytesPerChunk: Int)
    extends DeflateDecompressorBase(maxBytesPerChunk) {

  override def createLogic(attr: Attributes) = new DecompressorParsingLogic {
    override val inflater: Inflater = new Inflater(true)
    private val crc32: CRC32 = new CRC32

    override def afterInflate: ParseStep[ByteString] = ReadTrailer
    override def afterBytesRead(buffer: Array[Byte], offset: Int, length: Int): Unit =
      crc32.update(buffer, offset, length)

    trait Step extends ParseStep[ByteString] {
      override def onTruncation(): Unit = failStage(new ZipException("Truncated GZIP stream"))
    }
    override case object inflating extends Inflate(noPostProcessing = false) with Step
    startWith(ReadHeaders)

    /** Reading the header bytes */
    case object ReadHeaders extends Step {
      override def parse(reader: ByteStringParser.ByteReader): ParseResult[ByteString] = {
        import reader._
        if (readByte() != 0x1F || readByte() != 0x8B) fail("Not in GZIP format") // check magic header
        if (readByte() != 8) fail("Unsupported GZIP compression method") // check compression method
        val flags = readByte()
        skip(6) // skip MTIME, XFL and OS fields
        if ((flags & 4) > 0) skip(readShortLE()) // skip optional extra fields
        if ((flags & 8) > 0) skipZeroTerminatedString() // skip optional file name
        if ((flags & 16) > 0) skipZeroTerminatedString() // skip optional file comment
        if ((flags & 2) > 0 && crc16(fromStartToHere) != readShortLE()) fail("Corrupt GZIP header")

        inflater.reset()
        crc32.reset()
        ParseResult(None, inflating, acceptUpstreamFinish = false)
      }
    }

    private def fail(msg: String) = throw new ZipException(msg)

    /** Reading the trailer */
    case object ReadTrailer extends Step {
      override def parse(reader: ByteStringParser.ByteReader): ParseResult[ByteString] = {
        import reader._
        if (readIntLE() != crc32.getValue.toInt) fail("Corrupt data (CRC32 checksum error)")
        if (readIntLE() != inflater.getBytesWritten.toInt /* truncated to 32bit */ )
          fail("Corrupt GZIP trailer ISIZE")
        ParseResult(None, ReadHeaders, acceptUpstreamFinish = true)
      }
    }
  }
  private def crc16(data: ByteString) = {
    val crc = new CRC32
    crc.update(data.toArrayUnsafe())
    crc.getValue.toInt & 0xFFFF
  }
}

/** INTERNAL API */
@InternalApi private[akka] object GzipDecompressor {
  // RFC 1952: https://www.rfc-editor.org/rfc/rfc1952.html section 2.2
  private[impl] val Header = ByteString(0x1F, // ID1
    0x8B, // ID2
    8, // CM = Deflate
    0, // FLG
    0, // MTIME 1
    0, // MTIME 2
    0, // MTIME 3
    0, // MTIME 4
    0, // XFL
    0 // OS
    )
}
