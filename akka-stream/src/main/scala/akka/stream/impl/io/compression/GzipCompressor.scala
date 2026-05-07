/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io.compression

import java.util.zip.{ CRC32, Deflater }

import akka.annotation.InternalApi
import akka.util.ByteString

/** INTERNAL API */
@InternalApi private[akka] class GzipCompressor(compressionLevel: Int = Deflater.BEST_COMPRESSION)
    extends DeflateCompressor(compressionLevel, nowrap = true) {
  override protected lazy val deflater = new Deflater(compressionLevel, true)
  private val checkSum = new CRC32 // CRC32 of uncompressed data
  private var headerSent = false
  private var bytesRead = 0L

  override protected def compressWithBuffer(input: ByteString, buffer: Array[Byte]): ByteString = {
    updateCrc(input)
    header() ++ super.compressWithBuffer(input, buffer)
  }
  override protected def flushWithBuffer(buffer: Array[Byte]): ByteString = header() ++ super.flushWithBuffer(buffer)
  override protected def finishWithBuffer(buffer: Array[Byte]): ByteString =
    header() ++ super.finishWithBuffer(buffer) ++ trailer()

  private def updateCrc(input: ByteString): Unit = {
    checkSum.update(input.toArrayUnsafe())
    bytesRead += input.length
  }
  private def header(): ByteString =
    if (!headerSent) {
      headerSent = true
      GzipDecompressor.Header
    } else ByteString.empty

  private def trailer(): ByteString = {
    def int32(i: Int): ByteString = ByteString(i, i >> 8, i >> 16, i >> 24)
    val crc = checkSum.getValue.toInt
    val tot = bytesRead.toInt // truncated to 32bit as specified in https://tools.ietf.org/html/rfc1952#section-2
    val trailer = int32(crc) ++ int32(tot)

    trailer
  }
}
