/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.zip.Deflater

import akka.NotUsed
import akka.stream.impl.io.compression._
import akka.util.ByteString

object Compression {
  final val MaxBytesPerChunkDefault = 64 * 1024

  /**
   * Creates a flow that gzip-compresses a stream of ByteStrings. Note that the compressor
   * will SYNC_FLUSH after every [[ByteString]] so that it is guaranteed that every [[ByteString]]
   * coming out of the flow can be fully decompressed without waiting for additional data. This may
   * come at a compression performance cost for very small chunks.
   *
   * FIXME: should strategy / flush mode be configurable? See https://github.com/akka/akka-core/issues/21849
   */
  def gzip: Flow[ByteString, ByteString, NotUsed] = gzip(Deflater.BEST_COMPRESSION)

  /**
   * Same as [[gzip]] with a custom level.
   *
   * @param level Compression level (0-9)
   */
  def gzip(level: Int): Flow[ByteString, ByteString, NotUsed] =
    CompressionUtils.compressorFlow(() => new GzipCompressor(level))

  /**
   * Creates a Flow that decompresses a gzip-compressed stream of data.
   *
   * @param maxBytesPerChunk Maximum length of an output [[ByteString]] chunk.
   */
  def gunzip(maxBytesPerChunk: Int = MaxBytesPerChunkDefault): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].via(new GzipDecompressor(maxBytesPerChunk)).named("gunzip")

  /**
   * Creates a flow that deflate-compresses a stream of ByteString. Note that the compressor
   * will SYNC_FLUSH after every [[ByteString]] so that it is guaranteed that every [[ByteString]]
   * coming out of the flow can be fully decompressed without waiting for additional data. This may
   * come at a compression performance cost for very small chunks.
   *
   * FIXME: should strategy / flush mode be configurable? See https://github.com/akka/akka-core/issues/21849
   */
  def deflate: Flow[ByteString, ByteString, NotUsed] = deflate(Deflater.BEST_COMPRESSION, nowrap = false)

  /**
   * Same as [[deflate]] with configurable level and nowrap
   *
   * @param level Compression level (0-9)
   * @param nowrap if true then use GZIP compatible compression
   *
   */
  def deflate(level: Int, nowrap: Boolean): Flow[ByteString, ByteString, NotUsed] =
    CompressionUtils.compressorFlow(() => new DeflateCompressor(level, nowrap))

  /**
   * Creates a Flow that decompresses a deflate-compressed stream of data.
   *
   * @param maxBytesPerChunk Maximum length of an output [[ByteString]] chunk.
   */
  def inflate(maxBytesPerChunk: Int = MaxBytesPerChunkDefault): Flow[ByteString, ByteString, NotUsed] =
    inflate(maxBytesPerChunk, nowrap = false)

  /**
   * Creates a Flow that decompresses a deflate-compressed stream of data.
   *
   * @param maxBytesPerChunk Maximum length of an output [[ByteString]] chunk.
   * @param nowrap if true then use GZIP compatible decompression
   */
  def inflate(maxBytesPerChunk: Int, nowrap: Boolean): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].via(new DeflateDecompressor(maxBytesPerChunk, nowrap)).named("inflate")
}
