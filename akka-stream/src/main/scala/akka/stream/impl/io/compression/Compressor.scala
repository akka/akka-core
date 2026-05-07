/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io.compression

import akka.annotation.InternalApi
import akka.util.ByteString

/**
 * INTERNAL API
 *
 * A stateful object representing ongoing compression.
 */
@InternalApi private[akka] abstract class Compressor {

  /**
   * Compresses the given input and returns compressed data. The implementation
   * can and will choose to buffer output data to improve compression. Use
   * `flush` or `compressAndFlush` to make sure that all input data has been
   * compressed and pending output data has been returned.
   */
  def compress(input: ByteString): ByteString

  /**
   * Flushes any output data and returns the currently remaining compressed data.
   */
  def flush(): ByteString

  /**
   * Closes this compressed stream and return the remaining compressed data. After
   * calling this method, this Compressor cannot be used any further.
   */
  def finish(): ByteString

  /** Combines `compress` + `flush` */
  def compressAndFlush(input: ByteString): ByteString

  /** Combines `compress` + `finish` */
  def compressAndFinish(input: ByteString): ByteString

  /** Make sure any resources have been released */
  def close(): Unit
}
