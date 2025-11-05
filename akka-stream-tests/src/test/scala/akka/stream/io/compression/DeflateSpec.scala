/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io.compression

import java.io.{ InputStream, OutputStream }
import java.util.zip._

import akka.stream.impl.io.compression.{ Compressor, DeflateCompressor }
import akka.stream.scaladsl.{ Compression, Flow }
import akka.util.ByteString

class DeflateSpec extends CoderSpec("deflate") {
  import CompressionTestingTools._

  protected def newCompressor(): Compressor = new DeflateCompressor
  protected val encoderFlow: Flow[ByteString, ByteString, Any] = Compression.deflate
  protected def decoderFlow(maxBytesPerChunk: Int): Flow[ByteString, ByteString, Any] =
    Compression.inflate(maxBytesPerChunk)

  protected def newDecodedInputStream(underlying: InputStream): InputStream =
    new InflaterInputStream(underlying)

  protected def newEncodedOutputStream(underlying: OutputStream): OutputStream =
    new DeflaterOutputStream(underlying)

  override def extraTests(): Unit = {
    "throw early if header is corrupt" in {
      (the[RuntimeException] thrownBy {
        ourDecode(ByteString(0, 1, 2, 3, 4))
      }).ultimateCause should be(a[DataFormatException])
    }
  }
}
