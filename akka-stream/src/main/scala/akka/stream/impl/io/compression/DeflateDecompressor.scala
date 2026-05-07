/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io.compression

import java.util.zip.Inflater

import akka.annotation.InternalApi
import akka.stream.Attributes

/** INTERNAL API */
@InternalApi private[akka] class DeflateDecompressor(maxBytesPerChunk: Int, nowrap: Boolean)
    extends DeflateDecompressorBase(maxBytesPerChunk) {

  def this(maxBytesPerChunk: Int) = this(maxBytesPerChunk, nowrap = false) // for binary compatibility

  override def createLogic(attr: Attributes) = new DecompressorParsingLogic {
    override val inflater: Inflater = new Inflater(nowrap)

    override case object inflating extends Inflate(noPostProcessing = true) {
      override def onTruncation(): Unit = completeStage()
    }

    override def afterInflate = inflating
    override def afterBytesRead(buffer: Array[Byte], offset: Int, length: Int): Unit = {}

    startWith(inflating)
  }
}
