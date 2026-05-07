/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.io.{ ByteArrayOutputStream, InputStream }

package object serialization {

  /**
   * Converts an input stream to a byte array.
   */
  def streamToBytes(inputStream: InputStream): Array[Byte] = {
    val len = 16384
    val buf = new Array[Byte](len)
    val out = new ByteArrayOutputStream

    @scala.annotation.tailrec
    def copy(): Array[Byte] = {
      val n = inputStream.read(buf, 0, len)
      if (n != -1) {
        out.write(buf, 0, n); copy()
      } else out.toByteArray
    }

    copy()
  }
}
