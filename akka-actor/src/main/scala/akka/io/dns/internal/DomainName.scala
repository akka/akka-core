/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import akka.annotation.InternalApi
import akka.util.{ ByteIterator, ByteString, ByteStringBuilder }

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object DomainName {
  def length(name: String): Short = {
    (name.length + 2).toShort
  }

  def write(it: ByteStringBuilder, name: String): Unit = {
    for (label <- name.split('.')) {
      it.putByte(label.length.toByte)
      for (c <- label) {
        it.putByte(c.toByte)
      }
    }
    it.putByte(0)
  }

  def parse(it: ByteIterator, msg: ByteString): String = {
    val ret = new StringBuilder()
    while (true) {
      val length = it.getByte
      if (length == 0) {
        val r = ret.result()
        return r
      }

      if (ret.nonEmpty)
        ret.append('.')

      if ((length & 0xc0) == 0xc0) {
        val offset = ((length.toShort & 0x3f) << 8) | (it.getByte.toShort & 0x00ff)
        return ret.result() + parse(msg.iterator.drop(offset), msg)
      }

      ret.appendAll(it.clone().take(length).map(_.toChar))
      it.drop(length)
    }
    throw new RuntimeException(s"Unable to parse domain name from msg: $msg")
  }
}
