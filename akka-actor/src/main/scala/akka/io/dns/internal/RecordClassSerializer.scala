/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import akka.annotation.InternalApi
import akka.io.dns.RecordClass
import akka.util.{ ByteIterator, ByteStringBuilder }

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object RecordClassSerializer {

  def parse(it: ByteIterator): RecordClass = {
    it.getShort match {
      case 1       => RecordClass.IN
      case 2       => RecordClass.CS
      case 3       => RecordClass.CH
      case 255     => RecordClass.WILDCARD
      case unknown => throw new RuntimeException(s"Unexpected record class $unknown")
    }
  }

  def write(out: ByteStringBuilder, c: RecordClass): Unit = {
    out.putShort(c.code)
  }
}
