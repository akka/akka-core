/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import akka.annotation.InternalApi
import akka.io.dns.{ RecordClass, RecordType }
import akka.util.{ ByteIterator, ByteString, ByteStringBuilder }

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class Question(name: String, qType: RecordType, qClass: RecordClass) {
  def write(out: ByteStringBuilder): Unit = {
    DomainName.write(out, name)
    RecordTypeSerializer.write(out, qType)
    RecordClassSerializer.write(out, qClass)
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object Question {
  def parse(it: ByteIterator, msg: ByteString): Question = {
    val name = DomainName.parse(it, msg)
    val qType = RecordTypeSerializer.parse(it)
    val qClass = RecordClassSerializer.parse(it)
    Question(name, qType, qClass)
  }
}
