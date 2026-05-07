/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery.internal

import akka.annotation.InternalApi
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class ChunkedMessage(
    serialized: ByteString,
    firstChunk: Boolean,
    lastChunk: Boolean,
    serializerId: Int,
    manifest: String) {

  override def toString: String =
    s"ChunkedMessage(${serialized.size},$firstChunk,$lastChunk,$serializerId,$manifest)"
}
