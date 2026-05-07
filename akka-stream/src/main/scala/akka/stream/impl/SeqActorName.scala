/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import java.util.concurrent.atomic.AtomicLong

import akka.annotation.{ DoNotInherit, InternalApi }

/**
 * INTERNAL API
 * As discussed in https://github.com/akka/akka-core/issues/16613
 *
 * Generator of sequentially numbered actor names.
 * Pulled out from HTTP internals, most often used used by streams which materialize actors directly
 */
@DoNotInherit private[akka] abstract class SeqActorName {
  def next(): String
  def copy(name: String): SeqActorName
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SeqActorName {
  def apply(prefix: String) = new SeqActorNameImpl(prefix, new AtomicLong(0))
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class SeqActorNameImpl(val prefix: String, counter: AtomicLong) extends SeqActorName {
  def next(): String = prefix + '-' + counter.getAndIncrement()

  def copy(newPrefix: String): SeqActorName = new SeqActorNameImpl(newPrefix, counter)
}
