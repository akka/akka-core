/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import scala.annotation.tailrec
import scala.collection.immutable

/**
 * INTERNAL API
 */
private[akka] object Collections {

  case object EmptyImmutableSeq extends immutable.Seq[Nothing] {
    override final def iterator = Iterator.empty
    override final def apply(idx: Int): Nothing = throw new java.lang.IndexOutOfBoundsException(idx.toString)
    override final def length: Int = 0
  }

  abstract class PartialImmutableValuesIterable[From, To] extends immutable.Iterable[To] {
    def isDefinedAt(from: From): Boolean
    def apply(from: From): To
    def valuesIterator: Iterator[From]
    final def iterator: Iterator[To] = {
      val superIterator = valuesIterator
      new Iterator[To] {
        private[this] var _next: To = _
        private[this] var _hasNext = false

        override final def hasNext: Boolean = {
          @tailrec def tailrecHasNext(): Boolean = {
            if (!_hasNext && superIterator.hasNext) { // If we need and are able to look for the next value
              val potentiallyNext = superIterator.next()
              if (isDefinedAt(potentiallyNext)) {
                _next = apply(potentiallyNext)
                _hasNext = true
                true
              } else tailrecHasNext() //Attempt to find the next
            } else _hasNext // Return if we found one
          }

          tailrecHasNext()
        }

        override final def next(): To =
          if (hasNext) {
            val ret = _next
            _next = null.asInstanceOf[To] // Mark as consumed (nice to the GC, don't leak the last returned value)
            _hasNext = false // Mark as consumed (we need to look for the next value)
            ret
          } else throw new java.util.NoSuchElementException("next")
      }
    }

    override lazy val size: Int = iterator.size
    override def foreach[C](f: To => C) = iterator.foreach(f)
  }

}
