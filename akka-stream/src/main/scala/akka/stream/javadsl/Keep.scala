/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.javadsl

import akka.NotUsed
import akka.japi.Pair
import akka.japi.function

object Keep {
  private val _left = new function.Function2[Any, Any, Any] with ((Any, Any) => Any) { def apply(l: Any, r: Any) = l }
  private val _right = new function.Function2[Any, Any, Any] with ((Any, Any) => Any) { def apply(l: Any, r: Any) = r }
  private val _both = new function.Function2[Any, Any, Any] with ((Any, Any) => Any) {
    def apply(l: Any, r: Any) = new akka.japi.Pair(l, r)
  }
  private val _none = new function.Function2[Any, Any, NotUsed] with ((Any, Any) => NotUsed) {
    def apply(l: Any, r: Any) = NotUsed
  }

  def left[L, R]: function.Function2[L, R, L] = _left.asInstanceOf[function.Function2[L, R, L]]
  def right[L, R]: function.Function2[L, R, R] = _right.asInstanceOf[function.Function2[L, R, R]]
  def both[L, R]: function.Function2[L, R, L Pair R] = _both.asInstanceOf[function.Function2[L, R, L Pair R]]
  def none[L, R]: function.Function2[L, R, NotUsed] = _none.asInstanceOf[function.Function2[L, R, NotUsed]]
}
