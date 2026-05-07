/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.NotUsed

/**
 * Convenience functions for often-encountered purposes like keeping only the
 * left (first) or only the right (second) of two input values.
 */
object Keep {
  private val _left = (l: Any, _: Any) => l
  private val _right = (_: Any, r: Any) => r
  private val _both = (l: Any, r: Any) => (l, r)
  private val _none = (_: Any, _: Any) => NotUsed

  def left[L, R]: (L, R) => L = _left.asInstanceOf[(L, R) => L]
  def right[L, R]: (L, R) => R = _right.asInstanceOf[(L, R) => R]
  def both[L, R]: (L, R) => (L, R) = _both.asInstanceOf[(L, R) => (L, R)]
  def none[L, R]: (L, R) => NotUsed = _none.asInstanceOf[(L, R) => NotUsed]
}
