/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

package object javadsl {
  def combinerToScala[M1, M2, M](f: akka.japi.function.Function2[M1, M2, M]): (M1, M2) => M =
    f match {
      case x if x eq Keep.left   => scaladsl.Keep.left.asInstanceOf[(M1, M2) => M]
      case x if x eq Keep.right  => scaladsl.Keep.right.asInstanceOf[(M1, M2) => M]
      case s: Function2[_, _, _] => s.asInstanceOf[(M1, M2) => M]
      case other                 => other.apply _
    }
}
