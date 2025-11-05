/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.source

import akka.NotUsed
import akka.stream.scaladsl.Source

object Unfold {

  // #countdown
  def countDown(from: Int): Source[Int, NotUsed] =
    Source.unfold(from) { current =>
      if (current == 0) None
      else Some((current - 1, current))
    }
  // #countdown

  // #fibonacci
  def fibonacci: Source[BigInt, NotUsed] =
    Source.unfold((BigInt(0), BigInt(1))) {
      case (a, b) =>
        Some(((b, a + b), a))
    }
  // #fibonacci

}
