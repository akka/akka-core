/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.source

import akka.actor.ActorSystem
// #imports
import akka.stream.scaladsl.{ Concat, Merge, Source }
// ...

// #imports

object Combine {
  implicit val system: ActorSystem = null

  def merge(): Unit = {
    // #source-combine-merge
    val source1 = Source(1 to 3)
    val source2 = Source(8 to 10)
    val source3 = Source(12 to 14)
    val combined = Source.combine(source1, source2, source3)(Merge(_))
    combined.runForeach(println)
    // could print (order between sources is not deterministic)
    // 1
    // 12
    // 8
    // 9
    // 13
    // 14
    // 2
    // 10
    // 3
    // #source-combine-merge
  }

  @throws[Exception]
  def concat(): Unit = {
    // #source-combine-concat
    val source1 = Source(1 to 3)
    val source2 = Source(8 to 10)
    val source3 = Source(12 to 14)
    val sources = Source.combine(source1, source2, source3)(Concat(_))
    sources.runForeach(println)
    // prints (order is deterministic)
    // 1
    // 2
    // 3
    // 8
    // 9
    // 10
    // 12
    // 13
    // 14
    // #source-combine-concat
  }
}
