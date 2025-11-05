/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.sourceorflow

//#conflate
//#conflateWithSeed
import akka.stream.scaladsl.Source

//#conflateWithSeed
//#conflate

object Conflate {
  def conflateExample(): Unit = {
    //#conflate
    import scala.concurrent.duration._

    Source
      .cycle(() => List(1, 10, 100, 1000).iterator)
      .throttle(10, per = 1.second) // faster upstream
      .conflate((acc, el) => acc + el) // acc: Int, el: Int
      .throttle(1, per = 1.second) // slow downstream
    //#conflate
  }

  def conflateWithSeedExample(): Unit = {
    //#conflateWithSeed
    import scala.concurrent.duration._

    case class Summed(i: Int) {
      def sum(other: Summed) = Summed(this.i + other.i)
    }

    Source
      .cycle(() => List(1, 10, 100, 1000).iterator)
      .throttle(10, per = 1.second) // faster upstream
      .conflateWithSeed(el => Summed(el))((acc, el) => acc.sum(Summed(el))) // (Summed, Int) => Summed
      .throttle(1, per = 1.second) // slow downstream
    //#conflateWithSeed
  }

}
