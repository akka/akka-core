/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.ThreadLocalRandom.{ current => random }

import akka.stream.ActorAttributes._
import akka.stream.Supervision._
import akka.stream.testkit.ScriptedTest
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils.TE
import akka.stream.testkit.scaladsl.TestSink

class FlowCollectSpec extends StreamSpec with ScriptedTest {

  "A Collect" must {

    "collect" in {
      def script =
        Script(TestConfig.RandomTestRange.map { _ =>
          val x = random.nextInt(0, 10000)
          Seq(x) -> (if ((x & 1) == 0) Seq((x * x).toString) else Seq.empty[String])
        }: _*)
      TestConfig.RandomTestRange.foreach(_ =>
        runScript(script)(_.collect {
          case x if x % 2 == 0 => (x * x).toString
        }))
    }

    "restart when Collect throws" in {
      val pf: PartialFunction[Int, Int] = { case x: Int => if (x == 2) throw TE("") else x }
      Source(1 to 3)
        .collect(pf)
        .withAttributes(supervisionStrategy(restartingDecider))
        .runWith(TestSink[Int]())
        .request(1)
        .expectNext(1)
        .request(1)
        .expectNext(3)
        .request(1)
        .expectComplete()
    }

  }

}
