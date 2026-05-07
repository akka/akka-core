/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.ThreadLocalRandom.{ current => random }

import akka.stream.testkit._

class FlowMapSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) with ScriptedTest {

  "A Map" must {

    "map" in {
      def script =
        Script(TestConfig.RandomTestRange.map { _ =>
          val x = random.nextInt(); Seq(x) -> Seq(x.toString)
        }: _*)
      TestConfig.RandomTestRange.foreach(_ => runScript(script)(_.map(_.toString)))
    }

    "not blow up with high request counts" in {
      val probe = TestSubscriber.manualProbe[Int]()
      Source(List(1))
        .map(_ + 1)
        .map(_ + 1)
        .map(_ + 1)
        .map(_ + 1)
        .map(_ + 1)
        .runWith(Sink.asPublisher(false))
        .subscribe(probe)

      val subscription = probe.expectSubscription()
      for (_ <- 1 to 10000) {
        subscription.request(Int.MaxValue)
      }

      probe.expectNext(6)
      probe.expectComplete()

    }

  }

}
