/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.concurrent.ThreadLocalRandom.{ current => random }

import akka.stream.testkit._

class FlowDropSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) with ScriptedTest {

  "A Drop" must {

    "drop" in {
      def script(d: Int) =
        Script(TestConfig.RandomTestRange.map { n =>
          Seq(n) -> (if (n <= d) Nil else Seq(n))
        }: _*)
      TestConfig.RandomTestRange.foreach { _ =>
        val d = Math.min(Math.max(random.nextInt(-10, 60), 0), 50)
        runScript(script(d))(_.drop(d))
      }
    }

    "not drop anything for negative n" in {
      val probe = TestSubscriber.manualProbe[Int]()
      Source(List(1, 2, 3)).drop(-1).to(Sink.fromSubscriber(probe)).run()
      probe.expectSubscription().request(10)
      probe.expectNext(1)
      probe.expectNext(2)
      probe.expectNext(3)
      probe.expectComplete()
    }

  }

}
