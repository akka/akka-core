/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.stream.testkit.{ StreamSpec, TestSubscriber }
import akka.testkit.DefaultTimeout

class NeverSourceSpec extends StreamSpec with DefaultTimeout {

  "The Never Source" must {

    "never completes" in {
      val neverSource = Source.never[Int]
      val pubSink = Sink.asPublisher[Int](false)

      val neverPub = neverSource.toMat(pubSink)(Keep.right).run()

      val c = TestSubscriber.manualProbe[Int]()
      neverPub.subscribe(c)
      val subs = c.expectSubscription()

      subs.request(1)
      c.expectNoMessage(300.millis)

      subs.cancel()
    }
  }
}
