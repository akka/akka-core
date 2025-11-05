/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._

import akka.stream.testkit._

class FlowBatchWeightedSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
  """) {

  "BatchWeighted" must {
    "Not aggregate heavy elements" in {
      val publisher = TestPublisher.probe[Int]()
      val subscriber = TestSubscriber.manualProbe[Int]()

      Source
        .fromPublisher(publisher)
        .batchWeighted(max = 3, _ => 4, seed = i => i)(aggregate = _ + _)
        .to(Sink.fromSubscriber(subscriber))
        .run()
      val sub = subscriber.expectSubscription()

      publisher.sendNext(1)
      publisher.sendNext(2)

      sub.request(1)
      subscriber.expectNext(1)

      publisher.sendNext(3)
      subscriber.expectNoMessage(1.second)

      sub.request(2)
      subscriber.expectNext(2)
      subscriber.expectNext(3)

      sub.cancel()
    }
  }
}
