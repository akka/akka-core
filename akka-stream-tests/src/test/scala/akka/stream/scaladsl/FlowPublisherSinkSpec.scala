/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream.testkit.StreamSpec

class FlowPublisherSinkSpec extends StreamSpec {

  "A FlowPublisherSink" must {

    "work with SubscriberSource" in {
      val (sub, pub) =
        JavaFlowSupport.Source.asSubscriber[Int].toMat(JavaFlowSupport.Sink.asPublisher(false))(Keep.both).run()
      Source(1 to 100).to(JavaFlowSupport.Sink.fromSubscriber(sub)).run()
      Await.result(JavaFlowSupport.Source.fromPublisher(pub).limit(1000).runWith(Sink.seq), 3.seconds) should ===(
        1 to 100)
    }

    "be able to use Publisher in materialized value transformation" in {
      val f = Source(1 to 3).runWith(JavaFlowSupport.Sink.asPublisher[Int](false).mapMaterializedValue { p =>
        JavaFlowSupport.Source.fromPublisher(p).runFold(0)(_ + _)
      })

      Await.result(f, 3.seconds) should be(6)
    }
  }

}
