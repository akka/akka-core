/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream.ClosedShape
import akka.stream.testkit.StreamSpec

class PublisherSinkSpec extends StreamSpec {

  "A PublisherSink" must {

    "be unique when created twice" in {

      val (pub1, pub2) = RunnableGraph
        .fromGraph(GraphDSL.createGraph(Sink.asPublisher[Int](false), Sink.asPublisher[Int](false))(Keep.both) {
          implicit b => (p1, p2) =>
            import GraphDSL.Implicits._

            val bcast = b.add(Broadcast[Int](2))

            Source(0 to 5) ~> bcast.in
            bcast.out(0).map(_ * 2) ~> p1.in
            bcast.out(1) ~> p2.in
            ClosedShape
        })
        .run()

      val f1 = Source.fromPublisher(pub1).map(identity).runFold(0)(_ + _)
      val f2 = Source.fromPublisher(pub2).map(identity).runFold(0)(_ + _)

      Await.result(f1, 3.seconds) should be(30)
      Await.result(f2, 3.seconds) should be(15)

    }

    "work with SubscriberSource" in {
      val (sub, pub) = Source.asSubscriber[Int].toMat(Sink.asPublisher(false))(Keep.both).run()
      Source(1 to 100).to(Sink.fromSubscriber(sub)).run()
      Await.result(Source.fromPublisher(pub).limit(1000).runWith(Sink.seq), 3.seconds) should ===(1 to 100)
    }

    "be able to use Publisher in materialized value transformation" in {
      val f = Source(1 to 3)
        .runWith(Sink.asPublisher[Int](false).mapMaterializedValue(p => Source.fromPublisher(p).runFold(0)(_ + _)))

      Await.result(f, 3.seconds) should be(6)
    }
  }

}
