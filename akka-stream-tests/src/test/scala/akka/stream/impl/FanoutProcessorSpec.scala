/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl

import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils.TE
import akka.testkit.TestProbe

class FanoutProcessorSpec extends StreamSpec {

  "The FanoutProcessor" must {

    // #25634
    "not leak running actors on failed upstream without subscription" in {
      val probe = TestProbe()
      val (promise, publisher) = Source.maybe[Int].toMat(Sink.asPublisher(true))(Keep.both).run()
      val publisherRef = publisher.asInstanceOf[ActorPublisher[Int]].impl
      probe.watch(publisherRef)
      promise.failure(TE("boom"))
      probe.expectTerminated(publisherRef)
    }

    // #25634
    "not leak running actors on failed upstream with one subscription" in {
      val probe = TestProbe()
      val (promise, publisher) = Source.maybe[Int].toMat(Sink.asPublisher(true))(Keep.both).run()
      val publisherRef = publisher.asInstanceOf[ActorPublisher[Int]].impl
      Source.fromPublisher(publisher).runWith(Sink.ignore)
      probe.watch(publisherRef)
      val boom = TE("boom")
      promise.failure(boom)
      probe.expectTerminated(publisherRef)
    }

    // #25634
    "not leak running actors on failed upstream with multiple subscriptions" in {
      val probe = TestProbe()
      val (promise, publisher) = Source.maybe[Int].toMat(Sink.asPublisher(true))(Keep.both).run()
      val publisherRef = publisher.asInstanceOf[ActorPublisher[Int]].impl
      probe.watch(publisherRef)
      Source.fromPublisher(publisher).runWith(Sink.ignore)
      Source.fromPublisher(publisher).runWith(Sink.ignore)
      val boom = TE("boom")
      promise.failure(boom)
      probe.expectTerminated(publisherRef)
    }

    "not leak running actors on completed upstream no subscriptions" in {
      val probe = TestProbe()
      val (promise, publisher) = Source.maybe[Int].toMat(Sink.asPublisher(true))(Keep.both).run()
      val publisherRef = publisher.asInstanceOf[ActorPublisher[Int]].impl
      probe.watch(publisherRef)
      promise.success(None)

      probe.expectTerminated(publisherRef)
    }

    "not leak running actors on completed upstream with one subscription" in {
      val probe = TestProbe()
      val (promise, publisher) = Source.maybe[Int].toMat(Sink.asPublisher(true))(Keep.both).run()
      val publisherRef = publisher.asInstanceOf[ActorPublisher[Int]].impl
      val completed = Source.fromPublisher(publisher).runWith(Sink.ignore)
      probe.watch(publisherRef)

      promise.success(None)

      probe.expectTerminated(publisherRef)
      // would throw if not completed
      completed.futureValue
    }

    "not leak running actors on completed upstream with multiple subscriptions" in {
      val probe = TestProbe()
      val (promise, publisher) = Source.maybe[Int].toMat(Sink.asPublisher(true))(Keep.both).run()
      val publisherRef = publisher.asInstanceOf[ActorPublisher[Int]].impl
      val completed1 = Source.fromPublisher(publisher).runWith(Sink.ignore)
      val completed2 = Source.fromPublisher(publisher).runWith(Sink.ignore)
      probe.watch(publisherRef)
      promise.success(None)

      probe.expectTerminated(publisherRef)
      // would throw if not completed
      completed1.futureValue
      completed2.futureValue
    }

    "not leak running actors on failed downstream" in {
      val probe = TestProbe()
      val (_, publisher) = Source.repeat(1).toMat(Sink.asPublisher(true))(Keep.both).run()
      val publisherRef = publisher.asInstanceOf[ActorPublisher[Int]].impl
      probe.watch(publisherRef)
      Source.fromPublisher(publisher).map(_ => throw TE("boom")).runWith(Sink.ignore)
      probe.expectTerminated(publisherRef)
    }

  }

}
