/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import akka.Done
import akka.pattern.pipe
import akka.stream._
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource

class FlowWatchTerminationSpec extends StreamSpec {

  "A WatchTermination" must {

    "complete future when stream is completed" in {
      val (future, p) = Source(1 to 4).watchTermination()(Keep.right).toMat(TestSink[Int]())(Keep.both).run()
      p.request(4).expectNext(1, 2, 3, 4)
      future.futureValue should ===(Done)
      p.expectComplete()
    }

    "complete future when stream is cancelled from downstream" in {
      val (future, p) = Source(1 to 4).watchTermination()(Keep.right).toMat(TestSink[Int]())(Keep.both).run()
      p.request(3).expectNext(1, 2, 3).cancel()
      future.futureValue should ===(Done)
    }

    "fail future when stream is failed" in {
      val ex = new RuntimeException("Stream failed.") with NoStackTrace
      val (p, future) = TestSource[Int]().watchTermination()(Keep.both).to(Sink.ignore).run()
      p.sendNext(1)
      p.sendError(ex)
      whenReady(future.failed) { _ shouldBe (ex) }
    }

    "complete the future for an empty stream" in {
      val (future, p) = Source.empty[Int].watchTermination()(Keep.right).toMat(TestSink[Int]())(Keep.both).run()
      p.request(1)
      future.futureValue should ===(Done)
    }

    "complete future for graph" in {
      implicit val ec = system.dispatcher

      val ((sourceProbe, future), sinkProbe) =
        TestSource[Int]().watchTermination()(Keep.both).concat(Source(2 to 5)).toMat(TestSink[Int]())(Keep.both).run()
      future.pipeTo(testActor)
      sinkProbe.request(5)
      sourceProbe.sendNext(1)
      sinkProbe.expectNext(1)
      expectNoMessage(300.millis)

      sourceProbe.sendComplete()
      expectMsg(Done)

      sinkProbe.expectNextN(2 to 5).expectComplete()
    }

    "fail future when stream abruptly terminated" in {
      val mat = Materializer(system)

      val (_, future) = TestSource[Int]().watchTermination()(Keep.both).to(Sink.ignore).run()(mat)
      mat.shutdown()

      future.failed.futureValue shouldBe an[AbruptTerminationException]
    }

  }

}
