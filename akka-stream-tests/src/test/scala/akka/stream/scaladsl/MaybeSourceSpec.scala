/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import akka.stream.{ AbruptStageTerminationException, KillSwitches, Materializer }
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.Utils.TE
import akka.testkit.DefaultTimeout

class MaybeSourceSpec extends StreamSpec with DefaultTimeout {

  "The Maybe Source" must {

    "complete materialized promise with None when stream cancels" in {
      val neverSource = Source.maybe[Int]
      val pubSink = Sink.asPublisher[Int](false)

      val (f, neverPub) = neverSource.toMat(pubSink)(Keep.both).run()

      val c = TestSubscriber.manualProbe[Int]()
      neverPub.subscribe(c)
      val subs = c.expectSubscription()

      subs.request(1000)
      c.expectNoMessage(300.millis)

      subs.cancel()
      f.future.futureValue shouldEqual None
    }

    "complete materialized promise with None when stream cancels with a failure cause" in {
      val (promise, killswitch) = Source.maybe[Int].viaMat(KillSwitches.single)(Keep.both).to(Sink.ignore).run()
      val boom = TE("Boom")
      killswitch.abort(boom)
      // Could make sense to fail it with the propagated exception instead but that breaks
      // the assumptions in the CoupledTerminationFlowSpec
      promise.future.futureValue should ===(None)
    }

    "allow external triggering of empty completion" in {
      val neverSource = Source.maybe[Int].filter(_ => false)
      val counterSink = Sink.fold[Int, Int](0) { (acc, _) =>
        acc + 1
      }

      val (neverPromise, counterFuture) = neverSource.toMat(counterSink)(Keep.both).run()

      // external cancellation
      neverPromise.trySuccess(None) shouldEqual true

      counterFuture.futureValue shouldEqual 0
    }

    "allow external triggering of empty completion when there was no demand" in {
      val probe = TestSubscriber.probe[Int]()
      val promise = Source.maybe[Int].to(Sink.fromSubscriber(probe)).run()

      // external cancellation
      probe.ensureSubscription()
      promise.trySuccess(None) shouldEqual true
      probe.expectComplete()
    }

    "allow external triggering of non-empty completion" in {
      val neverSource = Source.maybe[Int]
      val counterSink = Sink.head[Int]

      val (neverPromise, counterFuture) = neverSource.toMat(counterSink)(Keep.both).run()

      // external cancellation
      neverPromise.trySuccess(Some(6)) shouldEqual true

      counterFuture.futureValue shouldEqual 6
    }

    "allow external triggering of onError" in {
      val neverSource = Source.maybe[Int]
      val counterSink = Sink.fold[Int, Int](0) { (acc, _) =>
        acc + 1
      }

      val (neverPromise, counterFuture) = neverSource.toMat(counterSink)(Keep.both).run()

      // external cancellation
      neverPromise.tryFailure(new Exception("Boom") with NoStackTrace) shouldEqual true

      counterFuture.failed.futureValue.getMessage should include("Boom")
    }

    "complete materialized future when materializer is shutdown" in {
      val mat = Materializer(system)
      val neverSource = Source.maybe[Int]
      val pubSink = Sink.asPublisher[Int](false)

      val (f, neverPub) = neverSource.toMat(pubSink)(Keep.both).run()(mat)

      val c = TestSubscriber.manualProbe[Int]()
      neverPub.subscribe(c)
      c.expectSubscription()

      mat.shutdown()
      f.future.failed.futureValue shouldBe an[AbruptStageTerminationException]
    }

  }
}
