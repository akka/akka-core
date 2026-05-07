/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.stream.AbruptTerminationException
import akka.stream.Materializer
import akka.stream.testkit._

class HeadSinkSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) with ScriptedTest {

  "A Flow with Sink.head" must {

    "yield the first value for simple source" in {
      implicit val ec = system.dispatcher
      //#head-operator-example
      val source = Source(1 to 10)
      val result: Future[Int] = source.runWith(Sink.head)
      result.map(println)
      // 1
      //#head-operator-example
      result.futureValue shouldEqual 1
    }

    "yield the first value" in {
      val p = TestPublisher.manualProbe[Int]()
      val f: Future[Int] = Source.fromPublisher(p).map(identity).runWith(Sink.head)
      val proc = p.expectSubscription()
      proc.expectRequest()
      proc.sendNext(42)
      Await.result(f, 100.millis) should be(42)
      proc.expectCancellation()
    }

    "yield the first value when actively constructing" in {
      val p = TestPublisher.manualProbe[Int]()
      val f = Sink.head[Int]
      val s = Source.asSubscriber[Int]
      val (subscriber, future) = s.toMat(f)(Keep.both).run()

      p.subscribe(subscriber)
      val proc = p.expectSubscription()
      proc.expectRequest()
      proc.sendNext(42)
      Await.result(future, 100.millis) should be(42)
      proc.expectCancellation()
    }

    "yield the first error" in {
      val ex = new RuntimeException("ex")
      (intercept[RuntimeException] {
        Await.result(Source.failed[Int](ex).runWith(Sink.head), 1.second)
      } should be).theSameInstanceAs(ex)
    }

    "yield NoSuchElementException for empty stream" in {
      intercept[NoSuchElementException] {
        Await.result(Source.empty[Int].runWith(Sink.head), 1.second)
      }.getMessage should be("head of empty stream")
    }

  }
  "A Flow with Sink.headOption" must {

    "yield the first value" in {
      val p = TestPublisher.manualProbe[Int]()
      val f: Future[Option[Int]] = Source.fromPublisher(p).map(identity).runWith(Sink.headOption)
      val proc = p.expectSubscription()
      proc.expectRequest()
      proc.sendNext(42)
      Await.result(f, 100.millis) should be(Some(42))
      proc.expectCancellation()
    }

    "yield the first error" in {
      val ex = new RuntimeException("ex")
      (intercept[RuntimeException] {
        Await.result(Source.failed[Int](ex).runWith(Sink.head), 1.second)
      } should be).theSameInstanceAs(ex)
    }

    "yield None for empty stream" in {
      Await.result(Source.empty[Int].runWith(Sink.headOption), 1.second) should be(None)
    }

    "fail on abrupt termination" in {
      val mat = Materializer(system)
      val source = TestPublisher.probe()
      val f = Source.fromPublisher(source).runWith(Sink.headOption)(mat)
      mat.shutdown()

      // this one always fails with the AbruptTerminationException rather than the
      // AbruptStageTerminationException for some reason
      f.failed.futureValue shouldBe an[AbruptTerminationException]
    }

  }

}
