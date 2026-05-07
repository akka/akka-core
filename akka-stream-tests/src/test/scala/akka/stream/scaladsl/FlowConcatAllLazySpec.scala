/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import java.util.StringJoiner
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NoStackTrace

import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSink

class FlowConcatAllLazySpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
    akka.stream.materializer.max-input-buffer-size = 2
  """) {

  "ConcatAllLazy" must {

    val testException = new Exception("test") with NoStackTrace

    "work in the happy case" in {
      val s1 = Source(1 to 2)
      val s2 = Source(List.empty[Int])
      val s3 = Source(List(3))
      val s4 = Source(4 to 6)
      val s5 = Source(7 to 10)
      val s6 = Source.empty
      val s7 = Source.single(11)

      val sub = s1.concatAllLazy(s2, s3, s4, s5, s6, s7).runWith(TestSink[Int]());
      sub.expectSubscription().request(11)
      sub.expectNextN(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)).expectComplete()
    }

    "concat single upstream elements to its downstream" in {
      val sub = Source(1 to 3).concatAllLazy().runWith(TestSink[Int]())
      sub.expectSubscription().request(3)
      sub.expectNextN(List(1, 2, 3)).expectComplete()
    }

    "can cancel other upstream sources" in {
      val pub1 = TestPublisher.probe[Int]()
      val pub2 = TestPublisher.probe[Int]()
      Source(1 to 3)
        .concatAllLazy(Source.fromPublisher(pub1), Source.fromPublisher(pub2))
        .runWith(TestSink[Int]())
        .request(2)
        .expectNext(1, 2)
        .cancel()
        .expectNoMessage()
      pub1.expectCancellation()
      pub2.expectCancellation()
    }

    "can cancel other upstream sources with error" in {
      val pub1 = TestPublisher.probe[Int]()
      val pub2 = TestPublisher.probe[Int]()
      val (promise, sub) = Source
        .maybe[Int]
        .concatAllLazy(Source.fromPublisher(pub1), Source.fromPublisher(pub2))
        .toMat(TestSink[Int]())(Keep.both)
        .run()
      promise.tryFailure(testException)
      sub.expectSubscriptionAndError(testException)
      pub1.expectCancellationWithCause(testException)
      pub2.expectCancellationWithCause(testException)
    }

    "lazy materialization other sources" in {
      val materialized = new AtomicBoolean()
      Source(1 to 3)
        .concatAllLazy(Source.lazySource(() => {
          materialized.set(true)
          Source.single(4)
        }))
        .runWith(TestSink())
        .request(2)
        .expectNext(1, 2)
        .cancel()
        .expectNoMessage()
      materialized.get() shouldBe (false)
    }

    "work in example" in {
      //#concatAllLazy
      val sourceA = Source(List(1, 2, 3))
      val sourceB = Source(List(4, 5, 6))
      val sourceC = Source(List(7, 8, 9))
      sourceA
        .concatAllLazy(sourceB, sourceC)
        .fold(new StringJoiner(","))((joiner, input) => joiner.add(String.valueOf(input)))
        .runWith(Sink.foreach(println))
      //prints 1,2,3,4,5,6,7,8,9
      //#concatAllLazy
    }

  }

}
