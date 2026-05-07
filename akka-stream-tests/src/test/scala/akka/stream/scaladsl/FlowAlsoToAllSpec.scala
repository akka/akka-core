/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.stream.testkit._
import akka.stream.testkit.scaladsl.TestSink

class FlowAlsoToAllSpec extends StreamSpec("""
    akka.stream.materializer.initial-input-buffer-size = 2
  """) with ScriptedTest {

  "An also to all" must {
    "publish elements to all its downstream" in {
      val (sub1, sink1) = TestSink[Int]().preMaterialize();
      val (sub2, sink2) = TestSink[Int]().preMaterialize();
      val (sub3, sink3) = TestSink[Int]().preMaterialize();
      Source.single(1).alsoToAll(sink1, sink2).runWith(sink3)
      sub1.expectSubscription().request(1)
      sub2.expectSubscription().request(1)
      sub3.expectSubscription().request(1)
      sub1.expectNext(1).expectComplete()
      sub2.expectNext(1).expectComplete()
      sub3.expectNext(1).expectComplete()
    }

    "publish elements to its only downstream" in {
      val (sub1, sink1) = TestSink[Int]().preMaterialize();
      Source.single(1).alsoToAll().runWith(sink1)
      sub1.expectSubscription().request(1)
      sub1.expectNext(1).expectComplete()
    }

  }
}
