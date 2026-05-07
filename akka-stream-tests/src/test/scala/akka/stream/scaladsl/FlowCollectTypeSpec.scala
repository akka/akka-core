/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.stream.testkit.StreamSpec

class FlowCollectTypeSpec extends StreamSpec {

  sealed class Fruit
  class Orange extends Fruit
  object Orange extends Orange
  class Apple extends Fruit
  object Apple extends Apple

  "A CollectType" must {

    "collectType" in {
      val fruit = Source(List(Orange, Apple, Apple, Orange))

      val apples = fruit.collectType[Apple].runWith(Sink.seq).futureValue
      apples should equal(List(Apple, Apple))
      val oranges = fruit.collectType[Orange].runWith(Sink.seq).futureValue
      oranges should equal(List(Orange, Orange))
      val all = fruit.collectType[Fruit].runWith(Sink.seq).futureValue
      all should equal(List(Orange, Apple, Apple, Orange))
    }

  }

}
