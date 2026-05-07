/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.NotUsed
import akka.stream.scaladsl._
import akka.stream.testkit._

import scala.concurrent.duration._
import akka.testkit.TestLatch

import scala.annotation.nowarn
import scala.concurrent.Await

@nowarn("msg=never used") // sample snippets
class RecipeMissedTicks extends RecipeSpec {

  "Recipe for collecting missed ticks" must {

    "work" in {
      type Tick = Unit

      val pub = TestPublisher.probe[Tick]()
      val sub = TestSubscriber.manualProbe[Int]()
      val tickStream = Source.fromPublisher(pub)
      val sink = Sink.fromSubscriber(sub)

      //#missed-ticks
      val missedTicks: Flow[Tick, Int, NotUsed] =
        Flow[Tick].conflateWithSeed(seed = _ => 0)((missedTicks, _) => missedTicks + 1)
      //#missed-ticks
      val latch = TestLatch(3)
      val realMissedTicks: Flow[Tick, Int, NotUsed] =
        Flow[Tick].conflateWithSeed(seed = _ => 0)((missedTicks, _) => { latch.countDown(); missedTicks + 1 })

      tickStream.via(realMissedTicks).to(sink).run()

      pub.sendNext(())
      pub.sendNext(())
      pub.sendNext(())
      pub.sendNext(())

      val subscription = sub.expectSubscription()
      Await.ready(latch, 1.second)

      subscription.request(1)
      sub.expectNext(3)

      subscription.request(1)
      sub.expectNoMessage()

      pub.sendNext(())
      sub.expectNext(0)

      pub.sendComplete()
      subscription.request(1)
      sub.expectComplete()
    }

  }

}
