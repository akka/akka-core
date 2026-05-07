/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.dispatch

import java.util.concurrent.CountDownLatch

import akka.actor._
import akka.testkit.AkkaSpec

/**
 * Tests the behavior of the executor based event driven dispatcher when multiple actors are being dispatched on it.
 */
class DispatcherActorsSpec extends AkkaSpec {
  class SlowActor(finishedCounter: CountDownLatch) extends Actor {

    def receive = {
      case _: Int => {
        Thread.sleep(50) // slow actor
        finishedCounter.countDown()
      }
    }
  }

  class FastActor(finishedCounter: CountDownLatch) extends Actor {
    def receive = {
      case _: Int => {
        finishedCounter.countDown()
      }
    }
  }

  "A dispatcher and two actors" must {
    "not block fast actors by slow actors" in {
      val sFinished = new CountDownLatch(50)
      val fFinished = new CountDownLatch(10)
      val s = system.actorOf(Props(new SlowActor(sFinished)))
      val f = system.actorOf(Props(new FastActor(fFinished)))

      // send a lot of stuff to s
      for (i <- 1 to 50) {
        s ! i
      }

      // send some messages to f
      for (i <- 1 to 10) {
        f ! i
      }

      // now assert that f is finished while s is still busy
      fFinished.await()
      assert(sFinished.getCount > 0)
      sFinished.await()
      assert(sFinished.getCount === 0L)
      system.stop(f)
      system.stop(s)
    }
  }
}
