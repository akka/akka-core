/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import akka.stream.SubscriptionWithCancelException
import akka.stream.testkit.StreamSpec
import akka.testkit.LongRunningTest
import akka.util.ConstantFun

class InterpreterStressSpec extends StreamSpec with GraphInterpreterSpecKit {

  val chainLength = 1000 * 1000
  val halfLength = chainLength / 2
  val repetition = 100

  val map = Map((x: Int) => x + 1)

  // GraphStages can be reused
  val dropOne = Drop(1)
  val takeOne = Take(1)
  val takeHalfOfRepetition = Take(repetition / 2)

  "Interpreter" must {

    "work with a massive chain of maps" taggedAs LongRunningTest in new OneBoundedSetup[Int](
      Vector.fill(chainLength)(map): _*) {
      lastEvents() should be(Set.empty)
      val tstamp = System.nanoTime()

      var i = 0
      while (i < repetition) {
        downstream.requestOne()
        lastEvents() should be(Set(RequestOne))

        upstream.onNext(i)
        lastEvents() should be(Set(OnNext(i + chainLength)))
        i += 1
      }

      upstream.onComplete()
      lastEvents() should be(Set(OnComplete))

      val time = (System.nanoTime() - tstamp) / (1000.0 * 1000.0 * 1000.0)
      // Not a real benchmark, just for ballpark check
      info(s"Chain finished in $time seconds ${(chainLength * repetition) / (time * 1000 * 1000)} million maps/s")
    }

    "work with a massive chain of maps with early complete" taggedAs LongRunningTest in new OneBoundedSetup[Int](
      Vector.fill(halfLength)(map) ++
      Seq(takeHalfOfRepetition) ++
      Vector.fill(halfLength)(map): _*) {

      lastEvents() should be(Set.empty)
      val tstamp = System.nanoTime()

      var i = 0
      while (i < (repetition / 2) - 1) {
        downstream.requestOne()
        lastEvents() should be(Set(RequestOne))

        upstream.onNext(i)
        lastEvents() should be(Set(OnNext(i + chainLength)))
        i += 1
      }

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(
        Set(Cancel(SubscriptionWithCancelException.StageWasCompleted), OnComplete, OnNext(0 + chainLength)))

      val time = (System.nanoTime() - tstamp) / (1000.0 * 1000.0 * 1000.0)
      // Not a real benchmark, just for ballpark check
      info(s"Chain finished in $time seconds ${(chainLength * repetition) / (time * 1000 * 1000)} million maps/s")
    }

    "work with a massive chain of takes" in new OneBoundedSetup[Int](Vector.fill(chainLength / 10)(takeOne): _*) {
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      upstream.onNext(0)
      lastEvents() should be(Set(Cancel(SubscriptionWithCancelException.StageWasCompleted), OnNext(0), OnComplete))

    }

    "work with a massive chain of drops" in new OneBoundedSetup[Int](Vector.fill(chainLength / 1000)(dropOne): _*) {
      lastEvents() should be(Set.empty)

      downstream.requestOne()
      lastEvents() should be(Set(RequestOne))

      var i = 0
      while (i < (chainLength / 1000)) {
        upstream.onNext(0)
        lastEvents() should be(Set(RequestOne))
        i += 1
      }

      upstream.onNext(0)
      lastEvents() should be(Set(OnNext(0)))

    }

    "work with a massive chain of batches by overflowing to the heap" in {

      val batch = Batch(0L, ConstantFun.zeroLong, (in: Int) => in, (agg: Int, in: Int) => agg + in)

      new OneBoundedSetup[Int](Vector.fill(chainLength / 10)(batch): _*) {

        lastEvents() should be(Set(RequestOne))

        var i = 0
        while (i < repetition) {
          upstream.onNext(1)
          lastEvents() should be(Set(RequestOne))
          i += 1
        }
      }
    }
  }

}
