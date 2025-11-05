/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils.TE
import akka.testkit.EventFilter

class GraphInterpreterFailureModesSpec extends StreamSpec with GraphInterpreterSpecKit {

  "GraphInterpreter" must {

    "handle failure on onPull" in new FailingStageSetup {
      lastEvents() should be(Set(PreStart(insideOutStage)))

      downstream.pull()
      failOnNextEvent()
      stepAll()

      lastEvents() should be(
        Set(Cancel(upstream, testException), OnError(downstream, testException), PostStop(insideOutStage)))
    }

    "handle failure on onPush" in new FailingStageSetup {
      lastEvents() should be(Set(PreStart(insideOutStage)))

      downstream.pull()
      stepAll()
      clearEvents()
      upstream.push(0)
      failOnNextEvent()
      stepAll()

      lastEvents() should be(
        Set(Cancel(upstream, testException), OnError(downstream, testException), PostStop(insideOutStage)))
    }

    "handle failure on onPull while cancel is pending" in new FailingStageSetup {
      lastEvents() should be(Set(PreStart(insideOutStage)))

      downstream.pull()
      downstream.cancel()
      failOnNextEvent()
      stepAll()

      lastEvents() should be(Set(Cancel(upstream, testException), PostStop(insideOutStage)))
    }

    "handle failure on onPush while complete is pending" in new FailingStageSetup {
      lastEvents() should be(Set(PreStart(insideOutStage)))

      downstream.pull()
      stepAll()
      clearEvents()
      upstream.push(0)
      upstream.complete()
      failOnNextEvent()
      stepAll()

      lastEvents() should be(Set(OnError(downstream, testException), PostStop(insideOutStage)))
    }

    "handle failure on onUpstreamFinish" in new FailingStageSetup {
      lastEvents() should be(Set(PreStart(insideOutStage)))

      upstream.complete()
      failOnNextEvent()
      stepAll()

      lastEvents() should be(Set(OnError(downstream, testException), PostStop(insideOutStage)))
    }

    "handle failure on onUpstreamFailure" in new FailingStageSetup {
      lastEvents() should be(Set(PreStart(insideOutStage)))

      upstream.fail(TE("another exception")) // this is not the exception that will be propagated
      failOnNextEvent()
      stepAll()

      lastEvents() should be(Set(OnError(downstream, testException), PostStop(insideOutStage)))
    }

    "handle failure on onDownstreamFinish" in new FailingStageSetup {
      lastEvents() should be(Set(PreStart(insideOutStage)))

      downstream.cancel()
      failOnNextEvent()
      stepAll()

      lastEvents() should be(Set(Cancel(upstream, testException), PostStop(insideOutStage)))
    }

    "handle failure in preStart" in new FailingStageSetup(initFailOnNextEvent = true) {
      stepAll()

      lastEvents() should be(
        Set(Cancel(upstream, testException), OnError(downstream, testException), PostStop(insideOutStage)))
    }

    "handle failure in postStop" in new FailingStageSetup {
      lastEvents() should be(Set(PreStart(insideOutStage)))

      upstream.complete()
      downstream.cancel()
      failOnPostStop()

      EventFilter.error("Error during postStop in [stage]").intercept {
        stepAll()
        lastEvents() should be(Set.empty)
      }

    }

  }

}
