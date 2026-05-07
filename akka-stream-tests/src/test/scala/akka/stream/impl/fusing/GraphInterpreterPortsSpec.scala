/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.fusing

import akka.stream.SubscriptionWithCancelException
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.Utils._

class GraphInterpreterPortsSpec extends StreamSpec with GraphInterpreterSpecKit {

  "Port states" must {

    // FIXME test failure scenarios

    for (chasing <- List(false, true)) {

      s"properly transition on push and pull (chasing = $chasing)" in new PortTestSetup(chasing) {
        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.pull()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        stepAll()

        lastEvents() should be(Set(RequestOne(out)))
        out.isAvailable should be(true)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.push(0)

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        stepAll()

        lastEvents() should be(Set(OnNext(in, 0)))
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(true)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }

        in.grab() should ===(0)

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        // Cycle completed
      }

      s"drop ungrabbed element on pull (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        step()
        clearEvents()
        out.push(0)
        step()

        lastEvents() should be(Set(OnNext(in, 0)))

        in.pull()

        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate complete while downstream is active (chasing = $chasing)" in new PortTestSetup(chasing) {
        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)

        out.complete()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }

        stepAll()

        lastEvents() should be(Set(OnComplete(in)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.cancel() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate complete while upstream is active (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        stepAll()

        lastEvents() should be(Set(RequestOne(out)))
        out.isAvailable should be(true)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)

        out.complete()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }

        stepAll()

        lastEvents() should be(Set(OnComplete(in)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.cancel() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

      }

      s"propagate complete while pull is in flight (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)

        out.complete()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }

        stepAll()

        lastEvents() should be(Set(OnComplete(in)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.cancel() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate complete while push is in flight (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        stepAll()
        clearEvents()

        out.push(0)

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)

        out.complete()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }

        step()

        lastEvents() should be(Set(OnNext(in, 0)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(true)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        in.grab() should ===(0)
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        step()

        lastEvents() should be(Set(OnComplete(in)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate complete while push is in flight and keep ungrabbed element (chasing = $chasing)" in new PortTestSetup(
        chasing) {
        in.pull()
        stepAll()
        clearEvents()

        out.push(0)
        out.complete()
        step()

        lastEvents() should be(Set(OnNext(in, 0)))
        step()

        lastEvents() should be(Set(OnComplete(in)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(true)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        in.grab() should ===(0)
      }

      s"propagate complete while push is in flight and pulled after the push (chasing = $chasing)" in new PortTestSetup(
        chasing) {
        in.pull()
        stepAll()
        clearEvents()

        out.push(0)
        out.complete()
        step()

        lastEvents() should be(Set(OnNext(in, 0)))
        in.grab() should ===(0)

        in.pull()
        stepAll()

        lastEvents() should be(Set(OnComplete(in)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"ignore pull while completing (chasing = $chasing)" in new PortTestSetup(chasing) {
        out.complete()
        in.pull()
        // While the pull event is not enqueued at this point, we should still report the state correctly
        in.hasBeenPulled should be(true)

        stepAll()

        lastEvents() should be(Set(OnComplete(in)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate cancel while downstream is active (chasing = $chasing)" in new PortTestSetup(chasing) {
        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)

        in.cancel()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        stepAll()

        lastEvents() should be(Set(Cancel(out, SubscriptionWithCancelException.NoMoreElementsNeeded)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.cancel() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate cancel while upstream is active (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        stepAll()

        lastEvents() should be(Set(RequestOne(out)))
        out.isAvailable should be(true)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)

        in.cancel()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(true)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        stepAll()

        lastEvents() should be(Set(Cancel(out, SubscriptionWithCancelException.NoMoreElementsNeeded)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.cancel() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

      }

      s"propagate cancel while pull is in flight (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)

        in.cancel()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        stepAll()

        lastEvents() should be(Set(Cancel(out, SubscriptionWithCancelException.NoMoreElementsNeeded)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.cancel() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate cancel while push is in flight (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        stepAll()
        clearEvents()

        out.push(0)

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)

        in.cancel()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }

        stepAll()

        lastEvents() should be(Set(Cancel(out, SubscriptionWithCancelException.NoMoreElementsNeeded)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.cancel() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"ignore push while cancelling (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        stepAll()
        clearEvents()

        in.cancel()
        out.push(0)
        // While the push event is not enqueued at this point, we should still report the state correctly
        out.isAvailable should be(false)

        stepAll()

        lastEvents() should be(Set(Cancel(out, SubscriptionWithCancelException.NoMoreElementsNeeded)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"clear ungrabbed element even when cancelled (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        stepAll()
        clearEvents()
        out.push(0)
        stepAll()

        lastEvents() should be(Set(OnNext(in, 0)))

        in.cancel()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        stepAll()
        lastEvents() should be(Set(Cancel(out, SubscriptionWithCancelException.NoMoreElementsNeeded)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"ignore any completion if they are concurrent (cancel first) (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.cancel()
        out.complete()

        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"ignore any completion if they are concurrent (complete first) (chasing = $chasing)" in new PortTestSetup(
        chasing) {
        out.complete()
        in.cancel()

        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"ignore completion from a push-complete if cancelled while in flight (chasing = $chasing)" in new PortTestSetup(
        chasing) {
        in.pull()
        stepAll()
        clearEvents()

        out.push(0)
        out.complete()
        in.cancel()

        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"ignore completion from a push-complete if cancelled after onPush (chasing = $chasing)" in new PortTestSetup(
        chasing) {
        in.pull()
        stepAll()
        clearEvents()

        out.push(0)
        out.complete()

        step()

        lastEvents() should be(Set(OnNext(in, 0)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(true)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        in.grab() should ===(0)

        in.cancel()
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"not allow to grab element before it arrives (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        stepAll()
        out.push(0)

        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"not allow to grab element if already cancelled (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        stepAll()

        out.push(0)
        in.cancel()

        stepAll()

        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate failure while downstream is active (chasing = $chasing)" in new PortTestSetup(chasing) {
        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)

        out.fail(TE("test"))

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }

        stepAll()

        lastEvents() should be(Set(OnError(in, TE("test"))))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.cancel() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate failure while upstream is active (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        stepAll()

        lastEvents() should be(Set(RequestOne(out)))
        out.isAvailable should be(true)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)

        out.fail(TE("test"))

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }

        stepAll()

        lastEvents() should be(Set(OnError(in, TE("test"))))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.cancel() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

      }

      s"propagate failure while pull is in flight (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)

        out.fail(TE("test"))

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }

        stepAll()

        lastEvents() should be(Set(OnError(in, TE("test"))))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        in.cancel() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate failure while push is in flight (chasing = $chasing)" in new PortTestSetup(chasing) {
        in.pull()
        stepAll()
        clearEvents()

        out.push(0)

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(false)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)

        out.fail(TE("test"))

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(true)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }

        step()

        lastEvents() should be(Set(OnNext(in, 0)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(true)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        in.grab() should ===(0)
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        step()

        lastEvents() should be(Set(OnError(in, TE("test"))))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }

        out.complete() // This should have no effect now
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"propagate failure while push is in flight and keep ungrabbed element (chasing = $chasing)" in new PortTestSetup(
        chasing) {
        in.pull()
        stepAll()
        clearEvents()

        out.push(0)
        out.fail(TE("test"))
        step()

        lastEvents() should be(Set(OnNext(in, 0)))
        step()

        lastEvents() should be(Set(OnError(in, TE("test"))))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(true)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        in.grab() should ===(0)
      }

      s"ignore pull while failing (chasing = $chasing)" in new PortTestSetup(chasing) {
        out.fail(TE("test"))
        in.pull()
        in.hasBeenPulled should be(true)

        stepAll()

        lastEvents() should be(Set(OnError(in, TE("test"))))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"ignore any failure completion if they are concurrent (cancel first) (chasing = $chasing)" in new PortTestSetup(
        chasing) {
        in.cancel()
        out.fail(TE("test"))

        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"ignore any failure completion if they are concurrent (complete first) (chasing = $chasing)" in new PortTestSetup(
        chasing) {
        out.fail(TE("test"))
        in.cancel()

        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"ignore failure from a push-then-fail if cancelled while in flight (chasing = $chasing)" in new PortTestSetup(
        chasing) {
        in.pull()
        stepAll()
        clearEvents()

        out.push(0)
        out.fail(TE("test"))
        in.cancel()

        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }

      s"ignore failure from a push-then-fail if cancelled after onPush (chasing = $chasing)" in new PortTestSetup(
        chasing) {
        in.pull()
        stepAll()
        clearEvents()

        out.push(0)
        out.fail(TE("test"))

        step()

        lastEvents() should be(Set(OnNext(in, 0)))
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(true)
        in.hasBeenPulled should be(false)
        in.isClosed should be(false)
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        in.grab() should ===(0)

        in.cancel()
        stepAll()

        lastEvents() should be(Set.empty)
        out.isAvailable should be(false)
        out.isClosed should be(true)
        in.isAvailable should be(false)
        in.hasBeenPulled should be(false)
        in.isClosed should be(true)
        an[IllegalArgumentException] should be thrownBy { in.pull() }
        an[IllegalArgumentException] should be thrownBy { out.push(0) }
        an[IllegalArgumentException] should be thrownBy { in.grab() }
      }
    }

  }

}
