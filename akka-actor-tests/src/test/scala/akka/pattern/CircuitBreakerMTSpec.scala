/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import scala.collection.immutable
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

import akka.testkit._

class CircuitBreakerMTSpec extends AkkaSpec {
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  "A circuit breaker being called by many threads" must {
    val callTimeout = 2.seconds.dilated
    val resetTimeout = 3.seconds.dilated
    val maxFailures = 5
    def newBreaker = new CircuitBreaker(system.scheduler, maxFailures, callTimeout, resetTimeout)
    val numberOfTestCalls = 100

    def openBreaker(breaker: CircuitBreaker): Unit = {
      // returns true if the breaker is open
      def failingCall(): Boolean =
        Await.result(breaker.withCircuitBreaker(Future.failed(new RuntimeException("FAIL"))).recover {
          case _: CircuitBreakerOpenException => true
          case _                              => false
        }, remainingOrDefault)

      // fire some failing calls
      (1 to (maxFailures + 1)).foreach { _ =>
        failingCall()
      }
      // and then continue with failing calls until the breaker is open
      awaitCond(failingCall())
    }

    def testCallsWithBreaker(breaker: CircuitBreaker): immutable.IndexedSeq[Future[String]] = {
      val aFewActive = new TestLatch(5)
      for (_ <- 1 to numberOfTestCalls)
        yield breaker
          .withCircuitBreaker(Future {
            aFewActive.countDown()
            Await.ready(aFewActive, 5.seconds.dilated)
            "succeed"
          })
          .recoverWith {
            case _: CircuitBreakerOpenException =>
              aFewActive.countDown()
              Future.successful("CBO")
          }
    }

    "allow many calls while in closed state with no errors" taggedAs TimingTest in {
      val futures = testCallsWithBreaker(newBreaker)
      val result = Await.result(Future.sequence(futures), 5.second.dilated)
      result.size should ===(numberOfTestCalls)
      result.toSet should ===(Set("succeed"))
    }

    "transition to open state upon reaching failure limit and fail-fast" taggedAs TimingTest in {
      val breaker = newBreaker
      openBreaker(breaker)
      val futures = testCallsWithBreaker(breaker)
      val result = Await.result(Future.sequence(futures), 5.second.dilated)
      result.size should ===(numberOfTestCalls)
      result.toSet should ===(Set("CBO"))
    }

    "allow a single call through in half-open state" taggedAs TimingTest in {
      val breaker = newBreaker
      val halfOpenLatch = new TestLatch(1)
      breaker.onHalfOpen(halfOpenLatch.countDown())

      openBreaker(breaker)

      // breaker should become half-open after a while
      Await.ready(halfOpenLatch, resetTimeout + 1.seconds.dilated)

      val futures = testCallsWithBreaker(breaker)
      val result = Await.result(Future.sequence(futures), 5.second.dilated)
      result.size should ===(numberOfTestCalls)
      result.toSet should ===(Set("succeed", "CBO"))
    }

    "recover and reset the breaker after the reset timeout" taggedAs TimingTest in {
      val breaker = newBreaker

      val halfOpenLatch = new TestLatch(1)
      breaker.onHalfOpen(halfOpenLatch.countDown())
      openBreaker(breaker)

      // breaker should become half-open after a while
      Await.ready(halfOpenLatch, resetTimeout + 1.seconds.dilated)

      // one successful call should close the latch
      val closedLatch = new TestLatch(1)
      breaker.onClose(closedLatch.countDown())
      breaker.withCircuitBreaker(Future("succeed"))
      Await.ready(closedLatch, 5.seconds.dilated)

      val futures = testCallsWithBreaker(breaker)
      val result = Await.result(Future.sequence(futures), 5.second.dilated)
      result.size should ===(numberOfTestCalls)
      result.toSet should ===(Set("succeed"))
    }
  }
}
