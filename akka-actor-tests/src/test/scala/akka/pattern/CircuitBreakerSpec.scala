/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.testkit._

object CircuitBreakerSpec {

  class TestException extends RuntimeException
  class AllowException extends RuntimeException
  case class CBSuccess(value: FiniteDuration)
  case class CBFailure(value: FiniteDuration)
  case class CBTimeout(value: FiniteDuration)

  class Breaker(val instance: CircuitBreaker)(implicit system: ActorSystem) {
    val probe = TestProbe()
    val halfOpenLatch = new TestLatch(1)
    val openLatch = new TestLatch(1)
    val closedLatch = new TestLatch(1)
    val callSuccessLatch = new TestLatch(1)
    val callFailureLatch = new TestLatch(1)
    val callTimeoutLatch = new TestLatch(1)
    val callBreakerOpenLatch = new TestLatch(1)

    def resetAll() = {
      halfOpenLatch.reset()
      openLatch.reset()
      closedLatch.reset()
      callSuccessLatch.reset()
      callFailureLatch.reset()
      callTimeoutLatch.reset()
      callBreakerOpenLatch.reset()
    }

    def apply(): CircuitBreaker = instance
    instance
      .onClose(closedLatch.countDown())
      .onHalfOpen(halfOpenLatch.countDown())
      .onOpen(openLatch.countDown())
      .onCallSuccess(value => {
        probe.ref ! CBSuccess(value.nanos)
        callSuccessLatch.countDown()
      })
      .onCallFailure(value => {
        probe.ref ! CBFailure(value.nanos)
        callFailureLatch.countDown()
      })
      .onCallTimeout(value => {
        probe.ref ! CBTimeout(value.nanos)
        callTimeoutLatch.countDown()
      })
      .onCallBreakerOpen(callBreakerOpenLatch.countDown())
  }

  def multiFailureCb()(implicit system: ActorSystem, ec: ExecutionContext): Breaker =
    new Breaker(new CircuitBreaker(system.scheduler, 5, 200.millis.dilated, 500.millis.dilated))

  def nonOneFactorCb()(implicit system: ActorSystem, ec: ExecutionContext): Breaker =
    new Breaker(new CircuitBreaker(system.scheduler, 1, 2000.millis.dilated, 1000.millis.dilated, 1.day.dilated, 5, 0))

  val evenNumberIsFailure: Try[Int] => Boolean = {
    case Success(i) => i % 2 == 0
    case _          => true
  }

  val anyExceptionIsFailure: Try[Int] => Boolean = {
    case Success(_) => false
    case _          => true
  }
}

class CircuitBreakerSpec extends AkkaSpec("""
    akka.circuit-breaker {
      identified {
        max-failures = 1
        call-timeout = 100 ms
        reset-timeout = 200 ms
        exception-allowlist = [
          "akka.pattern.CircuitBreakerSpec$AllowException"
        ]
      }
    }
    """) {

  import CircuitBreakerSpec._
  implicit def ec: ExecutionContextExecutor = system.dispatcher

  val awaitTimeout = 2.seconds.dilated

  val shortCallTimeout = 50.millis.dilated
  def shortCallTimeoutCb()(implicit system: ActorSystem, ec: ExecutionContext): Breaker =
    new Breaker(new CircuitBreaker(system.scheduler, 1, shortCallTimeout, 500.millis.dilated))

  val shortResetTimeout = 50.millis.dilated
  def shortResetTimeoutCb()(implicit system: ActorSystem, ec: ExecutionContext): Breaker =
    new Breaker(new CircuitBreaker(system.scheduler, 1, 1000.millis.dilated, shortResetTimeout))

  def longCallTimeoutCb()(implicit system: ActorSystem, ec: ExecutionContext): Breaker =
    new Breaker(new CircuitBreaker(system.scheduler, 1, 5 seconds, 500.millis.dilated))

  val longResetTimeout = 5.seconds.dilated
  def longResetTimeoutCb()(implicit system: ActorSystem, ec: ExecutionContext): Breaker =
    new Breaker(new CircuitBreaker(system.scheduler, 1, 100.millis.dilated, longResetTimeout))

  def checkLatch(latch: TestLatch): Unit = Await.ready(latch, awaitTimeout)

  def throwException = throw new TestException

  def throwAllowException = throw new AllowException

  def sayHi = "hi"

  "A synchronous circuit breaker that is open" must {
    "throw exceptions when called before reset timeout" taggedAs TimingTest in {
      val breaker = longResetTimeoutCb()

      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }

      checkLatch(breaker.openLatch)

      val e = intercept[CircuitBreakerOpenException] { breaker().withSyncCircuitBreaker(sayHi) }
      e.remainingDuration should be > Duration.Zero
      e.remainingDuration should be <= longResetTimeout
    }

    "transition to half-open on reset timeout" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)
    }

    "still be in open state after calling success method" taggedAs TimingTest in {
      val breaker = longResetTimeoutCb()
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.openLatch)
      breaker().succeed()
      breaker().isOpen should ===(true)
    }

    "still be in open state after calling fail method" taggedAs TimingTest in {
      val breaker = longResetTimeoutCb()
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.openLatch)
      breaker().fail()
      breaker().isOpen should ===(true)
    }

    "invoke onHalfOpen during transition to half-open state" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)
    }

    "invoke onCallBreakerOpen when called before reset timeout" taggedAs TimingTest in {
      val breaker = longResetTimeoutCb()
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.openLatch)
      intercept[CircuitBreakerOpenException] { breaker().withSyncCircuitBreaker(sayHi) }
      checkLatch(breaker.callBreakerOpenLatch)
    }

    "invoke onCallFailure when call results in exception" taggedAs TimingTest in {
      val breaker = longResetTimeoutCb()

      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.callFailureLatch)

      val failure = breaker.probe.expectMsgType[CBFailure]
      failure.value should (be > Duration.Zero and be < longResetTimeout)
    }
  }

  "A synchronous circuit breaker that is half-open" must {
    "pass through next call and close on success" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)
      assert("hi" == breaker().withSyncCircuitBreaker(sayHi))
      checkLatch(breaker.closedLatch)
    }

    "pass through next call and close on exception" when {
      "exception is defined as call succeeded" taggedAs TimingTest in {
        val breaker = shortResetTimeoutCb()
        intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
        checkLatch(breaker.halfOpenLatch)

        val allReturnIsSuccess: Try[String] => Boolean = _ => false

        intercept[TestException] { breaker().withSyncCircuitBreaker(throwException, allReturnIsSuccess) }
        checkLatch(breaker.closedLatch)
      }
    }

    "open on exception in call" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)
      breaker.openLatch.reset()
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.openLatch)
    }

    "open on even number" when {
      "even number is defined as failure" taggedAs TimingTest in {
        val breaker = shortResetTimeoutCb()
        intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
        checkLatch(breaker.halfOpenLatch)
        breaker.openLatch.reset()
        breaker().withSyncCircuitBreaker(2, evenNumberIsFailure)
        checkLatch(breaker.openLatch)
      }
    }

    "open on calling fail method" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)
      breaker.openLatch.reset()
      breaker().fail()
      checkLatch(breaker.openLatch)
    }

    "close on calling success method" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)
      breaker().succeed()
      checkLatch(breaker.closedLatch)
    }

    "pass through next call and invoke onCallSuccess on success" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()

      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)
      breaker.probe.expectMsgType[CBFailure]

      breaker().withSyncCircuitBreaker(sayHi)
      checkLatch(breaker.callSuccessLatch)

      val success = breaker.probe.expectMsgType[CBSuccess]
      success.value should (be > Duration.Zero and be < shortResetTimeout)
    }

    "pass through next call and invoke onCallFailure on failure" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()

      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)
      checkLatch(breaker.callFailureLatch)

      breaker.callFailureLatch.reset()

      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.callFailureLatch)

      breaker.probe.expectMsgType[CBFailure]
      val failure = breaker.probe.expectMsgType[CBFailure]
      failure.value should (be > Duration.Zero and be < shortResetTimeout)
    }

    "pass through next call and invoke onCallTimeout on timeout" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)

      intercept[TimeoutException] { breaker().withSyncCircuitBreaker(Thread.sleep(200.millis.dilated.toMillis)) }
      checkLatch(breaker.callTimeoutLatch)

      breaker.probe.expectMsgType[CBFailure]
      val timeout = breaker.probe.expectMsgType[CBTimeout]
      timeout.value should (be > Duration.Zero and be < (shortCallTimeout * 2).dilated)
    }

    "pass through next call and invoke onCallBreakerOpen while executing other" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()

      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)

      breaker().withCircuitBreaker(Future(Thread.sleep(250.millis.dilated.toMillis)))
      intercept[CircuitBreakerOpenException] { breaker().withSyncCircuitBreaker(sayHi) }

      checkLatch(breaker.callBreakerOpenLatch)
    }

    "pass through next call and invoke onCallSuccess after transition to open state" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()

      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.halfOpenLatch)

      breaker().withSyncCircuitBreaker(Future.successful(sayHi))
      checkLatch(breaker.callSuccessLatch)
    }
  }

  "A synchronous circuit breaker that is closed" must {
    "allow calls through" taggedAs TimingTest in {
      val breaker = longCallTimeoutCb()
      breaker().withSyncCircuitBreaker(sayHi) should ===("hi")
    }

    "increment failure count on failure" taggedAs TimingTest in {
      val breaker = longCallTimeoutCb()
      breaker().currentFailureCount should ===(0)
      intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
      checkLatch(breaker.openLatch)
      breaker().currentFailureCount should ===(1)
    }

    "increment failure count on even number" when {
      "even number is considered failure" taggedAs TimingTest in {
        val breaker = longCallTimeoutCb()
        breaker().currentFailureCount should ===(0)
        val result = breaker().withSyncCircuitBreaker(2, evenNumberIsFailure)
        checkLatch(breaker.openLatch)

        breaker().currentFailureCount should ===(1)
        result should ===(2)
      }
    }

    "increment failure count on fail method" taggedAs TimingTest in {
      val breaker = longCallTimeoutCb()
      breaker().currentFailureCount should ===(0)
      breaker().fail()
      checkLatch(breaker.openLatch)
      breaker().currentFailureCount should ===(1)
    }

    "reset failure count after success" taggedAs TimingTest in {
      val breaker = multiFailureCb()
      breaker().currentFailureCount should ===(0)
      intercept[TestException] {
        val ct = Thread.currentThread() // Ensure that the thunk is executed in the tests thread
        breaker().withSyncCircuitBreaker({ if (Thread.currentThread() eq ct) throwException else "fail" })
      }
      breaker().currentFailureCount should ===(1)
      breaker().withSyncCircuitBreaker(sayHi)
      breaker().currentFailureCount should ===(0)
    }

    "reset failure count after exception in call" when {
      "exception is defined as Success" taggedAs TimingTest in {
        val breaker = multiFailureCb()
        breaker().currentFailureCount should ===(0)
        intercept[TestException] {
          val ct = Thread.currentThread() // Ensure that the thunk is executed in the tests thread
          breaker().withSyncCircuitBreaker({ if (Thread.currentThread() eq ct) throwException else "fail" })
        }
        breaker().currentFailureCount should ===(1)

        val harmlessException = new TestException
        val harmlessExceptionAsSuccess: Try[String] => Boolean = {
          case Success(_)  => false
          case Failure(ex) => ex != harmlessException
        }

        intercept[TestException] {
          breaker().withSyncCircuitBreaker(throw harmlessException, harmlessExceptionAsSuccess)
        }

        breaker().currentFailureCount should ===(0)
      }
    }

    "reset failure count after success method" taggedAs TimingTest in {
      val breaker = multiFailureCb()
      breaker().currentFailureCount should ===(0)
      intercept[TestException] {
        val ct = Thread.currentThread() // Ensure that the thunk is executed in the tests thread
        breaker().withSyncCircuitBreaker({ if (Thread.currentThread() eq ct) throwException else "fail" })
      }
      breaker().currentFailureCount should ===(1)
      breaker().succeed()
      breaker().currentFailureCount should ===(0)
    }

    "throw TimeoutException on callTimeout" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()
      intercept[TimeoutException] {
        breaker().withSyncCircuitBreaker {
          Thread.sleep(200.millis.dilated.toMillis)
        }
      }
      breaker().currentFailureCount should ===(1)
    }

    "increment failure count on callTimeout before call finishes" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()
      Future {
        breaker().withSyncCircuitBreaker {
          Thread.sleep(1.second.dilated.toMillis)
        }
      }
      within(900.millis) {
        awaitCond(breaker().currentFailureCount == 1, 100.millis.dilated)
      }
    }

    "invoke onCallSuccess if call succeeds" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      breaker().withSyncCircuitBreaker(sayHi)
      checkLatch(breaker.callSuccessLatch)

      val success = breaker.probe.expectMsgType[CBSuccess]
      success.value should (be > Duration.Zero and be < shortCallTimeout)
    }

    "invoke onCallTimeout if call timeouts" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      intercept[TimeoutException](breaker().withSyncCircuitBreaker(Thread.sleep(250.millis.dilated.toMillis)))
      checkLatch(breaker.callTimeoutLatch)

      val timeout = breaker.probe.expectMsgType[CBTimeout]
      timeout.value should (be > Duration.Zero and be < (shortCallTimeout * 2))
    }

    "invoke onCallFailure if call fails" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      intercept[TestException](breaker().withSyncCircuitBreaker(throwException))
      checkLatch(breaker.callFailureLatch)

      val failure = breaker.probe.expectMsgType[CBFailure]
      failure.value should (be > Duration.Zero and be < shortCallTimeout)
    }

    "invoke onOpen if call fails and breaker transits to open state" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      intercept[TestException](breaker().withSyncCircuitBreaker(throwException))
      checkLatch(breaker.openLatch)
    }
  }

  "An asynchronous circuit breaker that is open" must {
    "throw exceptions when called before reset timeout" taggedAs TimingTest in {
      val breaker = longResetTimeoutCb()
      breaker().withCircuitBreaker(Future(throwException))

      checkLatch(breaker.openLatch)

      intercept[CircuitBreakerOpenException] { Await.result(breaker().withCircuitBreaker(Future(sayHi)), awaitTimeout) }
    }

    "transition to half-open on reset timeout" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()
      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.halfOpenLatch)
    }

    "increase the reset timeout after it transits to open again" taggedAs TimingTest in {
      val breaker = nonOneFactorCb()
      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.openLatch)

      val e1 = intercept[CircuitBreakerOpenException] { breaker().withSyncCircuitBreaker(sayHi) }
      val shortRemainingDuration = e1.remainingDuration

      Thread.sleep(1000.millis.dilated.toMillis)
      checkLatch(breaker.halfOpenLatch)

      // transit to open again
      breaker.openLatch.reset()
      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.openLatch)

      val e2 = intercept[CircuitBreakerOpenException] { breaker().withSyncCircuitBreaker(sayHi) }
      val longRemainingDuration = e2.remainingDuration

      shortRemainingDuration should be < longRemainingDuration
    }

    "invoke onHalfOpen during transition to half-open state" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()

      intercept[TestException] { Await.result(breaker().withCircuitBreaker(Future(throwException)), awaitTimeout) }
      checkLatch(breaker.halfOpenLatch)
    }

    "invoke onCallBreakerOpen when called before reset timeout" taggedAs TimingTest in {
      val breaker = longResetTimeoutCb()

      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.openLatch)

      breaker().withCircuitBreaker(Future(sayHi))
      checkLatch(breaker.callBreakerOpenLatch)
    }

    "invoke onCallFailure when call results in exception" taggedAs TimingTest in {
      val breaker = longResetTimeoutCb()

      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.callFailureLatch)

      val failure = breaker.probe.expectMsgType[CBFailure]
      failure.value should (be > Duration.Zero and be < longResetTimeout)
    }
  }

  "An asynchronous circuit breaker that is half-open" must {
    "pass through next call and close on success" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()
      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.halfOpenLatch)
      Await.result(breaker().withCircuitBreaker(Future(sayHi)), awaitTimeout) should ===("hi")
      checkLatch(breaker.closedLatch)
    }

    "pass through next call and close on exception" when {
      "exception is defined as call succeeded" taggedAs TimingTest in {
        val breaker = shortResetTimeoutCb()
        breaker().withCircuitBreaker(Future(throwException))
        checkLatch(breaker.halfOpenLatch)
        val allReturnIsSuccess: Try[String] => Boolean = _ => false
        Await.ready(breaker().withCircuitBreaker(Future(throwException), allReturnIsSuccess), awaitTimeout)
        checkLatch(breaker.closedLatch)
      }
    }

    "re-open on exception in call" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()
      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.halfOpenLatch)
      breaker.openLatch.reset()
      intercept[TestException] { Await.result(breaker().withCircuitBreaker(Future(throwException)), awaitTimeout) }
      checkLatch(breaker.openLatch)
    }

    "re-open on even number" when {
      "even number is defined as failure" taggedAs TimingTest in {
        val breaker = shortResetTimeoutCb()
        intercept[TestException] { breaker().withSyncCircuitBreaker(throwException) }
        checkLatch(breaker.halfOpenLatch)
        breaker.openLatch.reset()
        Await.result(breaker().withCircuitBreaker(Future(2), evenNumberIsFailure), awaitTimeout)
        checkLatch(breaker.openLatch)
      }
    }

    "re-open on async failure" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()
      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.halfOpenLatch)

      breaker.openLatch.reset()
      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.openLatch)
    }

    "pass through next call and invoke onCallSuccess on success" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()

      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.halfOpenLatch)

      breaker().withCircuitBreaker(Future(sayHi))
      checkLatch(breaker.callSuccessLatch)

      breaker.probe.expectMsgType[CBFailure]
      val success = breaker.probe.expectMsgType[CBSuccess]
      success.value should (be > Duration.Zero and be < shortResetTimeout)
    }

    "pass through next call and invoke onCallFailure on failure" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()

      breaker().withCircuitBreaker(Future(throwException))

      checkLatch(breaker.halfOpenLatch)
      checkLatch(breaker.callFailureLatch)
      breaker.callFailureLatch.reset()

      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.callFailureLatch)

      breaker.probe.expectMsgType[CBFailure]
      val failure = breaker.probe.expectMsgType[CBFailure]
      failure.value should (be > Duration.Zero and be < shortResetTimeout)
    }

    "pass through next call and invoke onCallTimeout on timeout" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.halfOpenLatch)

      breaker().withCircuitBreaker(Future(Thread.sleep(200.millis.dilated.toMillis)))
      checkLatch(breaker.callTimeoutLatch)

      breaker.probe.expectMsgType[CBFailure]
      val timeout = breaker.probe.expectMsgType[CBTimeout]
      timeout.value should (be > Duration.Zero and be < (shortCallTimeout * 2).dilated)
    }

    "pass through next call and invoke onCallBreakerOpen while executing other" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()

      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.halfOpenLatch)

      breaker().withCircuitBreaker(Future(Thread.sleep(250.millis.dilated.toMillis)))
      breaker().withCircuitBreaker(Future(sayHi))
      checkLatch(breaker.callBreakerOpenLatch)
    }

    "pass through next call and invoke onOpen after transition to open state" taggedAs TimingTest in {
      val breaker = shortResetTimeoutCb()

      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.halfOpenLatch)

      breaker().withCircuitBreaker(Future(sayHi))
      checkLatch(breaker.callSuccessLatch)
    }
  }

  "An asynchronous circuit breaker that is closed" must {
    "allow calls through" taggedAs TimingTest in {
      val breaker = longCallTimeoutCb()
      Await.result(breaker().withCircuitBreaker(Future(sayHi)), awaitTimeout) should ===("hi")
    }

    "increment failure count on exception" taggedAs TimingTest in {
      val breaker = longCallTimeoutCb()
      intercept[TestException] { Await.result(breaker().withCircuitBreaker(Future(throwException)), awaitTimeout) }
      checkLatch(breaker.openLatch)
      breaker().currentFailureCount should ===(1)
    }

    "increment failure count on even number" when {
      "even number is considered failure" taggedAs TimingTest in {
        val breaker = longCallTimeoutCb()
        breaker().currentFailureCount should ===(0)
        val result =
          Await.result(breaker().withCircuitBreaker(Future(2), evenNumberIsFailure), awaitTimeout)
        checkLatch(breaker.openLatch)
        breaker().currentFailureCount should ===(1)
        result should ===(2)
      }
    }

    "increment failure count on async failure" taggedAs TimingTest in {
      val breaker = longCallTimeoutCb()
      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.openLatch)
      breaker().currentFailureCount should ===(1)
    }

    "reset failure count after success" taggedAs TimingTest in {
      val breaker = multiFailureCb()
      breaker().withCircuitBreaker(Future(sayHi))
      for (_ <- 1 to 4) breaker().withCircuitBreaker(Future(throwException))
      awaitAssert(breaker().currentFailureCount shouldBe 4, awaitTimeout)
      breaker().withCircuitBreaker(Future(sayHi))
      awaitAssert(breaker().currentFailureCount shouldBe 0, awaitTimeout)
    }

    "reset failure count after exception in call" when {
      "exception is defined as Success" taggedAs TimingTest in {
        val breaker: Breaker = multiFailureCb()

        for (_ <- 1 to 4) breaker().withCircuitBreaker(Future(throwException))
        awaitAssert(breaker().currentFailureCount shouldBe 4, awaitTimeout)

        val harmlessException = new TestException
        val harmlessExceptionAsSuccess: Try[String] => Boolean = {
          case Success(_)  => false
          case Failure(ex) => ex != harmlessException
        }

        breaker().withCircuitBreaker(Future(throw harmlessException), harmlessExceptionAsSuccess)
        awaitAssert(breaker().currentFailureCount shouldBe 0, awaitTimeout)
      }
    }

    "increment failure count on callTimeout" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      val fut = breaker().withCircuitBreaker(Future {
        Thread.sleep(150.millis.dilated.toMillis)
        throwException
      })
      checkLatch(breaker.openLatch)
      breaker().currentFailureCount should ===(1)
      // Since the timeout should have happened before the inner code finishes
      // we expect a timeout, not TestException
      intercept[TimeoutException] {
        Await.result(fut, awaitTimeout)
      }

    }

    "invoke onCallSuccess if call succeeds" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      breaker().withCircuitBreaker(Future(sayHi))
      checkLatch(breaker.callSuccessLatch)

      val success = breaker.probe.expectMsgType[CBSuccess]
      success.value should (be > Duration.Zero and be < shortCallTimeout)
    }

    "invoke onCallTimeout if call timeouts" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      breaker().withCircuitBreaker(Future(Thread.sleep(250.millis.dilated.toMillis)))
      checkLatch(breaker.callTimeoutLatch)

      val timeout = breaker.probe.expectMsgType[CBTimeout]
      timeout.value should (be > Duration.Zero and be < (shortCallTimeout * 2).dilated)
    }

    "invoke onCallFailure if call fails" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.callFailureLatch)

      val failure = breaker.probe.expectMsgType[CBFailure]
      failure.value should (be > Duration.Zero and be < shortCallTimeout)
    }

    "invoke onOpen if call fails and breaker transits to open state" taggedAs TimingTest in {
      val breaker = shortCallTimeoutCb()

      breaker().withCircuitBreaker(Future(throwException))
      checkLatch(breaker.openLatch)
    }
  }

  "An identified asynchronous circuit breaker" must {

    // verify that new signature is source compatible
    CircuitBreaker("identified")(system.asInstanceOf[ExtendedActorSystem])
    CircuitBreaker.lookup("identified", system.asInstanceOf[ExtendedActorSystem])

    val breaker = new Breaker(CircuitBreaker("identified")(system))
    val cb = breaker()

    "be closed after success result" taggedAs TimingTest in {
      breaker.resetAll()
      Await.result(cb.withCircuitBreaker(Future(sayHi)), awaitTimeout) should ===("hi")
      checkLatch(breaker.callSuccessLatch)
    }

    "be closed after throw allowable exception" taggedAs TimingTest in {
      breaker.resetAll()
      intercept[AllowException] { Await.result(cb.withCircuitBreaker(Future(throwAllowException)), awaitTimeout) }
      checkLatch(breaker.callSuccessLatch)
    }

    "be open after throw exception and half-open after reset timeout" taggedAs TimingTest in {
      breaker.resetAll()
      intercept[TestException] { Await.result(cb.withCircuitBreaker(Future(throwException)), awaitTimeout) }
      checkLatch(breaker.openLatch)
      Thread.sleep(250.millis.dilated(system).toMillis)
      checkLatch(breaker.halfOpenLatch)
    }

    "be closed again after success result" taggedAs TimingTest in {
      breaker.resetAll()
      Await.result(cb.withCircuitBreaker(Future(sayHi)), awaitTimeout) should ===("hi")
      checkLatch(breaker.callSuccessLatch)
      checkLatch(breaker.closedLatch)
    }

    "be open after pass custom failure function and throw allowable exception" taggedAs TimingTest in {
      breaker.resetAll()
      intercept[AllowException] {
        Await.result(cb.withCircuitBreaker(Future(throwAllowException), anyExceptionIsFailure), awaitTimeout)
      }
      checkLatch(breaker.openLatch)
    }
  }
}
