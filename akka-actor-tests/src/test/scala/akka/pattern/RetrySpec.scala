/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pattern

import scala.concurrent.{ Await, ExecutionContextExecutor, Future }
import scala.concurrent.duration._

import language.postfixOps

import akka.actor.Scheduler
import akka.testkit.AkkaSpec

class RetrySpec extends AkkaSpec with RetrySupport {
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val scheduler: Scheduler = system.scheduler

  "pattern.retry" must {
    "run a successful Future immediately" in {
      val retried = retry(() => Future.successful(5), 5, 1 second)

      within(3 seconds) {
        Await.result(retried, remaining) should ===(5)
      }
    }

    "run a successful Future only once" in {
      @volatile var counter = 0
      val retried = retry(
        () =>
          Future.successful({
            counter += 1
            counter
          }),
        5,
        1 second)

      within(3 seconds) {
        Await.result(retried, remaining) should ===(1)
      }
    }

    "eventually return a failure for a Future that will never succeed" in {
      val retried = retry(() => Future.failed(new IllegalStateException("Mexico")), 5, 100 milliseconds)

      within(3 second) {
        intercept[IllegalStateException] { Await.result(retried, remaining) }.getMessage should ===("Mexico")
      }
    }

    "return a success for a Future that succeeds eventually" in {
      @volatile var failCount = 0

      def attempt() = {
        if (failCount < 5) {
          failCount += 1
          Future.failed(new IllegalStateException(failCount.toString))
        } else Future.successful(5)
      }

      val retried = retry(() => attempt(), 10, 100 milliseconds)

      within(3 seconds) {
        Await.result(retried, remaining) should ===(5)
      }
    }

    "return a failure for a Future that would have succeeded but retires were exhausted" in {
      @volatile var failCount = 0

      def attempt() = {
        if (failCount < 10) {
          failCount += 1
          Future.failed(new IllegalStateException(failCount.toString))
        } else Future.successful(5)
      }

      val retried = retry(() => attempt(), 5, 100 milliseconds)

      within(3 seconds) {
        intercept[IllegalStateException] { Await.result(retried, remaining) }.getMessage should ===("6")
      }
    }

    "return a failure for a Future that would have succeeded but retires were exhausted with delay function" in {
      @volatile var failCount = 0
      @volatile var attemptedCount = 0;

      def attempt() = {
        if (failCount < 10) {
          failCount += 1
          Future.failed(new IllegalStateException(failCount.toString))
        } else Future.successful(5)
      }

      val retried = retry(() => attempt(), 5, attempted => {
        attemptedCount = attempted
        Some(100.milliseconds * attempted)
      })
      within(30000000 seconds) {
        intercept[IllegalStateException] { Await.result(retried, remaining) }.getMessage should ===("6")
        attemptedCount shouldBe 5
      }
    }

    "retry can be attempted without any delay" in {
      @volatile var failCount = 0

      def attempt() = {
        if (failCount < 1000) {
          failCount += 1
          Future.failed(new IllegalStateException(failCount.toString))
        } else Future.successful(1)
      }
      val start = System.currentTimeMillis()
      val retried = retry(() => attempt(), 999)

      within(1 seconds) {
        intercept[IllegalStateException] {
          Await.result(retried, remaining)
        }.getMessage should ===("1000")
        val elapse = System.currentTimeMillis() - start
        elapse <= 100 shouldBe true
      }
    }

    "handle thrown exceptions in same way as failed Future" in {
      @volatile var failCount = 0

      def attempt() = {
        if (failCount < 5) {
          failCount += 1
          throw new IllegalStateException(failCount.toString)
        } else Future.successful(5)
      }

      val retried = retry(() => attempt(), 10, 100 milliseconds)

      within(3 seconds) {
        Await.result(retried, remaining) should ===(5)
      }
    }
  }

  "support short-circuiting failures that are not amenable to retries" in {
    @volatile var failCount = 0

    def retriable(cause: Throwable): Boolean =
      cause match {
        case _: IllegalArgumentException => false
        case _                           => true
      }

    def attempt() =
      failCount match {
        case _ if (failCount % 3) < 2 =>
          failCount += 1
          Future.failed(new NoSuchElementException(failCount.toString))

        case _ =>
          failCount += 1
          Future.failed(new IllegalArgumentException(failCount.toString))
      }

    val retried = retry(() => attempt(), retriable(_), 10)

    within(3 seconds) {
      Await.ready(retried, remaining)
      failCount shouldBe 3
      retried.value.get shouldBe a[scala.util.Failure[_]]
      retried.failed.value.get.get shouldBe an[IllegalArgumentException]
    }
  }

}
