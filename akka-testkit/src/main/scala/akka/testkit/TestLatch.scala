/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import java.util.concurrent.{ CountDownLatch, TimeUnit, TimeoutException }

import scala.concurrent.{ Awaitable, CanAwait }
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorSystem

/**
 * A count down latch wrapper for use in testing.
 * It always uses a timeout when waiting and timeouts are specified as durations.
 * There's a default timeout of 5 seconds and the default count is 1.
 * Timeouts will always throw an exception (no need to wrap in assert in tests).
 * Timeouts are multiplied by the testing time factor for Jenkins builds.
 */
object TestLatch {
  val DefaultTimeout = Duration(5, TimeUnit.SECONDS)

  def apply(count: Int = 1)(implicit system: ActorSystem) = new TestLatch(count)
}

class TestLatch(count: Int = 1)(implicit system: ActorSystem) extends Awaitable[Unit] {
  private var latch = new CountDownLatch(count)

  def countDown() = latch.countDown()
  def isOpen: Boolean = latch.getCount == 0
  def open() = while (!isOpen) countDown()
  def reset() = latch = new CountDownLatch(count)

  @throws(classOf[TimeoutException])
  def ready(atMost: Duration)(implicit permit: CanAwait) = {
    val waitTime = atMost match {
      case f: FiniteDuration => f
      case _                 => throw new IllegalArgumentException("TestLatch does not support waiting for " + atMost)
    }
    val opened = latch.await(waitTime.dilated.toNanos, TimeUnit.NANOSECONDS)
    if (!opened)
      throw new TimeoutException(
        "Timeout of %s with time factor of %s".format(atMost.toString, TestKitExtension(system).TestTimeFactor))
    this
  }
  @throws(classOf[Exception])
  def result(atMost: Duration)(implicit permit: CanAwait): Unit = {
    ready(atMost)
  }
}
