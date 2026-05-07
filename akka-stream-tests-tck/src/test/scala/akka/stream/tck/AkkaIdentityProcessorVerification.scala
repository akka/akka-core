/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import org.reactivestreams.{ Processor, Publisher, Subscriber, Subscription }
import org.reactivestreams.tck.IdentityProcessorVerification
import org.reactivestreams.tck.TestEnvironment
import org.scalatestplus.testng.TestNGSuiteLike
import org.testng.annotations.AfterClass

import akka.stream.testkit.TestPublisher

abstract class AkkaIdentityProcessorVerification[T](env: TestEnvironment, publisherShutdownTimeout: Long)
    extends IdentityProcessorVerification[T](env, publisherShutdownTimeout)
    with TestNGSuiteLike
    with ActorSystemLifecycle {

  def this(printlnDebug: Boolean) =
    this(
      new TestEnvironment(Timeouts.defaultTimeoutMillis, Timeouts.defaultNoSignalsTimeoutMillis, printlnDebug),
      Timeouts.publisherShutdownTimeoutMillis)

  def this() = this(false)

  override def createFailedPublisher(): Publisher[T] =
    TestPublisher.error(new Exception("Unable to serve subscribers right now!"))

  def processorFromSubscriberAndPublisher(sub: Subscriber[T], pub: Publisher[T]): Processor[T, T] = {
    new Processor[T, T] {
      override def onSubscribe(s: Subscription): Unit = sub.onSubscribe(s)
      override def onError(t: Throwable): Unit = sub.onError(t)
      override def onComplete(): Unit = sub.onComplete()
      override def onNext(t: T): Unit = sub.onNext(t)
      override def subscribe(s: Subscriber[_ >: T]): Unit = pub.subscribe(s)
    }
  }

  /** By default Akka Publishers do not support Fanout! */
  override def maxSupportedSubscribers: Long = 1L

  override lazy val publisherExecutorService: ExecutorService =
    Executors.newFixedThreadPool(3)

  @AfterClass
  def shutdownPublisherExecutorService(): Unit = {
    publisherExecutorService.shutdown()
    publisherExecutorService.awaitTermination(3, TimeUnit.SECONDS)
  }
}
