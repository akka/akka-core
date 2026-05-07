/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import scala.collection.immutable

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.reactivestreams.Publisher
import org.reactivestreams.tck.PublisherVerification
import org.reactivestreams.tck.TestEnvironment
import org.scalatestplus.testng.TestNGSuiteLike

import akka.stream.testkit.TestPublisher

abstract class AkkaPublisherVerification[T](val env: TestEnvironment, publisherShutdownTimeout: Long)
    extends PublisherVerification[T](env, publisherShutdownTimeout)
    with TestNGSuiteLike
    with ActorSystemLifecycle {

  override def additionalConfig: Config =
    ConfigFactory.parseString("""
      akka.stream.materializer.initial-input-buffer-size = 512
      akka.stream.materializer.max-input-buffer-size = 512
    """)

  def this(printlnDebug: Boolean) =
    this(
      new TestEnvironment(Timeouts.defaultTimeoutMillis, Timeouts.defaultNoSignalsTimeoutMillis, printlnDebug),
      Timeouts.publisherShutdownTimeoutMillis)

  def this() = this(false)

  override def createFailedPublisher(): Publisher[T] =
    TestPublisher.error(new Exception("Unable to serve subscribers right now!"))

  def iterable(elements: Long): immutable.Iterable[Int] =
    if (elements > Int.MaxValue)
      new immutable.Iterable[Int] { override def iterator = Iterator.from(0) } else
      0 until elements.toInt
}
