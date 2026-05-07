/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import org.reactivestreams.tck.SubscriberBlackboxVerification
import org.reactivestreams.tck.SubscriberWhiteboxVerification
import org.reactivestreams.tck.TestEnvironment
import org.scalatestplus.testng.TestNGSuiteLike

import akka.actor.ActorSystem

abstract class AkkaSubscriberBlackboxVerification[T](env: TestEnvironment)
    extends SubscriberBlackboxVerification[T](env)
    with TestNGSuiteLike
    with AkkaSubscriberVerificationLike
    with ActorSystemLifecycle {

  def this(printlnDebug: Boolean) =
    this(new TestEnvironment(Timeouts.defaultTimeoutMillis, Timeouts.defaultNoSignalsTimeoutMillis, printlnDebug))

  def this() = this(false)
}

abstract class AkkaSubscriberWhiteboxVerification[T](env: TestEnvironment)
    extends SubscriberWhiteboxVerification[T](env)
    with TestNGSuiteLike
    with AkkaSubscriberVerificationLike {

  def this(printlnDebug: Boolean) =
    this(new TestEnvironment(Timeouts.defaultTimeoutMillis, Timeouts.defaultNoSignalsTimeoutMillis, printlnDebug))

  def this() = this(false)
}

trait AkkaSubscriberVerificationLike {
  implicit def system: ActorSystem
}
